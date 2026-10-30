package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type serviceAccountVerifierFunc func(context.Context, ServiceAccountWriteIntent) error

func (f serviceAccountVerifierFunc) VerifyServiceAccountIntent(ctx context.Context, i ServiceAccountWriteIntent) error {
	return f(ctx, i)
}

// Synthetic REST responses prove ordering/body validation only. They do not
// prove native defaults, journal persistence, admission or parent fencing.
type serviceAccountFixture struct {
	binding                 *NamespaceBinding
	writer                  ownership.Writer
	desired                 *corev1.ServiceAccount
	intent                  ownership.NamespacedIntent
	parent                  *corev1.Namespace
	mu                      sync.Mutex
	object                  *corev1.ServiceAccount
	posts, gets, postStatus int
	tamper                  func(*corev1.ServiceAccount)
}

func newServiceAccountFixture(t *testing.T) *serviceAccountFixture {
	t.Helper()
	// Reuse only the existing synthetic namespace/context bootstrap. SA REST
	// responses below replace its core client before any SA action exists.
	base := newDeploymentFixture(t)
	f := &serviceAccountFixture{binding: base.binding, writer: base.writer, parent: base.parent}
	transport := deploymentRoundTrip(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		status := 200
		var out any
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/kube-system":
			out = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}}
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/"+f.parent.Name:
			out = f.parent.DeepCopy()
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/serviceaccounts"):
			f.posts++
			if f.postStatus != 0 {
				status = f.postStatus
				reason := metav1.StatusReasonServiceUnavailable
				if status == 409 {
					reason = metav1.StatusReasonAlreadyExists
				}
				out = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(status), Reason: reason, Message: "PRIVATE_PROVIDER_MESSAGE"}
				break
			}
			var object corev1.ServiceAccount
			if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
				return nil, err
			}
			object.UID = "serviceaccount-uid"
			object.ResourceVersion = "20"
			object.CreationTimestamp = metav1.NewTime(time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			if f.tamper != nil {
				f.tamper(&object)
			}
			f.object = &object
			out = &object
			status = 201
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/serviceaccounts/backend"):
			f.gets++
			if f.object == nil {
				status = 404
				out = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: 404, Reason: metav1.StatusReasonNotFound}
			} else {
				out = f.object.DeepCopy()
			}
		default:
			return nil, errors.New("unexpected synthetic ServiceAccount call")
		}
		data, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		headers := http.Header{"Content-Type": []string{"application/json"}}
		if status == 503 {
			headers.Set("Retry-After", "0")
		}
		return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(string(data)))}, nil
	})
	core, err := coreclient.NewForConfigAndClient(&rest.Config{Host: "https://127.0.0.1:6443"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	f.binding.client.core = core
	f.desired = &corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: f.parent.Name, Labels: map[string]string{"fixture": "PRIVATE_AUTHORED_CONFIG"}}, AutomountServiceAccountToken: deploymentPtr(false)}
	f.refresh(t)
	return f
}
func (f *serviceAccountFixture) refresh(t *testing.T) {
	t.Helper()
	fp, err := ServiceAccountFingerprint(f.desired)
	if err != nil {
		t.Fatal(err)
	}
	f.intent, err = f.binding.bound.Intent("service-account", f.desired.Name, "revision-one", "create-serviceaccount-one", fp)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *serviceAccountFixture) request() ServiceAccountCreate {
	return ServiceAccountCreate{Desired: f.desired, Intent: f.intent, Writer: f.writer, WriteID: "journal-write-sa-one", Verifier: serviceAccountVerifierFunc(func(context.Context, ServiceAccountWriteIntent) error { return nil })}
}
func (f *serviceAccountFixture) read(record ownership.NamespacedRecord) ServiceAccountRead {
	return ServiceAccountRead{Desired: f.desired, Intent: f.intent, Record: record}
}

func TestServiceAccountFrozenCreateAndOriginalNativeReceipt(t *testing.T) {
	f := newServiceAccountFixture(t)
	r := f.request()
	original := f.desired.DeepCopy()
	r.Verifier = serviceAccountVerifierFunc(func(_ context.Context, i ServiceAccountWriteIntent) error {
		if i.Owner != f.writer.Owner || i.Parent != f.binding.Parent() || i.OperationID != f.binding.attemptID || i.ProviderOperationID != r.Intent.Intent.OperationID || i.WriteID != r.WriteID || i.Name != "backend" || i.Revision != "revision-one" || i.Fingerprint != r.Intent.Intent.SpecFingerprint || i.WriterEpoch != 1 || i.WriterID != f.writer.ID {
			t.Fatal("verifier metadata mismatch")
		}
		data, _ := json.Marshal(i)
		if strings.Contains(string(data), "PRIVATE_AUTHORED") {
			t.Fatal("raw body entered verifier")
		}
		f.desired.Labels["fixture"] = "changed-after-freeze"
		*f.desired.AutomountServiceAccountToken = true
		return nil
	})
	created, err := f.binding.CreateServiceAccount(context.Background(), r)
	if err != nil || f.posts != 1 || f.gets != 0 || created.Record.Record.Ref.ProviderID != "serviceaccount-uid" || created.CreationResourceVersion != "20" || f.object.Labels["fixture"] != "PRIVATE_AUTHORED_CONFIG" || *f.object.AutomountServiceAccountToken {
		t.Fatal("frozen create or original response receipt changed", err)
	}
	f.object.ResourceVersion = "21"
	observed, err := f.binding.ObserveServiceAccount(context.Background(), ServiceAccountRead{Desired: original, Intent: r.Intent, Record: created.Record})
	if err != nil || !observed.View().ObservedMinimalPolicy || observed.View().ResourceVersion != "21" || created.CreationResourceVersion != "20" || f.posts != 1 || f.gets != 1 {
		t.Fatal("later GET substituted creation RV or repeated POST", err)
	}
	copy := observed.ObjectForProvider()
	copy.Labels["fixture"] = "changed"
	*copy.AutomountServiceAccountToken = true
	if observed.ObjectForProvider().Labels["fixture"] != "PRIVATE_AUTHORED_CONFIG" || *observed.ObjectForProvider().AutomountServiceAccountToken {
		t.Fatal("snapshot aliases explicit provider copy")
	}
}

func TestServiceAccountUnsupportedAuthoringNeverDispatches(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*corev1.ServiceAccount)
	}{
		{"nil-automount", func(o *corev1.ServiceAccount) { o.AutomountServiceAccountToken = nil }},
		{"enabled-automount", func(o *corev1.ServiceAccount) { o.AutomountServiceAccountToken = deploymentPtr(true) }},
		{"secret", func(o *corev1.ServiceAccount) { o.Secrets = []corev1.ObjectReference{{Name: "PRIVATE_SECRET_REF"}} }},
		{"pull-secret", func(o *corev1.ServiceAccount) {
			o.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "PRIVATE_PULL_REF"}}
		}},
		{"irsa", func(o *corev1.ServiceAccount) {
			o.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "PRIVATE_ROLE"}
		}},
		{"unknown-annotation", func(o *corev1.ServiceAccount) { o.Annotations = map[string]string{"arbitrary": "PRIVATE_ANNOTATION"} }},
		{"finalizer", func(o *corev1.ServiceAccount) { o.Finalizers = []string{"private.example/retain"} }},
		{"authored-uid", func(o *corev1.ServiceAccount) { o.UID = "adopted-uid" }},
		{"authored-rv", func(o *corev1.ServiceAccount) { o.ResourceVersion = "99" }},
		{"owner-reference", func(o *corev1.ServiceAccount) { o.OwnerReferences = []metav1.OwnerReference{{Name: "foreign"}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceAccountFixture(t)
			tc.change(f.desired)
			f.refresh(t)
			got, err := f.binding.CreateServiceAccount(context.Background(), f.request())
			if err == nil || f.posts != 0 || !reflect.DeepEqual(got, ServiceAccountCreation{}) {
				t.Fatal("unsupported SA authoring dispatched")
			}
			if strings.Contains(fmt.Sprint(err), "PRIVATE_") {
				t.Fatal("raw authored input in error")
			}
		})
	}
}

func TestServiceAccountGuardParentOwnerAndEpochDenials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*serviceAccountFixture, *ServiceAccountCreate)
	}{
		{"missing-verifier", func(f *serviceAccountFixture, r *ServiceAccountCreate) { r.Verifier = nil }},
		{"missing-write", func(f *serviceAccountFixture, r *ServiceAccountCreate) { r.WriteID = "" }},
		{"denied-verifier", func(f *serviceAccountFixture, r *ServiceAccountCreate) {
			r.Verifier = serviceAccountVerifierFunc(func(context.Context, ServiceAccountWriteIntent) error { return errors.New("PRIVATE_PENDING_FAILURE") })
		}},
		{"foreign-writer", func(f *serviceAccountFixture, r *ServiceAccountCreate) { r.Writer.Owner.ProjectUID = "foreign" }},
		{"stale-epoch", func(f *serviceAccountFixture, r *ServiceAccountCreate) { r.Writer.Epoch++ }},
		{"expired-writer", func(f *serviceAccountFixture, r *ServiceAccountCreate) {
			r.Writer.ExpiresAt = time.Now().Add(-time.Second)
		}},
		{"foreign-parent-intent", func(f *serviceAccountFixture, r *ServiceAccountCreate) { r.Intent.Parent.UID = "foreign-parent" }},
		{"changed-parent", func(f *serviceAccountFixture, r *ServiceAccountCreate) { f.parent.UID = "replaced-parent" }},
		{"unprotected-parent", func(f *serviceAccountFixture, r *ServiceAccountCreate) { f.parent.Finalizers = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceAccountFixture(t)
			r := f.request()
			tc.change(f, &r)
			got, err := f.binding.CreateServiceAccount(context.Background(), r)
			if err == nil || f.posts != 0 || !reflect.DeepEqual(got, ServiceAccountCreation{}) {
				t.Fatal("denied create dispatched or yielded receipt")
			}
			if strings.Contains(fmt.Sprint(err), "PRIVATE_") {
				t.Fatal("raw guard failure leaked")
			}
		})
	}
}

func TestServiceAccountMalformedSuccessAndTransportFailureRetainOutcome(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		tamper func(*corev1.ServiceAccount)
	}{
		{"already-exists", 409, nil}, {"unavailable", 503, nil},
		{"missing-uid", 0, func(o *corev1.ServiceAccount) { o.UID = "" }},
		{"missing-version", 0, func(o *corev1.ServiceAccount) { o.ResourceVersion = "" }},
		{"missing-created-at", 0, func(o *corev1.ServiceAccount) { o.CreationTimestamp = metav1.Time{} }},
		{"unexpected-generation", 0, func(o *corev1.ServiceAccount) { o.Generation = 1 }},
		{"admission-annotation", 0, func(o *corev1.ServiceAccount) { o.Annotations["eks.amazonaws.com/role-arn"] = "PRIVATE_IDENTITY" }},
		{"admission-secret", 0, func(o *corev1.ServiceAccount) { o.Secrets = []corev1.ObjectReference{{Name: "PRIVATE_SECRET_REF"}} }},
		{"admission-automount", 0, func(o *corev1.ServiceAccount) { o.AutomountServiceAccountToken = deploymentPtr(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceAccountFixture(t)
			f.postStatus = tc.status
			f.tamper = tc.tamper
			r := f.request()
			got, err := f.binding.CreateServiceAccount(context.Background(), r)
			if err == nil || !reflect.DeepEqual(got, ServiceAccountCreation{}) || f.posts != 1 || f.gets != 0 {
				t.Fatal("unproven native receipt escaped or extra call occurred")
			}
			if tc.status == 0 {
				var safe *Error
				if !errors.As(err, &safe) || safe.Code != UnknownWriteOutcome {
					t.Fatal("malformed POST not retained unknown")
				}
			}
			if tc.status == 409 {
				requireCode(t, err, OwnershipRejected)
				// A known rejection is persisted by the real journal caller;
				// this always-allow synthetic verifier cannot prove that rule.
			} else {
				_, again := f.binding.CreateServiceAccount(context.Background(), r)
				requireCode(t, again, UnknownWriteOutcome)
				if f.posts != 1 || f.gets != 0 {
					t.Fatal("unknown create adopted or retried")
				}
			}
			if strings.Contains(fmt.Sprint(err), "PRIVATE_") {
				t.Fatal("raw native failure leaked")
			}
		})
	}
}

func TestServiceAccountSameUIDSpecAndProvenanceTamperRejected(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*corev1.ServiceAccount)
	}{
		{"automount", func(o *corev1.ServiceAccount) { o.AutomountServiceAccountToken = deploymentPtr(true) }},
		{"secret", func(o *corev1.ServiceAccount) { o.Secrets = []corev1.ObjectReference{{Name: "PRIVATE_SECRET"}} }},
		{"pull-secret", func(o *corev1.ServiceAccount) {
			o.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "PRIVATE_PULL"}}
		}},
		{"identity-annotation", func(o *corev1.ServiceAccount) { o.Annotations["eks.amazonaws.com/role-arn"] = "PRIVATE_ROLE" }},
		{"owner", func(o *corev1.ServiceAccount) { o.Annotations[OwnerAnnotation] = "{}" }},
		{"creation-key", func(o *corev1.ServiceAccount) { o.Annotations[CreationKeyAnnotation] = "changed" }},
		{"uid", func(o *corev1.ServiceAccount) { o.UID = "replacement" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceAccountFixture(t)
			created, err := f.binding.CreateServiceAccount(context.Background(), f.request())
			if err != nil {
				t.Fatal(err)
			}
			tc.change(f.object)
			got, err := f.binding.ObserveServiceAccount(context.Background(), f.read(created.Record))
			if err == nil || got.View().ObservedMinimalPolicy || f.posts != 1 {
				t.Fatal("copied intent fingerprint authorized mutated SA")
			}
		})
	}
	f := newServiceAccountFixture(t)
	_, err := f.binding.ObserveServiceAccount(context.Background(), f.read(ownership.NamespacedRecord{}))
	if err == nil || f.gets != 0 {
		t.Fatal("read learned unpinned identity by name")
	}
}

func TestServiceAccountOpaqueFormattingAndNilEmptyPolicy(t *testing.T) {
	f := newServiceAccountFixture(t)
	f.desired.Secrets = []corev1.ObjectReference{}
	f.desired.ImagePullSecrets = []corev1.LocalObjectReference{}
	f.refresh(t)
	created, err := f.binding.CreateServiceAccount(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	observed, err := f.binding.ObserveServiceAccount(context.Background(), f.read(created.Record))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{f.request(), f.read(created.Record), observed, &observed} {
		for _, format := range []string{"%v", "%+v", "%#v", "%q", "%x", "%p"} {
			if strings.Contains(fmt.Sprintf(format, value), "PRIVATE_AUTHORED_CONFIG") {
				t.Fatal("private SA payload in formatted boundary")
			}
		}
		data, err := json.Marshal(value)
		if err != nil || strings.Contains(string(data), "PRIVATE_AUTHORED_CONFIG") {
			t.Fatal("private SA body in JSON boundary")
		}
	}
	if (ServiceAccountSnapshot{}).View().ObservedMinimalPolicy || (ServiceAccountSnapshot{}).ObjectForProvider() != nil {
		t.Fatal("zero snapshot invented verified policy")
	}
}

func TestServiceAccountVerifierExpiryCannotDispatch(t *testing.T) {
	f := newServiceAccountFixture(t)
	r := f.request()
	r.Writer.ExpiresAt = time.Now().Add(80 * time.Millisecond)
	r.Verifier = serviceAccountVerifierFunc(func(context.Context, ServiceAccountWriteIntent) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	_, err := f.binding.CreateServiceAccount(context.Background(), r)
	requireCode(t, err, OwnershipRejected)
	if f.posts != 0 {
		t.Fatal("writer expired during verifier was dispatched")
	}
}
