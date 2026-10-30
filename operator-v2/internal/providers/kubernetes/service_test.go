package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type serviceVerifierFunc func(context.Context, ServiceWriteIntent) error

func (f serviceVerifierFunc) VerifyServiceIntent(c context.Context, i ServiceWriteIntent) error {
	return f(c, i)
}

type serviceRoundTrip func(*http.Request) (*http.Response, error)

func (f serviceRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// This fake is an explicitly synthetic proof of call ordering, ownership,
// mutation retry policy and spec drift detection. Its simplified defaulting is
// not evidence of server defaults; that requires the separately owned API tier.
type serviceFixture struct {
	binding     *NamespaceBinding
	writer      ownership.Writer
	desired     *corev1.Service
	intent      ownership.NamespacedIntent
	parent      *corev1.Namespace
	mu          sync.Mutex
	object      *corev1.Service
	posts, gets int
	postStatus  int
	tamper      func(*corev1.Service)
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "fixture", ProjectUID: "project-one", Lifetime: ownership.Disposable, RunID: "run-one"}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "owned-local"}}})
	if err != nil {
		t.Fatal(err)
	}
	parentIntent, err := base.Intent(ownership.Kubernetes, "runtime-namespace", "attempt-one", "revision-one", "namespace-create-one", "namespace-spec-one", 63)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "writer-one", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	annotations := CreationAnnotations(parentIntent, writer)
	annotations[AttemptAnnotation] = parentIntent.OperationID
	parent := &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: parentIntent.Name, UID: "parent-uid", ResourceVersion: "10", Annotations: annotations, Finalizers: []string{NamespaceLifecycleFinalizer}}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	record := ownership.Record{Ref: ownership.ResourceRef{Scope: parentIntent.Scope, Purpose: parentIntent.Purpose, Name: parentIntent.Name, ProviderID: string(parent.UID)}, Owner: parentIntent.Owner, CreationOperationID: parentIntent.OperationID, CreationIdempotencyKey: parentIntent.IdempotencyKey, Revision: parentIntent.Revision, SpecFingerprint: parentIntent.SpecFingerprint, WriterEpoch: 1}
	f := &serviceFixture{writer: writer, parent: parent}
	transport := serviceRoundTrip(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		status := 200
		var out any
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/kube-system":
			out = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}}
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/"+parent.Name:
			out = parent.DeepCopy()
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/services"):
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
			var object corev1.Service
			if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
				return nil, err
			}
			normalized, err := serviceBaseline(&object)
			if err != nil {
				return nil, err
			}
			normalized.UID = "service-uid"
			normalized.ResourceVersion = "20"
			normalized.Spec.ClusterIP = "10.96.0.12"
			normalized.Spec.ClusterIPs = []string{"10.96.0.12"}
			normalized.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
			if f.tamper != nil {
				f.tamper(normalized)
			}
			f.object = normalized
			out = normalized
			status = 201
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/services/backend"):
			f.gets++
			if f.object == nil {
				status = 404
				out = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: 404, Reason: metav1.StatusReasonNotFound}
			} else {
				out = f.object.DeepCopy()
			}
		default:
			return nil, errors.New("unexpected synthetic provider call")
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
	httpClient := &http.Client{Transport: transport}
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}
	core, err := coreclient.NewForConfigAndClient(cfg, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{core: core, identity: ClusterIdentity{ID: "owned-local", KubeSystemUID: "cluster-uid"}, timeout: time.Second}
	f.binding, err = c.BindNamespace(context.Background(), base, record, parentIntent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	f.desired = &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: parent.Name, Annotations: map[string]string{"fixture": "PRIVATE_AUTHORED_CONFIG"}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{"app": "PRIVATE_SELECTOR"}, Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP}}}}
	f.refreshIntent(t)
	return f
}
func (f *serviceFixture) refreshIntent(t *testing.T) {
	t.Helper()
	fp, err := ServiceFingerprint(f.desired)
	if err != nil {
		t.Fatal(err)
	}
	f.intent, err = f.binding.bound.Intent("service", f.desired.Name, "revision-one", "create-service-one", fp)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *serviceFixture) request() ServiceCreate {
	return ServiceCreate{Desired: f.desired, Intent: f.intent, Writer: f.writer, WriteID: "journal-write-one", Verifier: serviceVerifierFunc(func(context.Context, ServiceWriteIntent) error { return nil })}
}

func TestServiceCreateFreezesBodyBeforeVerifierAndDispatch(t *testing.T) {
	f := newServiceFixture(t)
	r := f.request()
	verified := false
	r.Verifier = serviceVerifierFunc(func(_ context.Context, i ServiceWriteIntent) error {
		verified = true
		if i.Owner != f.writer.Owner || i.Parent != f.binding.Parent() || i.OperationID != f.binding.attemptID || i.ProviderOperationID != r.Intent.Intent.OperationID || i.WriteID != r.WriteID || i.Name != "backend" || i.Revision != "revision-one" || i.Fingerprint != r.Intent.Intent.SpecFingerprint || i.WriterEpoch != 1 || i.WriterID != f.writer.ID {
			t.Fatal("verifier metadata binding differs")
		}
		data, _ := json.Marshal(i)
		if strings.Contains(string(data), "PRIVATE_AUTHORED") {
			t.Fatal("Service body crossed verifier seam")
		}
		f.desired.Spec.Selector["app"] = "changed-after-freeze"

		return nil
	})
	record, err := f.binding.CreateService(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !verified || f.posts != 1 || record.Resource.Record.Ref.ProviderID != "service-uid" || f.object.Spec.Selector["app"] != "PRIVATE_SELECTOR" || record.SpecContract != ServiceSpecContract || !serviceReceiptDigest.MatchString(record.AllocatedSpecDigest) {
		t.Fatal("frozen desired changed before dispatch")
	}
}

func TestServiceCreateRequiresVerifierAndNeverAdoptsOrRetries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		deny   bool
		code   Code
	}{{"no-verifier", 0, true, ServiceIntentUnverified}, {"already-exists", 409, false, OwnershipRejected}, {"ambiguous-503", 503, false, UnknownWriteOutcome}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceFixture(t)
			f.postStatus = tc.status
			r := f.request()
			if tc.deny {
				r.Verifier = nil
			}
			_, err := f.binding.CreateService(context.Background(), r)
			requireCode(t, err, tc.code)
			expected := 1
			if tc.deny {
				expected = 0
			}
			if f.posts != expected || f.gets != 0 {
				t.Fatal("mutation retried or adopted by name")
			}
			if tc.code == UnknownWriteOutcome {
				state := f.binding.client.Lifecycle(f.parent.Name)
				if len(state.Unresolved) != 1 || !state.Unresolved[0].Unknown || state.Unresolved[0].TargetFingerprint != f.intent.Intent.SpecFingerprint {
					t.Fatal("ambiguous write lost exact tombstone")
				}
				_, err = f.binding.CreateService(context.Background(), r)
				requireCode(t, err, UnknownWriteOutcome)
				if f.posts != 1 {
					t.Fatal("unknown outcome repeated mutation")
				}
			}
		})
	}
	f := newServiceFixture(t)
	r := f.request()
	r.Verifier = serviceVerifierFunc(func(context.Context, ServiceWriteIntent) error { return errors.New("PRIVATE_DURABLE_ERROR") })
	_, err := f.binding.CreateService(context.Background(), r)
	requireCode(t, err, ServiceIntentUnverified)
	if f.posts != 0 || strings.Contains(err.Error(), "PRIVATE_") {
		t.Fatal("unverified intent dispatched or leaked")
	}
}

func TestServiceResponseDriftRetainsUnknownAndBlocksTeardown(t *testing.T) {
	f := newServiceFixture(t)
	f.tamper = func(d *corev1.Service) {
		d.Spec.Selector["app"] = "injected"
	}
	_, err := f.binding.CreateService(context.Background(), f.request())
	requireCode(t, err, UnknownWriteOutcome)
	requireCode(t, f.binding.client.lifecycle.drain(context.Background(), f.parent.Name, string(f.parent.UID), f.binding.attemptID), UnknownWriteOutcome)
}

func TestServiceRejectsStaleParentWriterAndChangedIntent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*serviceFixture, *ServiceCreate)
		code   Code
	}{
		{"foreign-writer", func(f *serviceFixture, r *ServiceCreate) { r.Writer.Owner.ProjectUID = "foreign" }, OwnershipRejected},
		{"stale-writer", func(f *serviceFixture, r *ServiceCreate) { r.Writer.Epoch++ }, OwnershipRejected},
		{"expired-writer", func(f *serviceFixture, r *ServiceCreate) { r.Writer.ExpiresAt = time.Now().Add(-time.Second) }, OwnershipRejected},
		{"parent-replaced", func(f *serviceFixture, r *ServiceCreate) { f.parent.UID = "replacement-parent" }, OwnershipRejected},
		{"parent-unprotected", func(f *serviceFixture, r *ServiceCreate) { f.parent.Finalizers = nil }, ParentUnprotected},
		{"changed-body", func(f *serviceFixture, r *ServiceCreate) {
			r.Desired.Spec.Ports[0].TargetPort = intstr.FromInt32(9090)
		}, OwnershipRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceFixture(t)
			r := f.request()
			called := false
			r.Verifier = serviceVerifierFunc(func(context.Context, ServiceWriteIntent) error { called = true; return nil })
			tc.change(f, &r)
			_, err := f.binding.CreateService(context.Background(), r)
			requireCode(t, err, tc.code)
			if called || f.posts != 0 {
				t.Fatal("stale authority reached durable verifier or provider dispatch")
			}
		})
	}
}

func TestServiceObserveChecksReceiptFullSpecAndSafeProjection(t *testing.T) {
	f := newServiceFixture(t)
	receipt, err := f.binding.CreateService(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	read := ServiceRead{Desired: f.desired, Intent: f.intent, Record: receipt}
	snapshot, err := f.binding.ObserveService(context.Background(), read)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.View().ObservedAllocated || snapshot.View().BackendReadiness != "NotEvaluated" {
		t.Fatal("allocation confused with endpoint readiness")
	}
	for _, value := range []any{snapshot, &snapshot, f.request(), read} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%p", "%q"} {
			out := fmt.Sprintf(verb, value)
			if strings.Contains(out, "PRIVATE_") || strings.Contains(out, "50524956415445") || strings.Contains(out, "10.96.0.12") {
				t.Fatalf("unsafe snapshot/request %s", verb)
			}
		}
		data, _ := json.Marshal(value)
		if strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), "10.96.0.12") {
			t.Fatal("raw spec/status serialized")
		}
	}
	copy := snapshot.ObjectForProvider()
	copy.Spec.Selector["app"] = "changed"
	if snapshot.ObjectForProvider().Spec.Selector["app"] != "PRIVATE_SELECTOR" {
		t.Fatal("snapshot aliases accessor")
	}
	baseline := f.object.DeepCopy()
	cases := []struct {
		name   string
		change func(*ServiceRead)
		code   Code
	}{
		{"missing-receipt", func(r *ServiceRead) { r.Record.AllocatedSpecDigest = "" }, ServiceIntentUnverified},
		{"wrong-contract", func(r *ServiceRead) { r.Record.SpecContract = "future" }, ServiceIntentUnverified},
		{"wrong-digest", func(r *ServiceRead) { r.Record.AllocatedSpecDigest = strings.Repeat("a", 64) }, ServiceSpecMismatch},
		{"selector", func(r *ServiceRead) { f.object.Spec.Selector["app"] = "tampered" }, ServiceSpecMismatch},
		{"port", func(r *ServiceRead) { f.object.Spec.Ports[0].TargetPort = intstr.FromInt32(8081) }, ServiceSpecMismatch},
		{"allocation", func(r *ServiceRead) {
			f.object.Spec.ClusterIP = "10.96.0.13"
			f.object.Spec.ClusterIPs[0] = "10.96.0.13"
		}, ServiceSpecMismatch},
		{"foreign-uid", func(r *ServiceRead) { f.object.UID = "foreign" }, OwnershipRejected},
		{"parent-marker", func(r *ServiceRead) { f.object.Annotations[ownership.ParentNamespaceUIDAnnotation] = "foreign" }, OwnershipRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.object = baseline.DeepCopy()
			r := read
			before := f.gets
			tc.change(&r)
			_, err := f.binding.ObserveService(context.Background(), r)
			requireCode(t, err, tc.code)
			if tc.code == ServiceIntentUnverified && f.gets != before {
				t.Fatal("missing receipt learned from GET")
			}
		})
	}
}
func TestServiceUnsupportedFeaturesAndFieldsNeverDispatch(t *testing.T) {
	for _, change := range []func(*corev1.Service){
		func(d *corev1.Service) { d.Spec.Type = corev1.ServiceTypeNodePort }, func(d *corev1.Service) { d.Spec.Type = corev1.ServiceTypeLoadBalancer }, func(d *corev1.Service) { d.Spec.ExternalIPs = []string{"203.0.113.1"} }, func(d *corev1.Service) { d.Spec.ExternalName = "other.example" }, func(d *corev1.Service) { d.Spec.ClusterIP = "None" }, func(d *corev1.Service) { d.Spec.ClusterIP = "10.96.0.12" }, func(d *corev1.Service) { d.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol} }, func(d *corev1.Service) { p := corev1.IPFamilyPolicyPreferDualStack; d.Spec.IPFamilyPolicy = &p }, func(d *corev1.Service) { d.Spec.Ports[0].NodePort = 30000 }, func(d *corev1.Service) { d.Spec.Ports[0].TargetPort = intstr.FromString("http") }, func(d *corev1.Service) { d.Spec.PublishNotReadyAddresses = true }, func(d *corev1.Service) { d.Spec.Selector = nil }, func(d *corev1.Service) { d.Spec.Ports[0].Name = "" }, func(d *corev1.Service) { d.Spec.Ports[0].Protocol = corev1.ProtocolUDP }, func(d *corev1.Service) { d.Spec.Ports[0].AppProtocol = deploymentPtr("http") }, func(d *corev1.Service) { d.Spec.SessionAffinity = corev1.ServiceAffinityClientIP }, func(d *corev1.Service) {
			d.Spec.InternalTrafficPolicy = deploymentPtr(corev1.ServiceInternalTrafficPolicyLocal)
		}, func(d *corev1.Service) { d.Spec.TrafficDistribution = deploymentPtr("PreferClose") }, func(d *corev1.Service) { d.Spec.Ports = append(d.Spec.Ports, d.Spec.Ports[0]) },
	} {
		f := newServiceFixture(t)
		change(f.desired)
		f.refreshIntent(t)
		_, err := f.binding.CreateService(context.Background(), f.request())
		requireCode(t, err, ServiceUnsupportedSpec)
		if f.posts != 0 {
			t.Fatal("unsupported Service dispatched")
		}
	}
}
func TestServiceResponseAllocationAndSpecDriftRetainsUnknown(t *testing.T) {
	for _, tamper := range []func(*corev1.Service){
		func(d *corev1.Service) { d.Spec.ClusterIPs = nil }, func(d *corev1.Service) { d.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv6Protocol} }, func(d *corev1.Service) { d.Spec.ClusterIP = "127.0.0.1"; d.Spec.ClusterIPs = []string{"127.0.0.1"} }, func(d *corev1.Service) { d.Spec.ExternalIPs = []string{"203.0.113.1"} }, func(d *corev1.Service) { d.Spec.Selector["app"] = "injected" }, func(d *corev1.Service) { d.Spec.ClusterIPs[0] = "10.96.0.13" }, func(d *corev1.Service) {
			d.Spec.IPFamilies = append(d.Spec.IPFamilies, corev1.IPv6Protocol)
			d.Spec.ClusterIPs = append(d.Spec.ClusterIPs, "fd00::10")
		},
	} {
		f := newServiceFixture(t)
		f.tamper = tamper
		receipt, err := f.binding.CreateService(context.Background(), f.request())
		requireCode(t, err, UnknownWriteOutcome)
		if receipt != (ServiceRecord{}) {
			t.Fatal("partial receipt returned")
		}
		requireCode(t, f.binding.client.lifecycle.drain(context.Background(), f.parent.Name, string(f.parent.UID), f.binding.attemptID), UnknownWriteOutcome)
	}
}
func TestServiceIPv6AllocationAndDefaultContract(t *testing.T) {
	f := newServiceFixture(t)
	f.desired.Spec.Ports[0].Protocol = ""
	f.refreshIntent(t)
	f.tamper = func(d *corev1.Service) {
		d.Spec.ClusterIP = "fd00::12"
		d.Spec.ClusterIPs = []string{"fd00::12"}
		d.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv6Protocol}
	}
	receipt, err := f.binding.CreateService(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.binding.ObserveService(context.Background(), ServiceRead{Desired: f.desired, Intent: f.intent, Record: receipt})
	if err != nil || !snapshot.View().ObservedAllocated {
		t.Fatal("IPv6/default contract failed", err)
	}
	if f.object.Spec.Ports[0].Protocol != corev1.ProtocolTCP || f.object.Spec.SessionAffinity != corev1.ServiceAffinityNone || f.object.Spec.InternalTrafficPolicy == nil || *f.object.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster || f.object.Spec.IPFamilyPolicy == nil || *f.object.Spec.IPFamilyPolicy != corev1.IPFamilyPolicySingleStack {
		t.Fatal("defaults differ")
	}
}
func TestServiceVerifierExpiryCannotDispatch(t *testing.T) {
	f := newServiceFixture(t)
	r := f.request()
	// Expire the copied writer while the verifier callback is running.
	r.Writer.ExpiresAt = time.Now().Add(25 * time.Millisecond)
	r.Verifier = serviceVerifierFunc(func(context.Context, ServiceWriteIntent) error { time.Sleep(40 * time.Millisecond); return nil })
	_, err := f.binding.CreateService(context.Background(), r)
	requireCode(t, err, OwnershipRejected)
	if f.posts != 0 {
		t.Fatal("expired writer dispatched")
	}
}
