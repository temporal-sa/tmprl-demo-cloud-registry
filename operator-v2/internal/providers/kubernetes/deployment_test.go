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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type deploymentVerifierFunc func(context.Context, DeploymentWriteIntent) error

func (f deploymentVerifierFunc) VerifyDeploymentIntent(c context.Context, i DeploymentWriteIntent) error {
	return f(c, i)
}

type deploymentRoundTrip func(*http.Request) (*http.Response, error)

func (f deploymentRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// This fake is an explicitly synthetic proof of call ordering, ownership,
// mutation retry policy and spec drift detection. Its simplified defaulting is
// not evidence of server defaults; that requires the separately owned API tier.
type deploymentFixture struct {
	binding     *NamespaceBinding
	writer      ownership.Writer
	desired     *appsv1.Deployment
	intent      ownership.NamespacedIntent
	parent      *corev1.Namespace
	mu          sync.Mutex
	object      *appsv1.Deployment
	posts, gets int
	postStatus  int
	tamper      func(*appsv1.Deployment)
}

func newDeploymentFixture(t *testing.T) *deploymentFixture {
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
	f := &deploymentFixture{writer: writer, parent: parent}
	transport := deploymentRoundTrip(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		status := 200
		var out any
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/kube-system":
			out = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}}
		case r.Method == "GET" && r.URL.Path == "/api/v1/namespaces/"+parent.Name:
			out = parent.DeepCopy()
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/deployments"):
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
			var object appsv1.Deployment
			if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
				return nil, err
			}
			normalized, err := deploymentBaseline(&object)
			if err != nil {
				return nil, err
			}
			normalized.UID = "deployment-uid"
			normalized.ResourceVersion = "20"
			normalized.Generation = 1
			if f.tamper != nil {
				f.tamper(normalized)
			}
			f.object = normalized
			out = normalized
			status = 201
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/deployments/backend"):
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
	apps, err := appsclient.NewForConfigAndClient(cfg, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{core: core, apps: apps, identity: ClusterIdentity{ID: "owned-local", KubeSystemUID: "cluster-uid"}, timeout: time.Second}
	f.binding, err = c.BindNamespace(context.Background(), base, record, parentIntent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	f.desired = &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: parent.Name, Labels: map[string]string{"app": "backend"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: deploymentPtr[int32](1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "backend"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "backend"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: "backend", AutomountServiceAccountToken: deploymentPtr(false),
					Containers: []corev1.Container{{
						Name: "backend", Image: "example.invalid/owned/backend@sha256:" + strings.Repeat("a", 64),
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env:             []corev1.EnvVar{{Name: "APP_CONFIG", Value: "PRIVATE_AUTHORED_CONFIG"}},
						ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8080)}}},
					}},
				},
			},
		},
	}
	f.refreshIntent(t)
	return f
}
func (f *deploymentFixture) refreshIntent(t *testing.T) {
	t.Helper()
	fp, err := DeploymentFingerprint(f.desired)
	if err != nil {
		t.Fatal(err)
	}
	f.intent, err = f.binding.bound.Intent("deployment", f.desired.Name, "revision-one", "create-deployment-one", fp)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *deploymentFixture) request() DeploymentCreate {
	return DeploymentCreate{Desired: f.desired, Intent: f.intent, Writer: f.writer, WriteID: "journal-write-one", Verifier: deploymentVerifierFunc(func(context.Context, DeploymentWriteIntent) error { return nil })}
}

func TestDeploymentCreateFreezesBodyBeforeVerifierAndDispatch(t *testing.T) {
	f := newDeploymentFixture(t)
	r := f.request()
	verified := false
	r.Verifier = deploymentVerifierFunc(func(_ context.Context, i DeploymentWriteIntent) error {
		verified = true
		if i.Owner != f.writer.Owner || i.Parent != f.binding.Parent() || i.OperationID != f.binding.attemptID || i.ProviderOperationID != r.Intent.Intent.OperationID || i.WriteID != r.WriteID || i.Name != "backend" || i.Revision != "revision-one" || i.Fingerprint != r.Intent.Intent.SpecFingerprint || i.WriterEpoch != 1 || i.WriterID != f.writer.ID {
			t.Fatal("verifier metadata binding differs")
		}
		data, _ := json.Marshal(i)
		if strings.Contains(string(data), "PRIVATE_AUTHORED") {
			t.Fatal("PodSpec crossed verifier seam")
		}
		f.desired.Spec.Template.Spec.Containers[0].Image = "unapproved:latest"
		f.desired.Spec.Template.Spec.Containers[0].Env[0].Value = "changed-after-freeze"
		return nil
	})
	record, err := f.binding.CreateDeployment(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !verified || f.posts != 1 || record.Record.Ref.ProviderID != "deployment-uid" || f.object.Spec.Template.Spec.Containers[0].Env[0].Value != "PRIVATE_AUTHORED_CONFIG" || !strings.Contains(f.object.Spec.Template.Spec.Containers[0].Image, "@sha256:") {
		t.Fatal("frozen desired changed before dispatch")
	}
}

func TestDeploymentCreateRequiresVerifierAndNeverAdoptsOrRetries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		deny   bool
		code   Code
	}{{"no-verifier", 0, true, DeploymentIntentUnverified}, {"already-exists", 409, false, OwnershipRejected}, {"ambiguous-503", 503, false, UnknownWriteOutcome}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeploymentFixture(t)
			f.postStatus = tc.status
			r := f.request()
			if tc.deny {
				r.Verifier = nil
			}
			_, err := f.binding.CreateDeployment(context.Background(), r)
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
				_, err = f.binding.CreateDeployment(context.Background(), r)
				requireCode(t, err, UnknownWriteOutcome)
				if f.posts != 1 {
					t.Fatal("unknown outcome repeated mutation")
				}
			}
		})
	}
	f := newDeploymentFixture(t)
	r := f.request()
	r.Verifier = deploymentVerifierFunc(func(context.Context, DeploymentWriteIntent) error { return errors.New("PRIVATE_DURABLE_ERROR") })
	_, err := f.binding.CreateDeployment(context.Background(), r)
	requireCode(t, err, DeploymentIntentUnverified)
	if f.posts != 0 || strings.Contains(err.Error(), "PRIVATE_") {
		t.Fatal("unverified intent dispatched or leaked")
	}
}

func TestDeploymentResponseDriftRetainsUnknownAndBlocksTeardown(t *testing.T) {
	f := newDeploymentFixture(t)
	f.tamper = func(d *appsv1.Deployment) {
		d.Spec.Template.Spec.Containers = append(d.Spec.Template.Spec.Containers, corev1.Container{Name: "injected", Image: "other"})
	}
	_, err := f.binding.CreateDeployment(context.Background(), f.request())
	requireCode(t, err, UnknownWriteOutcome)
	requireCode(t, f.binding.client.lifecycle.drain(context.Background(), f.parent.Name, string(f.parent.UID), f.binding.attemptID), UnknownWriteOutcome)
}

func TestDeploymentRejectsStaleParentWriterAndChangedIntent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*deploymentFixture, *DeploymentCreate)
		code   Code
	}{
		{"foreign-writer", func(f *deploymentFixture, r *DeploymentCreate) { r.Writer.Owner.ProjectUID = "foreign" }, OwnershipRejected},
		{"stale-writer", func(f *deploymentFixture, r *DeploymentCreate) { r.Writer.Epoch++ }, OwnershipRejected},
		{"expired-writer", func(f *deploymentFixture, r *DeploymentCreate) { r.Writer.ExpiresAt = time.Now().Add(-time.Second) }, OwnershipRejected},
		{"parent-replaced", func(f *deploymentFixture, r *DeploymentCreate) { f.parent.UID = "replacement-parent" }, OwnershipRejected},
		{"parent-unprotected", func(f *deploymentFixture, r *DeploymentCreate) { f.parent.Finalizers = nil }, ParentUnprotected},
		{"changed-body", func(f *deploymentFixture, r *DeploymentCreate) {
			r.Desired.Spec.Template.Spec.Containers[0].Args = []string{"changed-after-journal"}
		}, OwnershipRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeploymentFixture(t)
			r := f.request()
			called := false
			r.Verifier = deploymentVerifierFunc(func(context.Context, DeploymentWriteIntent) error { called = true; return nil })
			tc.change(f, &r)
			_, err := f.binding.CreateDeployment(context.Background(), r)
			requireCode(t, err, tc.code)
			if called || f.posts != 0 {
				t.Fatal("stale authority reached durable verifier or provider dispatch")
			}
		})
	}
}

func TestDeploymentUnsupportedFeaturesFailBeforeMutation(t *testing.T) {
	for _, change := range []func(*appsv1.Deployment){
		func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers = append(d.Spec.Template.Spec.Containers, corev1.Container{Name: "sidecar"})
		},
		func(d *appsv1.Deployment) { d.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "unsupported"}} },
		func(d *appsv1.Deployment) { d.Spec.Template.Spec.HostNetwork = true },
		func(d *appsv1.Deployment) { d.Spec.Template.Spec.EnableServiceLinks = deploymentPtr(true) },
		func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Image = "example.invalid/owned/backend:latest"
		},
		func(d *appsv1.Deployment) { d.Spec.Template.Spec.AutomountServiceAccountToken = deploymentPtr(true) },
		func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet = &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}
		},
	} {
		f := newDeploymentFixture(t)
		change(f.desired)
		f.refreshIntent(t)
		_, err := f.binding.CreateDeployment(context.Background(), f.request())
		requireCode(t, err, DeploymentUnsupportedSpec)
		if f.posts != 0 {
			t.Fatal("unsupported PodSpec dispatched")
		}
	}
}

func TestDeploymentObserveChecksFullSpecAndSafeCurrentReadiness(t *testing.T) {
	f := newDeploymentFixture(t)
	record, err := f.binding.CreateDeployment(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	read := DeploymentRead{Desired: f.desired, Intent: f.intent, Record: record}
	f.object.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, TerminatingReplicas: deploymentPtr[int32](0), Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "PRIVATE_REASON", Message: "PRIVATE_PROVIDER_MESSAGE"}}}
	snapshot, err := f.binding.ObserveDeployment(context.Background(), read)
	if err != nil || snapshot.View().Readiness.State != "Ready" {
		t.Fatal(err)
	}
	for _, value := range []any{snapshot, &snapshot, f.request(), read} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%p", "%q"} {
			out := fmt.Sprintf(verb, value)
			if strings.Contains(out, "PRIVATE_") || strings.Contains(out, "50524956415445") {
				t.Fatalf("unsafe snapshot/request %s", verb)
			}
		}
		data, _ := json.Marshal(value)
		if strings.Contains(string(data), "PRIVATE_") {
			t.Fatal("raw PodSpec/status serialized")
		}
	}
	copy := snapshot.ObjectForProvider()
	copy.Spec.Template.Spec.Containers[0].Env[0].Value = "mutated-copy"
	if snapshot.ObjectForProvider().Spec.Template.Spec.Containers[0].Env[0].Value != "PRIVATE_AUTHORED_CONFIG" {
		t.Fatal("private object accessor aliases snapshot")
	}
	f.object.Spec.Template.Spec.Containers[0].Env[0].Value = "tampered-same-uid-fingerprint"
	_, err = f.binding.ObserveDeployment(context.Background(), read)
	requireCode(t, err, DeploymentSpecMismatch)
	f.object.Spec.Template.Spec.Containers[0].Env[0].Value = "PRIVATE_AUTHORED_CONFIG"
	f.object.Annotations["deployment.kubernetes.io/revision"] = "1"
	f.object.Status.ObservedGeneration = 0
	snapshot, err = f.binding.ObserveDeployment(context.Background(), read)
	if err != nil || snapshot.View().Readiness.State == "Ready" {
		t.Fatal("stale controller status reported ready")
	}
	f.object.UID = "foreign-uid"
	_, err = f.binding.ObserveDeployment(context.Background(), read)
	requireCode(t, err, OwnershipRejected)
}

func TestDeploymentReadinessFixedFailureAndConvergence(t *testing.T) {
	f := newDeploymentFixture(t)
	d, err := deploymentBaseline(f.desired)
	if err != nil {
		t.Fatal(err)
	}
	d.Generation = 2
	for _, tc := range []struct {
		name        string
		status      appsv1.DeploymentStatus
		state, code string
	}{
		{"stale-failure", appsv1.DeploymentStatus{ObservedGeneration: 1, Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue}}}, "Waiting", "DeploymentControllerPending"},
		{"replica-failure", appsv1.DeploymentStatus{ObservedGeneration: 2, Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "secret", Message: "secret"}}}, "Failed", "DeploymentReplicaFailure"},
		{"deadline", appsv1.DeploymentStatus{ObservedGeneration: 2, Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded", Message: "secret"}}}, "Failed", "DeploymentProgressDeadlineExceeded"},
		{"old-replica-retained", appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}, "Waiting", "DeploymentReplicasPending"},
		{"future-generation", appsv1.DeploymentStatus{ObservedGeneration: 3}, "Unknown", "DeploymentInvalidGeneration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d.Status = tc.status
			r := deploymentReadiness(d)
			if r.State != tc.state || r.Code != tc.code {
				t.Fatalf("unexpected readiness: %+v", r)
			}
		})
	}
	for _, count := range []*int32{nil, deploymentPtr[int32](0), deploymentPtr[int32](1), deploymentPtr[int32](-1)} {
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, TerminatingReplicas: count, Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}}
		r := deploymentReadiness(d)
		if r.TerminatingKnown != (count != nil) {
			t.Fatal("optional drain observation was invented")
		}
		want := "Ready"
		if count != nil && *count > 0 {
			want = "Waiting"
		}
		if count != nil && *count < 0 {
			want = "Unknown"
		}
		if r.State != want {
			t.Fatalf("availability/drain distinction lost: %+v", r)
		}
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, CollisionCount: deploymentPtr[int32](3), Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}}
	r := deploymentReadiness(d)
	if r.State != "Ready" || !r.CollisionKnown || r.CollisionCount != 3 {
		t.Fatal("resolved historical collision blocked healthy Deployment")
	}
	d.Status.CollisionCount = deploymentPtr[int32](-1)
	r = deploymentReadiness(d)
	if r.State != "Unknown" || r.Code != "DeploymentInvalidCollisionCount" {
		t.Fatal("invalid collision count accepted")
	}
}
