package kubernetes

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const coreProject = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: banking
spec:
  source:
    repo: https://github.com/temporal-sa/banking
  builds:
    backend:
      context: .
      dockerfile: Dockerfile
    frontend:
      context: .
      dockerfile: Dockerfile
  components:
    backend:
      build: backend
      replicas: 0
      command: [python]
      args: [-m, banking]
      terminationGracePeriodSeconds: 45
      resources:
        requests:
          cpu: 200m
        limits:
          memory: 512Mi
      ports:
        - containerPort: 8080
    frontend:
      build: frontend
      env:
        - name: BACKEND_URL
          value: http://backend:8080
        - name: RELEASE
          valueFrom:
            inputRef: platform.rolloutVersion
      ports:
        - containerPort: 3000
          servicePort: 80
`

func document(t *testing.T) project.Document {
	t.Helper()
	d, err := project.Decode([]byte(coreProject))
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func bindings(t *testing.T) (Bindings, ownership.Context, ownership.Observation) {
	t.Helper()
	context, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "local", InstallationID: "render-test", ProjectUID: "project-uid", Lifetime: ownership.Disposable, RunID: "render-run"}, ResourceRoot: "rgtest", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "local-test"}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := RenderNamespace(context, NamespaceRequest{Revision: "git-abcdef1", OperationID: "render-operation", Fingerprint: "normalized-project"})
	if err != nil {
		t.Fatal(err)
	}
	observed := ownership.Observation{Ref: ownership.ResourceRef{Scope: plan.Intent.Scope, Purpose: plan.Intent.Purpose, Name: plan.Intent.Name, ProviderID: "namespace-canonical-uid"}, Owner: plan.Intent.Owner, CreationIdempotencyKey: plan.Intent.IdempotencyKey, Revision: plan.Intent.Revision, SpecFingerprint: plan.Intent.SpecFingerprint, WriterEpoch: 1, Version: "rv-1"}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	writer := ownership.Writer{ID: "render-writer", Owner: context.Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	record, err := context.BindCreated(plan.Intent, observed, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := context.BindNamespace(record, observed)
	if err != nil {
		t.Fatal(err)
	}
	return Bindings{Namespace: authority, Revision: "git-abcdef1", OperationID: "render-operation", Images: map[string]string{"backend": "example.invalid/backend:git-abcdef1", "frontend": "example.invalid/frontend@sha256:" + strings.Repeat("a", 64)}}, context, observed
}
func object[T runtime.Object](t *testing.T, b Bundle, name string) T {
	t.Helper()
	for _, desired := range b.Objects {
		if desired.Object.GetName() == name {
			if value, ok := any(desired.Object).(T); ok {
				return value
			}
		}
	}
	t.Fatalf("object %s of requested type absent", name)
	var zero T
	return zero
}

func TestCoreObjectsPreserveLogicalDNSAndBehavior(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	before, _ := json.Marshal(d)
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(d)
	if string(before) != string(after) {
		t.Fatal("renderer mutated authoring state")
	}
	backend := object[*appsv1.Deployment](t, bundle, "backend")
	frontend := object[*appsv1.Deployment](t, bundle, "frontend")
	service := object[*corev1.Service](t, bundle, "backend")
	if service.Namespace != b.Namespace.NamespaceName() || service.Name != "backend" || backend.Name != "backend" {
		t.Fatal("namespace isolation changed application-visible object names")
	}
	if frontend.Spec.Template.Spec.Containers[0].Env[0].Value != "http://backend:8080" {
		t.Fatal("internal backend DNS reference was rewritten")
	}
	if service.Spec.Selector["app.kubernetes.io/component"] != "backend" || !reflect.DeepEqual(service.Spec.Selector, backend.Spec.Selector.MatchLabels) {
		t.Fatal("Service no longer selects its candidate workload")
	}
	if *backend.Spec.Replicas != 0 || backend.Spec.Template.Spec.Containers[0].Command[0] != "python" || *backend.Spec.Template.Spec.TerminationGracePeriodSeconds != 45 {
		t.Fatal("explicit replicas/command/grace period lost")
	}
	container := backend.Spec.Template.Spec.Containers[0]
	if container.Resources.Requests.Cpu().String() != "200m" || container.Resources.Requests.Memory().String() != "256Mi" || container.Resources.Limits.Memory().String() != "512Mi" {
		t.Fatal("resource request defaults/overrides changed")
	}
	if container.ReadinessProbe == nil || container.ReadinessProbe.TCPSocket.Port.IntVal != 8080 || container.ReadinessProbe.FailureThreshold != 30 {
		t.Fatal("TCP readiness gating was lost")
	}
	if *backend.Spec.Template.Spec.AutomountServiceAccountToken || backend.Spec.Template.Spec.ServiceAccountName != "backend" {
		t.Fatal("default workload credential boundary changed")
	}
	if frontend.Spec.Template.Spec.Containers[0].Env[1].Value != b.Revision {
		t.Fatal("rollout-version input was not bound explicitly")
	}
	for _, deployment := range []*appsv1.Deployment{backend, frontend} {
		account := object[*corev1.ServiceAccount](t, bundle, deployment.Name)
		if account.Name != deployment.Spec.Template.Spec.ServiceAccountName || account.Namespace != deployment.Namespace || account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken || deployment.Spec.Template.Spec.AutomountServiceAccountToken == nil || *deployment.Spec.Template.Spec.AutomountServiceAccountToken {
			t.Fatal("dedicated ServiceAccount or disabled automount changed")
		}
	}
	for _, desired := range bundle.Objects {
		if desired.Object.GetObjectKind().GroupVersionKind().Group == rbacv1.GroupName {
			t.Fatal("candidate emitted RBAC without a permission-bearing contract")
		}
		switch desired.Object.(type) {
		case *rbacv1.Role, *rbacv1.RoleBinding, *rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding:
			t.Fatal("candidate emitted RBAC through an incomplete object kind")
		}
		if desired.Object.GetNamespace() != b.Namespace.NamespaceName() || desired.Object.GetAnnotations()[ParentUIDAnnotation] != b.Namespace.Parent().UID || desired.Intent.Parent != b.Namespace.Parent() {
			t.Fatal("child lost canonical parent namespace identity")
		}
		body, err := json.Marshal(desired.Object)
		if err != nil {
			t.Fatal(err)
		}
		fingerprint := sha256.Sum256(body)
		if desired.Intent.Intent.Name != desired.Object.GetName() || desired.Intent.Intent.Revision != b.Revision || desired.Intent.Intent.OperationID != b.OperationID || desired.Intent.Intent.Owner != b.Namespace.Owner() || desired.Intent.Intent.Scope != b.Namespace.Scope() || desired.Intent.Intent.SpecFingerprint != hex.EncodeToString(fingerprint[:]) {
			t.Fatal("desired intent does not identify the actual logical object")
		}
	}
	second, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bundle, second) {
		t.Fatal("rendering is nondeterministic")
	}
	backend.Labels["changed"] = "yes"
	if _, exists := service.Labels["changed"]; exists {
		t.Fatal("rendered object metadata aliases another object")
	}
}

func TestPublicRenderBoundariesValidatePresentEmptyBeforeEncoding(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	c := d.Spec.Components["backend"]
	c.Serverless = &project.Serverless{Provider: "aws-lambda"}
	c.Ports, c.Command, c.Args = nil, nil, nil
	c.Replicas, c.TerminationGracePeriodSeconds, c.Resources = nil, nil, nil
	c.Secrets = []project.DNSLabel{}
	d.Spec.Components["backend"] = c
	for _, render := range []func() error{
		func() error { _, err := RenderCandidate(d, b); return err },
		func() error { _, err := RenderPromotion(d, PromotionBindings{}); return err },
	} {
		var safe *diagnostics.FailureError
		if err := render(); !errors.As(err, &safe) || safe.Diagnostic().Code != "InvalidProject" {
			t.Fatalf("invalid programmatic field passed authoring boundary: %v", err)
		}
	}
}

func TestNamespaceBootstrapIsSeparateFromObservedParentPhase(t *testing.T) {
	b, context, _ := bindings(t)
	plan, err := RenderNamespace(context, NamespaceRequest{Revision: b.Revision, OperationID: b.OperationID, Fingerprint: "normalized-project"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Object.UID != "" || plan.Object.Namespace != "" || plan.Intent.Scope.Namespace != "" || plan.Intent.Purpose != "runtime-namespace" {
		t.Fatal("bootstrap invented a parent UID or emitted a namespaced Namespace")
	}
	again, err := RenderNamespace(context, NamespaceRequest{Revision: b.Revision, OperationID: b.OperationID, Fingerprint: "normalized-project"})
	if err != nil {
		t.Fatal(err)
	}
	next, err := RenderNamespace(context, NamespaceRequest{Revision: b.Revision, OperationID: "new-accepted-intent-attempt", Fingerprint: "normalized-project"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Object.Name != plan.Object.Name || next.Object.Name == plan.Object.Name {
		t.Fatal("attempt naming is not stable for retry and distinct for an intentional revisit")
	}
	wrongRevision := b
	wrongRevision.Revision = "git-abcdef2"
	if _, err = RenderCandidate(document(t), wrongRevision); err == nil {
		t.Fatal("candidate rendering could overwrite a differently bound revision namespace")
	}
	_, err = RenderCandidate(document(t), Bindings{Revision: b.Revision, OperationID: b.OperationID, Images: b.Images})
	if err == nil {
		t.Fatal("children rendered without verified parent authority")
	}
	namespaced, err := ownership.NewContext(ownership.Config{Owner: context.Owner(), ResourceRoot: "rgtest", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "local-test", Namespace: "not-cluster-scope"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RenderNamespace(namespaced, NamespaceRequest{Revision: b.Revision, OperationID: b.OperationID, Fingerprint: "spec"}); err == nil {
		t.Fatal("namespace bootstrap accepted namespaced authority")
	}
}

func TestSecretAndTemporalProjectionNeverRendersValues(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	d.Spec.Secrets = map[string]project.UserSecret{"database": {RequiredProperties: []project.EnvName{"PASSWORD"}}}
	c := d.Spec.Components["backend"]
	c.Worker = &project.Worker{Enabled: true}
	c.Secrets = []project.DNSLabel{"database"}
	c.Env = project.EnvList{{Name: "DB_PASSWORD", ValueFrom: &project.EnvValueFrom{InputRef: "secrets.database.PASSWORD"}}, {Name: "CUSTOM_NAMESPACE", ValueFrom: &project.EnvValueFrom{InputRef: "temporal.namespace"}}}
	d.Spec.Components["backend"] = c
	secret := SecretBinding{Namespace: b.Namespace.NamespaceName(), NamespaceUID: b.Namespace.Parent().UID, Owner: b.Namespace.Owner(), Name: "database-local", Keys: map[string]string{"PASSWORD": "PASSWORD"}}
	temporal := SecretBinding{Namespace: secret.Namespace, NamespaceUID: secret.NamespaceUID, Owner: secret.Owner, Name: "temporal-local", Keys: StandardTemporalKeys()}
	b.ProjectSecrets = map[string]SecretBinding{"database": secret}
	b.Temporal = map[string]TemporalBinding{"primary": {Mode: "apiKey", Secret: temporal}}
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	pod := object[*appsv1.Deployment](t, bundle, "backend").Spec.Template.Spec
	if len(pod.Containers[0].EnvFrom) != 2 || pod.Containers[0].Env[0].ValueFrom.SecretKeyRef.Name != "database-local" || pod.Containers[0].Env[1].ValueFrom.SecretKeyRef.Key != "TEMPORAL_NAMESPACE" {
		t.Fatal("typed Secret projection changed")
	}
	if len(bundle.RequiredSecrets) != 2 {
		t.Fatal("required local secrets were not surfaced to observation/admission")
	}
	for _, item := range bundle.Objects {
		if _, ok := item.Object.(*corev1.Secret); ok {
			t.Fatal("renderer emitted empty/replacement Secret data")
		}
	}
	bad := b
	bad.ProjectSecrets = map[string]SecretBinding{"database": secret}
	foreign := secret
	foreign.NamespaceUID = "recreated-namespace"
	bad.ProjectSecrets["database"] = foreign
	if _, err = RenderCandidate(d, bad); err == nil {
		t.Fatal("foreign namespace incarnation Secret binding accepted")
	}
	bad = b
	bad.ProjectSecrets = map[string]SecretBinding{"database": secret}
	missing := secret
	missing.Keys = map[string]string{}
	bad.ProjectSecrets["database"] = missing
	if _, err = RenderCandidate(d, bad); err == nil {
		t.Fatal("missing required Secret property silently ignored")
	}
}

func TestUnsafeOrUnsupportedCapabilitiesFailWithSafeDiagnostics(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*project.Document, *Bindings)
	}{
		{"mutable image", func(_ *project.Document, b *Bindings) { b.Images["backend"] = "example.invalid/backend:latest" }},
		{"missing image", func(_ *project.Document, b *Bindings) { delete(b.Images, "backend") }},
		{"overflowing replica count", func(d *project.Document, _ *Bindings) {
			c := d.Spec.Components["backend"]
			c.Replicas = ptr(2147483648)
			d.Spec.Components["backend"] = c
		}},
		{"unapproved token", func(d *project.Document, _ *Bindings) {
			c := d.Spec.Components["backend"]
			c.AutomountServiceAccountToken = ptr(true)
			d.Spec.Components["backend"] = c
		}},
		{"unapproved role", func(d *project.Document, _ *Bindings) {
			c := d.Spec.Components["backend"]
			c.AWSIAMRoleARN = ptr("arn:aws:iam::123456789012:role/secret-sentinel")
			d.Spec.Components["backend"] = c
		}},
		{"isolated executor", func(d *project.Document, _ *Bindings) {
			c := d.Spec.Components["backend"]
			c.IsolatedExecutor = ptr(true)
			d.Spec.Components["backend"] = c
		}},
		{"autoscaling before capability slice", func(d *project.Document, _ *Bindings) {
			c := d.Spec.Components["backend"]
			c.Replicas = nil
			c.Worker = &project.Worker{Enabled: true}
			c.Autoscaling = &project.Autoscaling{TaskQueue: ptr("jobs")}
			d.Spec.Components["backend"] = c
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			d := document(t)
			b, _, _ := bindings(t)
			scenario.change(&d, &b)
			bundle, err := RenderCandidate(d, b)
			if err == nil {
				t.Fatal("unsupported/unsafe capability was silently dropped")
			}
			var safe *diagnostics.FailureError
			if !errors.As(err, &safe) {
				t.Fatalf("expected safe diagnostic error, got %T", err)
			}
			if len(bundle.Objects) != 0 || strings.Contains(err.Error(), "secret-sentinel") {
				t.Fatal("partial plan or rejected scalar escaped a rendering failure")
			}
		})
	}
}
