package candidateprep

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	render "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const privateMarker = "private-candidate-payload-do-not-publish"
const authored = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: candidate-example
spec:
  source:
    repo: https://github.com/temporal-sa/private-candidate-source
  builds:
    app:
      context: .
      dockerfile: Dockerfile
    unused:
      context: .
      dockerfile: Otherfile
  components:
    backend:
      build: app
      serviceName: backend-api
      command: [private-candidate-payload-do-not-publish]
      args: [private-candidate-payload-do-not-publish]
      env:
        - name: PRIVATE_VALUE
          value: private-candidate-payload-do-not-publish
      ports:
        - containerPort: 8080
`

func pointer[T any](v T) *T { return &v }

func authority(t *testing.T, root, cluster, run string) ownership.Context {
	t.Helper()
	c, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "local", InstallationID: "candidate-fixture", ProjectUID: "project-uid", Lifetime: ownership.Disposable, RunID: run}, ResourceRoot: root, Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: cluster}}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func config(t *testing.T) Config {
	t.Helper()
	d, err := project.Decode([]byte(authored))
	if err != nil {
		t.Fatal(err)
	}
	return Config{Document: d, Authority: authority(t, "cpfixture", "owned-test-cluster", "accepted-run"), Subject: rollout.Subject{Environment: "local", Installation: "candidate-fixture", ProjectUID: "project-uid"}, Generation: 3, AttemptID: "accepted-attempt-1", Component: "backend", Images: map[string]string{"app": "example.invalid/private-candidate-image@sha256:" + strings.Repeat("a", 64)}, Profile: LocalLiteralPairV1}
}
func plan(t *testing.T, c Config) Plan {
	t.Helper()
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func code(t *testing.T, err error, want string) {
	t.Helper()
	var safe *diagnostics.FailureError
	if !errors.As(err, &safe) || safe == nil || safe.Diagnostic().Code != want || strings.Contains(fmt.Sprint(err), privateMarker) {
		t.Fatal("missing exact safe preparation failure", want)
	}
}

// These are synthetic typed ownership observations. They test composition of
// existing pure contracts, not native API creation, receipts or parent fencing.
func syntheticNamespace(t *testing.T, p Plan, c ownership.Context, attempt string) ownership.NamespacedContext {
	t.Helper()
	desired, err := p.Desired()
	if err != nil {
		t.Fatal(err)
	}
	// Independently follow the existing Worker namespace contract rather than
	// using Plan.Namespace as both producer and expected value.
	ns, err := render.RenderNamespace(c, render.NamespaceRequest{Revision: desired.Revision, OperationID: attempt, Fingerprint: journal.DesiredDigest(desired)})
	if err != nil {
		t.Fatal(err)
	}
	observed := ownership.Observation{Ref: ownership.ResourceRef{Scope: ns.Intent.Scope, Purpose: ns.Intent.Purpose, Name: ns.Intent.Name, ProviderID: "synthetic-namespace-uid"}, Owner: ns.Intent.Owner, CreationIdempotencyKey: ns.Intent.IdempotencyKey, Revision: ns.Intent.Revision, SpecFingerprint: ns.Intent.SpecFingerprint, WriterEpoch: 1, Version: "rv-synthetic-1"}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	writer := ownership.Writer{ID: "synthetic-writer", Owner: c.Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	record, err := c.BindCreated(ns.Intent, observed, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := c.BindNamespace(record, observed)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func TestCompleteAcceptedPlanIdentityAndStableAttempt(t *testing.T) {
	c := config(t)
	p := plan(t, c)
	if second := plan(t, config(t)); p.View() != second.View() {
		t.Fatal("same accepted attempt is not deterministic")
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"literal", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.Env[0].Value = pointer("changed-literal")
			c.Document.Spec.Components["backend"] = v
		}},
		{"command", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.Command[0] = "changed-command"
			c.Document.Spec.Components["backend"] = v
		}},
		{"image", func(c *Config) {
			c.Images["app"] = "example.invalid/private-candidate-image@sha256:" + strings.Repeat("b", 64)
		}},
		{"source", func(c *Config) { c.Document.Spec.Source.Repo = "https://github.com/temporal-sa/changed-source" }},
		{"unused-build", func(c *Config) {
			v := c.Document.Spec.Builds["unused"]
			v.Dockerfile = pointer("Changedfile")
			c.Document.Spec.Builds["unused"] = v
		}},
		{"unused-resolved-image", func(c *Config) { c.Images["unused"] = "example.invalid/unused@sha256:" + strings.Repeat("c", 64) }},
		{"metadata-extension", func(c *Config) {
			c.Document.Metadata.Extra = map[string]json.RawMessage{"acceptedExtension": json.RawMessage(`{"value":"changed"}`)}
		}},
		{"attempt", func(c *Config) { c.AttemptID = "accepted-attempt-2" }},
		{"generation", func(c *Config) { c.Generation++ }},
		{"cluster", func(c *Config) { c.Authority = authority(t, "cpfixture", "another-owned-cluster", "accepted-run") }},
		{"namespace-root", func(c *Config) { c.Authority = authority(t, "cpother", "owned-test-cluster", "accepted-run") }},
		{"run", func(c *Config) { c.Authority = authority(t, "cpfixture", "owned-test-cluster", "different-run") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := config(t)
			tc.change(&changed)
			next := plan(t, changed)
			if next.View().Revision == p.View().Revision || next.View().ReconcileKey == p.View().ReconcileKey {
				t.Fatal("complete accepted private intent was not bound")
			}
		})
	}
}

func TestPrivateAcceptanceAndExecutionCopiesDoNotAlias(t *testing.T) {
	c := config(t)
	p := plan(t, c)
	bound := syntheticNamespace(t, p, c.Authority, c.AttemptID)
	pair, err := p.Render(bound)
	if err != nil {
		t.Fatal(err)
	}
	desired, _ := p.Desired()
	d, _ := pair.Deployment()
	s, _ := pair.Service()
	sa, _ := pair.ServiceAccount()
	beforeD, _ := json.Marshal(d.Object)
	beforeS, _ := json.Marshal(s.Object)
	beforeSA, _ := json.Marshal(sa.Object)
	c.Images["app"] = "mutated"
	component := c.Document.Spec.Components["backend"]
	component.Command[0] = "mutated"
	*component.Env[0].Value = "mutated"
	c.Document.Spec.Components["backend"] = component
	c.Document.Spec.Source.Repo = "mutated"
	desired.Images["app"] = "mutated"
	desired.RequiredBuilds[0] = "mutated"
	d.Object.(*appsv1.Deployment).Spec.Template.Spec.Containers[0].Env[0].Value = "mutated"
	d.Object.SetAnnotations(map[string]string{"mutated": "yes"})
	s.Object.(*corev1.Service).Spec.Selector["mutated"] = "yes"
	sa.Object.GetAnnotations()[render.ParentUIDAnnotation] = "mutated"
	*sa.Object.(*corev1.ServiceAccount).AutomountServiceAccountToken = true
	d2, _ := pair.Deployment()
	s2, _ := pair.Service()
	sa2, _ := pair.ServiceAccount()
	afterD, _ := json.Marshal(d2.Object)
	afterS, _ := json.Marshal(s2.Object)
	afterSA, _ := json.Marshal(sa2.Object)
	if string(beforeD) != string(afterD) || string(beforeS) != string(afterS) || string(beforeSA) != string(afterSA) {
		t.Fatal("private pair aliases caller execution copies")
	}
	pair2, err := p.Render(bound)
	if err != nil {
		t.Fatal(err)
	}
	d3, _ := pair2.Deployment()
	actual, _ := json.Marshal(d3.Object)
	if string(actual) != string(beforeD) {
		t.Fatal("accepted plan aliases original authoring/image input")
	}
	namespace, _ := p.Namespace()
	namespace.Object.Labels["mutated"] = "yes"
	namespace2, _ := p.Namespace()
	if namespace2.Object.Labels["mutated"] != "" {
		t.Fatal("namespace render aliases previous output")
	}
}

func TestLiteralPairMatchesWorkerNamespaceDefaultsAndExplicitPrerequisites(t *testing.T) {
	c := config(t)
	p := plan(t, c)
	bound := syntheticNamespace(t, p, c.Authority, c.AttemptID)
	pair, err := p.Render(bound)
	if err != nil {
		t.Fatal(err)
	}
	desired, _ := p.Desired()
	expected, _ := render.RenderNamespace(c.Authority, render.NamespaceRequest{Revision: desired.Revision, OperationID: c.AttemptID, Fingerprint: journal.DesiredDigest(desired)})
	actual, _ := p.Namespace()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("namespace creation key differs from actual Worker contract")
	}
	d, _ := pair.Deployment()
	s, _ := pair.Service()
	deployment, service := d.Object.(*appsv1.Deployment), s.Object.(*corev1.Service)
	container := deployment.Spec.Template.Spec.Containers[0]
	prerequisite, err := pair.Prerequisite()
	if err != nil || prerequisite.ServiceAccountName != "backend" || prerequisite.AutomountServiceAccountToken || deployment.Name != "backend" || service.Name != "backend-api" || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 || container.Ports[0].Name != "http" || service.Spec.Ports[0].Port != 8080 || !reflect.DeepEqual(service.Spec.Selector, deployment.Spec.Template.Labels) || d.Intent.Parent != bound.Parent() || s.Intent.Parent != bound.Parent() || d.Intent.Intent.OperationID != c.AttemptID || s.Intent.Intent.Revision != desired.Revision || pair.View().Promotion != PromotionNotAuthorized {
		t.Fatal("literal defaults/pair/prerequisite/identity contract changed")
	}
	if container.Env[0].Value != privateMarker || container.Command[0] != privateMarker {
		t.Fatal("private authored runtime values were dropped")
	}
	sa, _ := pair.ServiceAccount()
	account := sa.Object.(*corev1.ServiceAccount)
	if account.Name != prerequisite.ServiceAccountName || account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken || len(account.Secrets) != 0 || len(account.ImagePullSecrets) != 0 || sa.Intent.Parent != bound.Parent() || sa.Intent.Intent.Purpose != "service-account" || sa.Intent.Intent.Revision != desired.Revision || sa.Intent.Intent.OperationID != c.AttemptID {
		t.Fatal("private SA selection lost exact minimal prerequisite identity")
	}
	if _, present := deployment.Annotations["eks.amazonaws.com/role-arn"]; present {
		t.Fatal("unexpected role projection")
	}
}

func TestUnsupportedAndInvalidAcceptanceFailsSafely(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*Config)
	}{
		{"profile", "PreparationProfileUnsupported", func(c *Config) { c.Profile = "future-profile" }},
		{"missing-authority", "PreparationAuthorityMismatch", func(c *Config) { c.Authority = ownership.Context{} }},
		{"subject", "PreparationAuthorityMismatch", func(c *Config) { c.Subject.ProjectUID = "foreign-project" }},
		{"generation", "PreparationAuthorityMismatch", func(c *Config) { c.Generation = 0 }},
		{"attempt", "PreparationAuthorityMismatch", func(c *Config) { c.AttemptID = "" }},
		{"metadata-uid", "PreparationProjectIdentityMismatch", func(c *Config) { c.Document.Metadata.UID = "foreign-project" }},
		{"metadata-generation", "PreparationProjectIdentityMismatch", func(c *Config) { c.Document.Metadata.Generation = 99 }},
		{"selection", "PreparationSelectionUnsupported", func(c *Config) { c.Component = "absent" }},
		{"multi-component", "PreparationSelectionUnsupported", func(c *Config) {
			other := c.Document.Spec.Components["backend"]
			other.ServiceName = nil
			c.Document.Spec.Components["another"] = other
		}},
		{"worker-false-presence", "PreparationCapabilityUnsupported", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.Worker = &project.Worker{Enabled: false}
			c.Document.Spec.Components["backend"] = v
		}},
		{"proxy-disabled-presence", "PreparationCapabilityUnsupported", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.TemporalProxy = &project.TemporalProxy{Enabled: false}
			c.Document.Spec.Components["backend"] = v
		}},
		{"isolated-disabled-presence", "PreparationCapabilityUnsupported", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.IsolatedExecutor = pointer(false)
			c.Document.Spec.Components["backend"] = v
		}},
		{"temporal-disabled-presence", "PreparationCapabilityUnsupported", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.TemporalAccess = pointer(false)
			c.Document.Spec.Components["backend"] = v
		}},
		{"autoscaling-disabled-presence", "PreparationCapabilityUnsupported", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.Autoscaling = &project.Autoscaling{Enabled: pointer(false)}
			c.Document.Spec.Components["backend"] = v
		}},
		{"valueFrom", "PreparationEnvironmentUnsupported", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.Env[0].Value = nil
			v.Env[0].ValueFrom = &project.EnvValueFrom{InputRef: "platform.rolloutVersion"}
			c.Document.Spec.Components["backend"] = v
		}},
		{"portless", "PreparationServiceRequired", func(c *Config) {
			v := c.Document.Spec.Components["backend"]
			v.Ports = nil
			c.Document.Spec.Components["backend"] = v
		}},
		{"mutable-image", "PreparationImageBindingInvalid", func(c *Config) { c.Images["app"] = "example.invalid/image:git-abcdef1" }},
		{"credential-image", "PreparationImageBindingInvalid", func(c *Config) {
			c.Images["app"] = "user:password@example.invalid/image@sha256:" + strings.Repeat("a", 64)
		}},
		{"foreign-build", "PreparationImageBindingInvalid", func(c *Config) { c.Images["foreign"] = "example.invalid/image@sha256:" + strings.Repeat("a", 64) }},
		{"missing-selected-image", "PreparationImageBindingMissing", func(c *Config) {
			delete(c.Images, "app")
			c.Images["unused"] = "example.invalid/image@sha256:" + strings.Repeat("a", 64)
		}},
		{"platform-catalog", "PreparationCapabilityUnsupported", func(c *Config) {
			c.Document.Metadata.Annotations = map[string]string{"registry.tmprl-demo.cloud/catalog": "true"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { c := config(t); tc.change(&c); _, err := New(c); code(t, err, tc.want) })
	}
	// Unsupported authored fields are rejected before omitempty/defaulting can
	// erase forbidden empty presence. The project boundary owns that rule.
	c := config(t)
	v := c.Document.Spec.Components["backend"]
	v.Serverless = &project.Serverless{Provider: "aws-lambda"}
	v.Secrets = []project.DNSLabel{}
	c.Document.Spec.Components["backend"] = v
	_, err := New(c)
	code(t, err, "PreparationProjectInvalid")
}

func TestWrongNamespaceAndPairCannotBeSelected(t *testing.T) {
	c := config(t)
	p := plan(t, c)
	for _, tc := range []struct {
		name  string
		bound func() ownership.NamespacedContext
	}{
		{"zero", func() ownership.NamespacedContext { return ownership.NamespacedContext{} }},
		{"attempt", func() ownership.NamespacedContext { return syntheticNamespace(t, p, c.Authority, "other-attempt") }},
		{"cluster", func() ownership.NamespacedContext {
			return syntheticNamespace(t, p, authority(t, "cpfixture", "foreign-cluster", "accepted-run"), c.AttemptID)
		}},
		{"run-owner", func() ownership.NamespacedContext {
			return syntheticNamespace(t, p, authority(t, "cpfixture", "owned-test-cluster", "foreign-run"), c.AttemptID)
		}},
		{"naming", func() ownership.NamespacedContext {
			return syntheticNamespace(t, p, authority(t, "cpother", "owned-test-cluster", "accepted-run"), c.AttemptID)
		}},
		{"revision", func() ownership.NamespacedContext {
			other := config(t)
			other.Document.Spec.Source.Repo = "https://github.com/temporal-sa/other"
			return syntheticNamespace(t, plan(t, other), other.Authority, other.AttemptID)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := p.Render(tc.bound()); code(t, err, "PreparationNamespaceMismatch") })
	}
	bound := syntheticNamespace(t, p, c.Authority, c.AttemptID)
	pair, err := p.Render(bound)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(render.DesiredObject, render.DesiredObject)
	}{
		{"selector", func(d, s render.DesiredObject) { s.Object.(*corev1.Service).Spec.Selector["foreign"] = "value" }},
		{"port", func(d, s render.DesiredObject) { s.Object.(*corev1.Service).Spec.Ports[0].TargetPort.IntVal++ }},
		{"node-port", func(d, s render.DesiredObject) { s.Object.(*corev1.Service).Spec.Ports[0].NodePort = 30000 }},
		{"secret-env", func(d, s render.DesiredObject) {
			d.Object.(*appsv1.Deployment).Spec.Template.Spec.Containers[0].Env[0].ValueFrom = &corev1.EnvVarSource{}
		}},
		{"sidecar", func(d, s render.DesiredObject) {
			pod := &d.Object.(*appsv1.Deployment).Spec.Template.Spec
			pod.Containers = append(pod.Containers, pod.Containers[0])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := pair.Deployment()
			s, _ := pair.Service()
			sa, _ := pair.ServiceAccount()
			tc.change(d, s)
			code(t, p.validatePair(bound, d, s, sa), "PreparationPairMismatch")
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*corev1.ServiceAccount)
	}{
		{"automount", func(o *corev1.ServiceAccount) { *o.AutomountServiceAccountToken = true }},
		{"secret", func(o *corev1.ServiceAccount) { o.Secrets = []corev1.ObjectReference{{Name: "private-secret"}} }},
		{"pull-secret", func(o *corev1.ServiceAccount) {
			o.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "private-pull"}}
		}},
		{"parent-marker", func(o *corev1.ServiceAccount) { o.Annotations[render.ParentUIDAnnotation] = "foreign" }},
	} {
		t.Run("service-account-"+tc.name, func(t *testing.T) {
			d, _ := pair.Deployment()
			s, _ := pair.Service()
			sa, _ := pair.ServiceAccount()
			tc.change(sa.Object.(*corev1.ServiceAccount))
			code(t, p.validatePair(bound, d, s, sa), "PreparationPairMismatch")
		})
	}
	d, _ := pair.Deployment()
	s, _ := pair.Service()
	sa, _ := pair.ServiceAccount()
	_, err = p.pairFromBundle(bound, render.Bundle{Objects: []render.DesiredObject{d, s, sa, sa}})
	code(t, err, "PreparationPairMismatch")
	_, err = p.pairFromBundle(bound, render.Bundle{Objects: []render.DesiredObject{d, s}})
	code(t, err, "PreparationPairMismatch")
}

func TestPrivatePlanFormattingSerializationAndMissingInput(t *testing.T) {
	c := config(t)
	p := plan(t, c)
	pair, err := p.Render(syntheticNamespace(t, p, c.Authority, c.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{p, &p, pair, &pair} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%p"} {
			printed := fmt.Sprintf(format, value)
			for _, forbidden := range []string{privateMarker, "private-candidate-source", "private-candidate-image", "PRIVATE_VALUE"} {
				if strings.Contains(printed, forbidden) {
					t.Fatal("private preparation formatting leaked payload")
				}
			}
		}
		if _, err := json.Marshal(value); err == nil || strings.Contains(err.Error(), privateMarker) {
			t.Fatal("private preparation accepted automatic JSON serialization")
		}
	}
	view, err := json.Marshal(p.View())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{privateMarker, "private-candidate-source", "private-candidate-image", "PRIVATE_VALUE", "https://", "@sha256:"} {
		if strings.Contains(string(view), forbidden) {
			t.Fatal("safe view leaked execution payload or locator")
		}
	}
	if p.View().Promotion != PromotionNotAuthorized || p.View().OperationID != c.AttemptID {
		t.Fatal("safe view lost explicit scope/accepted attempt")
	}
	var absent Plan
	_, err = absent.Desired()
	code(t, err, "PreparationPlanMissing")
	_, err = absent.Namespace()
	code(t, err, "PreparationPlanMissing")
	_, err = absent.Render(ownership.NamespacedContext{})
	code(t, err, "PreparationPlanMissing")
	if absent.View().Promotion != PromotionNotAuthorized || (Pair{}).View().Promotion != PromotionNotAuthorized {
		t.Fatal("zero plan/pair invented promotion authority")
	}
}
