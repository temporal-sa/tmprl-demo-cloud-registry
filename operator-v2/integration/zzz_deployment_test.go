//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/deploymentjournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const deploymentProject = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: deployment-fixture
spec:
  source:
    repo: https://github.com/temporal-sa/fixture
  builds:
    backend:
      context: .
      dockerfile: Dockerfile
  secrets:
    harmless:
      requiredProperties: [VALUE]
  components:
    backend:
      build: backend
      args: [-revision=deployment-fixture]
      secrets: [harmless]
      env:
        - name: APP_LITERAL
          value: PRIVATE_AUTHORED_FIXTURE_ONLY
        - name: APP_SECRET
          valueFrom:
            inputRef: secrets.harmless.VALUE
      resources:
        requests:
          cpu: 10m
          memory: 16Mi
        limits:
          cpu: 100m
          memory: 64Mi
      ports:
        - containerPort: 8080
`

type deploymentAPI struct {
	bound   *provider.NamespaceBinding
	plan    provider.NamespaceBootstrap
	gate    *admission.Gate
	guard   *deploymentjournal.Guard
	desired *appsv1.Deployment
	intent  ownership.NamespacedIntent
	write   journal.Write
}

func deploymentGateCommit(t *testing.T, gate *admission.Gate, next journal.Envelope) journal.Snapshot {
	t.Helper()
	s, err := gate.Commit(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newDeploymentAPI(t *testing.T, suffix string, missingSA bool) *deploymentAPI {
	t.Helper()
	if !boundaryVerified {
		TestOwnedAPIAndNetworkBoundary(t)
	}
	if !strings.Contains(local.ImageDigest, "@sha256:") {
		t.Fatal("explicit owned loaded manifest digest required; use the updated local harness")
	}
	plan := managedNamespacePlan(t, "deployment-"+suffix)
	rendered, err := renderer.RenderNamespace(plan.Context, renderer.NamespaceRequest{Revision: plan.Intent.Revision, OperationID: plan.Intent.OperationID, Fingerprint: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	plan.Intent, plan.Object = rendered.Intent, rendered.Object
	adapter, cellConfig := controlCell(t, "deployment-"+suffix, plan.Context.Owner().ProjectUID)
	repository := openRepository(t, cellConfig)
	claim, err := admission.BeginInitial(context.Background(), admission.Config{Mode: admission.LocalCleanHandoff, Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := claim.Confirm(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s, err := gate.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next := s.Envelope
	next.Attempts = []journal.Attempt{{Number: 1, OperationID: plan.Intent.OperationID, NamespaceName: plan.Object.Name, State: journal.Reserved, Revision: plan.Intent.Revision, DesiredDigest: strings.Repeat("b", 64)}}
	s = deploymentGateCommit(t, gate, next)
	next = s.Envelope
	next.Writes = []journal.Write{{ID: "create-namespace", OperationID: plan.Intent.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: plan.Object.Name, Fingerprint: plan.Intent.SpecFingerprint, TargetRevision: plan.Intent.Revision, ProviderOperationID: plan.Intent.OperationID}}}
	s = deploymentGateCommit(t, gate, next)
	bound := bootstrapManaged(t, adapter, plan)
	parent, err := bound.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next = s.Envelope
	next.Attempts = append([]journal.Attempt(nil), next.Attempts...)
	next.Writes = append([]journal.Write(nil), next.Writes...)
	next.Attempts[0].State, next.Attempts[0].NamespaceUID = journal.Observed, bound.Parent().UID
	next.Writes[0].State, next.Writes[0].ResultUID, next.Writes[0].ResultVersion = journal.Committed, bound.Parent().UID, parent.Version
	deploymentGateCommit(t, gate, next)
	guard, err := deploymentjournal.New(context.Background(), deploymentjournal.Config{Gate: gate, Owner: plan.Context.Owner(), Parent: bound.Parent(), WriterID: plan.Writer.ID})
	if err != nil {
		t.Fatal(err)
	}
	deploymentPrerequisites(t, bound)
	document, err := project.Decode([]byte(deploymentProject))
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := renderer.RenderCandidate(document, renderer.Bindings{Namespace: bound.Context(), Revision: plan.Intent.Revision, OperationID: plan.Intent.OperationID, Images: map[string]string{"backend": local.ImageDigest}, ProjectSecrets: map[string]renderer.SecretBinding{"harmless": {Namespace: bound.Parent().Name, NamespaceUID: bound.Parent().UID, Owner: plan.Context.Owner(), Name: "harmless", Keys: map[string]string{"VALUE": "VALUE"}}}})
	if err != nil {
		t.Fatal(err)
	}
	var desired *appsv1.Deployment
	for _, o := range bundle.Objects {
		if d, ok := o.Object.(*appsv1.Deployment); ok && d.Name == "backend" {
			desired = d.DeepCopy()
		}
	}
	if desired == nil {
		t.Fatal("renderer did not produce ordinary Deployment")
	}
	// The renderer produces env literals, Secret refs/EnvFrom, resources/TCP/DNS
	// defaults. These extra harmless typed fixture refs exercise provider-supported
	// ConfigMap and downward-API defaulting; they are not authoring feature claims.
	c := &desired.Spec.Template.Spec.Containers[0]
	c.ImagePullPolicy = corev1.PullNever
	c.Env = append(c.Env, corev1.EnvVar{Name: "APP_CONFIG", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "harmless"}, Key: "value"}}}, corev1.EnvVar{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}})
	c.EnvFrom = append(c.EnvFrom, corev1.EnvFromSource{Prefix: "CONFIG_", ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "harmless"}}})
	if missingSA {
		desired.Spec.Template.Spec.ServiceAccountName = "deliberately-missing"
	}
	fingerprint, err := provider.DeploymentFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := bound.Context().Intent("deployment", desired.Name, plan.Intent.Revision, plan.Intent.OperationID, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	write := journal.Write{ID: "create-deployment", OperationID: plan.Intent.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create", Name: desired.Name, ParentUID: bound.Parent().UID, Fingerprint: fingerprint, TargetRevision: plan.Intent.Revision, ProviderOperationID: intent.Intent.OperationID}}
	return &deploymentAPI{bound, plan, gate, guard, desired, intent, write}
}

// Independently controlled fixture bootstrap: harmless configuration only, in
// the exact freshly created owned namespace, with canonical parent UID owner
// references. These are not generic provider Apply capabilities or real secrets.
func deploymentPrerequisites(t *testing.T, bound *provider.NamespaceBinding) {
	t.Helper()
	parent := bound.Parent()
	metadata := func(name string) object {
		return object{"name": name, "namespace": parent.Name, "annotations": object{ownership.ParentNamespaceUIDAnnotation: parent.UID}, "ownerReferences": []object{{"apiVersion": "v1", "kind": "Namespace", "name": parent.Name, "uid": parent.UID}}}
	}
	request(t, "POST", "/api/v1/namespaces/"+parent.Name+"/serviceaccounts", object{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata("backend"), "automountServiceAccountToken": false}, 201)
	request(t, "POST", "/api/v1/namespaces/"+parent.Name+"/secrets", object{"apiVersion": "v1", "kind": "Secret", "metadata": metadata("harmless"), "type": "Opaque", "stringData": object{"VALUE": "fixture-only-noncredential"}}, 201)
	request(t, "POST", "/api/v1/namespaces/"+parent.Name+"/configmaps", object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata("harmless"), "data": object{"value": "fixture-only-configuration"}}, 201)
}

func (f *deploymentAPI) create(t *testing.T) ownership.NamespacedRecord {
	t.Helper()
	s, err := f.gate.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next := s.Envelope
	next.Writes = append(append([]journal.Write(nil), next.Writes...), f.write)
	deploymentGateCommit(t, f.gate, next)
	record, err := f.bound.CreateDeployment(context.Background(), provider.DeploymentCreate{Desired: f.desired, Intent: f.intent, Writer: f.plan.Writer, WriteID: f.write.ID, Verifier: f.guard})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.bound.ObserveDeployment(context.Background(), provider.DeploymentRead{Desired: f.desired, Intent: f.intent, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	s, err = f.gate.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next = s.Envelope
	next.Writes = append([]journal.Write(nil), next.Writes...)
	last := len(next.Writes) - 1
	next.Writes[last].State, next.Writes[last].ResultUID, next.Writes[last].ResultVersion = journal.Committed, record.Record.Ref.ProviderID, snapshot.View().ResourceVersion
	deploymentGateCommit(t, f.gate, next)
	return record
}

func TestTypedDeploymentRealJournalRenderingReadinessAndSpecTamper(t *testing.T) {
	f := newDeploymentAPI(t, "ready", false)
	_, err := f.bound.CreateDeployment(context.Background(), provider.DeploymentCreate{Desired: f.desired, Intent: f.intent, Writer: f.plan.Writer, WriteID: f.write.ID, Verifier: f.guard})
	requireProviderCode(t, err, provider.DeploymentIntentUnverified)
	request(t, "GET", "/apis/apps/v1/namespaces/"+f.bound.Parent().Name+"/deployments/backend", nil, 404)
	record := f.create(t)
	read := provider.DeploymentRead{Desired: f.desired, Intent: f.intent, Record: record}
	var snapshot provider.DeploymentSnapshot
	wait(t, func() bool {
		var err error
		snapshot, err = f.bound.ObserveDeployment(context.Background(), read)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.View().Readiness.State == "Failed" {
			t.Fatalf("owned healthy fixture failed: %s", snapshot.View().Readiness.Code)
		}
		return snapshot.View().Readiness.State == "Ready"
	})
	view := snapshot.View()
	if view.UID != record.Record.Ref.ProviderID || view.Readiness.Generation != view.Readiness.ObservedGeneration || view.Readiness.Available != 1 {
		t.Fatal("readiness identity/freshness differs")
	}
	data, _ := json.Marshal(snapshot)
	if strings.Contains(string(data), "PRIVATE_AUTHORED") || strings.Contains(string(data), "fixture-only-noncredential") {
		t.Fatal("raw app config or secret values escaped safe snapshot")
	}
	actual := snapshot.ObjectForProvider()
	pod := actual.Spec.Template.Spec
	if pod.Containers[0].Env[len(pod.Containers[0].Env)-1].ValueFrom.FieldRef.APIVersion != "v1" || pod.RestartPolicy != corev1.RestartPolicyAlways || pod.DeprecatedServiceAccount != "backend" || pod.Containers[0].ReadinessProbe.TimeoutSeconds != 1 {
		t.Fatal("documented nested defaults unverified")
	}
	// The real controller may report resolved historical collision counts; unit
	// tests cover that semantic without falsifying the live controller's status.
	path := "/apis/apps/v1/namespaces/" + f.bound.Parent().Name + "/deployments/backend"
	object := request(t, "GET", path, nil, 200)
	containers := field(field(field(object, "spec"), "template"), "spec")["containers"].([]any)
	first := containers[0].(map[string]any)
	first["args"] = []string{"-revision=tampered"}
	request(t, "PUT", path, object, 200)
	_, err = f.bound.ObserveDeployment(context.Background(), read)
	requireProviderCode(t, err, provider.DeploymentSpecMismatch)
	// Reusing the committed request does not regain pending-write authority.
	_, err = f.bound.CreateDeployment(context.Background(), provider.DeploymentCreate{Desired: f.desired, Intent: f.intent, Writer: f.plan.Writer, WriteID: f.write.ID, Verifier: f.guard})
	requireProviderCode(t, err, provider.DeploymentIntentUnverified)
}

func TestTypedDeploymentRealReplicaFailureHasSafeOriginalCause(t *testing.T) {
	f := newDeploymentAPI(t, "replica-failure", true)
	record := f.create(t)
	read := provider.DeploymentRead{Desired: f.desired, Intent: f.intent, Record: record}
	var snapshot provider.DeploymentSnapshot
	wait(t, func() bool {
		var err error
		snapshot, err = f.bound.ObserveDeployment(context.Background(), read)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot.View().Readiness.State == "Failed"
	})
	if snapshot.View().Readiness.Code != "DeploymentReplicaFailure" {
		t.Fatalf("unexpected safe cause: %s", snapshot.View().Readiness.Code)
	}
	data, _ := json.Marshal(snapshot)
	if strings.Contains(string(data), "forbidden") || strings.Contains(string(data), "serviceaccount") || strings.Contains(string(data), "PRIVATE_AUTHORED") {
		t.Fatal("raw controller/container detail escaped public view")
	}
	current := request(t, "GET", "/apis/apps/v1/namespaces/"+f.bound.Parent().Name+"/deployments/backend", nil, 200)
	if text(metadata(current), "uid") != record.Record.Ref.ProviderID {
		t.Fatal("failed Deployment was replaced or deleted")
	}
}
