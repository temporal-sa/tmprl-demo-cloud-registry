package kubernetes

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func localTemporal(b Bindings, mode string) TemporalBinding {
	secret := SecretBinding{Namespace: b.Namespace.NamespaceName(), NamespaceUID: b.Namespace.Parent().UID, Owner: b.Namespace.Owner(), Name: "temporal-local", Keys: StandardTemporalKeys()}
	runtime := secret
	runtime.Name = "temporal-runtime"
	runtime.Keys = map[string]string{"temporal.apiKey": "TEMPORAL_API_KEY", "temporal.tls": "TEMPORAL_TLS"}
	return TemporalBinding{Mode: mode, Secret: secret, RuntimeSecret: &runtime, Address: "localhost:7233", CloudNamespace: "local-temporal"}
}
func crd(t *testing.T, b Bundle, kind, name string) *unstructured.Unstructured {
	t.Helper()
	for _, value := range b.Objects {
		if value.Object.GetName() == name && value.Object.GetObjectKind().GroupVersionKind().Kind == kind {
			u, ok := value.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatal("custom resource is not a Kubernetes object")
			}
			return u
		}
	}
	t.Fatalf("%s/%s not rendered", kind, name)
	return nil
}

func TestVersionedWorkerConnectionAndPerVersionAutoscaling(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	c := d.Spec.Components["backend"]
	c.Replicas = nil
	c.Worker = &project.Worker{Enabled: true, Versioning: &project.WorkerVersioning{Enabled: true}}
	empty := map[string]json.RawMessage{}
	c.Worker.Versioning.Rollout = &project.WorkerRollout{Gate: &project.WorkerRolloutGate{WorkflowType: "LocalGate", Input: &empty}}
	c.Env = project.EnvList{{Name: "TEMPORAL_TASK_QUEUE", Value: ptr("banking-jobs")}}
	d.Spec.Components["backend"] = c
	b.Temporal = map[string]TemporalBinding{"primary": localTemporal(b, "apiKey")}
	b.WorkerController = &WorkerControllerBinding{DeploymentNames: map[string]string{"backend": "banking-worker-local"}}
	b.AllowAutoscaling = true
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	worker := crd(t, bundle, "WorkerDeployment", "banking-worker-local")
	if _, present, _ := unstructured.NestedFieldNoCopy(worker.Object, "spec", "replicas"); present {
		t.Fatal("fixed replicas fight per-version autoscaling")
	}
	if namespace, _, _ := unstructured.NestedString(worker.Object, "spec", "workerOptions", "temporalNamespace"); namespace != "local-temporal" {
		t.Fatal("worker namespace was inferred instead of bound")
	}
	if delay, _, _ := unstructured.NestedString(worker.Object, "spec", "sunset", "deleteDelay"); delay != "24h" {
		t.Fatal("worker sunset default changed")
	}
	if input, present, _ := unstructured.NestedMap(worker.Object, "spec", "rollout", "gate", "input"); !present || len(input) != 0 {
		t.Fatal("explicit empty gate input was dropped")
	}
	connection := crd(t, bundle, "Connection", "temporal")
	if endpoint, _, _ := unstructured.NestedString(connection.Object, "spec", "hostPort"); endpoint != "localhost:7233" {
		t.Fatal("worker endpoint was not bound")
	}
	if secret, _, _ := unstructured.NestedString(connection.Object, "spec", "apiKeySecretRef", "name"); secret != "temporal-local" {
		t.Fatal("controller API key reference changed")
	}
	template := crd(t, bundle, "WorkerResourceTemplate", "banking-worker-local")
	if count, _, _ := unstructured.NestedInt64(template.Object, "spec", "template", "spec", "maxReplicaCount"); count != 10 {
		t.Fatal("autoscaling defaults/types lost")
	}
	triggers, _, _ := unstructured.NestedSlice(template.Object, "spec", "template", "spec", "triggers")
	meta := triggers[0].(map[string]any)["metadata"].(map[string]any)
	if meta["workerDeploymentName"] != "" || meta["workerDeploymentBuildId"] != "" || meta["taskQueue"] != "banking-jobs" {
		t.Fatal("per-version identity placeholders were not preserved")
	}
	for _, value := range bundle.Objects {
		if value.Object.GetObjectKind().GroupVersionKind().Kind == "Deployment" && value.Object.GetName() == "backend" {
			t.Fatal("versioned worker also emitted an ordinary Deployment")
		}
	}
}

func TestRepositoryBankingFixtureRetainsBackendDNSAndDefaults(t *testing.T) {
	data, err := os.ReadFile("../../../../projects/demo/banking-money-transfer.yaml")
	if err != nil {
		t.Fatal(err)
	}
	d, err := project.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	b, _, _ := bindings(t)
	b.AllowAutoscaling = true
	b.Images["worker"] = "example.invalid/worker:git-abcdef1"
	b.Temporal = map[string]TemporalBinding{"primary": localTemporal(b, "apiKey")}
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	backend := object[*appsv1.Deployment](t, bundle, "backend")
	if backend.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name != "temporal-local" {
		t.Fatal("repository backend lost scoped Temporal access")
	}
	worker := object[*appsv1.Deployment](t, bundle, "worker")
	if worker.Spec.Template.Spec.Containers[0].Env[0].Value != "http://backend:8000" {
		t.Fatal("repository worker's backend DNS stopped resolving")
	}
	scaled := crd(t, bundle, "ScaledObject", "worker")
	triggers, _, _ := unstructured.NestedSlice(scaled.Object, "spec", "triggers")
	meta := triggers[0].(map[string]any)["metadata"].(map[string]any)
	if meta["taskQueue"] != "MoneyTransfer" || worker.Spec.Replicas != nil {
		t.Fatal("repository crashable-base task queue no longer drives implicit scaling")
	}
	if !bundle.DeferredPromotion {
		t.Fatal("repository ingress was published in candidate stage")
	}
}

func TestWorkerGateInputRequiresSeparateNoncredentialPolicy(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	c := d.Spec.Components["backend"]
	c.Worker = &project.Worker{Enabled: true, Versioning: &project.WorkerVersioning{Enabled: true, Rollout: &project.WorkerRollout{Gate: &project.WorkerRolloutGate{WorkflowType: "LocalGate", InputFrom: &project.WorkerRolloutGateInputFrom{SecretKeyRef: &project.LocalObjectKeyRef{Name: "temporal-runtime", Key: "TEMPORAL_API_KEY"}}}}}}
	d.Spec.Components["backend"] = c
	b.Temporal = map[string]TemporalBinding{"primary": localTemporal(b, "apiKey")}
	b.WorkerController = &WorkerControllerBinding{DeploymentNames: map[string]string{"backend": "banking-worker"}}
	if _, err := RenderCandidate(d, b); err == nil {
		t.Fatal("ordinary runtime credential binding implicitly authorized a gate input")
	}
	policy := GateInputBinding{Owner: b.Namespace.Owner(), NamespaceUID: b.Namespace.Parent().UID, Component: "backend", Kind: "Secret", Name: "temporal-runtime", Key: "TEMPORAL_API_KEY", Policy: NonCredentialGateConfig}
	b.GateInputs = map[string]GateInputBinding{"backend": policy}
	if _, err := RenderCandidate(d, b); err == nil {
		t.Fatal("noncredential assertion reclassified a known runtime credential Secret")
	}
	c.Worker.Versioning.Rollout.Gate.InputFrom.SecretKeyRef = &project.LocalObjectKeyRef{Name: "gate-config", Key: "payload"}
	d.Spec.Components["backend"] = c
	policy.Name, policy.Key = "gate-config", "payload"
	b.GateInputs["backend"] = policy
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.RequiredConfigInputs) != 1 || bundle.RequiredConfigInputs[0].Kind != "Secret" || bundle.RequiredConfigInputs[0].NamespaceUID != b.Namespace.Parent().UID {
		t.Fatal("gate configuration source policy/preflight reference lost")
	}
	found := false
	for _, source := range bundle.RequiredSecrets {
		if source.Name == "gate-config" && len(source.Keys) == 1 && source.Keys[0] == "payload" {
			found = true
		}
	}
	if !found {
		t.Fatal("approved configuration Secret was absent from preflight requirements")
	}
	policy.NamespaceUID = "recreated-namespace"
	b.GateInputs["backend"] = policy
	if _, err = RenderCandidate(d, b); err == nil {
		t.Fatal("gate policy survived namespace incarnation change")
	}
	policy.NamespaceUID = b.Namespace.Parent().UID
	policy.Component = "frontend"
	b.GateInputs["backend"] = policy
	if _, err = RenderCandidate(d, b); err == nil {
		t.Fatal("one component's gate policy authorized another component")
	}
	c.Worker.Versioning.Rollout.Gate.InputFrom = &project.WorkerRolloutGateInputFrom{ConfigMapKeyRef: &project.LocalObjectKeyRef{Name: "gate-settings", Key: "config.json"}}
	d.Spec.Components["backend"] = c
	policy.Component, policy.Kind, policy.Name, policy.Key = "backend", "ConfigMap", "gate-settings", "config.json"
	b.GateInputs["backend"] = policy
	bundle, err = RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.RequiredConfigInputs) != 1 || bundle.RequiredConfigInputs[0].Kind != "ConfigMap" {
		t.Fatal("ConfigMap gate dependency was not recorded")
	}
	literal := map[string]json.RawMessage{"api_key": json.RawMessage("\"literal-secret-sentinel\"")}
	c.Worker.Versioning.Rollout.Gate.InputFrom = nil
	c.Worker.Versioning.Rollout.Gate.Input = &literal
	d.Spec.Components["backend"] = c
	if _, err = RenderCandidate(d, b); err == nil || strings.Contains(err.Error(), "literal-secret-sentinel") {
		t.Fatal("credential literal entered gate arguments or rejection text")
	}
}

func TestImplicitAutoscalingPreservesOmittedReplicaIntent(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	c := d.Spec.Components["backend"]
	c.Replicas = nil
	c.Worker = &project.Worker{Enabled: true}
	c.Env = project.EnvList{{Name: "TEMPORAL_TASK_QUEUE", Value: ptr("jobs")}}
	d.Spec.Components["backend"] = c
	b.Temporal = map[string]TemporalBinding{"primary": localTemporal(b, "apiKey")}
	b.AllowAutoscaling = true
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if object[*appsv1.Deployment](t, bundle, "backend").Spec.Replicas != nil {
		t.Fatal("normalization invented a fixed count for implicit autoscaling")
	}
	crd(t, bundle, "ScaledObject", "backend")
	c.Replicas = ptr(0)
	d.Spec.Components["backend"] = c
	bundle, err = RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if *object[*appsv1.Deployment](t, bundle, "backend").Spec.Replicas != 0 {
		t.Fatal("explicit scale-to-zero was lost")
	}
	for _, value := range bundle.Objects {
		if value.Object.GetObjectKind().GroupVersionKind().Kind == "ScaledObject" && value.Object.GetName() == "backend" {
			t.Fatal("implicit autoscaling overrode explicit replicas")
		}
	}
	c.Replicas = nil
	c.Autoscaling = &project.Autoscaling{TaskQueue: ptr("jobs"), CooldownPeriod: ptr(2147483648)}
	d.Spec.Components["backend"] = c
	if _, err = RenderCandidate(d, b); err == nil {
		t.Fatal("autoscaling int32 overflow was accepted")
	}
}

func TestProxyKeepsGeneratedCredentialsOutOfApplicationContainer(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	c := d.Spec.Components["backend"]
	c.Worker = &project.Worker{Enabled: true}
	c.TemporalProxy = &project.TemporalProxy{Enabled: true}
	d.Spec.Components["backend"] = c
	b.Temporal = map[string]TemporalBinding{"primary": localTemporal(b, "apiKey")}
	b.ProxyImage = "example.invalid/proxy:git-abcdef1"
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	pod := object[*appsv1.Deployment](t, bundle, "backend").Spec.Template.Spec
	if len(pod.Containers) != 2 || len(pod.Containers[0].EnvFrom) != 0 || pod.Containers[1].EnvFrom[0].SecretRef.Name != "temporal-local" {
		t.Fatal("proxy credentials crossed into application env")
	}
	env := map[string]string{}
	for _, value := range pod.Containers[0].Env {
		env[value.Name] = value.Value
	}
	if env["TEMPORAL_ADDRESS"] != "127.0.0.1:7233" || env["TEMPORAL_NAMESPACE"] != "default" || env["TEMPORAL_TLS"] != "false" {
		t.Fatal("proxy local connection contract changed")
	}
}

func TestMTLSFilesAreExplicitReadOnlySecretProjections(t *testing.T) {
	d := document(t)
	b, _, _ := bindings(t)
	d.Spec.Temporal = &project.Temporal{Auth: &project.TemporalAuth{Mode: ptr("mtls")}}
	c := d.Spec.Components["backend"]
	c.Worker = &project.Worker{Enabled: true}
	c.TemporalMtls = &project.TemporalMtls{CertFiles: &project.TemporalMtlsCertFiles{MountPath: ptr("/run/banking/temporal")}}
	d.Spec.Components["backend"] = c
	b.Temporal = map[string]TemporalBinding{"primary": localTemporal(b, "mtls")}
	bundle, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	pod := object[*appsv1.Deployment](t, bundle, "backend").Spec.Template.Spec
	if len(pod.Volumes) != 1 || pod.Volumes[0].Secret.SecretName != "temporal-local" || len(pod.Volumes[0].Secret.Items) != 3 || !pod.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatal("mTLS file projection lost key/read-only binding")
	}
	c.TemporalMtls.CertFiles.Paths = &project.TemporalMtlsCertFilePaths{CACert: ptr("/same/file"), ClientCert: ptr("/same/file"), ClientKey: ptr("/same/file")}
	d.Spec.Components["backend"] = c
	if _, err = RenderCandidate(d, b); err == nil {
		t.Fatal("overlapping certificate file paths were accepted")
	}
}

func routingAuthority(t *testing.T, context ownership.Context) ownership.NamespacedContext {
	t.Helper()
	intent, err := context.Intent(ownership.Kubernetes, "namespace", "routing", "control-plane", "routing-operation", "routing-spec", 63)
	if err != nil {
		t.Fatal(err)
	}
	observed := ownership.Observation{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: "routing-namespace-uid"}, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1, Version: "rv-1"}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	writer := ownership.Writer{ID: "render-writer", Owner: context.Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	record, err := context.BindCreated(intent, observed, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := context.BindNamespace(record, observed)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func TestTrafficPromotionIsSeparateAndStripsSpoofedIdentity(t *testing.T) {
	d := document(t)
	b, context, _ := bindings(t)
	d.Spec.Ingress = &project.Ingress{Subdomain: "banking", Routes: []project.IngressRoute{{Component: "frontend", Port: "http"}, {Component: "backend", Port: "http", PathPrefix: ptr("/webhook"), Public: ptr(true), PassHostHeader: ptr(false)}}}
	candidate, err := RenderCandidate(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if !candidate.DeferredPromotion {
		t.Fatal("candidate phase lost declared traffic intent")
	}
	for _, value := range candidate.Objects {
		if value.Object.GetObjectKind().GroupVersionKind().Group == "traefik.io" {
			t.Fatal("candidate rendering published traffic before promotion")
		}
	}
	promotion := PromotionBindings{Namespace: routingAuthority(t, context), Backend: b.Namespace, Revision: b.Revision, OperationID: b.OperationID, BaseDomain: "test.invalid", AuthURL: "http://auth.local.invalid/_auth/verify", UseDefaultTLSStore: true, AllowPublicIngress: true, AllowCrossNamespaceBackend: true}
	promotion.Routing = &RoutingCapability{NamespaceUID: promotion.Namespace.Parent().UID, BaseDomain: promotion.BaseDomain}
	bundle, err := RenderPromotion(d, promotion)
	if err != nil {
		t.Fatal(err)
	}
	route := crd(t, bundle, "IngressRoute", "banking")
	routes, _, _ := unstructured.NestedSlice(route.Object, "spec", "routes")
	protected := routes[0].(map[string]any)
	public := routes[1].(map[string]any)
	if len(protected["middlewares"].([]any)) != 2 || len(public["middlewares"].([]any)) != 1 {
		t.Fatal("public/protected forward-auth distinction changed")
	}
	if !strings.Contains(protected["match"].(string), "banking.test.invalid") {
		t.Fatal("host was not bound to explicit test domain")
	}
	service := public["services"].([]any)[0].(map[string]any)
	if service["name"] != "backend" || service["namespace"] != b.Namespace.NamespaceName() || service["passHostHeader"] != false {
		t.Fatal("promotion rewrote logical backend or pass-host setting")
	}
	http := crd(t, bundle, "IngressRoute", "banking-http")
	if _, present, _ := unstructured.NestedFieldNoCopy(http.Object, "spec", "tls"); present {
		t.Fatal("HTTP redirect route incorrectly carries TLS")
	}
	promotion.AllowPublicIngress = false
	if _, err = RenderPromotion(d, promotion); err == nil {
		t.Fatal("public webhook exception lacked explicit approval")
	}
	promotion.AllowPublicIngress = true
	promotion.AllowCrossNamespaceBackend = false
	if _, err = RenderPromotion(d, promotion); err == nil {
		t.Fatal("cross-namespace backend lacked explicit installation approval")
	}
	promotion.AllowCrossNamespaceBackend = true
	wrong := promotion
	wrong.Namespace = b.Namespace
	if _, err = RenderPromotion(d, wrong); err == nil {
		t.Fatal("promotion accepted an ephemeral candidate routing namespace")
	}
	wrong = promotion
	wrong.Routing = &RoutingCapability{NamespaceUID: "different-namespace-uid", BaseDomain: promotion.BaseDomain}
	if _, err = RenderPromotion(d, wrong); err == nil {
		t.Fatal("routing capability outlived its namespace incarnation")
	}
	promotion.AllowCrossNamespaceBackend = true
	d.Spec.Ingress.Routes[1].PathPrefix = ptr("/webhook" + string(rune(96)) + ") || Host(" + string(rune(96)) + "other.invalid")
	if _, err = RenderPromotion(d, promotion); err == nil {
		t.Fatal("rule delimiter injection was accepted")
	}
}
