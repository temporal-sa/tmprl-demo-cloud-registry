//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/servicejournal"
	corev1 "k8s.io/api/core/v1"
)

const serviceProject = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: service-fixture
spec:
  source:
    repo: https://github.com/temporal-sa/fixture
  builds:
    backend:
      context: .
      dockerfile: Dockerfile
  components:
    backend:
      build: backend
      serviceName: logical-backend
      ports:
        - name: web
          containerPort: 8080
          servicePort: 80
        - name: metrics
          containerPort: 9090
          servicePort: 9090
`

type serviceAPI struct {
	bound   *provider.NamespaceBinding
	plan    provider.NamespaceBootstrap
	gate    *admission.Gate
	guard   *servicejournal.Guard
	desired *corev1.Service
	intent  ownership.NamespacedIntent
	write   journal.Write
}

func newServiceAPI(t *testing.T) *serviceAPI {
	t.Helper()
	if !boundaryVerified {
		TestOwnedAPIAndNetworkBoundary(t)
	}
	plan := managedNamespacePlan(t, "service-allocation")
	rendered, err := renderer.RenderNamespace(plan.Context, renderer.NamespaceRequest{Revision: plan.Intent.Revision, OperationID: plan.Intent.OperationID, Fingerprint: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	plan.Intent, plan.Object = rendered.Intent, rendered.Object
	adapter, cellConfig := controlCell(t, "service-allocation", plan.Context.Owner().ProjectUID)
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
	guard, err := servicejournal.New(context.Background(), servicejournal.Config{Gate: gate, Owner: plan.Context.Owner(), Parent: bound.Parent(), WriterID: plan.Writer.ID})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := project.Decode([]byte(serviceProject))
	if err != nil {
		t.Fatal(err)
	}
	// The image is only a render binding. No workload is created; this fixture
	// proves Service allocation/receipt integrity, never backend/traffic readiness.
	bundle, err := renderer.RenderCandidate(doc, renderer.Bindings{Namespace: bound.Context(), Revision: plan.Intent.Revision, OperationID: plan.Intent.OperationID, Images: map[string]string{"backend": local.ImageDigest}})
	if err != nil {
		t.Fatal(err)
	}
	var desired *corev1.Service
	var intent ownership.NamespacedIntent
	for _, o := range bundle.Objects {
		if service, ok := o.Object.(*corev1.Service); ok && service.Name == "logical-backend" {
			desired = service.DeepCopy()
			intent = o.Intent
		}
	}
	if desired == nil {
		t.Fatal("renderer did not preserve logical Service name")
	}
	fp, err := provider.ServiceFingerprint(desired)
	if err != nil || fp != intent.Intent.SpecFingerprint {
		t.Fatal("rendered fingerprint differs", err)
	}
	write := journal.Write{ID: "create-service", OperationID: plan.Intent.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Service", Action: "Create", Name: desired.Name, ParentUID: bound.Parent().UID, Fingerprint: fp, TargetRevision: plan.Intent.Revision, ProviderOperationID: intent.Intent.OperationID}}
	if write.Input != (journal.InputReference{}) {
		t.Fatal("provider-only proof must not imply retained Service body")
	}
	return &serviceAPI{bound: bound, plan: plan, gate: gate, guard: guard, desired: desired, intent: intent, write: write}
}

// Restore metadata ONLY from the independently acknowledged journal and pinned
// owned binding. The original desired body remains an explicit fixture input;
// this is not retained-private-input storage or process-restart proof.
func (f *serviceAPI) restoredRecord(t *testing.T) provider.ServiceRecord {
	t.Helper()
	s, err := f.gate.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range s.Envelope.Writes {
		if w.ID != f.write.ID {
			continue
		}
		p := w.Preconditions
		if w.State != journal.Committed || w.Input != (journal.InputReference{}) || p.Kind != "Service" || p.Action != "Create" || w.OperationID != f.plan.Intent.OperationID || p.ParentUID != f.bound.Parent().UID || w.ResultUID == "" || w.ResultVersion == "" || w.ResultSpecContract != journal.ServiceSpecContract || len(w.ResultSpecDigest) != 64 {
			t.Fatal("durable Service receipt incomplete")
		}
		intent, err := f.bound.Context().Intent("service", p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
		if err != nil {
			t.Fatal(err)
		}
		resource := ownership.NamespacedRecord{Parent: f.bound.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Intent.Scope, Purpose: "service", Name: p.Name, ProviderID: w.ResultUID}, Owner: f.bound.Context().Owner(), CreationOperationID: p.ProviderOperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: p.TargetRevision, SpecFingerprint: p.Fingerprint, WriterEpoch: w.WriterEpoch}}
		return provider.ServiceRecord{Resource: resource, SpecContract: w.ResultSpecContract, AllocatedSpecDigest: w.ResultSpecDigest}
	}
	t.Fatal("committed Service receipt missing")
	return provider.ServiceRecord{}
}

func TestTypedServiceRealJournalAllocationReceiptAndSpecTamper(t *testing.T) {
	f := newServiceAPI(t)
	ctx := context.Background()
	create := provider.ServiceCreate{Desired: f.desired, Intent: f.intent, Writer: f.plan.Writer, WriteID: f.write.ID, Verifier: f.guard}
	_, err := f.bound.CreateService(ctx, create)
	requireProviderCode(t, err, provider.ServiceIntentUnverified)
	path := "/api/v1/namespaces/" + f.bound.Parent().Name + "/services/" + f.desired.Name
	request(t, "GET", path, nil, 404)
	s, err := f.gate.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := s.Envelope
	next.Writes = append(append([]journal.Write(nil), next.Writes...), f.write)
	deploymentGateCommit(t, f.gate, next)
	receipt, err := f.bound.CreateService(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.bound.ObserveService(ctx, provider.ServiceRead{Desired: f.desired, Intent: f.intent, Record: receipt})
	if err != nil {
		t.Fatal(err)
	}
	actual := snapshot.ObjectForProvider()
	if actual.Name != "logical-backend" || len(actual.Spec.Ports) != 2 || actual.Spec.Type != corev1.ServiceTypeClusterIP || actual.Spec.SessionAffinity != corev1.ServiceAffinityNone || actual.Spec.InternalTrafficPolicy == nil || *actual.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster || actual.Spec.IPFamilyPolicy == nil || *actual.Spec.IPFamilyPolicy != corev1.IPFamilyPolicySingleStack || len(actual.Spec.ClusterIPs) != 1 || actual.Spec.ClusterIP != actual.Spec.ClusterIPs[0] || len(actual.Spec.IPFamilies) != 1 {
		t.Fatal("actual Service/default/allocation contract differs")
	}
	if snapshot.View().BackendReadiness != "NotEvaluated" || !snapshot.View().ObservedAllocated {
		t.Fatal("Service confused with ready backend")
	}
	data, _ := json.Marshal(snapshot)
	if strings.Contains(string(data), actual.Spec.ClusterIP) || strings.Contains(string(data), "selector") || strings.Contains(string(data), "ownerReferences") {
		t.Fatal("private Service body escaped safe view")
	}
	s, err = f.gate.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next = s.Envelope
	next.Writes = append([]journal.Write(nil), next.Writes...)
	last := len(next.Writes) - 1
	next.Writes[last].State, next.Writes[last].ResultUID, next.Writes[last].ResultVersion = journal.Committed, receipt.Resource.Record.Ref.ProviderID, snapshot.View().ResourceVersion
	next.Writes[last].ResultSpecDigest, next.Writes[last].ResultSpecContract = receipt.AllocatedSpecDigest, receipt.SpecContract
	deploymentGateCommit(t, f.gate, next)
	restored := f.restoredRecord(t)
	read := provider.ServiceRead{Desired: f.desired.DeepCopy(), Intent: f.intent, Record: restored}
	snapshot, err = f.bound.ObserveService(ctx, read)
	if err != nil || snapshot.View().UID != restored.Resource.Record.Ref.ProviderID {
		t.Fatal("durable receipt observation failed", err)
	}
	_, err = f.bound.CreateService(ctx, create)
	requireProviderCode(t, err, provider.ServiceIntentUnverified)
	missing := read
	missing.Record.AllocatedSpecDigest = ""
	_, err = f.bound.ObserveService(ctx, missing)
	requireProviderCode(t, err, provider.ServiceIntentUnverified)
	wrongUID := read
	wrongUID.Record.Resource.Record.Ref.ProviderID = "foreign-uid"
	_, err = f.bound.ObserveService(ctx, wrongUID)
	requireProviderCode(t, err, provider.OwnershipRejected)
	// Independently controlled fixture tampering uses the live canonical RV.
	// Same UID/copied fingerprint must not authorize changed selector/target port.
	obj := request(t, "GET", path, nil, 200)
	originalSelector := field(obj, "spec")["selector"]
	field(obj, "spec")["selector"] = object{"fixture-tampered": "true"}
	request(t, "PUT", path, obj, 200)
	_, err = f.bound.ObserveService(ctx, read)
	requireProviderCode(t, err, provider.ServiceSpecMismatch)
	obj = request(t, "GET", path, nil, 200)
	field(obj, "spec")["selector"] = originalSelector
	ports := field(obj, "spec")["ports"].([]any)
	ports[0].(map[string]any)["targetPort"] = 8081
	request(t, "PUT", path, obj, 200)
	_, err = f.bound.ObserveService(ctx, read)
	requireProviderCode(t, err, provider.ServiceSpecMismatch)
	current := request(t, "GET", path, nil, 200)
	if text(metadata(current), "uid") != restored.Resource.Record.Ref.ProviderID {
		t.Fatal("tamper fixture replaced UID")
	}
}
