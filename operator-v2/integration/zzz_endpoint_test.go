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
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/servicejournal"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func newEndpointServiceAPI(t *testing.T) *serviceAPI {
	t.Helper()
	if !boundaryVerified {
		TestOwnedAPIAndNetworkBoundary(t)
	}
	plan := managedNamespacePlan(t, "endpoint-publication")
	rendered, err := renderer.RenderNamespace(plan.Context, renderer.NamespaceRequest{Revision: plan.Intent.Revision, OperationID: plan.Intent.OperationID, Fingerprint: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	plan.Intent, plan.Object = rendered.Intent, rendered.Object
	adapter, cellConfig := controlCell(t, "endpoint-publication", plan.Context.Owner().ProjectUID)
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
	// The immutable image also binds the controlled workload created later in
	// this Endpoint fixture. Its observation proves current ancestry/publication,
	// never Pod payload equivalence, creator origin or network readiness.
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

func TestTypedEndpointPublicationRealCanonicalAncestryAndForeignSlice(t *testing.T) {
	f := newEndpointServiceAPI(t)
	ctx := context.Background()
	s, err := f.gate.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := s.Envelope
	next.Writes = append(append([]journal.Write(nil), next.Writes...), f.write)
	deploymentGateCommit(t, f.gate, next)
	serviceReceipt, err := f.bound.CreateService(ctx, provider.ServiceCreate{Desired: f.desired, Intent: f.intent, Writer: f.plan.Writer, WriteID: f.write.ID, Verifier: f.guard})
	if err != nil {
		t.Fatal(err)
	}
	serviceObserved, err := f.bound.ObserveService(ctx, provider.ServiceRead{Desired: f.desired, Intent: f.intent, Record: serviceReceipt})
	if err != nil {
		t.Fatal(err)
	}
	s, err = f.gate.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next = s.Envelope
	next.Writes = append([]journal.Write(nil), next.Writes...)
	last := len(next.Writes) - 1
	next.Writes[last].State, next.Writes[last].ResultUID, next.Writes[last].ResultVersion = journal.Committed, serviceReceipt.Resource.Record.Ref.ProviderID, serviceObserved.View().ResourceVersion
	next.Writes[last].ResultSpecDigest, next.Writes[last].ResultSpecContract = serviceReceipt.AllocatedSpecDigest, serviceReceipt.SpecContract
	deploymentGateCommit(t, f.gate, next)
	// Render the SAME accepted component/revision. Do not create its SA yet: this
	// makes absent publication deterministic without updating the immutable body.
	document, err := project.Decode([]byte(serviceProject))
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := renderer.RenderCandidate(document, renderer.Bindings{Namespace: f.bound.Context(), Revision: f.plan.Intent.Revision, OperationID: f.plan.Intent.OperationID, Images: map[string]string{"backend": local.ImageDigest}})
	if err != nil {
		t.Fatal(err)
	}
	var deployment *appsv1.Deployment
	for _, desired := range bundle.Objects {
		if d, ok := desired.Object.(*appsv1.Deployment); ok && d.Name == "backend" {
			deployment = d.DeepCopy()
		}
	}
	if deployment == nil {
		t.Fatal("component Deployment absent")
	}
	deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullNever
	fingerprint, err := provider.DeploymentFingerprint(deployment)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := f.bound.Context().Intent("deployment", deployment.Name, f.plan.Intent.Revision, f.plan.Intent.OperationID, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := deploymentjournal.New(ctx, deploymentjournal.Config{Gate: f.gate, Owner: f.plan.Context.Owner(), Parent: f.bound.Parent(), WriterID: f.plan.Writer.ID})
	if err != nil {
		t.Fatal(err)
	}
	pending := journal.Write{ID: "create-deployment", OperationID: f.plan.Intent.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create", Name: deployment.Name, ParentUID: f.bound.Parent().UID, Fingerprint: fingerprint, TargetRevision: f.plan.Intent.Revision, ProviderOperationID: intent.Intent.OperationID}}
	s, err = f.gate.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next = s.Envelope
	next.Writes = append(append([]journal.Write(nil), next.Writes...), pending)
	deploymentGateCommit(t, f.gate, next)
	record, err := f.bound.CreateDeployment(ctx, provider.DeploymentCreate{Desired: deployment, Intent: intent, Writer: f.plan.Writer, WriteID: pending.ID, Verifier: guard})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := f.bound.ObserveDeployment(ctx, provider.DeploymentRead{Desired: deployment, Intent: intent, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	s, err = f.gate.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next = s.Envelope
	next.Writes = append([]journal.Write(nil), next.Writes...)
	last = len(next.Writes) - 1
	next.Writes[last].State, next.Writes[last].ResultUID, next.Writes[last].ResultVersion = journal.Committed, record.Record.Ref.ProviderID, observed.View().ResourceVersion
	committed := deploymentGateCommit(t, f.gate, next).Envelope.Writes[last]
	// Restore UID/provenance from the independently acknowledged journal. Original
	// bodies remain explicit fixture inputs, not a retained-input-store proof.
	record = ownership.NamespacedRecord{Parent: f.bound.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Intent.Scope, Purpose: "deployment", Name: committed.Preconditions.Name, ProviderID: committed.ResultUID}, Owner: f.bound.Context().Owner(), CreationOperationID: committed.Preconditions.ProviderOperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: committed.Preconditions.TargetRevision, SpecFingerprint: committed.Preconditions.Fingerprint, WriterEpoch: committed.WriterEpoch}}
	read := provider.EndpointRead{Service: provider.ServiceRead{Desired: f.desired, Intent: f.intent, Record: f.restoredRecord(t)}, Deployment: provider.DeploymentRead{Desired: deployment, Intent: intent, Record: record}}
	snap, err := f.bound.ObserveEndpoints(ctx, read)
	if err != nil || !snap.View().Complete || snap.View().PublicationPresent || snap.View().State != "Waiting" {
		t.Fatalf("missing SA did not give publication wait: %v %+v", err, snap.View())
	}
	parent := f.bound.Parent()
	// Harmless owned fixture bootstrap, not a generic provider apply capability.
	request(t, "POST", "/api/v1/namespaces/"+parent.Name+"/serviceaccounts", object{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": object{"name": "backend", "namespace": parent.Name, "annotations": object{ownership.ParentNamespaceUIDAnnotation: parent.UID}, "ownerReferences": []object{{"apiVersion": "v1", "kind": "Namespace", "name": parent.Name, "uid": parent.UID}}}, "automountServiceAccountToken": false}, 201)
	wait(t, func() bool {
		var e error
		snap, e = f.bound.ObserveEndpoints(ctx, read)
		if e != nil {
			t.Fatal(e)
		}
		return snap.View().Complete && snap.View().PublicationPresent && snap.View().Counts.ReadyPods == 1
	})
	view := snap.View()
	if view.Claim != "current_ancestry_publication" || view.PayloadVerified || view.PromotionAuthorized || view.ServiceUID != read.Service.Record.Resource.Record.Ref.ProviderID || view.DeploymentUID != record.Record.Ref.ProviderID || view.Counts.VerifiedPods != 1 || view.CompletedAt.Before(view.StartedAt) {
		t.Fatal("publication was upgraded into stronger authority")
	}
	data, _ := json.Marshal(snap)
	if strings.Contains(string(data), "addresses") || strings.Contains(string(data), "PodSpec") || strings.Contains(string(data), "conditions") || strings.Contains(string(data), "10.244.") {
		t.Fatal("raw descendant data escaped safe view")
	}
	// A foreign-source Service-labelled slice is relevant even though managed-by
	// is different. Namespace owner keeps the controlled conflict alive for proof;
	// no endpoint addresses are authored and no traffic probe is performed.
	family := serviceObserved.ObjectForProvider().Spec.IPFamilies[0]
	collection := "/apis/discovery.k8s.io/v1/namespaces/" + parent.Name + "/endpointslices"
	conflict := request(t, "POST", collection, object{"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "metadata": object{"name": "controlled-foreign-source", "namespace": parent.Name, "labels": object{"kubernetes.io/service-name": f.desired.Name, "endpointslice.kubernetes.io/managed-by": "fixture-conflict"}, "ownerReferences": []object{{"apiVersion": "v1", "kind": "Namespace", "name": parent.Name, "uid": parent.UID}}}, "addressType": string(family), "ports": []object{{"name": "web", "port": 8080, "protocol": "TCP"}, {"name": "metrics", "port": 9090, "protocol": "TCP"}}, "endpoints": []object{}}, 201)
	snap, err = f.bound.ObserveEndpoints(ctx, read)
	if err != nil || snap.View().Complete || snap.View().PublicationPresent || snap.View().Code != "EndpointSliceUnverified" {
		t.Fatal("foreign relevant slice hidden", err)
	}
	request(t, "DELETE", collection+"/controlled-foreign-source", object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": text(metadata(conflict), "uid"), "resourceVersion": text(metadata(conflict), "resourceVersion")}}, 200)
	wait(t, func() bool {
		snapshot, e := f.bound.ObserveEndpoints(ctx, read)
		if e != nil {
			t.Fatal(e)
		}
		return snapshot.View().Complete && snapshot.View().PublicationPresent
	})
}
