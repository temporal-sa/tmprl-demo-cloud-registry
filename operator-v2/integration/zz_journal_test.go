//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

func controlCell(t *testing.T, suffix string, projectUID ...string) (*provider.Client, kubejournal.Config) {
	t.Helper()
	if !boundaryVerified {
		TestOwnedAPIAndNetworkBoundary(t)
	}
	adapter, _ := typedClient(t)
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: local.RunID, ProjectUID: "control-" + suffix, Lifetime: ownership.Disposable, RunID: local.RunID}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := base.Intent(ownership.Kubernetes, "namespace", "journal-control", "control-v1", "bootstrap-control-"+suffix, "control-namespace-spec", 63)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "control-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	annotations := provider.CreationAnnotations(intent, writer)
	annotations[provider.AttemptAnnotation] = intent.OperationID
	// Independent fixture bootstrap, never candidate renderer or runtime create.
	created := request(t, "POST", "/api/v1/namespaces", object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": intent.Name, "annotations": annotations, "finalizers": []string{provider.NamespaceLifecycleFinalizer}}}, 201)
	parent := ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: text(metadata(created), "uid")}, Owner: base.Owner(), CreationOperationID: intent.OperationID, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1}
	bound, err := adapter.BindNamespace(context.Background(), base, parent, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	subject := rollout.Subject{Environment: "lab", Installation: local.RunID, ProjectUID: "project-" + suffix}
	if len(projectUID) == 1 {
		subject.ProjectUID = projectUID[0]
	}
	initial, err := journal.Initial(subject, 1)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := kubejournal.BootstrapObject(intent.Name, initial)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := provider.ConfigMapFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	cellIntent, err := bound.Context().Intent("configmap", desired.Name, kubejournal.Revision, "bootstrap-cell-"+suffix, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desired, Intent: cellIntent, Writer: writer})
	if err != nil {
		t.Fatal(err)
	}
	return adapter, kubejournal.Config{Namespace: bound, Pinned: pinned, Subject: subject, Writer: func() ownership.Writer { return writer }}
}

func openRepository(t *testing.T, config kubejournal.Config) *journal.Repository {
	t.Helper()
	cell, err := kubejournal.New(config)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := journal.New(cell, config.Subject, config.Pinned.Record.Ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}
func commitJournal(t *testing.T, r *journal.Repository, s journal.Snapshot, next journal.Envelope) journal.Snapshot {
	t.Helper()
	written, err := r.Commit(context.Background(), s, next)
	if err != nil {
		t.Fatal(err)
	}
	return written
}

func TestJournalDurableReloadSurvivesCandidateTeardownAndRejectsStaleCAS(t *testing.T) {
	candidate := managedNamespacePlan(t, "journal-candidate")
	adapter, config := controlCell(t, "persist", candidate.Context.Owner().ProjectUID)
	owner := candidate.Context.Owner()
	if config.Subject != (rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}) {
		t.Fatal("journal subject differs from dispatch project authority")
	}
	repo := openRepository(t, config)
	initial, err := repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderer.RenderNamespace(candidate.Context, renderer.NamespaceRequest{Revision: candidate.Intent.Revision, OperationID: candidate.Intent.OperationID, Fingerprint: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	candidate.Object, candidate.Intent = rendered.Object, rendered.Intent
	next := initial.Envelope
	next.Attempts = []journal.Attempt{{Number: 1, OperationID: candidate.Intent.OperationID, NamespaceName: candidate.Object.Name, State: journal.Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("b", 64)}}
	saved := commitJournal(t, repo, initial, next)
	next = saved.Envelope
	next.Writes = []journal.Write{{ID: "create-runtime", OperationID: candidate.Intent.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: candidate.Object.Name, Fingerprint: candidate.Intent.SpecFingerprint, TargetRevision: candidate.Intent.Revision, ProviderOperationID: candidate.Intent.OperationID}}}
	saved = commitJournal(t, repo, saved, next)
	bound := bootstrapManaged(t, adapter, candidate)
	observed, err := bound.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next = saved.Envelope
	next.Attempts = append([]journal.Attempt(nil), next.Attempts...)
	next.Writes = append([]journal.Write(nil), next.Writes...)
	next.Attempts[0].State, next.Attempts[0].NamespaceUID = journal.Observed, bound.Parent().UID
	next.Writes[0].State, next.Writes[0].ResultUID, next.Writes[0].ResultVersion = journal.Committed, bound.Parent().UID, observed.Version
	saved = commitJournal(t, repo, saved, next)
	stale := initial.Envelope
	stale.Attempts = []journal.Attempt{{Number: 1, OperationID: "different", NamespaceName: "lab-different", State: journal.Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("b", 64)}}
	if _, err = repo.Commit(context.Background(), initial, stale); !errors.Is(err, journal.ErrConflict) {
		t.Fatalf("stale journal CAS was accepted: %v", err)
	}
	// Persist teardown intent before this legacy cleanup fixture; cell is in an
	// independent namespace. Prepared-ticket tests separately prove exact RPC bytes.
	next = saved.Envelope
	next.Attempts = append([]journal.Attempt(nil), next.Attempts...)
	next.Attempts[0].State, next.Attempts[0].DrainReason = journal.Draining, journal.DrainProjectFinalization
	saved = commitJournal(t, repo, saved, next)
	next = saved.Envelope
	next.Writes = append(append([]journal.Write(nil), next.Writes...), journal.Write{ID: "delete-runtime", OperationID: candidate.Intent.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Delete", Name: candidate.Object.Name, UID: bound.Parent().UID, Version: observed.Version, Fingerprint: candidate.Intent.SpecFingerprint, TargetRevision: candidate.Intent.Revision, ProviderOperationID: candidate.Intent.OperationID}})
	saved = commitJournal(t, repo, saved, next)
	finishManaged(t, bound, candidate.Writer, "journal-candidate-teardown")
	next = saved.Envelope
	next.Attempts = append([]journal.Attempt(nil), next.Attempts...)
	next.Writes = append([]journal.Write(nil), next.Writes...)
	next.Attempts[0].State, next.Attempts[0].CanonicalAbsent = journal.Consumed, true
	next.Writes[1].State, next.Writes[1].ConfirmedAbsent = journal.Committed, true
	saved = commitJournal(t, repo, saved, next)
	// New repository/cell instance gets all facts from the actual API. This is
	// persistence evidence, not permission to bypass another client's uncertainty.
	fresh := openRepository(t, config)
	reloaded, err := fresh.Load(context.Background())
	if err != nil || reloaded.Envelope.Digest != saved.Envelope.Digest || reloaded.Envelope.Attempts[0].State != journal.Consumed || reloaded.Envelope.Attempts[0].NamespaceUID != bound.Parent().UID {
		t.Fatalf("journal lost after candidate cleanup: %v", err)
	}
	if config.Namespace.Context().NamespaceName() == bound.Context().NamespaceName() {
		t.Fatal("journal shared candidate lifetime")
	}
}

func TestJournalRefusesReplacementCellWithPerfectCopiedMetadata(t *testing.T) {
	_, config := controlCell(t, "replacement")
	repo := openRepository(t, config)
	path := "/api/v1/namespaces/" + config.Namespace.Context().NamespaceName() + "/configmaps/" + config.Pinned.Record.Ref.Name
	before := request(t, "GET", path, nil, 200)
	request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": config.Pinned.Record.Ref.ProviderID, "resourceVersion": text(metadata(before), "resourceVersion")}}, 200)
	recreated := request(t, "POST", "/api/v1/namespaces/"+config.Namespace.Context().NamespaceName()+"/configmaps", object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": config.Pinned.Record.Ref.Name, "namespace": config.Namespace.Context().NamespaceName(), "annotations": field(metadata(before), "annotations"), "labels": field(metadata(before), "labels"), "ownerReferences": metadata(before)["ownerReferences"]}, "data": field(before, "data")}, 201)
	if text(metadata(recreated), "uid") == config.Pinned.Record.Ref.ProviderID {
		t.Fatal("fixture did not replace cell UID")
	}
	if _, err := repo.Load(context.Background()); !errors.Is(err, journal.ErrMissing) {
		t.Fatalf("same-name journal replacement was adopted: %v", err)
	}
}

func TestJournalRecoversAcceptedUpdateResponseLossWithoutRepeatingWrite(t *testing.T) {
	_, config := controlCell(t, "lost-response")
	path := "/api/v1/namespaces/" + config.Namespace.Context().NamespaceName() + "/configmaps/" + config.Pinned.Record.Ref.Name
	var pauseReads atomic.Bool
	pauseReads.Store(true)
	options, attempts := uncertainWriteProxy(t, "PUT", path, true, &pauseReads)
	adapter, err := provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	// Bind the same independently pinned stable namespace through the fault proxy.
	parent := request(t, "GET", "/api/v1/namespaces/"+config.Namespace.Context().NamespaceName(), nil, 200)
	base, err := ownership.NewContext(ownership.Config{Owner: config.Pinned.Record.Owner, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}})
	if err != nil {
		t.Fatal(err)
	}
	parentRecord := ownership.Record{Ref: ownership.ResourceRef{Scope: ownership.Scope{Provider: ownership.Kubernetes, ClusterID: local.RunID}, Purpose: "namespace", Name: text(metadata(parent), "name"), ProviderID: text(metadata(parent), "uid")}, Owner: base.Owner(), CreationOperationID: field(metadata(parent), "annotations")[provider.CreationOperationAnnotation].(string), CreationIdempotencyKey: field(metadata(parent), "annotations")[provider.CreationKeyAnnotation].(string), Revision: "control-v1", SpecFingerprint: "control-namespace-spec", WriterEpoch: 1}
	bound, err := adapter.BindNamespace(context.Background(), base, parentRecord, parentRecord.CreationOperationID)
	if err != nil {
		t.Fatal(err)
	}
	config.Namespace = bound
	repo := openRepository(t, config)
	initial, err := repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next := initial.Envelope
	next.Attempts = []journal.Attempt{{Number: 1, OperationID: "reserved", NamespaceName: "lab-reserved", State: journal.Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("b", 64)}}
	_, err = repo.Commit(context.Background(), initial, next)
	if !errors.Is(err, journal.ErrUnknown) || len(adapter.Lifecycle(parentRecord.Ref.Name).Unresolved) != 1 {
		t.Fatalf("unconfirmed update was acknowledged: %v", err)
	}
	pauseReads.Store(false)
	saved := commitJournal(t, repo, initial, next)
	if attempts.Load() != 1 || len(adapter.Lifecycle(parentRecord.Ref.Name).Unresolved) != 0 || saved.Envelope.Sequence != 2 {
		t.Fatal("exact committed update was not resolved")
	}
	if _, err = repo.Commit(context.Background(), initial, next); err != nil || attempts.Load() != 1 {
		t.Fatalf("replay duplicated ambiguous update: %v", err)
	}
	next = saved.Envelope
	next.WriterEpoch = 2
	if _, err = repo.Commit(context.Background(), saved, next); err != nil || attempts.Load() != 2 {
		t.Fatalf("resolved local tombstone blocked the next CAS: %v", err)
	}
}
