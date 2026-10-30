package runtimeconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func requireCode(t *testing.T, err error, code Code) {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

func TestRecoveryHashesActualBodyInsteadOfCopiedFingerprintAnnotation(t *testing.T) {
	intent := ownership.NamespacedIntent{Intent: ownership.Intent{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "project", Lifetime: ownership.Disposable, RunID: "run-one"}, Revision: "revision-one", OperationID: "provider-one"}, Parent: ownership.NamespaceParent{Name: "owned-namespace", UID: "canonical-parent"}}
	writer := ownership.Writer{Owner: intent.Intent.Owner, Epoch: 1}
	annotations := kube.CreationAnnotations(intent.Intent, writer)
	desired := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "platform-project-metadata", Namespace: intent.Parent.Name, Labels: map[string]string{"test": "rendered"}, Annotations: map[string]string{kube.OwnerAnnotation: annotations[kube.OwnerAnnotation], kube.RevisionAnnotation: intent.Intent.Revision, kube.OperationAnnotation: intent.Intent.OperationID, ownership.ParentNamespaceUIDAnnotation: intent.Parent.UID}}, Data: map[string]string{"project.json": "actual desired body"}}
	fingerprint, err := kube.ConfigMapFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	actual := desired.DeepCopy()
	actual.UID = "canonical-child"
	actual.ResourceVersion = "rv-two"
	actual.CreationTimestamp = metav1.Now()
	for key, value := range annotations {
		actual.Annotations[key] = value
	}
	actual.Annotations[kube.FingerprintAnnotation] = fingerprint
	if !renderedShape(desired, intent, writer) || !bodyMatchesStored(kube.ConfigMapSnapshot{Object: actual}, fingerprint) {
		t.Fatal("canonical server metadata did not roundtrip to original renderer fingerprint")
	}
	actual.Data["project.json"] = "tampered same UID and copied fingerprint"
	if bodyMatchesStored(kube.ConfigMapSnapshot{Object: actual}, fingerprint) {
		t.Fatal("copied fingerprint authorized different actual body")
	}
	delete(desired.Annotations, kube.OwnerAnnotation)
	if renderedShape(desired, intent, writer) {
		t.Fatal("ambiguous pre-stamp renderer metadata accepted")
	}
}
func TestCheckpointRequiresCompleteStrictBoundedTrustedHandoff(t *testing.T) {
	for _, data := range [][]byte{nil, []byte(`{}`), []byte(`{"schema":1,"secret":"private"}`), make([]byte, maxCheckpointBytes+1)} {
		_, err := ParseCheckpoint(data)
		requireCode(t, err, Invalid)
	}
	handoff, _ := json.Marshal(map[string]any{"schema": 1, "subject": rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}, "version": journal.Version{UID: "cell-uid", ResourceVersion: "rv-one"}, "sequence": 3, "digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "admission": journal.Admission{Generation: 1, SessionID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", State: journal.AdmissionReady, HandoffID: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}})
	value := Checkpoint{checkpointValue{Schema: 2, ProjectOwner: ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "project", Lifetime: ownership.Disposable, RunID: "test-run"}, Identity: kubejournal.Identity{Subject: rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}, CellUID: "cell-uid"}, Handoff: handoff}}
	encoded, err := value.JSON()
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := ParseCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := recovered.JSON(); !bytes.Equal(data, encoded) {
		t.Fatal("trusted handoff changed during serialization")
	}
	_, err = ParseCheckpoint(append(encoded, []byte(` {}`)...))
	requireCode(t, err, Invalid)
	value.value.Schema = 1
	_, err = value.JSON()
	requireCode(t, err, Invalid)
}

func TestConsumedOrChangedAttemptCannotBecomeFreshNamespaceCreation(t *testing.T) {
	attempt := journal.Attempt{Number: 1, OperationID: "accepted-one", Revision: "revision-one", DesiredDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", NamespaceName: "owned-attempt-one", NamespaceUID: "canonical-parent-uid", State: journal.Consumed, DrainReason: journal.DrainProjectFinalization, CanonicalAbsent: true}
	requireCode(t, admitAttempt(attempt, attempt.Revision, attempt.DesiredDigest, attempt.NamespaceName), Consumed)
	requireCode(t, admitAttempt(attempt, attempt.Revision, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", attempt.NamespaceName), Changed)
	attempt.State = journal.Observed
	attempt.DrainReason, attempt.CanonicalAbsent = "", false
	if err := admitAttempt(attempt, attempt.Revision, attempt.DesiredDigest, attempt.NamespaceName); err != nil {
		t.Fatal(err)
	}
	attempt.State = journal.Draining
	requireCode(t, admitAttempt(attempt, attempt.Revision, attempt.DesiredDigest, attempt.NamespaceName), Blocked)
	attempt.State = journal.Reserved
	attempt.NamespaceUID = ""
	requireCode(t, admitAttempt(attempt, attempt.Revision, attempt.DesiredDigest, attempt.NamespaceName), Rejected)
}

func TestDrainingBlocksNormalWorkAcrossAllAcceptedAttempts(t *testing.T) {
	w := &Worker{snapshot: journal.Snapshot{Envelope: journal.Envelope{Attempts: []journal.Attempt{{OperationID: "old", State: journal.Draining}, {OperationID: "new", State: journal.Observed}}}}}
	requireCode(t, w.normalWorkAllowed(), Blocked)
	w.snapshot.Envelope.Attempts[0].State = journal.Consumed
	if err := w.normalWorkAllowed(); err != nil {
		t.Fatal(err)
	}
}

func TestUnimplementedJournalKindCannotBeSilentlyAdmitted(t *testing.T) {
	for _, kind := range []string{"Deployment", "Service", "Secret", "ClusterRole"} {
		worker := &Worker{snapshot: journal.Snapshot{Envelope: journal.Envelope{Writes: []journal.Write{{State: journal.Committed, Preconditions: journal.Preconditions{Kind: kind}}}}}}
		requireCode(t, worker.preflight(context.Background()), Invalid)
	}
}
