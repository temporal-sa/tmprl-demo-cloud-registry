//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	corev1 "k8s.io/api/core/v1"
)

func retirementReasonNamespace(t *testing.T, name, uid string, absent bool) {
	t.Helper()
	response, err := client.Get(local.Server + "/api/v1/namespaces/" + name)
	if err != nil {
		t.Fatal("RetirementReasonNamespaceReadFailed")
	}
	defer response.Body.Close()
	if absent {
		if response.StatusCode != http.StatusNotFound {
			t.Fatal("RetirementReasonCanonicalAbsenceUnproven")
		}
		return
	}
	var object corev1.Namespace
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&object) != nil || string(object.UID) != uid || object.DeletionTimestamp != nil {
		t.Fatal("RetirementReasonLiveNamespaceIdentityUnproven")
	}
	own := false
	for _, finalizer := range object.Finalizers {
		if finalizer == provider.NamespaceLifecycleFinalizer {
			own = true
		}
	}
	if !own {
		t.Fatal("RetirementReasonLifecycleFinalizerMissing")
	}
}

func retirementReasonCanonicalReceipt(t *testing.T, snapshot journal.Snapshot, attempt string, receipt runtimeconfig.Receipt) {
	t.Helper()
	if receipt.Subject != snapshot.Envelope.Subject || receipt.AttemptID != attempt || receipt.Record.Record.Ref.ProviderID == "" || receipt.ResourceVersion == "" || receipt.Record.Parent.UID == "" {
		t.Fatal("RetirementReasonCanonicalReceiptMissing")
	}
	parent, child := false, false
	for _, write := range snapshot.Envelope.Writes {
		if write.OperationID != attempt || write.State != journal.Committed || write.Preconditions.Action != "Create" {
			continue
		}
		if write.Preconditions.Kind == "Namespace" && write.ResultUID == receipt.Record.Parent.UID && write.ResultVersion != "" {
			parent = true
		}
		if write.Preconditions.Kind == "ConfigMap" && write.Preconditions.ParentUID == receipt.Record.Parent.UID && write.ResultUID == receipt.Record.Record.Ref.ProviderID && write.ResultVersion == receipt.ResourceVersion {
			child = true
		}
	}
	if !parent || !child {
		t.Fatal("RetirementReasonReceiptBeforeCanonicalJournalACK")
	}
}

func retirementReasonResolved(t *testing.T, snapshot journal.Snapshot, input retirementReasonInput, proof *retirementReasonWitness) {
	t.Helper()
	if snapshot.Version.UID != input.Pin.Cell.Record.Ref.ProviderID || snapshot.Envelope.Subject != input.Pin.Subject || snapshot.Envelope.Admission == nil || snapshot.Envelope.Admission.State != journal.AdmissionReady {
		t.Fatal("RetirementReasonStableControlOrCleanHandoffLost")
	}
	cleanup := int64(0)
	committedDelete, committedFinalize := false, false
	for _, write := range snapshot.Envelope.Writes {
		if write.State == journal.Pending || write.State == journal.Unknown {
			t.Fatal("RetirementReasonUnresolvedWriteBypassed")
		}
		if write.Preconditions.Kind == "Namespace" && (write.Preconditions.Action == "Delete" || write.Preconditions.Action == "Finalize") {
			cleanup++
			if write.OperationID != input.Attempt || write.Preconditions.UID != snapshot.Envelope.Attempts[0].NamespaceUID || write.Preconditions.TargetRevision != input.Original.Revision {
				t.Fatal("RetirementReasonCleanupRetargeted")
			}
			if write.State == journal.Committed {
				if write.Preconditions.Action == "Delete" {
					committedDelete = write.ConfirmedAbsent
				} else {
					committedFinalize = true
				}
			}
		}
	}
	if cleanup != proof.cleanup.Load() || cleanup < 2 || !committedDelete || !committedFinalize {
		t.Fatal("RetirementReasonCleanupRPCWithoutExactDurableTicket")
	}
	projection := snapshot.Envelope.Projection
	// After supersession replacement, the exact completed recovery belongs to
	// the immutable archived operation, not the current replacement projection.
	if len(snapshot.Envelope.Attempts) > 1 && len(snapshot.Envelope.Archive) == 1 {
		projection = &snapshot.Envelope.Archive[0]
	}
	completed := false
	if projection != nil {
		for _, outcome := range projection.Recovery {
			if outcome.ID == retirementReasonRecoveryID(input.Attempt) && outcome.Step == "namespace-cleanup" && outcome.State == diagnostics.OutcomeCompleted && len(outcome.RetainedResources) == 0 {
				completed = true
			}
		}
	}
	if !completed {
		t.Fatal("RetirementReasonExactCompletedRecoveryMissing")
	}
}

func retirementReasonFixture(t *testing.T, reason journal.DrainReason) {
	t.Helper()
	if phase := os.Getenv("OPERATOR_RETIREMENT_REASON_PHASE"); phase != "" {
		retirementReasonChild(t, phase)
		return
	}
	TestOwnedAPIAndNetworkBoundary(t)
	suffix := "supersession"
	if reason == journal.DrainProjectFinalization {
		suffix = "finalization"
	}
	pin := runtimeControlPin(t, "reason-"+suffix)
	original := desiredRuntime(pin)
	replacement := desiredRuntime(pin)
	replacement.Generation = original.Generation + 1
	replacement.Revision = "git-reason-replacement"
	replacement.ReconcileKey = "accepted-reason-replacement-v2"
	input := retirementReasonInput{Pin: pin, Reason: reason, Original: original, Replacement: replacement, Attempt: runtimeAttempt(pin), ReplacementAttempt: "replacement-" + runtimeAttempt(pin), RawDocument: append([]byte(nil), []byte(runtimeProject)...)}
	endpoint, ca, proof := retirementReasonProxy(t, input)
	input.Endpoint, input.ProxyCA = endpoint, ca
	first := runRetirementReasonChild(t, input, "drain")
	input.Checkpoint = first.Checkpoint
	_, repository := retirementConfig(t, pin, "", nil)
	draining := retirementReasonLoad(t, repository)
	retirementReasonCanonicalReceipt(t, draining, input.Attempt, first.Receipt)
	state, step := retirementReasonState(reason)
	if proof.cleanup.Load() != 0 || proof.namespaces.Load() != 1 || proof.configmaps.Load() != 1 || len(draining.Envelope.Attempts) != 1 || first.Receipt.Record.Parent.UID == "" || first.Receipt.Record.Parent.UID == pin.Parent.Ref.ProviderID {
		t.Fatal("RetirementReasonDrainHandoffOrCanonicalApplyUnproven")
	}
	attempt := draining.Envelope.Attempts[0]
	if attempt.State != journal.Draining || attempt.DrainReason != reason || attempt.NamespaceUID != first.Receipt.Record.Parent.UID || draining.Envelope.Projection == nil || draining.Envelope.Projection.State != state || draining.Envelope.Projection.Step != step || draining.Envelope.Projection.Failure != nil || draining.Envelope.Projection.Availability.RouteState != diagnostics.RouteUnknown || draining.Envelope.Projection.Retry.Status != diagnostics.RetryUnknown {
		t.Fatal("RetirementReasonDrainingFactsLost")
	}
	retirementReasonNamespace(t, attempt.NamespaceName, attempt.NamespaceUID, false)
	// First command has exited/reaped before the cleanup-only successor starts.
	second := runRetirementReasonChild(t, input, "retire")
	input.Checkpoint = second.Checkpoint
	retired := retirementReasonLoad(t, repository)
	if len(retired.Envelope.Attempts) != 1 || retired.Envelope.Attempts[0].State != journal.Consumed || !retired.Envelope.Attempts[0].CanonicalAbsent || retired.Envelope.Attempts[0].DrainReason != reason || retired.Envelope.Attempts[0].NamespaceUID != attempt.NamespaceUID {
		t.Fatal("RetirementReasonConsumptionIdentityLost")
	}
	retirementReasonNamespace(t, attempt.NamespaceName, attempt.NamespaceUID, true)
	retirementReasonResolved(t, retired, input, proof)
	retiredProjection := retirementReasonProjection(t, retired)
	if proof.namespaces.Load() != 1 || proof.configmaps.Load() != 1 {
		t.Fatal("RetirementReasonCleanupRepostedManagedObjects")
	}
	if reason == journal.DrainProjectFinalization && (retired.Envelope.Projection.State != diagnostics.Finalized || retired.Envelope.Projection.Step != "runtime-finalized") {
		t.Fatal("RetirementReasonFinalStatusNotAcknowledged")
	}
	// Replacement/inspection starts only after the cleanup worker exits/reaps.
	third := runRetirementReasonChild(t, input, "after")
	input.Checkpoint = third.Checkpoint
	final := retirementReasonLoad(t, repository)
	retirementReasonResolved(t, final, input, proof)
	if final.Envelope.Attempts[0] != retired.Envelope.Attempts[0] || len(final.Envelope.Writes) < len(retired.Envelope.Writes) || !reflect.DeepEqual(final.Envelope.Writes[:len(retired.Envelope.Writes)], retired.Envelope.Writes) {
		t.Fatal("RetirementReasonConsumedHistoryChanged")
	}
	if reason == journal.DrainSuperseded {
		retirementReasonCanonicalReceipt(t, final, input.ReplacementAttempt, third.Receipt)
		if len(final.Envelope.Attempts) != 2 || len(final.Envelope.Archive) != 1 || final.Envelope.Attempts[1].OperationID != input.ReplacementAttempt || final.Envelope.Attempts[1].NamespaceName == attempt.NamespaceName || final.Envelope.Attempts[1].NamespaceUID == attempt.NamespaceUID || final.Envelope.Attempts[1].NamespaceUID != third.Receipt.Record.Parent.UID || third.Receipt.Record.Record.Ref.ProviderID == first.Receipt.Record.Record.Ref.ProviderID || proof.namespaces.Load() != 2 || proof.configmaps.Load() != 2 {
			t.Fatal("RetirementReasonReplacementReusedConsumedIdentity")
		}
		archived, err := final.Envelope.Archive[0].JSON()
		if err != nil || !bytes.Equal(archived, retiredProjection) || final.Envelope.Archive[0].Failure != nil || final.Envelope.Archive[0].State != diagnostics.Cancelled {
			t.Fatal("RetirementReasonSupersessionArchiveRewritten")
		}
		p := final.Envelope.Projection
		if p == nil || p.Identity.OperationID != input.ReplacementAttempt || p.Identity.ReconcileKey != replacement.ReconcileKey || p.Identity.Generation != replacement.Generation || p.Failure != nil {
			t.Fatal("RetirementReasonCurrentAndArchivedIntentConflated")
		}
	} else {
		if len(final.Envelope.Attempts) != 1 || len(final.Envelope.Archive) != 0 || !bytes.Equal(retiredProjection, retirementReasonProjection(t, final)) || proof.namespaces.Load() != 1 || proof.configmaps.Load() != 1 {
			t.Fatal("RetirementReasonFinalizedSubjectReopened")
		}
	}
	// A current direct read confirms the stable independent control cell remains
	// usable after candidate absence; this is not child-process/payload drain.
	_, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal("RetirementReasonControlJournalLostAfterCandidateTeardown")
	}
}

func TestRuntimeSupersessionRetiresCandidateAcrossCleanProcessHandoff(t *testing.T) {
	retirementReasonFixture(t, journal.DrainSuperseded)
}
func TestRuntimeProjectFinalizationRemainsClosedAcrossCleanProcessHandoff(t *testing.T) {
	retirementReasonFixture(t, journal.DrainProjectFinalization)
}
