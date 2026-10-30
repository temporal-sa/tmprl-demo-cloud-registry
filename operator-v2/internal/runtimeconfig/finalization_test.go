package runtimeconfig

import (
	"context"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

func finalizationFacts() journal.Envelope {
	return journal.Envelope{
		Subject: rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project-one"},
		Attempts: []journal.Attempt{
			{OperationID: "historical", State: journal.Consumed, NamespaceUID: "old-native-uid", CanonicalAbsent: true, DrainReason: journal.DrainCandidateFailed},
			{OperationID: "final-attempt", State: journal.Consumed, NamespaceUID: "final-native-uid", CanonicalAbsent: true, DrainReason: journal.DrainProjectFinalization},
		},
		Projection: &diagnostics.OperationProjection{Identity: diagnostics.Identity{Environment: "lab", Installation: "test", ProjectUID: "project-one"}, State: diagnostics.Finalizing, Recovery: []diagnostics.RecoveryOutcome{{ID: retirementRecoveryID("final-attempt"), Step: "namespace-cleanup", State: diagnostics.OutcomeCompleted}}},
	}
}

func TestRuntimeFinalizationRequiresAllKnownNamespaceCleanupFacts(t *testing.T) {
	if err := runtimeFinalizationReady(finalizationFacts()); err != nil {
		t.Fatal("acknowledged known namespace cleanup refused", err)
	}
	for name, change := range map[string]func(*journal.Envelope){
		"old attempt still observed": func(e *journal.Envelope) { e.Attempts[0].State = journal.Observed },
		"old UID lacks absence":      func(e *journal.Envelope) { e.Attempts[0].CanonicalAbsent = false },
		"final UID lacks absence":    func(e *journal.Envelope) { e.Attempts[1].CanonicalAbsent = false },
		"drain unfinished":           func(e *journal.Envelope) { e.Attempts[1].State = journal.Draining },
		"unresolved pending":         func(e *journal.Envelope) { e.Writes = []journal.Write{{State: journal.Pending}} },
		"unresolved unknown":         func(e *journal.Envelope) { e.Writes = []journal.Write{{State: journal.Unknown}} },
		"no finalization request":    func(e *journal.Envelope) { e.Attempts[1].DrainReason = journal.DrainSuperseded },
		"no attempts":                func(e *journal.Envelope) { e.Attempts = nil },
		"missing recovery":           func(e *journal.Envelope) { e.Projection.Recovery = nil },
		"wrong recovery operation":   func(e *journal.Envelope) { e.Projection.Recovery[0].ID = retirementRecoveryID("historical") },
		"wrong recovery step":        func(e *journal.Envelope) { e.Projection.Recovery[0].Step = "other-cleanup" },
		"retained recovery":          func(e *journal.Envelope) { e.Projection.Recovery[0].State = diagnostics.OutcomeRetained },
		"retained resource": func(e *journal.Envelope) {
			e.Projection.Recovery[0].RetainedResources = []diagnostics.ResourceRef{{Kind: "Namespace", ID: "still-held"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := finalizationFacts()
			change(&e)
			requireCode(t, runtimeFinalizationReady(e), Blocked)
			// Repeated completion cannot hide a later unresolved or inconsistent
			// retained fact merely because the projection already says Finalized.
			e.Projection.State = diagnostics.Finalized
			requireCode(t, runtimeFinalizationReady(e), Blocked)
		})
	}
	e := finalizationFacts()
	e.Attempts[1].NamespaceUID, e.Attempts[1].CanonicalAbsent = "", false
	if err := runtimeFinalizationReady(e); err != nil {
		t.Fatal("known non-dispatched consumed allocation needs no invented UID/absence", err)
	}
	e.Projection.Identity.ProjectUID = "foreign"
	requireCode(t, runtimeFinalizationReady(e), SubjectMismatch)
	for _, state := range []diagnostics.State{diagnostics.Pending, diagnostics.Running, diagnostics.Failed, diagnostics.Cancelled} {
		e = finalizationFacts()
		e.Projection.State = state
		requireCode(t, runtimeFinalizationReady(e), Invalid)
	}
}

func TestFinalizationBlocksNewNormalWorkAfterConsumptionWithoutBlockingInspection(t *testing.T) {
	for _, state := range []journal.AttemptState{journal.Draining, journal.Consumed} {
		w := &Worker{snapshot: journal.Snapshot{Envelope: finalizationFacts()}}
		w.snapshot.Envelope.Attempts[1].State = state
		requireCode(t, w.normalWorkAllowed(), Blocked)
		if state == journal.Consumed {
			if err := w.normalObservationAllowed(); err != nil {
				t.Fatal("consumed finalization blocked read-only inspection", err)
			}
		}
		w.snapshot.Envelope.Attempts[1].DrainReason = journal.DrainSuperseded
		if state == journal.Consumed {
			if err := w.normalWorkAllowed(); err != nil {
				t.Fatal("ordinary consumed supersession blocked next accepted attempt", err)
			}
		}
	}
}

func TestRuntimeFinalizationRejectsMissingWorkerAndInconsistentRequest(t *testing.T) {
	for _, w := range []*Worker{nil, {}} {
		requireCode(t, w.CompleteRuntimeFinalization(context.Background()), Invalid)
		_, err := w.BeginRetirement(context.Background(), RetirementRequest{Reason: journal.DrainProjectFinalization, Projection: diagnostics.OperationProjection{State: diagnostics.Running}})
		requireCode(t, err, Invalid)
	}
}
