package rollout

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func intent() (Desired, Observed) {
	subject := Subject{"lab", "installation-a", "project-uid"}
	desired := Desired{Subject: subject, Generation: 2, ReconcileKey: "intent-2", Revision: "candidate-2", RequiredBuilds: []string{"web"}, Images: map[string]string{"web": "example.test/web:git-abcdef1234"}, RequireSmoke: true}
	active := &Revision{ID: "active-1", ReconcileKey: "intent-1", ResourceID: "uid-active", ResourceVersion: "1"}
	observed := Observed{Subject: subject, Generation: 2, ObservationToken: "snapshot-1", Active: active, Route: &Route{Subject: subject, ResourceID: "route-uid", ResourceVersion: "7", Revision: active.ID, ConfirmedAt: time.Unix(100, 0).UTC()}}
	return desired, observed
}

func candidateFor(desired Desired) *Candidate {
	return &Candidate{Revision: Revision{ID: desired.Revision, ReconcileKey: desired.ReconcileKey, ResourceID: "uid-candidate", ResourceVersion: "2"}}
}

func TestRolloutGatesRequireFreshObservations(t *testing.T) {
	desired, observed := intent()
	for _, scenario := range []struct {
		name      string
		candidate *Candidate
		phase     Phase
		action    ActionKind
	}{
		{"create", nil, Deploying, EnsureCandidate},
		{"wait readiness", candidateFor(desired), WaitingForReadiness, ObserveReadiness},
		{"smoke gate", &Candidate{Revision: candidateFor(desired).Revision, Ready: true}, CheckingSmoke, CheckSmoke},
		{"promote", &Candidate{Revision: candidateFor(desired).Revision, Ready: true, Smoke: &SmokeResult{Passed: true}}, Promoting, PromoteCandidate},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			observed.Candidate = scenario.candidate
			plan, err := Compute(desired, observed)
			if err != nil || plan.Phase != scenario.phase || plan.Next == nil || plan.Next.Kind != scenario.action {
				t.Fatalf("plan = %+v, err = %v", plan, err)
			}
			if plan.ActiveRevision != "active-1" || !plan.ActiveRouteConfirmed || plan.Next.Preconditions.ObservationToken != observed.ObservationToken {
				t.Fatalf("lost active-route evidence or write precondition: %+v", plan)
			}
			if plan.Next.Preconditions.RouteResourceID != observed.Route.ResourceID || plan.Next.Preconditions.RouteResourceVersion != observed.Route.ResourceVersion || plan.Next.Preconditions.RouteMustBeAbsent {
				t.Fatalf("missing concrete conditional route identity: %+v", plan.Next.Preconditions)
			}
		})
	}
}

func TestFailurePreservesActiveRouteAndEvidence(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		evidence      EvidenceState
		cleanupFailed bool
		phase         Phase
		action        ActionKind
	}{
		{"capture before delete", EvidencePending, false, Failed, CaptureEvidence},
		{"record failure before delete", EvidenceCaptured, false, Failed, RecordFailureOutcome},
		{"retain when evidence fails", EvidenceFailed, false, Retained, ""},
		{"retain when cleanup fails", EvidenceCaptured, true, Retained, ""},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			desired, observed := intent()
			observed.Candidate = candidateFor(desired)
			observed.Candidate.FailureCode = "CandidateNotReady"
			observed.Candidate.Evidence, observed.Candidate.CleanupFailure = scenario.evidence, scenario.cleanupFailed
			plan, err := Compute(desired, observed)
			if err != nil || plan.Phase != scenario.phase || plan.Code != "CandidateNotReady" || plan.ActiveRevision != "active-1" || !plan.ActiveRouteConfirmed {
				t.Fatalf("failure lost cause or serving state: %+v, %v", plan, err)
			}
			if scenario.action == "" && plan.Next != nil || scenario.action != "" && (plan.Next == nil || plan.Next.Kind != scenario.action) {
				t.Fatalf("unexpected recovery action: %+v", plan.Next)
			}
		})
	}
}

func TestStaleCandidateCannotPassNewIntentGates(t *testing.T) {
	desired, observed := intent()
	observed.Candidate = candidateFor(desired)
	observed.Candidate.ReconcileKey = "old-intent"
	observed.Candidate.Ready = true
	observed.Candidate.Smoke = &SmokeResult{Passed: true}
	plan, err := Compute(desired, observed)
	if !errors.Is(err, ErrInvalidObservation) || plan.Next != nil {
		t.Fatalf("stale smoke result authorized promotion: %+v, %v", plan, err)
	}
}

func TestSucceededRequiresConfirmedRouteAndMatchingIntent(t *testing.T) {
	desired, observed := intent()
	observed.Active = &candidateFor(desired).Revision
	observed.Route.Revision = desired.Revision
	plan, err := Compute(desired, observed)
	if err != nil || plan.Phase != Succeeded || plan.Next != nil {
		t.Fatalf("confirmed matching active revision not settled: %+v, %v", plan, err)
	}
	observed.Route.ConfirmedAt = time.Time{}
	plan, err = Compute(desired, observed)
	if err != nil || plan.Phase == Succeeded || plan.ActiveRouteConfirmed {
		t.Fatalf("unconfirmed route reported healthy: %+v, %v", plan, err)
	}
}

func TestBlockersProduceNoMutation(t *testing.T) {
	desired, observed := intent()
	delete(desired.Images, "web")
	plan, err := Compute(desired, observed)
	if err != nil || plan.Phase != WaitingForImages || plan.Next != nil || !reflect.DeepEqual(plan.MissingBuilds, []string{"web"}) {
		t.Fatalf("missing images: %+v, %v", plan, err)
	}
	desired, observed = intent()
	observed.ExternalBlockers = []string{"secret-missing"}
	plan, err = Compute(desired, observed)
	if err != nil || plan.Phase != WaitingForExternal || plan.Next != nil || plan.Blockers[0] != "secret-missing" {
		t.Fatalf("external blocker: %+v, %v", plan, err)
	}
	plan.Blockers[0] = "changed"
	if observed.ExternalBlockers[0] != "secret-missing" {
		t.Fatal("plan aliases caller observation")
	}
}

func TestInvalidOrForeignInputsFailBeforePlanning(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*Desired, *Observed)
		want   error
	}{
		{"foreign owner", func(d *Desired, o *Observed) { o.Subject.ProjectUID = "other" }, ErrForeignObservation},
		{"stale desired", func(d *Desired, o *Observed) { o.Generation = 3 }, ErrStaleIntent},
		{"missing observation token", func(d *Desired, o *Observed) { o.ObservationToken = "" }, ErrInvalidObservation},
		{"mutable image", func(d *Desired, o *Observed) { d.Images["web"] = "example.test/web:latest" }, ErrInvalidIntent},
		{"missing owner", func(d *Desired, o *Observed) { d.Subject.Installation = "" }, ErrInvalidIntent},
		{"duplicate build", func(d *Desired, o *Observed) { d.RequiredBuilds = []string{"web", "web"} }, ErrInvalidIntent},
		{"unverified candidate ID", func(d *Desired, o *Observed) { o.Candidate = candidateFor(*d); o.Candidate.ResourceID = "" }, ErrInvalidObservation},
		{"foreign route", func(d *Desired, o *Observed) { o.Route.Subject.ProjectUID = "other" }, ErrForeignObservation},
		{"missing route version", func(d *Desired, o *Observed) { o.Route.ResourceVersion = "" }, ErrInvalidObservation},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			d, o := intent()
			scenario.change(&d, &o)
			_, err := Compute(d, o)
			if !errors.Is(err, scenario.want) {
				t.Fatalf("got %v, want %v", err, scenario.want)
			}
		})
	}
}

func TestActionIdentityIsStableAcrossRetryObservations(t *testing.T) {
	desired, observed := intent()
	first, err := Compute(desired, observed)
	if err != nil {
		t.Fatal(err)
	}
	observed.ObservationToken = "new-snapshot"
	second, err := Compute(desired, observed)
	if err != nil || first.Next.IdempotencyKey != second.Next.IdempotencyKey || first.Next.Preconditions == second.Next.Preconditions {
		t.Fatalf("retry identity or fresh preconditions incorrect: %+v, %+v, %v", first, second, err)
	}
	desired.Subject.Environment = "other-lab"
	observed.Subject = desired.Subject
	observed.Route.Subject = desired.Subject
	third, err := Compute(desired, observed)
	if err != nil {
		t.Fatal(err)
	}
	if first.Next.IdempotencyKey == third.Next.IdempotencyKey {
		t.Fatal("cross-environment action identity collision")
	}
}

func TestDeletedFailedCandidateDoesNotRestartSameIntent(t *testing.T) {
	desired, observed := intent()
	observed.SettledFailure = &FailureOutcome{Revision: desired.Revision, ReconcileKey: desired.ReconcileKey, Code: "CandidateNotReady"}
	plan, err := Compute(desired, observed)
	if err != nil || plan.Phase != Failed || plan.Code != "CandidateNotReady" || plan.Next != nil {
		t.Fatalf("failed intent restarted after cleanup: %+v, %v", plan, err)
	}
	desired.ReconcileKey = "explicit-retry-intent"
	plan, err = Compute(desired, observed)
	if err != nil || plan.Next == nil || plan.Next.Kind != EnsureCandidate {
		t.Fatalf("explicit new intent did not permit retry: %+v, %v", plan, err)
	}
}

func TestFailureLatchIsDurableBeforeCandidateDeletion(t *testing.T) {
	desired, observed := intent()
	observed.Candidate = candidateFor(desired)
	observed.Candidate.FailureCode = "CandidateNotReady"
	observed.Candidate.Evidence = EvidenceCaptured
	plan, err := Compute(desired, observed)
	if err != nil || plan.Next == nil || plan.Next.Kind != RecordFailureOutcome || plan.Next.FailureOutcome == nil {
		t.Fatalf("delete attempted before durable failure latch: %+v, %v", plan, err)
	}
	// Only after the orchestrator durably records the returned outcome may it
	// ask for deletion. A crash after that deletion still leaves this latch.
	observed.SettledFailure = plan.Next.FailureOutcome
	plan, err = Compute(desired, observed)
	if err != nil || plan.Next == nil || plan.Next.Kind != DeleteCandidate {
		t.Fatalf("latched failure did not allow owned cleanup: %+v, %v", plan, err)
	}
	observed.Candidate = nil
	plan, err = Compute(desired, observed)
	if err != nil || plan.Next != nil || plan.Phase != Failed {
		t.Fatalf("unknown delete outcome restarted failed intent: %+v, %v", plan, err)
	}
}

func TestInitialRouteRequiresAtomicAbsence(t *testing.T) {
	desired, observed := intent()
	observed.Active, observed.Route = nil, nil
	observed.Candidate = candidateFor(desired)
	observed.Candidate.Ready = true
	observed.Candidate.Smoke = &SmokeResult{Passed: true}
	plan, err := Compute(desired, observed)
	if err != nil || plan.Next == nil || plan.Next.Kind != PromoteCandidate || !plan.Next.Preconditions.RouteMustBeAbsent {
		t.Fatalf("first promotion lacks create-only route precondition: %+v, %v", plan, err)
	}
}
