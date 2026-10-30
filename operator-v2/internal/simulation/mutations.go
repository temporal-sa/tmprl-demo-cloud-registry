package simulation

import (
	"context"
	"errors"
	"reflect"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

func (w *World) RecordCandidateOutcome(_ context.Context, desired rollout.Desired, action rollout.Action, outcome engine.CandidateOutcome) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if prior, ok := w.results[action.IdempotencyKey]; ok {
		if prior.revision == desired.Revision && prior.reconcileKey == desired.ReconcileKey && reflect.DeepEqual(prior.outcome, outcome) {
			return nil
		}
		return ErrConflict
	}
	if err := w.verifyAction(action); err != nil {
		return err
	}
	if outcome.Readiness != nil {
		copy := *outcome.Readiness
		outcome.Readiness = &copy
	}
	if outcome.Smoke != nil {
		copy := *outcome.Smoke
		outcome.Smoke = &copy
	}
	if outcome.Evidence != nil {
		copy := *outcome.Evidence
		outcome.Evidence = &copy
	}
	w.results[action.IdempotencyKey] = outcomeRecord{revision: desired.Revision, reconcileKey: desired.ReconcileKey, operation: w.currentOperation, outcome: outcome}
	w.trace = append(w.trace, "persist-result:"+string(action.Kind))
	w.version++
	return nil
}

func (w *World) RecordFailureOutcome(_ context.Context, desired rollout.Desired, _ rollout.Action, outcome rollout.FailureOutcome, failure diagnostics.DomainFailure) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trace = append(w.trace, "persist-failure:"+outcome.Code)
	if w.FailFailureOutcome {
		return fault("FailureOutcomeStorageUnavailable", "simulated failure-outcome storage unavailable", diagnostics.Publication)
	}
	if outcome.Revision != desired.Revision || outcome.ReconcileKey != desired.ReconcileKey || outcome.Code == "" {
		return ErrConflict
	}
	if old := w.settled; old != nil && old.Revision == desired.Revision && old.ReconcileKey == desired.ReconcileKey {
		if old.Code != outcome.Code || old.CandidateRetained && !outcome.CandidateRetained {
			return ErrConflict
		}
		if *old == outcome {
			return nil
		}
	}
	copy := outcome
	w.settled = &copy
	w.settledByOperation[w.currentOperation] = &copy
	safe := diagnostics.SafeFailure(failure)
	w.failure = &safe
	w.failureByOperation[w.currentOperation] = &safe
	w.version++
	return nil
}

func (w *World) Delete(_ context.Context, desired rollout.Desired, action rollout.Action, permit engine.CandidatePermit) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.verifyAction(action); err != nil {
		return err
	}
	if err := w.verifyCandidate(desired, permit.Preconditions, ownership.Cleanup); err != nil {
		return err
	}
	if w.settled == nil || w.settled.Revision != desired.Revision || w.settled.ReconcileKey != desired.ReconcileKey {
		return errors.New("delete refused without durable failure outcome")
	}
	w.trace = append(w.trace, "delete-attempt:candidate")
	if w.scenario == CleanupFailure {
		return fault("CandidateDeleteDenied", "simulated candidate delete denied", diagnostics.Cleanup)
	}
	w.trace = append(w.trace, "delete:candidate")
	w.candidate = nil
	w.candidates[w.currentOperation] = nil
	w.version++
	return nil
}

func (w *World) Publish(_ context.Context, desired rollout.Desired, action rollout.Action, permit engine.RoutePermit) (ownership.Observation, error) {
	w.mu.Lock()
	hook := w.BeforePublish
	w.BeforePublish = nil
	w.mu.Unlock()
	if hook != nil {
		hook(w)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.drainingOperations[permit.OperationID] {
		return ownership.Observation{}, ErrConflict
	}
	if (permit.Create == nil) == (permit.Update == nil) {
		return ownership.Observation{}, errors.New("route requires exactly one create or conditional update permit")
	}
	if err := w.verifyAction(action); err != nil {
		return ownership.Observation{}, err
	}
	if err := w.verifyCandidate(desired, permit.Candidate, ownership.Promote); err != nil {
		return ownership.Observation{}, err
	}
	if w.candidate.observation.Revision != desired.Revision || w.candidate.observation.SpecFingerprint != desired.ReconcileKey {
		return ownership.Observation{}, ErrConflict
	}
	if permit.Create != nil {
		if !action.Preconditions.RouteMustBeAbsent || w.route != nil {
			return ownership.Observation{}, ErrConflict
		}
		intent := permit.Create.Intent
		if intent.Purpose != "route" || intent.Revision != desired.Revision || intent.SpecFingerprint != desired.ReconcileKey {
			return ownership.Observation{}, ErrConflict
		}
		current, err := w.owner.AuthorizeCreate(intent, w.writer, w.Now())
		if err != nil {
			return ownership.Observation{}, err
		}
		if current != *permit.Create {
			return ownership.Observation{}, ErrConflict
		}
		if stored, ok := w.intents[intent.IdempotencyKey]; !ok || stored != intent {
			return ownership.Observation{}, errors.New("route create preceded durable intent")
		}
		w.version++
		w.route = &resource{observation(intent, "route-uid-"+intent.IdempotencyKey[:12], w.token(), w.writer.Epoch)}
		w.trace = append(w.trace, "create:route")
	} else {
		if action.Preconditions.RouteMustBeAbsent {
			return ownership.Observation{}, ErrConflict
		}
		if err := w.verifyResource(w.route, *permit.Update, ownership.Promote); err != nil {
			return ownership.Observation{}, err
		}
		record := w.records[w.route.observation.Ref.ProviderID]
		intent, ok := w.mutations[mutationKey(permit.OperationID, "route")]
		if !ok || intent.Previous != record || intent.ExpectedVersion != w.route.observation.Version || intent.TargetRevision != desired.Revision || intent.TargetFingerprint != desired.ReconcileKey {
			return ownership.Observation{}, errors.New("route update preceded matching durable mutation intent")
		}
		// The lock models provider-side atomic compare-and-set; earlier
		// application authorization alone cannot fence a competing writer.
		if w.route.observation.Ref.ProviderID != action.Preconditions.RouteResourceID || w.route.observation.Version != action.Preconditions.RouteResourceVersion {
			return ownership.Observation{}, ErrConflict
		}
		w.version++
		w.route.observation.Revision = desired.Revision
		w.route.observation.SpecFingerprint = desired.ReconcileKey
		w.route.observation.Version = w.token()
		w.trace = append(w.trace, "update:route")
	}
	w.active = &rollout.Revision{ID: desired.Revision, ReconcileKey: desired.ReconcileKey, ResourceID: w.candidate.observation.Ref.ProviderID, ResourceVersion: w.candidate.observation.Version}
	return w.route.observation, nil
}

// Test helpers model independent provider changes without editing the ledger.
func (w *World) ChangeRouteVersion(version string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.route != nil {
		w.route.observation.Version = version
		w.version++
	}
}
func (w *World) ChangeRouteOwner(owner ownership.Owner) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.route != nil {
		w.route.observation.Owner = owner
		w.version++
	}
}
func (w *World) ChangeCandidateOwner(owner ownership.Owner) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.candidate != nil {
		w.candidate.observation.Owner = owner
		w.version++
	}
}
func (w *World) InjectRouteCollision() {
	w.mu.Lock()
	defer w.mu.Unlock()
	intent, _ := w.owner.Intent(ownership.Kubernetes, "route", "primary", "git-foreign", "foreign-operation", "foreign-key", 63)
	observed := observation(intent, "foreign-route-uid", "99", w.writer.Epoch)
	observed.Owner.EnvironmentID = "production"
	w.route = &resource{observed}
	w.version++
}
func (w *World) CandidateExists() bool { w.mu.Lock(); defer w.mu.Unlock(); return w.candidate != nil }
func (w *World) ActiveRevision() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return ""
	}
	return w.active.ID
}
