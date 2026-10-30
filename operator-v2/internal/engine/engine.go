package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type Engine struct {
	owner  ownership.Context
	writer ownership.Writer
	c      Capabilities
}

func New(owner ownership.Context, writer ownership.Writer, c Capabilities) (*Engine, error) {
	if c.Observer == nil || c.Candidates == nil || c.Routes == nil || c.Evidence == nil || c.Journal == nil || c.Now == nil {
		return nil, errors.New("engine requires explicit capabilities and clock")
	}
	if writer.Owner != owner.Owner() {
		return nil, errors.New("writer does not belong to engine owner")
	}
	return &Engine{owner: owner, writer: writer, c: c}, nil
}

// Step observes, plans and executes at most one capability action. The caller
// retains the returned safe diagnostic projection; publishing it is independent
// of repeating a provider operation. Returned control-flow errors may retain raw
// provider causes. Never log or persist them directly; use SafeBoundaryError at
// every Temporal/history/log boundary, or serialize the safe projection.
func (e *Engine) Step(ctx context.Context, request Request) (Result, error) {
	identity := diagnostics.Identity{Environment: request.Desired.Subject.Environment, Installation: request.Desired.Subject.Installation, ProjectUID: request.Desired.Subject.ProjectUID, ProjectName: request.ProjectName, OperationID: request.OperationID, ReconcileKey: request.Desired.ReconcileKey, Generation: request.Desired.Generation}
	p, err := diagnostics.NewProjection(identity)
	if err != nil {
		return Result{}, err
	}
	if request.Projection != nil {
		if request.Projection.Identity != identity {
			return Result{}, diagnostics.ErrIdentityMismatch
		}
		p = *request.Projection
	}
	if err := ctx.Err(); err != nil {
		return Result{Projection: p, Terminal: true}, err
	}
	owner := e.owner.Owner()
	if identity.Environment != owner.EnvironmentID || identity.Installation != owner.InstallationID || identity.ProjectUID != owner.ProjectUID {
		return Result{Projection: p}, errors.New("request is outside engine ownership")
	}
	if _, err = rollout.Compute(request.Desired, rollout.Observed{Subject: request.Desired.Subject, Generation: request.Desired.Generation, ObservationToken: "validate-request"}); err != nil {
		return e.executionError(p, "validate-request", err)
	}
	supersession, hasSupersession := e.c.Journal.(SupersessionJournal)
	if request.Supersede && !hasSupersession {
		return e.executionError(p, "supersede-capability", errors.New("durable supersession capability is required"))
	}
	if !request.Supersede && hasSupersession {
		if err = supersession.AuthorizeAttempt(ctx, request); err != nil {
			return e.executionError(p, "authorize-attempt", err)
		}
	}
	if request.Supersede {
		// Drain/acquire the executing barrier before observing or repairing any
		// receipt. An old promotion may finish while its writer is draining.
		// Only a fresh post-drain read can establish whether a candidate serves.
		if err = supersession.PrepareSupersession(ctx, request); err != nil {
			return e.executionError(p, "drain-previous-writes", err)
		}
	}
	snapshot, err := e.c.Observer.Snapshot(ctx, request)
	if err != nil {
		return e.executionError(p, "observe", err)
	}
	// A ledger repair is permitted only by a persisted matching intent and
	// independent provider provenance. It never issues another provider write.
	if err = e.recoverRecords(ctx, request, &snapshot); err != nil {
		return e.executionError(p, "recover-ownership", err)
	}
	if err = validateSnapshot(snapshot); err != nil {
		return e.executionError(p, "observe-consistency", err)
	}
	if err = e.verifyObservedOwnership(snapshot); err != nil {
		return e.executionError(p, "verify-observed-ownership", err)
	}
	if snapshot.Candidate != nil {
		expected, intentErr := e.intent(request, "candidate")
		if intentErr != nil {
			return e.executionError(p, "candidate-attempt", intentErr)
		}
		if snapshot.Candidate.Record.CreationIdempotencyKey != expected.IdempotencyKey || snapshot.Candidate.Observation.Ref.Name != expected.Name {
			return e.executionError(p, "candidate-attempt", errors.New("candidate binding belongs to a different accepted attempt"))
		}
	}
	var plan rollout.Plan
	if request.Supersede {
		plan, err = rollout.ComputeDrain(request.Desired, snapshot.State)
	} else {
		plan, err = rollout.Compute(request.Desired, snapshot.State)
	}
	if err != nil {
		return e.executionError(p, "plan", err)
	}
	if plan.Next != nil {
		plan.Next.OperationID = request.OperationID
		sum := sha256.Sum256([]byte(request.OperationID + "/" + plan.Next.IdempotencyKey))
		plan.Next.IdempotencyKey = hex.EncodeToString(sum[:])
	}
	event := diagnostics.OperationEvent{Identity: identity, Sequence: p.Sequence + 1, OccurredAt: e.c.Now(), State: diagnosticState(plan), Step: string(plan.Phase)}
	if snapshot.State.Route != nil {
		event.Availability = &diagnostics.Availability{ActiveRevision: plan.ActiveRevision, CandidateRevision: request.Desired.Revision, RouteState: diagnostics.RouteUnconfirmed, ConfirmedAt: snapshot.State.Route.ConfirmedAt}
		if plan.ActiveRouteConfirmed {
			event.Availability.RouteState = diagnostics.RouteActivePreserved
			if plan.Phase == rollout.Succeeded {
				event.Availability.RouteState = diagnostics.RoutePromoted
			}
		}
	}
	if plan.Code != "" && (plan.Phase == rollout.Failed || plan.Phase == rollout.Retained) {
		f := failureFor(plan.Code, string(plan.Phase), e.c.Now())
		if snapshot.Failure != nil {
			f = diagnostics.SafeFailure(*snapshot.Failure)
		}
		event.Failure = &f
		event.Retry = &diagnostics.RetryInfo{Status: diagnostics.RetryNotRetryable, Authoritative: true, ObservedAt: e.c.Now()}
	}
	result := Result{Plan: plan, Projection: p, Terminal: plan.Next == nil}
	if plan.Next != nil {
		result.Executed = true
		event.Step = string(plan.Next.Kind)
		if err = e.execute(ctx, request, snapshot, *plan.Next, &event); err != nil {
			event.State = diagnostics.Failed
			executionFailure := executionFailureFor(err, string(plan.Next.Kind), e.c.Now())
			if event.Failure == nil {
				event.Failure = &executionFailure
			} else {
				event.Recovery = append(event.Recovery, diagnostics.RecoveryOutcome{ID: "execution-" + string(plan.Next.Kind), Step: string(plan.Next.Kind), State: diagnostics.OutcomeFailed, Failure: &executionFailure})
			}
			result.Event = diagnostics.SafeEvent(event)
			next, applyErr := p.Apply(result.Event)
			if applyErr != nil {
				return result, errors.Join(err, applyErr)
			}
			result.Projection = next
			return result, err
		}
	}
	result.Event = diagnostics.SafeEvent(event)
	result.Projection, err = p.Apply(result.Event)
	if err == nil && request.Supersede && result.Terminal {
		disposition := AttemptAbsent
		if snapshot.Candidate != nil {
			disposition = AttemptRetained
			if plan.Code == "SupersededActiveRetained" {
				disposition = AttemptActive
			}
		} else if snapshot.State.SettledFailure != nil {
			disposition = AttemptDeleted
		}
		if result.Projection.Failure == nil || result.Projection.Failure.Code == "RolloutSuperseded" {
			result.Projection.State = diagnostics.Cancelled
			result.Event.State = diagnostics.Cancelled
		}
		outcome := SupersessionOutcome{OperationID: request.OperationID, Desired: request.Desired, Disposition: disposition, Candidate: snapshot.Candidate, Route: snapshot.Route, Projection: result.Projection}
		if err = supersession.SettleSupersession(ctx, outcome); err != nil {
			return e.executionError(result.Projection, "persist-supersession", err)
		}
		result.SupersessionComplete = true
	}
	return result, err
}

func (e *Engine) verifyObservedOwnership(snapshot Snapshot) error {
	for _, owned := range []*Owned{snapshot.Candidate, snapshot.Route} {
		if owned == nil {
			continue
		}
		if owned.Record == nil {
			return errors.New("observed resource has no trusted ownership record")
		}
		// Even an already-succeeded plan needs independently verified owner,
		// canonical provider identity/scope and an active executing writer.
		if _, err := e.permit(owned, ownership.Mutate, owned.Record.Revision); err != nil {
			return err
		}
	}
	return nil
}

func validateSnapshot(snapshot Snapshot) error {
	if (snapshot.State.Candidate == nil) != (snapshot.Candidate == nil) || (snapshot.State.Route == nil) != (snapshot.Route == nil) {
		return errors.New("planner observation and independent resource facts disagree")
	}
	if snapshot.Candidate != nil {
		state, owned := snapshot.State.Candidate, snapshot.Candidate.Observation
		if state.ResourceID != owned.Ref.ProviderID || state.ResourceVersion != owned.Version || state.ID != owned.Revision || state.ReconcileKey != owned.SpecFingerprint {
			return errors.New("candidate planner identity differs from independent provider fact")
		}
	}
	if snapshot.Route != nil {
		state, owned := snapshot.State.Route, snapshot.Route.Observation
		if state.ResourceID != owned.Ref.ProviderID || state.ResourceVersion != owned.Version || state.Revision != owned.Revision {
			return errors.New("route planner identity differs from independent provider fact")
		}
	}
	return nil
}

func (e *Engine) executionError(p diagnostics.OperationProjection, step string, cause error) (Result, error) {
	f := executionFailureFor(cause, step, e.c.Now())
	event := diagnostics.SafeEvent(diagnostics.OperationEvent{Identity: p.Identity, Sequence: p.Sequence + 1, OccurredAt: e.c.Now(), State: diagnostics.Failed, Step: step, Failure: &f})
	if p.Failure != nil {
		event.Failure = nil
		event.Recovery = []diagnostics.RecoveryOutcome{{ID: "execution-" + step, Step: step, State: diagnostics.OutcomeFailed, Failure: &f}}
	}
	next, err := p.Apply(event)
	if err != nil {
		return Result{Projection: p}, errors.Join(cause, err)
	}
	return Result{Projection: next, Event: event}, cause
}

func (e *Engine) intent(request Request, purpose string) (ownership.Intent, error) {
	logical := request.Desired.Revision + "-" + request.OperationID
	if purpose == "route" {
		logical = "primary"
	}
	return e.owner.Intent(ownership.Kubernetes, purpose, logical, request.Desired.Revision, request.OperationID, request.Desired.ReconcileKey, 63)
}

func (e *Engine) permit(owned *Owned, action ownership.Action, revision string) (ownership.Preconditions, error) {
	if owned == nil || owned.Record == nil {
		return ownership.Preconditions{}, errors.New("resource has no trusted ownership record")
	}
	return e.owner.Authorize(ownership.MutationRequest{Action: action, Record: *owned.Record, Observed: owned.Observation, ExpectedRevision: revision, Writer: e.writer, Now: e.c.Now(), Fencing: ownership.ProviderConditional})
}

func (e *Engine) recoverRecords(ctx context.Context, request Request, snapshot *Snapshot) error {
	for _, target := range []struct {
		purpose string
		owned   *Owned
	}{{"candidate", snapshot.Candidate}, {"route", snapshot.Route}} {
		if target.owned == nil {
			continue
		}
		owned := target.owned
		if owned.Record == nil {
			intent, err := e.intent(request, target.purpose)
			if err != nil {
				return err
			}
			persisted, found, err := e.c.Journal.LoadIntent(ctx, intent.IdempotencyKey)
			if err != nil {
				return err
			}
			if !found || persisted != intent {
				return errors.New("unbound provider object has no matching persisted creation intent")
			}
			record, err := e.owner.BindCreated(persisted, owned.Observation, e.writer, e.c.Now())
			if err != nil {
				return err
			}
			if err = e.c.Journal.SaveRecord(ctx, record); err != nil {
				return err
			}
			owned.Record = &record
		}
		if target.purpose == "route" && (owned.Record.Revision != owned.Observation.Revision || owned.Record.SpecFingerprint != owned.Observation.SpecFingerprint) {
			intent, found, err := e.c.Journal.LoadMutation(ctx, request.OperationID, "route")
			if err != nil {
				return err
			}
			if !found || intent.Previous != *owned.Record || intent.TargetRevision != request.Desired.Revision || intent.TargetFingerprint != request.Desired.ReconcileKey {
				return errors.New("route ledger drift lacks matching durable mutation intent")
			}
			repaired := *owned.Record
			repaired.Revision = intent.TargetRevision
			repaired.SpecFingerprint = intent.TargetFingerprint
			if _, err = e.owner.Authorize(ownership.MutationRequest{Action: ownership.Promote, Record: repaired, Observed: owned.Observation, ExpectedRevision: repaired.Revision, Writer: e.writer, Now: e.c.Now(), Fencing: ownership.ProviderConditional}); err != nil {
				return err
			}
			if err = e.c.Journal.SaveRecord(ctx, repaired); err != nil {
				return err
			}
			owned.Record = &repaired
		}
	}
	return nil
}

func (e *Engine) execute(ctx context.Context, request Request, snapshot Snapshot, action rollout.Action, event *diagnostics.OperationEvent) error {
	switch action.Kind {
	case rollout.EnsureCandidate:
		intent, err := e.intent(request, "candidate")
		if err != nil {
			return err
		}
		if err = e.c.Journal.SaveIntent(ctx, intent); err != nil {
			return err
		}
		pre, err := e.owner.AuthorizeCreate(intent, e.writer, e.c.Now())
		if err != nil {
			return err
		}
		observed, err := e.c.Candidates.Ensure(ctx, request.Desired, action, CreationPermit{pre})
		if err != nil {
			return err
		}
		record, err := e.owner.BindCreated(intent, observed, e.writer, e.c.Now())
		if err != nil {
			return err
		}
		return e.c.Journal.SaveRecord(ctx, record)
	case rollout.ObserveReadiness:
		pre, err := e.permit(snapshot.Candidate, ownership.Mutate, request.Desired.Revision)
		if err != nil {
			return err
		}
		result, err := e.c.Candidates.Readiness(ctx, request.Desired, action, CandidatePermit{pre})
		if err != nil {
			return err
		}
		return e.c.Journal.RecordCandidateOutcome(ctx, request.Desired, action, CandidateOutcome{Readiness: &result})
	case rollout.CheckSmoke:
		pre, err := e.permit(snapshot.Candidate, ownership.Mutate, request.Desired.Revision)
		if err != nil {
			return err
		}
		result, err := e.c.Candidates.Smoke(ctx, request.Desired, action, CandidatePermit{pre})
		if err != nil {
			return err
		}
		return e.c.Journal.RecordCandidateOutcome(ctx, request.Desired, action, CandidateOutcome{Smoke: &result})
	case rollout.PromoteCandidate:
		return e.promote(ctx, request, snapshot, action)
	case rollout.CaptureEvidence:
		pre, err := e.permit(snapshot.Candidate, ownership.Mutate, request.Desired.Revision)
		if err != nil {
			return err
		}
		ref, captureErr := e.c.Evidence.Capture(ctx, request.Desired, action, CandidatePermit{pre})
		state := rollout.EvidenceCaptured
		if captureErr != nil {
			state = rollout.EvidenceFailed
			f := recoveryFailureFor("EvidenceCaptureFailed", "capture-evidence", captureErr, e.c.Now())
			event.Recovery = []diagnostics.RecoveryOutcome{{ID: "evidence", Step: "capture evidence", State: diagnostics.OutcomeFailed, Failure: &f, RetainedResources: candidateRef(snapshot)}}
		} else {
			event.Evidence = []diagnostics.EvidenceRef{ref}
		}
		if err = e.c.Journal.RecordCandidateOutcome(ctx, request.Desired, action, CandidateOutcome{Evidence: &state}); err != nil {
			return err
		}
		if captureErr != nil && event.Failure != nil {
			return e.c.Journal.RecordFailureOutcome(ctx, request.Desired, action, rollout.FailureOutcome{Revision: request.Desired.Revision, ReconcileKey: request.Desired.ReconcileKey, Code: event.Failure.Code, CandidateRetained: true}, *event.Failure)
		}
		return nil
	case rollout.RecordFailureOutcome:
		if action.FailureOutcome == nil {
			return errors.New("planner omitted durable failure outcome")
		}
		f := failureFor(action.FailureOutcome.Code, "candidate-failed", e.c.Now())
		if event.Failure != nil {
			f = *event.Failure
		}
		// No DeleteCandidate is possible until a later observation contains this
		// acknowledged outcome. A crash here cannot erase the failure latch.
		return e.c.Journal.RecordFailureOutcome(ctx, request.Desired, action, *action.FailureOutcome, diagnostics.SafeFailure(f))
	case rollout.DeleteCandidate:
		if snapshot.State.SettledFailure == nil || snapshot.State.SettledFailure.Revision != request.Desired.Revision || snapshot.State.SettledFailure.ReconcileKey != request.Desired.ReconcileKey {
			return errors.New("candidate deletion requires acknowledged durable failure outcome")
		}
		pre, err := e.permit(snapshot.Candidate, ownership.Cleanup, request.Desired.Revision)
		if err != nil {
			return err
		}
		if err = e.c.Candidates.Delete(ctx, request.Desired, action, CandidatePermit{pre}); err != nil {
			f := recoveryFailureFor("CandidateCleanupFailed", "delete-candidate", err, e.c.Now())
			event.Recovery = []diagnostics.RecoveryOutcome{{ID: "cleanup", Step: "delete candidate", State: diagnostics.OutcomeRetained, Failure: &f, RetainedResources: candidateRef(snapshot)}}
			outcome := *snapshot.State.SettledFailure
			outcome.CandidateRetained = true
			if saveErr := e.c.Journal.RecordCandidateOutcome(ctx, request.Desired, action, CandidateOutcome{CleanupFailed: true}); saveErr != nil {
				return errors.Join(err, saveErr)
			}
			if saveErr := e.c.Journal.RecordFailureOutcome(ctx, request.Desired, action, outcome, *event.Failure); saveErr != nil {
				return errors.Join(err, saveErr)
			}
			return nil
		}
		event.Recovery = []diagnostics.RecoveryOutcome{{ID: "cleanup", Step: "delete candidate", State: diagnostics.OutcomeCompleted}}
		return nil
	default:
		return fmt.Errorf("unknown planner action %q", action.Kind)
	}
}

func (e *Engine) promote(ctx context.Context, request Request, snapshot Snapshot, action rollout.Action) error {
	candidate, err := e.permit(snapshot.Candidate, ownership.Promote, request.Desired.Revision)
	if err != nil {
		return err
	}
	permit := RoutePermit{OperationID: request.OperationID, Candidate: candidate}
	if snapshot.Route == nil {
		intent, err := e.intent(request, "route")
		if err != nil {
			return err
		}
		if err = e.c.Journal.SaveIntent(ctx, intent); err != nil {
			return err
		}
		create, err := e.owner.AuthorizeCreate(intent, e.writer, e.c.Now())
		if err != nil {
			return err
		}
		permit.Create = &create
		observed, err := e.c.Routes.Publish(ctx, request.Desired, action, permit)
		if err != nil {
			return err
		}
		record, err := e.owner.BindCreated(intent, observed, e.writer, e.c.Now())
		if err != nil {
			return err
		}
		return e.c.Journal.SaveRecord(ctx, record)
	}
	mutate, err := e.permit(snapshot.Route, ownership.Promote, snapshot.Route.Record.Revision)
	if err != nil {
		return err
	}
	intent := MutationIntent{OperationID: request.OperationID, Purpose: "route", Previous: *snapshot.Route.Record, ExpectedVersion: mutate.ProviderVersion, TargetRevision: request.Desired.Revision, TargetFingerprint: request.Desired.ReconcileKey}
	if err = e.c.Journal.SaveMutation(ctx, intent); err != nil {
		return err
	}
	permit.Update = &mutate
	observed, err := e.c.Routes.Publish(ctx, request.Desired, action, permit)
	if err != nil {
		return err
	}
	updated := *snapshot.Route.Record
	updated.Revision = request.Desired.Revision
	updated.SpecFingerprint = request.Desired.ReconcileKey
	if _, err = e.owner.Authorize(ownership.MutationRequest{Action: ownership.Promote, Record: updated, Observed: observed, ExpectedRevision: updated.Revision, Writer: e.writer, Now: e.c.Now(), Fencing: ownership.ProviderConditional}); err != nil {
		return err
	}
	return e.c.Journal.SaveRecord(ctx, updated)
}

func candidateRef(snapshot Snapshot) []diagnostics.ResourceRef {
	if snapshot.Candidate == nil {
		return nil
	}
	ref := snapshot.Candidate.Observation.Ref
	return []diagnostics.ResourceRef{{Provider: string(ref.Scope.Provider), Kind: "candidate", ID: ref.ProviderID}}
}

func diagnosticState(plan rollout.Plan) diagnostics.State {
	switch plan.Phase {
	case rollout.Succeeded:
		return diagnostics.Succeeded
	case rollout.Failed, rollout.Retained:
		return diagnostics.Failed
	case rollout.WaitingForExternal, rollout.WaitingForImages, rollout.WaitingForReadiness:
		return diagnostics.Waiting
	default:
		return diagnostics.Running
	}
}
