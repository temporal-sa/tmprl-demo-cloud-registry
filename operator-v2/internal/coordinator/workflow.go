package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Attempt is allocated durably by the actor when it starts an accepted intent.
// Returning to an old source after another intent must not reuse its deleted
// runtime namespace; activity retries and Continue-As-New retain the attempt.
func OperationID(subject rollout.Subject, intent IntentIdentity, attempt uint64) string {
	data, _ := json.Marshal(struct {
		Subject rollout.Subject
		Intent  IntentIdentity
		Attempt uint64
	}{subject, intent, attempt})
	digest := sha256.Sum256(data)
	return "rollout-" + hex.EncodeToString(digest[:16])
}

func identify(desired rollout.Desired) IntentIdentity {
	return IntentIdentity{desired.ReconcileKey, desired.Generation}
}

func validateDesired(subject rollout.Subject, desired rollout.Desired) error {
	if desired.Subject != subject {
		return errors.New("reconcile subject differs from configured actor")
	}
	_, err := rollout.Compute(desired, rollout.Observed{Subject: subject, Generation: desired.Generation, ObservationToken: "validate-intent"})
	return err
}

// RolloutWorkflow is a single-writer durable actor. Signals carry desired state;
// provider calls occur only in named activities. A queued newer intent replaces
// pending work only after the exact previous attempt has a durable drain/retain
// acknowledgment. Completed activities alone never prove a write is resolved.
func RolloutWorkflow(ctx workflow.Context, input Input) (Status, error) {
	if input.SchemaVersion != SchemaVersion {
		return Status{}, temporal.NewNonRetryableApplicationError("unsupported coordinator schema", "InvalidInput", nil)
	}
	if input.PollInterval == 0 {
		input.PollInterval = 5 * time.Second
	}
	if input.PollInterval < time.Millisecond || input.PollInterval > time.Hour {
		return Status{}, temporal.NewNonRetryableApplicationError("invalid rollout poll interval", "InvalidInput", nil)
	}
	if input.MaxCompleted == 0 {
		input.MaxCompleted = 50
	}
	if input.MaxCompleted > 1000 {
		return Status{}, temporal.NewNonRetryableApplicationError("invalid history rollover threshold", "InvalidInput", nil)
	}
	identity := diagnostics.Identity{Environment: input.Subject.Environment, Installation: input.Subject.Installation, ProjectUID: input.Subject.ProjectUID, ProjectName: input.ProjectName, OperationID: "validate-actor", ReconcileKey: "validate-actor", Generation: 1}
	if _, err := diagnostics.NewProjection(identity); err != nil {
		return Status{}, temporal.NewNonRetryableApplicationError("invalid actor identity", "InvalidInput", nil)
	}
	state := Carry{}
	if input.Carry != nil {
		state = *input.Carry
	}
	for _, desired := range []*rollout.Desired{input.Initial, state.Current, state.Pending, state.LastDesired} {
		if desired != nil && validateDesired(input.Subject, *desired) != nil {
			return Status{}, temporal.NewNonRetryableApplicationError("invalid carried reconcile intent", "InvalidInput", nil)
		}
	}
	if input.Carry != nil && input.Initial != nil {
		return Status{}, temporal.NewNonRetryableApplicationError("initial and carried state cannot be combined", "InvalidInput", nil)
	}
	if state.Current != nil && state.CurrentAttempt == 0 || state.LastDesired != nil && state.LastAttempt == 0 || state.NextAttempt < state.CurrentAttempt || state.NextAttempt < state.LastAttempt {
		return Status{}, temporal.NewNonRetryableApplicationError("invalid carried attempt identity", "InvalidInput", nil)
	}
	if input.Initial != nil {
		state.Pending = input.Initial
	}
	status := func(stopped bool) Status {
		nextAction := ""
		if state.ExecutionBlocked || state.SupersessionBlocked {
			nextAction = "Resolve the previous attempt's provider writes and ownership using authorized evidence, then resend its reconcile signal or the pending reconcile signal to explicitly retry. No newer attempt may bypass the drain acknowledgment."
		}
		return Status{SchemaVersion: SchemaVersion, Projection: state.Projection, PreviousFailure: state.PreviousFailure, LastRejected: state.LastRejected, CleanupFailure: state.CleanupFailure, Finalized: state.Finalized, Finalizing: state.Finalizing, FinalizeBlocked: state.FinalizeBlocked, Stopped: stopped, ExecutionBlocked: state.ExecutionBlocked, Superseding: state.Superseding, SupersessionBlocked: state.SupersessionBlocked, BlockedNextAction: nextAction}
	}
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (Status, error) { return status(false), nil }); err != nil {
		return Status{}, err
	}
	reconcile := workflow.GetSignalChannel(ctx, ReconcileSignal)
	finalize := workflow.GetSignalChannel(ctx, FinalizeSignal)
	shutdown := workflow.GetSignalChannel(ctx, ShutdownSignal)
	stopping := false
	reject := func() {
		failure := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: "reconcile-rejected", Code: "InvalidOrStaleReconcile", Category: diagnostics.Validation, Message: "Reconcile was rejected because its owner, generation or intent is invalid.", OccurredAt: workflow.Now(ctx), Step: "accept-reconcile"})
		state.LastRejected = &failure
	}
	accept := func(desired rollout.Desired) {
		if state.Finalizing || validateDesired(input.Subject, desired) != nil {
			reject()
			return
		}
		if state.LastDesired != nil && desired.Generation < state.LastDesired.Generation {
			reject()
			return
		}
		if state.Pending != nil && desired.Generation < state.Pending.Generation {
			reject()
			return
		}
		id := identify(desired)
		for _, bound := range []*rollout.Desired{state.Current, state.Pending, state.LastDesired} {
			if bound != nil && identify(*bound) == id {
				expected, _ := json.Marshal(bound)
				received, _ := json.Marshal(desired)
				if string(expected) != string(received) {
					reject()
					return
				}
			}
		}
		// An explicit repeated signal can retry a blocked observation/drain only;
		// the engine's independent write/journal guard still decides authority.
		if state.Current != nil && identify(*state.Current) == id {
			state.ExecutionBlocked = false
		}
		if state.Pending != nil && identify(*state.Pending) == id {
			state.SupersessionBlocked = false
		}
		if id == state.Settled || state.Current != nil && identify(*state.Current) == id || state.Pending != nil && identify(*state.Pending) == id {
			return
		}
		copy := desired
		state.Pending = &copy
	}
	drain := func() {
		var desired rollout.Desired
		for reconcile.ReceiveAsync(&desired) {
			accept(desired)
		}
		var ignored interface{}
		for finalize.ReceiveAsync(&ignored) {
			state.Finalizing = true
			state.FinalizeBlocked = false
		}
		for shutdown.ReceiveAsync(&ignored) {
			stopping = true
		}
	}
	wait := func(delay time.Duration) error {
		var timer workflow.Future
		if delay > 0 {
			timerCtx, cancel := workflow.WithCancel(ctx)
			defer cancel()
			timer = workflow.NewTimer(timerCtx, delay)
		}
		for {
			selector := workflow.NewSelector(ctx)
			woken := false
			selector.AddReceive(reconcile, func(channel workflow.ReceiveChannel, _ bool) {
				var desired rollout.Desired
				channel.Receive(ctx, &desired)
				previous := state.Pending
				wasExecutionBlocked, wasSupersessionBlocked := state.ExecutionBlocked, state.SupersessionBlocked
				accept(desired)
				woken = state.Pending != previous || wasExecutionBlocked != state.ExecutionBlocked || wasSupersessionBlocked != state.SupersessionBlocked
			})
			selector.AddReceive(finalize, func(channel workflow.ReceiveChannel, _ bool) {
				channel.Receive(ctx, nil)
				state.Finalizing = true
				state.FinalizeBlocked = false
				woken = true
			})
			selector.AddReceive(shutdown, func(channel workflow.ReceiveChannel, _ bool) {
				channel.Receive(ctx, nil)
				stopping = true
				woken = true
			})
			if timer != nil {
				selector.AddFuture(timer, func(workflow.Future) { woken = true })
			}
			selector.Select(ctx)
			if ctx.Err() != nil || woken {
				return ctx.Err()
			}
			// Duplicate/rejected signals do not reset the existing timer or
			// accelerate provider polling.
		}
	}
	options := workflow.ActivityOptions{StartToCloseTimeout: time.Minute, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 5 * time.Second, MaximumAttempts: 3, NonRetryableErrorTypes: []string{"ConfigurationError", "InvalidInput", "InvalidCleanupOutcome", "OwnershipViolation"}}}
	activityCtx := workflow.WithActivityOptions(ctx, options)
	cleanup := func(reason CleanupReason, disconnected bool) (CleanupResult, error) {
		desired, attempt := state.LastDesired, state.LastAttempt
		if desired == nil && reason == FinalizeProject {
			desired = state.Pending
		}
		if desired == nil {
			return CleanupResult{Complete: true}, nil
		}
		cleanupCtx := activityCtx
		if disconnected {
			independent, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(independent, options)
		}
		request := CleanupRequest{Desired: *desired, ProjectName: input.ProjectName, OperationID: OperationID(input.Subject, identify(*desired), attempt), Reason: reason, Projection: state.Projection}
		var result CleanupResult
		err := workflow.ExecuteActivity(cleanupCtx, CleanupActivityName, request).Get(cleanupCtx, &result)
		return result, err
	}
	for {
		if err := ctx.Err(); err != nil {
			result, cleanupErr := cleanup(CancelRollout, true)
			if result.Projection != nil {
				state.Projection = result.Projection
			}
			if cleanupErr != nil || !result.Complete {
				state.FinalizeBlocked = true
				state.CleanupFailure = cleanupFailure(ctx)
			}
			if state.Projection != nil {
				p := *state.Projection
				outcome := diagnostics.RecoveryOutcome{ID: "cancel-cleanup", Step: "cancel rollout cleanup", State: diagnostics.OutcomeCompleted}
				if state.CleanupFailure != nil {
					outcome.State = diagnostics.OutcomeRetained
					outcome.Failure = state.CleanupFailure
					outcome.RetainedResources = result.RetainedResources
				}
				event := diagnostics.SafeEvent(diagnostics.OperationEvent{Identity: p.Identity, Sequence: p.Sequence + 1, OccurredAt: workflow.Now(ctx), State: diagnostics.Cancelled, Step: "cancel-rollout", Recovery: []diagnostics.RecoveryOutcome{outcome}})
				if updated, applyErr := p.Apply(event); applyErr == nil {
					state.Projection = &updated
				}
			}
			// Persist a safe terminal view in cancellation details so retrieval
			// does not depend on a historical worker answering a query.
			return status(true), temporal.NewCanceledError(status(true))
		}
		drain()
		if stopping {
			return status(true), nil
		}
		if state.Finalizing && !state.FinalizeBlocked {
			result, err := cleanup(FinalizeProject, false)
			if result.Projection != nil {
				state.Projection = result.Projection
			}
			if err != nil {
				state.FinalizeBlocked = true
				state.CleanupFailure = cleanupFailure(ctx)
			} else if result.Complete {
				state.Finalizing = false
				state.Finalized = true
				state.CleanupFailure = nil
				return status(true), nil
			} else if err = wait(input.PollInterval); err != nil {
				continue
			}
			continue
		}
		if state.Finalizing && state.FinalizeBlocked {
			_ = wait(0)
			continue
		}
		if state.Pending != nil {
			if state.LastDesired != nil {
				oldOperation := OperationID(input.Subject, identify(*state.LastDesired), state.LastAttempt)
				if state.LastDrainedOperation != oldOperation {
					state.Superseding = true
					if state.SupersessionBlocked {
						_ = wait(0)
						continue
					}
					request := engine.Request{Desired: *state.LastDesired, ProjectName: input.ProjectName, OperationID: oldOperation, Projection: state.Projection, Supersede: true}
					var result engine.Result
					err := workflow.ExecuteActivity(activityCtx, StepActivityName, request).Get(activityCtx, &result)
					if err != nil {
						if !temporal.IsCanceledError(err) && ctx.Err() == nil {
							state.Projection = activityFailureProjection(ctx, request, err)
							state.PreviousFailure = state.Projection
							state.SupersessionBlocked = true
						}
						continue
					}
					projection := result.Projection
					state.Projection = &projection
					if result.SupersessionComplete && result.Terminal {
						state.LastDrainedOperation = oldOperation
						state.Superseding = false
						state.SupersessionBlocked = false
						state.ExecutionBlocked = false
						state.Current = nil
						state.CurrentAttempt = 0
						if projection.Failure != nil {
							state.PreviousFailure = &projection
						}
						state.Completed++
						if state.Completed >= input.MaxCompleted || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
							state.Completed = 0
							input.Initial = nil
							input.Carry = &state
							return status(false), workflow.NewContinueAsNewError(ctx, RolloutWorkflow, input)
						}
					}
					if !result.SupersessionComplete {
						// Incomplete drain observations may represent a provider wait.
						// Poll at the configured interval instead of scheduling a tight
						// chain of activities while Pending remains populated.
						_ = wait(input.PollInterval)
					}
					continue
				}
			}
			if state.Projection != nil && state.Projection.Failure != nil {
				state.PreviousFailure = state.Projection
			}
			state.Current, state.Pending = state.Pending, nil
			if state.NextAttempt == ^uint64(0) {
				return status(false), temporal.NewNonRetryableApplicationError("attempt identity space exhausted", "ConfigurationError", nil)
			}
			state.NextAttempt++
			state.CurrentAttempt = state.NextAttempt
			state.LastDesired = state.Current
			state.LastAttempt = state.CurrentAttempt
			state.Projection = nil
			state.ExecutionBlocked = false
		}
		if state.Current == nil {
			_ = wait(0)
			continue
		}
		if state.ExecutionBlocked {
			_ = wait(0)
			continue
		}
		desired := *state.Current
		request := engine.Request{Desired: desired, ProjectName: input.ProjectName, OperationID: OperationID(input.Subject, identify(desired), state.CurrentAttempt), Projection: state.Projection}
		var result engine.Result
		err := workflow.ExecuteActivity(activityCtx, StepActivityName, request).Get(activityCtx, &result)
		if err != nil {
			if temporal.IsCanceledError(err) || ctx.Err() != nil {
				continue
			}
			state.Projection = activityFailureProjection(ctx, request, err)
			state.PreviousFailure = state.Projection
			var application *temporal.ApplicationError
			if errors.As(err, &application) && application.Type() == "RolloutFailed" {
				state.Settled = identify(desired)
				state.Completed++
				state.Current = nil
				state.CurrentAttempt = 0
			} else {
				state.ExecutionBlocked = true
			}
		} else {
			projection := result.Projection
			state.Projection = &projection
			if result.Terminal && settledState(projection.State) {
				state.Settled = identify(desired)
				state.Completed++
				state.Current = nil
				state.CurrentAttempt = 0
				if projection.Failure != nil {
					state.PreviousFailure = &projection
				}
			}
		}
		drain()
		if stopping {
			return status(true), nil
		}
		if state.Completed >= input.MaxCompleted || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			state.Completed = 0
			input.Initial = nil
			input.Carry = &state
			return status(false), workflow.NewContinueAsNewError(ctx, RolloutWorkflow, input)
		}
		if state.Current != nil && state.Pending == nil && !state.Finalizing {
			_ = wait(input.PollInterval)
		}
	}
}

func settledState(state diagnostics.State) bool {
	return state == diagnostics.Succeeded || state == diagnostics.Failed || state == diagnostics.Cancelled || state == diagnostics.Finalized
}

func cleanupFailure(ctx workflow.Context) *diagnostics.DomainFailure {
	f := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: "project-cleanup-failed", Code: "ProjectCleanupFailed", Category: diagnostics.Cleanup, Message: "Project cleanup did not complete; owned resources and the finalizer must remain retained.", Step: "cleanup", OccurredAt: workflow.Now(ctx), NextAction: "Inspect the cleanup activity and verified resource ownership before requesting finalization again."})
	return &f
}

func activityFailureProjection(ctx workflow.Context, request engine.Request, err error) *diagnostics.OperationProjection {
	identity := diagnostics.Identity{Environment: request.Desired.Subject.Environment, Installation: request.Desired.Subject.Installation, ProjectUID: request.Desired.Subject.ProjectUID, ProjectName: request.ProjectName, OperationID: request.OperationID, ReconcileKey: request.Desired.ReconcileKey, Generation: request.Desired.Generation}
	p, _ := diagnostics.NewProjection(identity)
	if request.Projection != nil {
		p = *request.Projection
	}
	var application *temporal.ApplicationError
	if errors.As(err, &application) {
		var saved diagnostics.OperationProjection
		if application.Details(&saved) == nil && saved.Identity == identity {
			p = saved
		}
	}
	failure := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: "rollout-activity-failed", Code: "RolloutStepFailed", Category: diagnostics.Unknown, Message: "The rollout step failed after its activity retry policy completed. Inspect the workflow activity and authorized evidence.", OccurredAt: workflow.Now(ctx), Step: "rollout-step"})
	retry := diagnostics.RetryInfo{Status: diagnostics.RetryUnknown, ObservedAt: workflow.Now(ctx)}
	info := workflow.GetInfo(ctx)
	failure.Execution = diagnostics.ExecutionRef{Namespace: info.Namespace, WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID}
	var activityError *temporal.ActivityError
	if errors.As(err, &activityError) {
		failure.Execution.ActivityID = activityError.ActivityID()
		failure.Execution.ScheduledEventID = activityError.ScheduledEventID()
		if activityError.ActivityType() != nil {
			failure.Execution.ActivityType = activityError.ActivityType().Name
		}
		switch activityError.RetryState() {
		case enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED:
			retry.Status = diagnostics.RetryExhausted
			retry.Authoritative = true
			retry.MaximumAttempts = 3
		case enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE:
			retry.Status = diagnostics.RetryNotRetryable
			retry.Authoritative = true
		case enumspb.RETRY_STATE_TIMEOUT:
			retry.Status = diagnostics.RetryExhausted
			retry.Authoritative = true
		}
	}
	if application != nil && application.NonRetryable() {
		retry.Status = diagnostics.RetryNotRetryable
		retry.Authoritative = true
	}
	if p.Failure != nil && p.Failure.Execution.ActivityID == "" {
		original := diagnostics.SafeFailure(*p.Failure)
		original.Execution = failure.Execution
		p.Failure = &original
	}
	event := diagnostics.SafeEvent(diagnostics.OperationEvent{Identity: identity, Sequence: p.Sequence + 1, OccurredAt: workflow.Now(ctx), State: diagnostics.Failed, Step: "rollout-step", Failure: &failure, Retry: &retry})
	next, applyErr := p.Apply(event)
	if applyErr == nil {
		return &next
	}
	return &p
}
