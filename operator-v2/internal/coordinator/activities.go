package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Stepper interface {
	Step(context.Context, engine.Request) (engine.Result, error)
}
type EngineFactory func(context.Context, engine.Request) (Stepper, error)
type CleanupFunc func(context.Context, CleanupRequest) (CleanupResult, error)

type Activities struct {
	NewEngine        EngineFactory
	CleanupOperation CleanupFunc
	Now              func() time.Time
}

func (a *Activities) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now().UTC()
}

// Step converts raw local errors into a safe durable boundary. The original
// unwrap chain is deliberately not passed into Temporal failure conversion.
func (a *Activities) Step(ctx context.Context, request engine.Request) (*engine.Result, error) {
	if a.NewEngine == nil {
		return nil, temporal.NewNonRetryableApplicationError("rollout engine factory is not configured", "ConfigurationError", nil)
	}
	stepper, err := a.NewEngine(ctx, request)
	if err != nil {
		return nil, safeActivityError(err, "initialize-engine", a.now())
	}
	if stepper == nil {
		return nil, temporal.NewNonRetryableApplicationError("rollout engine factory returned no engine", "ConfigurationError", nil)
	}
	result, err := stepper.Step(ctx, request)
	if err == nil {
		if !projectionMatches(result.Projection, request) || result.Projection.Sequence == 0 {
			return nil, temporal.NewNonRetryableApplicationError("rollout activity returned an invalid operation identity", "InvalidStepOutcome", nil)
		}
		if request.Supersede && result.SupersessionComplete && !result.Terminal {
			return nil, temporal.NewNonRetryableApplicationError("supersession acknowledged before terminal drain", "InvalidStepOutcome", nil)
		}
		if request.Supersede && result.Terminal && !result.SupersessionComplete {
			return nil, temporal.NewNonRetryableApplicationError("terminal supersession lacks durable acknowledgment", "InvalidSupersessionOutcome", nil, result.Projection)
		}
		if !request.Supersede && result.SupersessionComplete {
			return nil, temporal.NewNonRetryableApplicationError("normal step cannot claim supersession acknowledgment", "InvalidSupersessionOutcome", nil)
		}
		if result.Terminal && result.Projection.State == diagnostics.Failed && !result.SupersessionComplete {
			return nil, temporal.NewNonRetryableApplicationError("The candidate rollout failed; its safe diagnosis is attached.", "RolloutFailed", nil, result.Projection)
		}
		return &result, nil
	}
	if errors.Is(err, context.Canceled) {
		return nil, temporal.NewCanceledError()
	}
	boundary := engine.SafeBoundaryError(err, "rollout-step", a.now())
	// The bounded projection remains available after retries exhaust. Raw input
	// or provider response bodies are never error details.
	var details []any
	if projectionMatches(result.Projection, request) && result.Projection.Sequence > 0 {
		details = []any{result.Projection}
	}
	return nil, temporal.NewApplicationErrorWithOptions(boundary.Error(), "RolloutStepFailed", temporal.ApplicationErrorOptions{NonRetryable: nonRetryableBoundary(boundary), Details: details})
}

func (a *Activities) Cleanup(ctx context.Context, request CleanupRequest) (CleanupResult, error) {
	if a.CleanupOperation == nil {
		return CleanupResult{}, temporal.NewNonRetryableApplicationError("project cleanup capability is not configured", "ConfigurationError", nil)
	}
	result, err := a.CleanupOperation(ctx, request)
	if err != nil {
		return CleanupResult{}, safeActivityError(err, "cleanup", a.now())
	}
	if result.Retained && len(result.RetainedResources) == 0 {
		return CleanupResult{}, temporal.NewNonRetryableApplicationError("retain outcome requires explicit resource references", "InvalidCleanupOutcome", nil)
	}
	if result.Projection != nil && !projectionMatches(*result.Projection, engine.Request{Desired: request.Desired, ProjectName: request.ProjectName, OperationID: request.OperationID}) {
		return CleanupResult{}, temporal.NewNonRetryableApplicationError("cleanup returned an invalid operation identity", "InvalidCleanupOutcome", nil)
	}
	return result, nil
}

func projectionMatches(p diagnostics.OperationProjection, r engine.Request) bool {
	return p.SchemaVersion == diagnostics.SchemaVersion && p.Identity == diagnostics.Identity{Environment: r.Desired.Subject.Environment, Installation: r.Desired.Subject.Installation, ProjectUID: r.Desired.Subject.ProjectUID, ProjectName: r.ProjectName, OperationID: r.OperationID, ReconcileKey: r.Desired.ReconcileKey, Generation: r.Desired.Generation}
}

func safeActivityError(err error, step string, at time.Time) error {
	if errors.Is(err, context.Canceled) {
		return temporal.NewCanceledError()
	}
	boundary := engine.SafeBoundaryError(err, step, at)
	return temporal.NewApplicationErrorWithOptions(boundary.Error(), "CapabilityExecutionFailed", temporal.ApplicationErrorOptions{NonRetryable: nonRetryableBoundary(boundary)})
}

func nonRetryableBoundary(err error) bool {
	var safe interface {
		Diagnostic() diagnostics.DomainFailure
	}
	if errors.As(err, &safe) {
		switch safe.Diagnostic().Category {
		case diagnostics.Validation, diagnostics.Configuration, diagnostics.Authentication:
			return true
		}
	}
	return false
}

type Registrar interface {
	RegisterWorkflowWithOptions(interface{}, workflow.RegisterOptions)
	RegisterActivityWithOptions(interface{}, activity.RegisterOptions)
}

func Register(registrar Registrar, activities *Activities) {
	registrar.RegisterWorkflowWithOptions(RolloutWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	registrar.RegisterActivityWithOptions(activities.Step, activity.RegisterOptions{Name: StepActivityName})
	registrar.RegisterActivityWithOptions(activities.Cleanup, activity.RegisterOptions{Name: CleanupActivityName})
}
