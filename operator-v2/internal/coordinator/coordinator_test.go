package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

var testTime = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

type stepperFunc func(context.Context, engine.Request) (engine.Result, error)

func (f stepperFunc) Step(ctx context.Context, r engine.Request) (engine.Result, error) {
	return f(ctx, r)
}

func testIntent(generation int64, key string) rollout.Desired {
	return rollout.Desired{Subject: rollout.Subject{Environment: "lab", Installation: "test-install", ProjectUID: "project-uid"}, Generation: generation, ReconcileKey: key, Revision: "git-abcdef1", RequiredBuilds: []string{"web"}, Images: map[string]string{"web": "example.invalid/web:git-abcdef1"}}
}
func actorInput(d rollout.Desired) Input {
	return Input{SchemaVersion: SchemaVersion, Subject: d.Subject, ProjectName: "example", Initial: &d, PollInterval: time.Second, MaxCompleted: 50}
}

func resultFor(r engine.Request, state diagnostics.State, terminal bool) engine.Result {
	p, _ := diagnostics.NewProjection(diagnostics.Identity{Environment: r.Desired.Subject.Environment, Installation: r.Desired.Subject.Installation, ProjectUID: r.Desired.Subject.ProjectUID, ProjectName: r.ProjectName, OperationID: r.OperationID, ReconcileKey: r.Desired.ReconcileKey, Generation: r.Desired.Generation})
	if r.Projection != nil {
		p = *r.Projection
	}
	event := diagnostics.OperationEvent{Identity: p.Identity, Sequence: p.Sequence + 1, OccurredAt: testTime, State: state, Step: "test-step"}
	if state == diagnostics.Failed {
		f := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: "candidate-failure", Code: "CandidateNotReady", Category: diagnostics.Readiness, Message: "The test candidate did not become ready.", OccurredAt: testTime})
		event.Failure = &f
	}
	p, _ = p.Apply(event)
	return engine.Result{Projection: p, Event: event, Terminal: terminal}
}

func testEnvironment(t *testing.T, step stepperFunc, cleanup CleanupFunc) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(testTime)
	env.SetTestTimeout(5 * time.Second)
	// Synthetic SDK-control-flow tests acknowledge drain explicitly. Real drain
	// authority/provider behavior is covered by one-World engine tests separately.
	Register(env, &Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		return stepperFunc(func(ctx context.Context, r engine.Request) (engine.Result, error) {
			if r.Supersede {
				result := resultFor(r, diagnostics.Cancelled, true)
				result.SupersessionComplete = true
				if r.Projection != nil {
					result.Projection.Retry = r.Projection.Retry
				}
				return result, nil
			}
			return step(ctx, r)
		}), nil
	}, CleanupOperation: cleanup, Now: func() time.Time { return testTime }})
	return env
}

func finalStatus(t *testing.T, env *testsuite.TestWorkflowEnvironment) Status {
	t.Helper()
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var status Status
	if err := env.GetWorkflowResult(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestSuccessSettlesAndRemainsAvailableUntilShutdown(t *testing.T) {
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		return resultFor(r, diagnostics.Succeeded, true), nil
	}, nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 10*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if !status.Stopped || status.Projection == nil || status.Projection.State != diagnostics.Succeeded {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestTerminalFailureHasNativeErrorAndAcceptsNextIntent(t *testing.T) {
	var mutex sync.Mutex
	calls := 0
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		mutex.Lock()
		calls++
		mutex.Unlock()
		if r.Desired.Generation == 1 {
			return resultFor(r, diagnostics.Failed, true), nil
		}
		return resultFor(r, diagnostics.Succeeded, true), nil
	}, nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(2, "two")) }, 10*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 20*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if calls != 2 || status.Projection.State != diagnostics.Succeeded || status.PreviousFailure == nil || status.PreviousFailure.Failure.Code != "CandidateNotReady" {
		t.Fatalf("failure/recovery lost: calls=%d %+v", calls, status)
	}
	if status.PreviousFailure.Retry.Status != diagnostics.RetryNotRetryable {
		t.Fatal("terminal failure should not be automatically retried")
	}
}

func TestDuplicateAndRejectedSignalsDoNotAcceleratePolling(t *testing.T) {
	var mutex sync.Mutex
	var calls []engine.Request
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		mutex.Lock()
		calls = append(calls, r)
		mutex.Unlock()
		return resultFor(r, diagnostics.Running, r.Desired.Generation == 2), nil
	}, nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(1, "one")) }, 100*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(0, "stale")) }, 200*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		foreign := testIntent(2, "foreign")
		foreign.Subject.ProjectUID = "foreign"
		env.SignalWorkflow(ReconcileSignal, foreign)
	}, 300*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(2, "two")) }, 400*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 500*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if len(calls) != 2 || calls[0].Desired.Generation != 1 || calls[1].Desired.Generation != 2 || status.LastRejected == nil {
		t.Fatalf("duplicate/stale signal triggered work: %+v", calls)
	}
}

func TestRevisitingSourceUsesNewDurableAttemptIdentity(t *testing.T) {
	var mutex sync.Mutex
	var operations []string
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		mutex.Lock()
		operations = append(operations, r.OperationID)
		mutex.Unlock()
		return resultFor(r, diagnostics.Succeeded, true), nil
	}, nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(1, "second-source")) }, 10*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(1, "first-source")) }, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 30*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "first-source")))
	_ = finalStatus(t, env)
	if len(operations) != 3 || operations[0] == operations[2] {
		t.Fatalf("revisited source reused a runtime namespace attempt: %v", operations)
	}
}

func TestExhaustedActivityRetriesPreserveSafeTerminalDiagnosis(t *testing.T) {
	var mutex sync.Mutex
	var operations []string
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		mutex.Lock()
		operations = append(operations, r.OperationID)
		mutex.Unlock()
		return engine.Result{}, errors.New("unlabeled-private-token-sentinel")
	}, nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 10*time.Second)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if len(operations) != 3 || operations[0] != operations[1] || operations[1] != operations[2] {
		t.Fatalf("retry bounds/identity incorrect: %v", operations)
	}
	encoded, _ := json.Marshal(status)
	if strings.Contains(string(encoded), "unlabeled-private-token-sentinel") || status.Projection == nil || status.Projection.State != diagnostics.Failed {
		t.Fatalf("unsafe or missing failure status: %s", encoded)
	}
}

func TestContinueAsNewCarriesPendingSignalAndAttemptState(t *testing.T) {
	var env *testsuite.TestWorkflowEnvironment
	env = testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		env.SignalWorkflow(ReconcileSignal, testIntent(2, "queued"))
		return resultFor(r, diagnostics.Succeeded, true), nil
	}, nil)
	input := actorInput(testIntent(1, "one"))
	input.MaxCompleted = 1
	env.ExecuteWorkflow(WorkflowName, input)
	var rollover *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &rollover) {
		t.Fatalf("expected rollover, got %v", env.GetWorkflowError())
	}
	var next Input
	if err := converter.GetDefaultDataConverter().FromPayloads(rollover.Input, &next); err != nil {
		t.Fatal(err)
	}
	if next.Initial != nil || next.Carry == nil || next.Carry.Pending == nil || next.Carry.Pending.ReconcileKey != "queued" || next.Carry.NextAttempt != 1 || next.Carry.LastAttempt != 1 || next.Carry.Completed != 0 {
		t.Fatalf("rollover lost queued intent/provenance: %+v", next.Carry)
	}
}

func TestCancellationPersistsCleanupOutcomeWithoutOriginalWorker(t *testing.T) {
	var mutex sync.Mutex
	var reasons []CleanupReason
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		return resultFor(r, diagnostics.Running, false), nil
	}, func(_ context.Context, r CleanupRequest) (CleanupResult, error) {
		mutex.Lock()
		reasons = append(reasons, r.Reason)
		mutex.Unlock()
		return CleanupResult{Complete: true}, nil
	})
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 100*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	var canceled *temporal.CanceledError
	if !errors.As(env.GetWorkflowError(), &canceled) {
		t.Fatalf("expected cancellation, got %v", env.GetWorkflowError())
	}
	var status Status
	if err := canceled.Details(&status); err != nil {
		t.Fatal(err)
	}
	if len(reasons) != 1 || reasons[0] != CancelRollout || status.Projection.State != diagnostics.Cancelled || !status.Stopped {
		t.Fatalf("cancellation cleanup/details lost: %+v %v", status, reasons)
	}
}

func TestFinalizeFailureRetainsStateAndAllowsExplicitRetry(t *testing.T) {
	var mutex sync.Mutex
	attempts := 0
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		return resultFor(r, diagnostics.Succeeded, true), nil
	}, func(_ context.Context, r CleanupRequest) (CleanupResult, error) {
		mutex.Lock()
		defer mutex.Unlock()
		attempts++
		if r.Reason != FinalizeProject {
			return CleanupResult{}, errors.New("wrong cleanup reason")
		}
		if attempts == 1 {
			return CleanupResult{}, diagnostics.NewError(diagnostics.DomainFailure{ID: "cleanup-denied", Code: "CleanupDenied", Category: diagnostics.Authentication, Message: "Cleanup permission was denied.", OccurredAt: testTime})
		}
		return CleanupResult{Complete: true}, nil
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(FinalizeSignal, nil) }, 10*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(FinalizeSignal, nil) }, 20*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if attempts != 2 || !status.Finalized || status.Finalizing || status.CleanupFailure != nil {
		t.Fatalf("finalization did not recover: %+v attempts=%d", status, attempts)
	}
}

func TestActivityBoundaryRejectsForeignProjectionAndOpaqueErrors(t *testing.T) {
	r := engine.Request{Desired: testIntent(1, "one"), ProjectName: "example", OperationID: "test-operation"}
	a := Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		return stepperFunc(func(_ context.Context, r engine.Request) (engine.Result, error) {
			result := resultFor(r, diagnostics.Succeeded, true)
			result.Projection.Identity.ProjectUID = "foreign"
			return result, nil
		}), nil
	}}
	_, err := a.Step(context.Background(), r)
	var application *temporal.ApplicationError
	if !errors.As(err, &application) || !application.NonRetryable() {
		t.Fatalf("foreign projection not rejected: %v", err)
	}
	a.NewEngine = func(context.Context, engine.Request) (Stepper, error) {
		return nil, errors.New("unlabeled-private-token-sentinel")
	}
	_, err = a.Step(context.Background(), r)
	if err == nil || strings.Contains(err.Error(), "unlabeled-private-token-sentinel") {
		t.Fatal("raw provider/factory error crossed durable boundary")
	}
}

func TestWaitingOutcomeIsNotMistakenForCompletedIntent(t *testing.T) {
	var mutex sync.Mutex
	calls := 0
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		mutex.Lock()
		defer mutex.Unlock()
		calls++
		if calls < 3 {
			result := resultFor(r, diagnostics.Waiting, true)
			result.Plan.Phase = rollout.WaitingForExternal
			return result, nil
		}
		return resultFor(r, diagnostics.Succeeded, true), nil
	}, nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 3*time.Second)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if calls != 3 || status.Projection.State != diagnostics.Succeeded {
		t.Fatalf("blocked intent was settled prematurely: calls=%d status=%+v", calls, status)
	}
}

func TestFinalizingBeforeFirstProvisionStillAcknowledgesCleanup(t *testing.T) {
	called := false
	env := testEnvironment(t, func(context.Context, engine.Request) (engine.Result, error) {
		t.Error("rollout must not start during carried finalization")
		return engine.Result{}, errors.New("unexpected rollout")
	}, func(_ context.Context, r CleanupRequest) (CleanupResult, error) {
		called = true
		if r.Reason != FinalizeProject || r.Desired.Subject.ProjectUID != "project-uid" {
			t.Error("cleanup lost configured project identity")
		}
		return CleanupResult{Complete: true}, nil
	})
	desired := testIntent(1, "one")
	input := actorInput(desired)
	input.Initial = nil
	input.Carry = &Carry{Pending: &desired, Finalizing: true}
	env.ExecuteWorkflow(WorkflowName, input)
	status := finalStatus(t, env)
	if !called || !status.Finalized {
		t.Fatalf("unstarted project finalizer was ignored: %+v", status)
	}
}

func TestCancellationWithPendingCleanupIsExplicitlyRetained(t *testing.T) {
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		return resultFor(r, diagnostics.Running, false), nil
	}, func(context.Context, CleanupRequest) (CleanupResult, error) {
		return CleanupResult{Complete: false}, nil
	})
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 100*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	var canceled *temporal.CanceledError
	if !errors.As(env.GetWorkflowError(), &canceled) {
		t.Fatalf("expected cancellation, got %v", env.GetWorkflowError())
	}
	var status Status
	if err := canceled.Details(&status); err != nil {
		t.Fatal(err)
	}
	if !status.FinalizeBlocked || status.CleanupFailure == nil || status.Projection.State != diagnostics.Cancelled {
		t.Fatalf("pending cleanup was hidden: %+v", status)
	}
}
