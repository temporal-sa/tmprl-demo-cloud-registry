package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/simulation"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func realEnvironment(t *testing.T, w *simulation.World, hook func(engine.Request, engine.Result, error) (engine.Result, error)) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(w.Now())
	env.SetTestTimeout(5 * time.Second)
	Register(env, &Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		w.SetTime(env.Now())
		executor, err := engine.New(w.Context(), w.Writer(), w.Capabilities())
		if err != nil {
			return nil, err
		}
		return stepperFunc(func(ctx context.Context, r engine.Request) (engine.Result, error) {
			result, err := executor.Step(ctx, r)
			if hook != nil {
				return hook(r, result, err)
			}
			return result, err
		}), nil
	}, Now: w.Now})
	return env
}
func supersessionInput() (Input, rollout.Desired) {
	r := simulation.Request()
	next := r.Desired
	next.Generation++
	next.ReconcileKey = "newer-intent"
	next.Revision = "git-ccccccc"
	return Input{SchemaVersion: SchemaVersion, Subject: r.Desired.Subject, ProjectName: r.ProjectName, Initial: &r.Desired, PollInterval: time.Millisecond}, next
}
func TestRealEngineQueuedIntentDrainsOneWorldAtEachFailureSeam(t *testing.T) {
	for _, tc := range []struct {
		name        string
		scenario    simulation.Scenario
		after       rollout.ActionKind
		disposition engine.AttemptDisposition
		cause       string
	}{
		{"creation", simulation.Success, rollout.EnsureCandidate, engine.AttemptDeleted, "RolloutSuperseded"},
		{"readiness", simulation.ReadinessFailure, rollout.ObserveReadiness, engine.AttemptDeleted, "ImagePullUnauthorized"},
		{"evidence", simulation.EvidenceFailure, rollout.CaptureEvidence, engine.AttemptRetained, "ImagePullUnauthorized"},
		{"cleanup", simulation.CleanupFailure, rollout.DeleteCandidate, engine.AttemptRetained, "ImagePullUnauthorized"},
		{"became active", simulation.Success, rollout.PromoteCandidate, engine.AttemptActive, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, err := simulation.New(tc.scenario, true)
			if err != nil {
				t.Fatal(err)
			}
			input, next := supersessionInput()
			oldID := OperationID(input.Subject, identify(*input.Initial), 1)
			var env *testsuite.TestWorkflowEnvironment
			var mutex sync.Mutex
			queued := false
			newCalls := 0
			env = realEnvironment(t, w, func(r engine.Request, result engine.Result, err error) (engine.Result, error) {
				mutex.Lock()
				defer mutex.Unlock()
				if !r.Supersede && r.Desired.Generation == next.Generation {
					newCalls++
					if _, ok := w.Supersession(oldID); !ok {
						t.Error("new activity ran before durable old attempt ACK")
					}
					env.SignalWorkflow(ShutdownSignal, nil)
				}
				if !queued && !r.Supersede && result.Plan.Next != nil && result.Plan.Next.Kind == tc.after {
					queued = true
					env.SignalWorkflow(ReconcileSignal, next)
				}
				return result, err
			})
			env.ExecuteWorkflow(WorkflowName, input)
			status := finalStatus(t, env)
			old, ok := w.Supersession(oldID)
			if !queued || newCalls != 1 || !ok || old.Disposition != tc.disposition || status.SupersessionBlocked || status.Projection.Identity.Generation != next.Generation {
				t.Fatalf("handoff failed: queued=%v calls=%d archived=%v disposition=%s status=%+v", queued, newCalls, ok, old.Disposition, status)
			}
			if tc.cause != "" && (old.Projection.Failure == nil || old.Projection.Failure.Code != tc.cause) {
				t.Fatal("old failure/evidence root was lost")
			}
			if tc.disposition == engine.AttemptActive && w.ActiveRevision() != input.Initial.Revision || tc.disposition != engine.AttemptActive && w.ActiveRevision() != "git-aaaaaaa" {
				t.Fatal("drain changed active traffic")
			}
			if tc.disposition == engine.AttemptRetained && !w.CandidateForAttempt(oldID) {
				t.Fatal("retained previous candidate binding overwritten")
			}
		})
	}
}
func TestRealEngineUnknownOutcomeBlocksUntilExplicitRepairAndRetry(t *testing.T) {
	w, err := simulation.New(simulation.Success, true)
	if err != nil {
		t.Fatal(err)
	}
	input, next := supersessionInput()
	oldID := OperationID(input.Subject, identify(*input.Initial), 1)
	var env *testsuite.TestWorkflowEnvironment
	var mutex sync.Mutex
	injected := false
	newCalls := 0
	env = realEnvironment(t, w, func(r engine.Request, result engine.Result, err error) (engine.Result, error) {
		mutex.Lock()
		defer mutex.Unlock()
		if !r.Supersede && r.Desired.Generation == next.Generation {
			newCalls++
			if _, ok := w.Supersession(oldID); !ok {
				t.Error("new operation bypassed unknown write")
			}
			env.SignalWorkflow(ShutdownSignal, nil)
		}
		if !injected && !r.Supersede && result.Plan.Next != nil && result.Plan.Next.Kind == rollout.EnsureCandidate {
			injected = true
			w.SetUnknownWrite(oldID, true)
			env.SignalWorkflow(ReconcileSignal, next)
			return result, errors.New("unknown-provider-result-private-sentinel")
		}
		return result, err
	})
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(StatusQuery)
		if err != nil {
			t.Error(err)
			return
		}
		var status Status
		if value.Get(&status) != nil || !status.ExecutionBlocked || !status.SupersessionBlocked || status.BlockedNextAction == "" {
			t.Error("unknown outcome was not visibly blocked")
		}
		if _, ok := w.Supersession(oldID); ok {
			t.Error("unknown outcome got settlement ACK")
		}
		if newCalls != 0 {
			t.Error("new generation executed before repair")
		}
	}, 10*time.Second)
	env.RegisterDelayedCallback(func() { w.SetUnknownWrite(oldID, false); env.SignalWorkflow(ReconcileSignal, next) }, 20*time.Second)
	env.ExecuteWorkflow(WorkflowName, input)
	status := finalStatus(t, env)
	if newCalls != 1 || status.SupersessionBlocked || status.ExecutionBlocked || w.ActiveRevision() != "git-aaaaaaa" {
		t.Fatal("repair did not safely resume pending intent")
	}
}
func TestRealEngineContinueAsNewCarriesDurableOldSettlementBeforeNewAttempt(t *testing.T) {
	w, err := simulation.New(simulation.Success, true)
	if err != nil {
		t.Fatal(err)
	}
	input, next := supersessionInput()
	input.MaxCompleted = 1
	var env *testsuite.TestWorkflowEnvironment
	queued := false
	env = realEnvironment(t, w, func(r engine.Request, result engine.Result, err error) (engine.Result, error) {
		if !queued && !r.Supersede && result.Plan.Next != nil && result.Plan.Next.Kind == rollout.EnsureCandidate {
			queued = true
			env.SignalWorkflow(ReconcileSignal, next)
		}
		return result, err
	})
	env.ExecuteWorkflow(WorkflowName, input)
	var rollover *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &rollover) {
		t.Fatalf("expected real-engine rollover: %v", env.GetWorkflowError())
	}
	var carried Input
	if converter.GetDefaultDataConverter().FromPayloads(rollover.Input, &carried) != nil || carried.Carry == nil || carried.Carry.Pending == nil || carried.Carry.LastDrainedOperation == "" || carried.Carry.Current != nil || carried.Carry.NextAttempt != 1 {
		t.Fatal("rollover lost old ACK or pending intent")
	}
	oldID := carried.Carry.LastDrainedOperation
	if _, ok := w.Supersession(oldID); !ok {
		t.Fatal("history carry replaced durable ACK")
	}
	var resumed *testsuite.TestWorkflowEnvironment
	resumed = realEnvironment(t, w, func(r engine.Request, result engine.Result, err error) (engine.Result, error) {
		if r.Supersede {
			t.Error("confirmed supersession repeated after restart")
		}
		resumed.SignalWorkflow(ShutdownSignal, nil)
		return result, err
	})
	resumed.ExecuteWorkflow(WorkflowName, carried)
	status := finalStatus(t, resumed)
	if status.Projection.Identity.Generation != next.Generation || status.Projection.Identity.OperationID == oldID || !w.CandidateForAttempt(status.Projection.Identity.OperationID) {
		t.Fatal("resumed attempt reused old runtime binding")
	}
}
func TestMissingSupersessionCapabilityCannotClaimCompletion(t *testing.T) {
	r := engine.Request{Desired: testIntent(1, "one"), ProjectName: "example", OperationID: "operation", Supersede: true}
	a := Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		return stepperFunc(func(context.Context, engine.Request) (engine.Result, error) {
			return resultFor(r, diagnostics.Cancelled, true), nil
		}), nil
	}}
	if _, err := a.Step(context.Background(), r); err == nil {
		t.Fatal("plain terminal status replaced durable drain ACK")
	}
}

func TestConflictingDuplicateCannotChangeAcceptedIntentOrTriggerRetry(t *testing.T) {
	calls := 0
	env := testEnvironment(t, func(_ context.Context, r engine.Request) (engine.Result, error) {
		calls++
		return resultFor(r, diagnostics.Running, false), nil
	}, nil)
	env.RegisterDelayedCallback(func() {
		conflict := testIntent(1, "one")
		conflict.RequireSmoke = true
		env.SignalWorkflow(ReconcileSignal, conflict)
	}, 100*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(ShutdownSignal, nil) }, 200*time.Millisecond)
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	status := finalStatus(t, env)
	if calls != 1 || status.LastRejected == nil {
		t.Fatal("conflicting duplicate was silently accepted or triggered work")
	}
}

func TestIncompleteSupersessionPollsWithoutDuplicateSignalAcceleration(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(testTime)
	env.SetTestTimeout(5 * time.Second)
	var mutex sync.Mutex
	var polls []time.Time
	Register(env, &Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		return stepperFunc(func(_ context.Context, r engine.Request) (engine.Result, error) {
			mutex.Lock()
			defer mutex.Unlock()
			if r.Supersede {
				polls = append(polls, env.Now())
				complete := len(polls) == 3
				result := resultFor(r, diagnostics.Running, false)
				if complete {
					result = resultFor(r, diagnostics.Cancelled, true)
					result.SupersessionComplete = true
				}
				return result, nil
			}
			if r.Desired.Generation == 1 {
				env.SignalWorkflow(ReconcileSignal, testIntent(2, "two"))
				return resultFor(r, diagnostics.Running, false), nil
			}
			env.SignalWorkflow(ShutdownSignal, nil)
			return resultFor(r, diagnostics.Succeeded, true), nil
		}), nil
	}, Now: func() time.Time { return testTime }})
	for _, delay := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond} {
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(ReconcileSignal, testIntent(2, "two")) }, delay)
	}
	env.ExecuteWorkflow(WorkflowName, actorInput(testIntent(1, "one")))
	_ = finalStatus(t, env)
	if len(polls) != 3 || polls[1].Sub(polls[0]) < time.Second || polls[2].Sub(polls[1]) < time.Second {
		t.Fatalf("incomplete drain bypassed poll interval: %v", polls)
	}
}
