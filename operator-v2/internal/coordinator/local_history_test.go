//go:build temporal_integration

package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/simulation"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

// This tier proves real server/SDK history behavior. Its fake provider journals
// survive worker replacement inside this process only, not a process crash.
func localClient(t *testing.T, index int) (context.Context, client.Client, string) {
	t.Helper()
	port := os.Getenv("OPERATOR_LOCAL_TEMPORAL_PORT")
	var namespaces []string
	if json.Unmarshal([]byte(os.Getenv("OPERATOR_LOCAL_TEMPORAL_NAMESPACES")), &namespaces) != nil || len(namespaces) <= index || !strings.HasPrefix(namespaces[index], "operator-local-") || port == "" {
		t.Fatal("Run through scripts/run_operator_temporal_tests.py --go; no ambient server fallback is allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	c, err := client.DialContext(ctx, client.Options{HostPort: net.JoinHostPort("127.0.0.1", port), Namespace: namespaces[index], Logger: log.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return ctx, c, namespaces[index]
}

func TestGoLocalNativeBoundary(t *testing.T) {
	ctx, c, _ := localClient(t, 0)
	if _, err := c.CheckHealth(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"127.0.0.1:9", "192.0.2.1:443", "[2001:db8::1]:443"} {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if conn != nil {
			conn.Close()
		}
		if !errors.Is(err, syscall.EPERM) && !errors.Is(err, syscall.EACCES) {
			t.Fatalf("OS permission denial not proven for %s: %v", address, err)
		}
	}
	if _, err := os.ReadFile(filepath.Join(os.Getenv("OPERATOR_BASELINE_TEMP"), "credential-store", "credentials")); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("credential boundary not enforced: %v", err)
	}
	if err := os.WriteFile(os.Getenv("OPERATOR_BASELINE_WRITE_PROBE"), []byte("changed"), 0600); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("write boundary not enforced: %v", err)
	}
	if err := exec.Command("/usr/bin/true").Run(); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("child process boundary not enforced: %v", err)
	}
}

func startLocalWorker(t *testing.T, c client.Client, queue string, a *Activities) worker.Worker {
	t.Helper()
	w := worker.New(c, queue, worker.Options{DisableRegistrationAliasing: true})
	Register(w, a)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	return w
}

func awaitLocalStatus(t *testing.T, ctx context.Context, c client.Client, id string, predicate func(Status) bool) Status {
	t.Helper()
	for {
		value, err := c.QueryWorkflow(ctx, id, "", StatusQuery)
		if err == nil {
			var status Status
			if err = value.Get(&status); err != nil {
				t.Fatal(err)
			}
			if predicate(status) {
				return status
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("status predicate did not become true: %v (last query %v)", ctx.Err(), err)
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func replayLocalHistory(t *testing.T, ctx context.Context, c client.Client, id, run string) *historypb.History {
	t.Helper()
	history := &historypb.History{}
	iterator := c.GetWorkflowHistory(ctx, id, run, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		history.Events = append(history.Events, event)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RolloutWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		t.Fatal(err)
	}
	return history
}

func TestGoLocalFailureWorkerRestartQueuedReconcileAndReplay(t *testing.T) {
	ctx, c, namespace := localClient(t, 1)
	failed, _ := simulation.New(simulation.ReadinessFailure, true)
	healthy, _ := simulation.New(simulation.Success, true)
	var recoveredCalls atomic.Int32
	a := &Activities{NewEngine: func(_ context.Context, request engine.Request) (Stepper, error) {
		world := failed
		if request.Desired.Generation == 3 {
			world = healthy
			recoveredCalls.Add(1)
		}
		world.SetTime(time.Now().UTC())
		return engine.New(world.Context(), world.Writer(), world.Capabilities())
	}}
	queue, id := namespace+"-queue", namespace+"-rollout"
	w := startLocalWorker(t, c, queue, a)
	defer func() {
		if w != nil {
			w.Stop()
		}
	}()
	request := simulation.Request()
	input := Input{SchemaVersion: SchemaVersion, Subject: request.Desired.Subject, ProjectName: request.ProjectName, Initial: &request.Desired, PollInterval: 20 * time.Millisecond}
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, WorkflowName, input)
	if err != nil {
		t.Fatal(err)
	}
	status := awaitLocalStatus(t, ctx, c, id, func(s Status) bool {
		return s.Projection != nil && s.Projection.State == diagnostics.Failed && s.PreviousFailure != nil
	})
	if status.Projection.Failure == nil || !status.Projection.Retry.Authoritative || status.Projection.Retry.Status != diagnostics.RetryNotRetryable || failed.CandidateExists() {
		encoded, _ := json.Marshal(status)
		t.Fatalf("missing terminal failure or confirmed cleanup: %s; candidate exists %v", encoded, failed.CandidateExists())
	}
	w.Stop()
	w = nil
	next := request.Desired
	next.Generation, next.ReconcileKey = 3, "queued-recovery"
	for i := 0; i < 2; i++ {
		if err := c.SignalWorkflow(ctx, id, "", ReconcileSignal, next); err != nil {
			t.Fatal(err)
		}
	}
	if recoveredCalls.Load() != 0 {
		t.Fatal("work executed without a worker")
	}
	w = startLocalWorker(t, c, queue, a)
	status = awaitLocalStatus(t, ctx, c, id, func(s Status) bool {
		return s.Projection != nil && s.Projection.Identity.Generation == 3 && s.Projection.State == diagnostics.Succeeded
	})
	if status.PreviousFailure == nil || status.PreviousFailure.Failure == nil || status.PreviousFailure.Identity.OperationID == status.Projection.Identity.OperationID {
		t.Fatal("restart lost original diagnosis or reused attempt identity")
	}
	if err := c.SignalWorkflow(ctx, id, "", ShutdownSignal, nil); err != nil {
		t.Fatal(err)
	}
	var terminal Status
	if err := run.Get(ctx, &terminal); err != nil {
		t.Fatal(err)
	}
	w.Stop()
	w = nil
	// A completed actor's safe result remains readable with all workers retired.
	if err := c.GetWorkflow(ctx, id, run.GetRunID()).Get(ctx, &terminal); err != nil || !terminal.Stopped {
		t.Fatalf("retired-worker status unavailable: %v", err)
	}
	history := replayLocalHistory(t, ctx, c, id, run.GetRunID())
	nativeFailure := false
	for _, event := range history.Events {
		if event.GetActivityTaskFailedEventAttributes().GetFailure().GetApplicationFailureInfo().GetType() == "RolloutFailed" {
			nativeFailure = true
		}
	}
	if !nativeFailure {
		t.Fatal("safe rollout failure absent from native activity history")
	}
}

func TestGoLocalSupersessionUnknownWriteWorkerReplacementAndReplay(t *testing.T) {
	ctx, c, namespace := localClient(t, 1)
	world, err := simulation.New(simulation.Success, true)
	if err != nil {
		t.Fatal(err)
	}
	request := simulation.Request()
	next := request.Desired
	next.Generation++
	next.ReconcileKey = "newer-after-unknown"
	next.Revision = "git-ccccccc"
	oldID := OperationID(request.Desired.Subject, identify(request.Desired), 1)
	queue, id := namespace+"-supersession-queue", namespace+"-supersession"
	var injected atomic.Bool
	var newCalls atomic.Int32
	a := &Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		world.SetTime(time.Now().UTC())
		executor, err := engine.New(world.Context(), world.Writer(), world.Capabilities())
		if err != nil {
			return nil, err
		}
		return stepperFunc(func(activityCtx context.Context, r engine.Request) (engine.Result, error) {
			if !r.Supersede && r.Desired.Generation == request.Desired.Generation && !injected.Load() {
				world.FailNextRecord = true
			}
			result, stepErr := executor.Step(activityCtx, r)
			if !r.Supersede && r.Desired.Generation == request.Desired.Generation && result.Plan.Next != nil && result.Plan.Next.Kind == rollout.EnsureCandidate && injected.CompareAndSwap(false, true) {
				world.SetUnknownWrite(oldID, true)
				if err := c.SignalWorkflow(activityCtx, id, "", ReconcileSignal, next); err != nil {
					return result, err
				}
				return result, errors.New("unknown-create-private-provider-sentinel")
			}
			if !r.Supersede && r.Desired.Generation == next.Generation {
				newCalls.Add(1)
				if _, ok := world.Supersession(oldID); !ok {
					return result, errors.New("new attempt bypassed old settlement")
				}
				if err := c.SignalWorkflow(activityCtx, id, "", ShutdownSignal, nil); err != nil {
					return result, err
				}
			}
			return result, stepErr
		}), nil
	}}
	w := startLocalWorker(t, c, queue, a)
	defer func() {
		if w != nil {
			w.Stop()
		}
	}()
	input := Input{SchemaVersion: SchemaVersion, Subject: request.Desired.Subject, ProjectName: request.ProjectName, Initial: &request.Desired, PollInterval: 20 * time.Millisecond}
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, WorkflowName, input)
	if err != nil {
		t.Fatal(err)
	}
	blocked := awaitLocalStatus(t, ctx, c, id, func(s Status) bool { return s.ExecutionBlocked && s.SupersessionBlocked })
	if blocked.BlockedNextAction == "" || newCalls.Load() != 0 || !world.CandidateForAttempt(oldID) || world.ActiveRevision() != "git-aaaaaaa" {
		t.Fatal("unknown old create did not block handoff")
	}
	w.Stop()
	w = nil
	// The test fixture explicitly supplies the independent resolved-write fact;
	// real adapters must prove it through their durable provider/write guard.
	world.SetUnknownWrite(oldID, false)
	if err = c.SignalWorkflow(ctx, id, "", ReconcileSignal, next); err != nil {
		t.Fatal(err)
	}
	if newCalls.Load() != 0 {
		t.Fatal("new attempt executed with no worker")
	}
	w = startLocalWorker(t, c, queue, a)
	var terminal Status
	if err = run.Get(ctx, &terminal); err != nil {
		t.Fatal(err)
	}
	archived, ok := world.Supersession(oldID)
	if !ok || archived.Disposition != engine.AttemptDeleted || newCalls.Load() != 1 || world.CandidateForAttempt(oldID) || !world.CandidateForAttempt(terminal.Projection.Identity.OperationID) || world.ActiveRevision() != "git-aaaaaaa" {
		t.Fatal("restart lost old cleanup or overwrote a candidate binding")
	}
	encoded, _ := json.Marshal(terminal)
	if strings.Contains(string(encoded), "private-provider-sentinel") || terminal.PreviousFailure == nil {
		t.Fatal("unsafe or missing previous diagnosis")
	}
	w.Stop()
	w = nil
	replayLocalHistory(t, ctx, c, id, run.GetRunID())
}

func TestGoLocalCancellationRetainsSafeCleanupDetailsAndReplay(t *testing.T) {
	ctx, c, namespace := localClient(t, 2)
	var cleanupCalls atomic.Int32
	a := &Activities{NewEngine: func(context.Context, engine.Request) (Stepper, error) {
		return stepperFunc(func(_ context.Context, r engine.Request) (engine.Result, error) {
			return resultFor(r, diagnostics.Running, false), nil
		}), nil
	}, CleanupOperation: func(context.Context, CleanupRequest) (CleanupResult, error) {
		cleanupCalls.Add(1)
		return CleanupResult{}, fmt.Errorf("unlabeled-private-cleanup-sentinel")
	}}
	queue, id := namespace+"-queue", namespace+"-cancel"
	w := startLocalWorker(t, c, queue, a)
	defer func() {
		if w != nil {
			w.Stop()
		}
	}()
	input := actorInput(testIntent(1, "cancel-one"))
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, WorkflowName, input)
	if err != nil {
		t.Fatal(err)
	}
	awaitLocalStatus(t, ctx, c, id, func(s Status) bool { return s.Projection != nil })
	if err := c.CancelWorkflow(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	err = run.Get(ctx, nil)
	w.Stop()
	w = nil
	var canceled *temporal.CanceledError
	if !errors.As(err, &canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	var status Status
	if err := canceled.Details(&status); err != nil {
		t.Fatal(err)
	}
	if !status.Stopped || !status.FinalizeBlocked || status.CleanupFailure == nil || status.Projection.State != diagnostics.Cancelled || cleanupCalls.Load() != 3 {
		t.Fatalf("cleanup outcome lost: %+v (attempts %d)", status, cleanupCalls.Load())
	}
	// Retrieve cancellation details again after worker retirement.
	err = c.GetWorkflow(ctx, id, run.GetRunID()).Get(ctx, nil)
	if !errors.As(err, &canceled) || canceled.Details(&status) != nil {
		t.Fatal("safe terminal cancellation details need the original worker")
	}
	history := replayLocalHistory(t, ctx, c, id, run.GetRunID())
	encoded, err := protojson.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "unlabeled-private-cleanup-sentinel") {
		t.Fatal("raw provider cause leaked into history")
	}
}
