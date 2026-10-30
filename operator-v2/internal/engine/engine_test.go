package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/simulation"
)

func setup(t *testing.T, scenario simulation.Scenario, active bool) (*simulation.World, *engine.Engine, engine.Request) {
	t.Helper()
	w, err := simulation.New(scenario, active)
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(w.Context(), w.Writer(), w.Capabilities())
	if err != nil {
		t.Fatal(err)
	}
	return w, e, simulation.Request()
}

func advanceTo(t *testing.T, w *simulation.World, e *engine.Engine, request engine.Request, kind rollout.ActionKind) engine.Request {
	t.Helper()
	for i := 0; i < 20; i++ {
		snapshot, err := w.Snapshot(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := rollout.Compute(request.Desired, snapshot.State)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Next != nil && plan.Next.Kind == kind {
			return request
		}
		result, err := e.Step(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if result.Terminal {
			t.Fatalf("terminated before %s", kind)
		}
		request.Projection = &result.Projection
	}
	t.Fatalf("never reached %s", kind)
	return request
}

func finish(t *testing.T, e *engine.Engine, request engine.Request) engine.Result {
	t.Helper()
	for i := 0; i < 24; i++ {
		result, err := e.Step(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if result.Terminal {
			return result
		}
		request.Projection = &result.Projection
	}
	t.Fatal("bounded execution did not settle")
	return engine.Result{}
}

func count(trace []string, value string) int {
	n := 0
	for _, entry := range trace {
		if entry == value {
			n++
		}
	}
	return n
}
func index(trace []string, prefix string) int {
	for i, entry := range trace {
		if strings.HasPrefix(entry, prefix) {
			return i
		}
	}
	return -1
}

func TestLocalScenariosPreserveServingAndOriginalFailure(t *testing.T) {
	for _, scenario := range []simulation.Scenario{simulation.Success, simulation.ReadinessFailure, simulation.SmokeFailure, simulation.EvidenceFailure, simulation.CleanupFailure} {
		t.Run(string(scenario), func(t *testing.T) {
			out, err := simulation.Run(context.Background(), scenario)
			if err != nil {
				t.Fatal(err)
			}
			if !out.LocalSimulation || !out.FakeProviders || out.Steps > 24 {
				t.Fatal("simulation claims real execution or unbounded run")
			}
			if scenario == simulation.Success {
				if out.Diagnostics.State != diagnostics.Succeeded || out.Diagnostics.Availability.RouteState != diagnostics.RoutePromoted || out.Diagnostics.Availability.ActiveRevision != "git-bbbbbbb" {
					t.Fatal("successful candidate was not conditionally promoted")
				}
				if count(out.Trace, "update:route") != 1 {
					t.Fatal("successful route written more than once")
				}
			} else {
				if out.Diagnostics.State != diagnostics.Failed || out.Diagnostics.Availability.ActiveRevision != "git-aaaaaaa" || out.Diagnostics.Availability.RouteState != diagnostics.RouteActivePreserved {
					t.Fatal("failed candidate affected healthy route")
				}
				if count(out.Trace, "update:route") != 0 || count(out.Trace, "create:route") != 0 {
					t.Fatal("failed candidate repointed route")
				}
				want := "ImagePullUnauthorized"
				if scenario == simulation.SmokeFailure {
					want = "SmokeCheckFailed"
				}
				if out.Diagnostics.Failure == nil || out.Diagnostics.Failure.Code != want {
					t.Fatal("cleanup/evidence replaced original cause")
				}
				if out.CandidateRetained != (scenario == simulation.EvidenceFailure || scenario == simulation.CleanupFailure) {
					t.Fatal("candidate retention does not match recovery outcome")
				}
				if !strings.Contains(diagnostics.Explain(out.Diagnostics), "Next:") {
					t.Fatal("no actionable human explanation")
				}
			}
			encoded, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(encoded), `"localSimulation":true`) {
				t.Fatal("safe JSON does not identify fake execution")
			}
		})
	}
}

func TestCreationIntentAcknowledgmentPrecedesProviderMutation(t *testing.T) {
	w, e, request := setup(t, simulation.Success, true)
	w.FailSaveIntent = true
	result, err := e.Step(context.Background(), request)
	if err == nil || w.CandidateExists() || count(w.Trace(), "create:candidate") != 0 {
		t.Fatal("provider mutation happened despite failed intent persistence")
	}
	w.FailSaveIntent = false
	request.Projection = &result.Projection
	finish(t, e, request)
	trace := w.Trace()
	if index(trace, "persist-intent:candidate") >= index(trace, "create:candidate") {
		t.Fatal("create preceded creation intent")
	}
	if count(trace, "create:candidate") != 1 {
		t.Fatal("retry created duplicate candidates")
	}
}

func TestUnknownCreateOutcomeRecoversUsingPersistedProvenance(t *testing.T) {
	w, e, request := setup(t, simulation.Success, true)
	w.FailNextRecord = true
	result, err := e.Step(context.Background(), request)
	if err == nil || !w.CandidateExists() {
		t.Fatal("failed to exercise create/ledger crash seam")
	}
	// A new executor instance shares the fake journal and provider. This tests
	// recovery ordering, not process durability of this in-memory implementation.
	restarted, err := engine.New(w.Context(), w.Writer(), w.Capabilities())
	if err != nil {
		t.Fatal(err)
	}
	request.Projection = &result.Projection
	terminal := finish(t, restarted, request)
	if terminal.Plan.Phase != rollout.Succeeded || count(w.Trace(), "create:candidate") != 1 {
		t.Fatal("recovery recreated the unknown-outcome object")
	}
}

func TestFailureOutcomeMustBeAcknowledgedBeforeDeletion(t *testing.T) {
	w, e, request := setup(t, simulation.ReadinessFailure, true)
	request = advanceTo(t, w, e, request, rollout.RecordFailureOutcome)
	w.FailFailureOutcome = true
	result, err := e.Step(context.Background(), request)
	if err == nil || !w.CandidateExists() || count(w.Trace(), "delete-attempt:candidate") != 0 {
		t.Fatal("candidate deletion escaped failed durable outcome acknowledgment")
	}
	if result.Projection.Failure == nil || result.Projection.Failure.Code != "ImagePullUnauthorized" || len(result.Projection.Recovery) == 0 || !strings.Contains(diagnostics.Explain(result.Projection), "storage unavailable") {
		t.Fatal("outcome persistence failure was lost or replaced the original cause")
	}
	w.FailFailureOutcome = false
	request.Projection = &result.Projection
	// Resume, then drop the diagnostic projection to prove the failure latch is
	// sufficient to settle the intent even when the candidate has been deleted.
	terminal := finish(t, e, request)
	if terminal.Plan.Phase != rollout.Failed || w.CandidateExists() {
		t.Fatal("failed candidate did not settle and clean up")
	}
	request.Projection = nil
	again, err := e.Step(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Terminal || again.Plan.Phase != rollout.Failed || again.Projection.Failure == nil || again.Projection.Failure.Code != "ImagePullUnauthorized" {
		t.Fatal("settled intent lost durable failure")
	}
	if count(w.Trace(), "create:candidate") != 1 {
		t.Fatal("deleted failed intent was recreated")
	}
	if index(w.Trace(), "persist-failure:") >= index(w.Trace(), "delete:candidate") {
		t.Fatal("failure outcome ordering violated")
	}
}

func TestRouteCASAndAtomicAbsenceRejectCompetingWriters(t *testing.T) {
	for _, active := range []bool{true, false} {
		t.Run(map[bool]string{true: "existing-route-version", false: "atomic-absence"}[active], func(t *testing.T) {
			w, e, request := setup(t, simulation.Success, active)
			request = advanceTo(t, w, e, request, rollout.PromoteCandidate)
			if active {
				w.BeforePublish = func(world *simulation.World) { world.ChangeRouteVersion("competing-version") }
			} else {
				w.BeforePublish = func(world *simulation.World) { world.InjectRouteCollision() }
			}
			_, err := e.Step(context.Background(), request)
			if !errors.Is(err, simulation.ErrConflict) {
				t.Fatalf("expected conditional-write conflict, got %v", err)
			}
			if count(w.Trace(), "update:route") != 0 || count(w.Trace(), "create:route") != 0 {
				t.Fatal("stale preconditions allowed route mutation")
			}
			if active && w.ActiveRevision() != "git-aaaaaaa" {
				t.Fatal("competing write lost healthy active route")
			}
		})
	}
}

func TestIndependentForeignOwnerCannotBeLaunderedThroughLedger(t *testing.T) {
	w, e, request := setup(t, simulation.Success, true)
	first, err := e.Step(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Projection = &first.Projection
	foreign := w.Context().Owner()
	foreign.EnvironmentID = "production"
	w.ChangeCandidateOwner(foreign)
	_, err = e.Step(context.Background(), request)
	var ownershipErr *ownership.Error
	if !errors.As(err, &ownershipErr) || ownershipErr.Code != ownership.ForeignOwner {
		t.Fatalf("independent foreign provider owner accepted: %v", err)
	}
	if count(w.Trace(), "observe:readiness") != 0 || count(w.Trace(), "update:route") != 0 {
		t.Fatal("foreign owner reached capability operation")
	}
}

func TestRouteWriteLedgerAcknowledgmentCrashRecoversWithoutSecondWrite(t *testing.T) {
	w, e, request := setup(t, simulation.Success, true)
	request = advanceTo(t, w, e, request, rollout.PromoteCandidate)
	w.BeforePublish = func(world *simulation.World) { world.FailNextRecord = true }
	result, err := e.Step(context.Background(), request)
	if err == nil || w.ActiveRevision() != "git-bbbbbbb" {
		t.Fatal("failed to exercise route-write/ledger-ack seam")
	}
	request.Projection = &result.Projection
	terminal := finish(t, e, request)
	if terminal.Plan.Phase != rollout.Succeeded || count(w.Trace(), "update:route") != 1 {
		t.Fatal("route recovery repeated provider mutation")
	}
}

func TestDuplicateCreateIntentReturnsOnlyOwnedOriginalObject(t *testing.T) {
	w, _, request := setup(t, simulation.Success, true)
	snapshot, err := w.Snapshot(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := rollout.Compute(request.Desired, snapshot.State)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := w.Context().Intent(ownership.Kubernetes, "candidate", request.Desired.Revision, request.Desired.Revision, request.OperationID, request.Desired.ReconcileKey, 63)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SaveIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	pre, err := w.Context().AuthorizeCreate(intent, w.Writer(), w.Now())
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.Ensure(context.Background(), request.Desired, *plan.Next, engine.CreationPermit{Preconditions: pre})
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Ensure(context.Background(), request.Desired, *plan.Next, engine.CreationPermit{Preconditions: pre})
	if err != nil {
		t.Fatal(err)
	}
	if first.Ref.ProviderID != second.Ref.ProviderID || count(w.Trace(), "create:candidate") != 1 {
		t.Fatal("identical retry created a second object")
	}
	foreign := w.Context().Owner()
	foreign.ProjectUID = "foreign"
	w.ChangeCandidateOwner(foreign)
	if _, err = w.Ensure(context.Background(), request.Desired, *plan.Next, engine.CreationPermit{Preconditions: pre}); err == nil {
		t.Fatal("name/token matching adopted foreign owner")
	}
	conflict := intent
	conflict.SpecFingerprint = "other"
	if err = w.SaveIntent(context.Background(), conflict); !errors.Is(err, simulation.ErrConflict) {
		t.Fatalf("conflicting duplicate intent accepted: %v", err)
	}
}

func TestCancelledRequestDoesNotReachProvider(t *testing.T) {
	w, e, request := setup(t, simulation.Success, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Step(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	if len(w.Trace()) != 0 || w.CandidateExists() {
		t.Fatal("cancelled request wrote an intent/provider object")
	}
}

type observerFunc func(context.Context, engine.Request) (engine.Snapshot, error)

func (f observerFunc) Snapshot(ctx context.Context, desired engine.Request) (engine.Snapshot, error) {
	return f(ctx, desired)
}

func TestTerminalSuccessCannotTrustForgedPlannerOwnership(t *testing.T) {
	w, e, request := setup(t, simulation.Success, true)
	terminal := finish(t, e, request)
	request.Projection = &terminal.Projection
	foreign := w.Context().Owner()
	foreign.EnvironmentID = "production"
	w.ChangeRouteOwner(foreign)
	caps := w.Capabilities()
	caps.Observer = observerFunc(func(ctx context.Context, desired engine.Request) (engine.Snapshot, error) {
		snapshot, err := w.Snapshot(ctx, desired)
		// Deliberately forge the planner-facing subject while retaining the
		// independently observed foreign provider ownership in Snapshot.Route.
		snapshot.State.Route.Subject = desired.Desired.Subject
		return snapshot, err
	})
	reader, err := engine.New(w.Context(), w.Writer(), caps)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Step(context.Background(), request)
	var rejected *ownership.Error
	if !errors.As(err, &rejected) || rejected.Code != ownership.ForeignOwner {
		t.Fatalf("terminal foreign route was reported healthy: %v", err)
	}
	if result.Terminal || result.Projection.State == diagnostics.Succeeded {
		t.Fatal("forged planner subject bypassed independent ownership verification")
	}
}

func TestUnknownErrorsAreSafeBeforeEveryDurableBoundary(t *testing.T) {
	w, _, request := setup(t, simulation.Success, true)
	raw := errors.New("unlabeled-provider-secret-782f31")
	caps := w.Capabilities()
	caps.Observer = observerFunc(func(context.Context, engine.Request) (engine.Snapshot, error) { return engine.Snapshot{}, raw })
	e, err := engine.New(w.Context(), w.Writer(), caps)
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.Step(context.Background(), request)
	if !errors.Is(err, raw) {
		t.Fatal("control-flow error identity was not preserved")
	}
	if strings.Contains(diagnostics.Explain(result.Projection), "unlabeled-provider-secret") {
		t.Fatal("unknown provider text reached diagnostic projection")
	}
	boundary := engine.SafeBoundaryError(err, "observe", w.Now())
	if strings.Contains(boundary.Error(), "unlabeled-provider-secret") || errors.Unwrap(boundary) != nil {
		t.Fatal("unsafe raw cause reached durable-boundary error")
	}
}
