package engine_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/simulation"
)

func drainAttempt(t *testing.T, e *engine.Engine, r engine.Request) engine.Result {
	t.Helper()
	r.Supersede = true
	for i := 0; i < 20; i++ {
		result, err := e.Step(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		if result.SupersessionComplete {
			return result
		}
		r.Projection = &result.Projection
	}
	t.Fatal("drain did not settle")
	return engine.Result{}
}
func nextAttempt(r engine.Request) engine.Request {
	r.OperationID = "next-operation"
	r.Desired.Generation++
	r.Desired.ReconcileKey = "next-key"
	r.Desired.Revision = "git-ccccccc"
	r.Projection = nil
	r.Supersede = false
	return r
}
func TestSupersessionCapturesSettlesDeletesBeforeNextAttempt(t *testing.T) {
	w, e, r := setup(t, simulation.Success, true)
	first, err := e.Step(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.Projection = &first.Projection
	next := nextAttempt(r)
	if _, err = e.Step(context.Background(), next); err == nil {
		t.Fatal("new attempt bypassed previous candidate drain")
	}
	result := drainAttempt(t, e, r)
	if w.CandidateForAttempt(r.OperationID) || w.ActiveRevision() != "git-aaaaaaa" || count(w.Trace(), "update:route") != 0 || count(w.Trace(), "capture:evidence") != 1 || count(w.Trace(), "delete:candidate") != 1 {
		t.Fatal("supersession changed traffic or skipped cleanup")
	}
	archived, ok := w.Supersession(r.OperationID)
	if !ok || archived.Disposition != engine.AttemptDeleted || archived.Projection.Identity.OperationID != r.OperationID || !result.SupersessionComplete {
		t.Fatal("old attempt acknowledgment missing")
	}
	if _, err = e.Step(context.Background(), next); err != nil || !w.CandidateForAttempt(next.OperationID) {
		t.Fatalf("acknowledged handoff failed: %v", err)
	}
	if count(w.Trace(), "create:candidate") != 2 {
		t.Fatal("new attempt reused old candidate")
	}
}
func TestSupersessionRetainsFailuresWithoutOverwritingBindings(t *testing.T) {
	for _, scenario := range []simulation.Scenario{simulation.EvidenceFailure, simulation.CleanupFailure} {
		t.Run(string(scenario), func(t *testing.T) {
			w, e, r := setup(t, scenario, true)
			r = advanceTo(t, w, e, r, rollout.CaptureEvidence)
			result := drainAttempt(t, e, r)
			archive, ok := w.Supersession(r.OperationID)
			if !ok || archive.Disposition != engine.AttemptRetained || archive.Projection.Failure == nil || archive.Projection.Failure.Code != "ImagePullUnauthorized" || len(result.Projection.Recovery) == 0 || !w.CandidateForAttempt(r.OperationID) {
				t.Fatal("original cause/retained attempt lost")
			}
			oldUID := archive.Candidate.Observation.Ref.ProviderID
			next := nextAttempt(r)
			if _, err := e.Step(context.Background(), next); err != nil {
				t.Fatal(err)
			}
			if !w.CandidateForAttempt(r.OperationID) || !w.CandidateForAttempt(next.OperationID) || w.ActiveRevision() != "git-aaaaaaa" {
				t.Fatal("new attempt overwrote retained candidate/route")
			}
			saved, _ := w.Supersession(r.OperationID)
			if saved.Candidate.Observation.Ref.ProviderID != oldUID {
				t.Fatal("old canonical binding replaced")
			}
		})
	}
}
func TestSupersessionNeverDeletesCandidateThatBecameActive(t *testing.T) {
	w, e, r := setup(t, simulation.Success, true)
	terminal := finish(t, e, r)
	r.Projection = &terminal.Projection
	result := drainAttempt(t, e, r)
	saved, ok := w.Supersession(r.OperationID)
	if !ok || saved.Disposition != engine.AttemptActive || !w.CandidateForAttempt(r.OperationID) || count(w.Trace(), "delete:candidate") != 0 || count(w.Trace(), "capture:evidence") != 0 || w.ActiveRevision() != r.Desired.Revision || !result.SupersessionComplete {
		t.Fatal("active candidate was not durably preserved")
	}
	next := nextAttempt(r)
	if _, err := e.Step(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if w.ActiveRevision() != r.Desired.Revision || !w.CandidateForAttempt(r.OperationID) {
		t.Fatal("handoff deleted serving attempt")
	}
}
func TestSupersessionUnknownWritesAndMissingDurableAckBlockNewAttempt(t *testing.T) {
	w, e, r := setup(t, simulation.Success, true)
	result, err := e.Step(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.Projection = &result.Projection
	r.Supersede = true
	w.SetUnknownWrite(r.OperationID, true)
	if result, err = e.Step(context.Background(), r); err == nil || result.SupersessionComplete || count(w.Trace(), "capture:evidence") != 0 {
		t.Fatal("ambiguous provider write was treated as settled")
	}
	if _, err = e.Step(context.Background(), nextAttempt(r)); err == nil {
		t.Fatal("new operation bypassed unknown write")
	}
	w.SetUnknownWrite(r.OperationID, false)
	w.FailSupersessionAck = true
	for i := 0; i < 10; i++ {
		result, err = e.Step(context.Background(), r)
		r.Projection = &result.Projection
		if err != nil {
			break
		}
	}
	if err == nil || result.SupersessionComplete || w.CandidateForAttempt(r.OperationID) {
		t.Fatal("did not exercise delete-before-settlement-ACK seam")
	}
	if _, err = e.Step(context.Background(), nextAttempt(r)); err == nil {
		t.Fatal("new attempt bypassed failed durable acknowledgment")
	}
	w.FailSupersessionAck = false
	result = drainAttempt(t, e, r)
	if !result.SupersessionComplete || count(w.Trace(), "delete:candidate") != 1 {
		t.Fatal("restart repeated delete rather than recovering old outcome")
	}
}

type legacyJournal struct{ engine.Journal }

type preparingJournal struct {
	*simulation.World
	prepare func(context.Context, engine.Request) error
}

func (j preparingJournal) PrepareSupersession(ctx context.Context, r engine.Request) error {
	return j.prepare(ctx, r)
}

func TestSupersessionObservesAfterPrepareDrainsDispatchedPromotion(t *testing.T) {
	w, oldEngine, r := setup(t, simulation.Success, true)
	r = advanceTo(t, w, oldEngine, r, rollout.PromoteCandidate)
	dispatched, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	w.BeforePublish = func(*simulation.World) { close(dispatched); <-release }
	finished := make(chan error, 1)
	go func() { _, err := oldEngine.Step(context.Background(), r); finished <- err }()
	select {
	case <-dispatched:
	case <-time.After(5 * time.Second):
		t.Fatal("old promotion did not reach dispatch seam")
	}
	var drained atomic.Bool
	caps := w.Capabilities()
	caps.Journal = preparingJournal{World: w, prepare: func(ctx context.Context, request engine.Request) error {
		// Model an already dispatched provider write finishing while the
		// executing guard drains it, before Prepare may return its barrier.
		once.Do(func() { close(release) })
		select {
		case err := <-finished:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := w.PrepareSupersession(ctx, request); err != nil {
			return err
		}
		drained.Store(true)
		return nil
	}}
	caps.Observer = observerFunc(func(ctx context.Context, request engine.Request) (engine.Snapshot, error) {
		if !drained.Load() {
			t.Error("supersession observed provider state before draining old writes")
		}
		return w.Snapshot(ctx, request)
	})
	executor, err := engine.New(w.Context(), w.Writer(), caps)
	if err != nil {
		t.Fatal(err)
	}
	r.Supersede = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := executor.Step(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	archive, ok := w.Supersession(r.OperationID)
	if !result.SupersessionComplete || !ok || archive.Disposition != engine.AttemptActive || !w.CandidateForAttempt(r.OperationID) || w.ActiveRevision() != r.Desired.Revision || count(w.Trace(), "delete:candidate") != 0 || count(w.Trace(), "capture:evidence") != 0 {
		t.Fatal("post-drain active candidate was treated as inactive")
	}
}

func TestSupersessionBarrierExcludesNormalWritesThroughFailedSettlement(t *testing.T) {
	w, e, r := setup(t, simulation.Success, true)
	result, err := e.Step(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.Projection = &result.Projection
	if err = w.PrepareSupersession(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Step(context.Background(), r); err == nil {
		t.Fatal("old normal writer resumed inside supersession guard")
	}
	w.FailSupersessionAck = true
	r.Supersede = true
	for i := 0; i < 10; i++ {
		result, err = e.Step(context.Background(), r)
		r.Projection = &result.Projection
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("settlement failure seam was not exercised")
	}
	r.Supersede = false
	if _, err = e.Step(context.Background(), r); err == nil {
		t.Fatal("failed settlement released old normal writer")
	}
	if _, err = e.Step(context.Background(), nextAttempt(r)); err == nil {
		t.Fatal("failed settlement admitted a new normal writer")
	}
}

func TestSupersessionMissingCapabilityAndUnrecognizedAttemptFailClosed(t *testing.T) {
	w, e, r := setup(t, simulation.Success, true)
	r.Supersede = true
	if result, err := e.Step(context.Background(), r); err == nil || result.SupersessionComplete {
		t.Fatal("unknown carried attempt got an absent-resource settlement")
	}
	caps := w.Capabilities()
	caps.Journal = legacyJournal{Journal: w}
	legacy, err := engine.New(w.Context(), w.Writer(), caps)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := legacy.Step(context.Background(), r); err == nil || result.SupersessionComplete {
		t.Fatal("missing durable supersession capability was accepted")
	}
	r.Supersede = false
	if _, err = e.Step(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	r.Desired.RequireSmoke = !r.Desired.RequireSmoke
	r.Supersede = true
	if result, err := e.Step(context.Background(), r); err == nil || result.SupersessionComplete {
		t.Fatal("operation ID was rebound to a different immutable intent")
	}
	if count(w.Trace(), "capture:evidence") != 0 || count(w.Trace(), "delete:candidate") != 0 {
		t.Fatal("invalid supersession reached provider mutation")
	}
}
