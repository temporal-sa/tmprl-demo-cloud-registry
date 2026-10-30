package admission

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type delayed struct {
	expected journal.Version
	data     []byte
}
type cell struct {
	mu                  sync.Mutex
	data                []byte
	version             journal.Version
	calls               int
	delay, blockConfirm bool
	readFail            bool
	held                []delayed
}

func (c *cell) Read(context.Context) ([]byte, journal.Version, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readFail {
		return nil, journal.Version{}, errors.New("temporary owned cell read failure")
	}
	return append([]byte(nil), c.data...), c.version, nil
}
func (c *cell) apply(expected journal.Version, data []byte) error {
	if expected != c.version {
		return journal.ErrConflict
	}
	c.data = append([]byte(nil), data...)
	n, _ := strconv.Atoi(c.version.ResourceVersion)
	c.version.ResourceVersion = strconv.Itoa(n + 1)
	return nil
}
func (c *cell) CompareAndSwap(_ context.Context, v journal.Version, data []byte, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.delay {
		c.held = append(c.held, delayed{v, append([]byte(nil), data...)})
		return journal.ErrUnknown
	}
	return c.apply(v, data)
}
func (c *cell) ConfirmCommit(context.Context, journal.Version, []byte, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blockConfirm {
		return journal.ErrUnknown
	}
	return nil
}
func (c *cell) deliver(index int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.apply(c.held[index].expected, c.held[index].data)
}
func setup(t *testing.T) (Config, *cell) {
	t.Helper()
	e, err := journal.Initial(rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := e.JSON()
	c := &cell{data: data, version: journal.Version{UID: "cell-uid", ResourceVersion: "1"}}
	r, err := journal.New(c, e.Subject, "cell-uid")
	if err != nil {
		t.Fatal(err)
	}
	return Config{LocalCleanHandoff, r}, c
}
func initial(t *testing.T, c Config) *Gate {
	t.Helper()
	claim, err := BeginInitial(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	g, err := claim.Confirm(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func ready(t *testing.T, g *Gate) Handoff {
	t.Helper()
	h, err := g.Ready(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func snapshot(t *testing.T, g *Gate) journal.Snapshot {
	t.Helper()
	s, err := g.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func reserve(e journal.Envelope) journal.Envelope {
	n := copyEnvelope(e)
	n.Attempts = append(n.Attempts, journal.Attempt{Number: 1, OperationID: "operation-one", NamespaceName: "lab-candidate", Revision: "revision-one", DesiredDigest: strings.Repeat("a", 64), State: journal.Reserved})
	return n
}

func raceClaims(t *testing.T, a, b *Claim) *Gate {
	t.Helper()
	var mu sync.Mutex
	var winner *Gate
	accepted, rejected := 0, 0
	var wg sync.WaitGroup
	for _, claim := range []*Claim{a, b} {
		wg.Go(func() {
			g, err := claim.Confirm(context.Background())
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
				winner = g
			} else if errors.Is(err, ErrChanged) {
				rejected++
			} else {
				t.Errorf("unexpected race result %v", err)
			}
		})
	}
	wg.Wait()
	if accepted != 1 || rejected != 1 {
		t.Fatalf("admitted %d, rejected %d", accepted, rejected)
	}
	return winner
}

func TestPristineCellAdmitsExactlyOneInitialSession(t *testing.T) {
	c, _ := setup(t)
	a, err := BeginInitial(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := BeginInitial(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	g := raceClaims(t, a, b)
	s := snapshot(t, g)
	if s.Envelope.Admission.Generation != 1 || s.Envelope.Admission.State != journal.AdmissionActive {
		t.Fatal("admission not persisted before active gate")
	}
	if _, err = BeginInitial(context.Background(), c); !errors.Is(err, ErrChanged) {
		t.Fatal("nonpristine cell admitted a fresh worker")
	}
}

func TestHandoffIsConsumedOnceAcrossConcurrentAndStaleReplays(t *testing.T) {
	c, _ := setup(t)
	old := initial(t, c)
	h := ready(t, old)
	if _, err := old.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal("quiesced old session reopened")
	}
	data, err := h.JSON()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	a, err := BeginResume(context.Background(), c, h)
	if err != nil {
		t.Fatal(err)
	}
	b, err := BeginResume(context.Background(), c, parsed)
	if err != nil {
		t.Fatal(err)
	}
	next := raceClaims(t, a, b)
	if snapshot(t, next).Envelope.Admission.Generation != 2 {
		t.Fatal("successor generation not advanced")
	}
	if _, err = BeginResume(context.Background(), c, h); !errors.Is(err, ErrChanged) {
		t.Fatal("consumed handoff admitted another worker")
	}
	_ = ready(t, next)
	if _, err = BeginResume(context.Background(), c, h); !errors.Is(err, ErrChanged) {
		t.Fatal("old handoff reused after later handoff")
	}
}

func TestLateUnacknowledgedClaimCannotDisplaceAdmittedSuccessor(t *testing.T) {
	c, store := setup(t)
	h := ready(t, initial(t, c))
	first, err := BeginResume(context.Background(), c, h)
	if err != nil {
		t.Fatal(err)
	}
	store.delay = true
	if g, err := first.Confirm(context.Background()); g != nil || !errors.Is(err, ErrUncertain) {
		t.Fatal("unknown claim returned dispatch authority")
	}
	second, err := BeginResume(context.Background(), c, h)
	if err != nil {
		t.Fatal(err)
	}
	store.delay = false
	winner, err := second.Confirm(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.deliver(0); !errors.Is(err, journal.ErrConflict) {
		t.Fatal("late old admission overwrote successor")
	}
	if g, err := first.Confirm(context.Background()); g != nil || !errors.Is(err, ErrChanged) {
		t.Fatal("losing claim recovered into an active gate")
	}
	if snapshot(t, winner).Envelope.Admission.SessionID == first.next.Admission.SessionID {
		t.Fatal("wrong winning identity")
	}
}

func TestUnknownMutationBlocksThenOnlyExactRecoveryResumesSession(t *testing.T) {
	c, store := setup(t)
	g := initial(t, c)
	s := snapshot(t, g)
	store.delay = true
	if _, err := g.Commit(context.Background(), reserve(s.Envelope)); !errors.Is(err, ErrUncertain) {
		t.Fatal("unknown mutation was acknowledged")
	}
	if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrUncertain) {
		t.Fatal("dispatch view accessible with unknown mutation")
	}
	if _, err := g.RecoverCommit(context.Background()); !errors.Is(err, ErrUncertain) {
		t.Fatal("old data mistaken for no late commit")
	}
	if err := store.deliver(0); err != nil {
		t.Fatal(err)
	}
	store.delay = false
	recovered, err := g.RecoverCommit(context.Background())
	if err != nil || len(recovered.Envelope.Attempts) != 1 {
		t.Fatalf("exact commit not recovered: %v", err)
	}
	if snapshot(t, g).Envelope.Digest != recovered.Envelope.Digest {
		t.Fatal("recovery changed admission")
	}
}

func TestLostHandoffAcknowledgementNeverReopensOldGate(t *testing.T) {
	c, store := setup(t)
	g := initial(t, c)
	store.blockConfirm = true
	if h, err := g.Ready(context.Background()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("unconfirmed handoff returned: %v %+v", err, h)
	}
	if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal("old gate reopened after uncertain ready CAS")
	}
	store.blockConfirm = false
	h, err := g.RecoverReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	claim, err := BeginResume(context.Background(), c, h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = claim.Confirm(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPendingProviderIntentPreventsHandoffAndCannotBeHidden(t *testing.T) {
	c, _ := setup(t)
	g := initial(t, c)
	s := snapshot(t, g)
	n := reserve(s.Envelope)
	n.Writes = []journal.Write{{ID: "create-namespace", OperationID: "operation-one", WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: "lab-candidate", Fingerprint: strings.Repeat("b", 64), TargetRevision: "revision-one", ProviderOperationID: "operation-one"}}}
	if _, err := g.Commit(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Ready(context.Background()); !errors.Is(err, ErrBlocked) {
		t.Fatal("unresolved provider write allowed handoff")
	}
	if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal("failed handoff reopened gate")
	}
}

func TestClosingUncertainCommitRetainsExactRecoveryWithoutReopening(t *testing.T) {
	for _, providerPending := range []bool{false, true} {
		t.Run(strconv.FormatBool(providerPending), func(t *testing.T) {
			config, store := setup(t)
			g := initial(t, config)
			n := reserve(snapshot(t, g).Envelope)
			if providerPending {
				n.Writes = []journal.Write{{ID: "create-namespace", OperationID: "operation-one", WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: "lab-candidate", Fingerprint: strings.Repeat("b", 64), TargetRevision: "revision-one", ProviderOperationID: "operation-one"}}}
			}
			store.delay = true
			if _, err := g.Commit(context.Background(), n); !errors.Is(err, ErrUncertain) {
				t.Fatalf("commit: %v", err)
			}
			if _, err := g.Ready(context.Background()); !errors.Is(err, ErrUncertain) {
				t.Fatalf("ready: %v", err)
			}
			if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed gate: %v", err)
			}
			if _, err := g.RecoverReady(context.Background()); !errors.Is(err, ErrUncertain) {
				t.Fatalf("unacknowledged handoff: %v", err)
			}
			if err := store.deliver(0); err != nil {
				t.Fatal(err)
			}
			store.delay = false
			if _, err := g.RecoverCommit(context.Background()); err != nil {
				t.Fatalf("exact recovery: %v", err)
			}
			if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatalf("reopened gate: %v", err)
			}
			h, err := g.RecoverReady(context.Background())
			if providerPending {
				if !errors.Is(err, ErrBlocked) {
					t.Fatalf("pending provider handoff: %v", err)
				}
				if _, err := h.JSON(); err == nil {
					t.Fatal("pending provider returned a handoff")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			claim, err := BeginResume(context.Background(), config, h)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := claim.Confirm(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := BeginResume(context.Background(), config, h); !errors.Is(err, ErrChanged) {
				t.Fatalf("reused handoff: %v", err)
			}
		})
	}
}

func TestReadyObservationFailureRetainsClosedConditionalRecovery(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(strconv.FormatBool(changed), func(t *testing.T) {
			config, store := setup(t)
			g := initial(t, config)
			previous := snapshot(t, g)
			store.readFail = true
			if _, err := g.Ready(context.Background()); !errors.Is(err, ErrBlocked) {
				t.Fatalf("ready: %v", err)
			}
			if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed: %v", err)
			}
			store.readFail = false
			if changed {
				// A bypassing trusted actor changes the cell while the read is down.
				// The closed session's retained proposal still cannot overwrite it.
				if _, err := config.Repository.Commit(context.Background(), previous, reserve(previous.Envelope)); err != nil {
					t.Fatal(err)
				}
			}
			h, err := g.RecoverReady(context.Background())
			if changed {
				if !errors.Is(err, ErrChanged) {
					t.Fatalf("stale recovery: %v", err)
				}
				if _, err := h.JSON(); err == nil {
					t.Fatal("stale recovery returned handoff")
				}
			} else if err != nil {
				t.Fatalf("conditional recovery: %v", err)
			}
			if _, err := g.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatal("reopened after recovery")
			}
		})
	}
}

func TestReadySessionCannotMutateFactsOrLoseAdmission(t *testing.T) {
	c, _ := setup(t)
	g := initial(t, c)
	_ = ready(t, g)
	s, err := c.Repository.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*journal.Envelope){func(n *journal.Envelope) { n.Admission = nil }, func(n *journal.Envelope) { *n = reserve(*n) }, func(n *journal.Envelope) { n.Admission.State = journal.AdmissionActive; n.Admission.HandoffID = "" }, func(n *journal.Envelope) {
		n.Admission.Generation += 2
		n.Admission.State = journal.AdmissionActive
		n.Admission.HandoffID = ""
	}} {
		n := copyEnvelope(s.Envelope)
		change(&n)
		if _, err := c.Repository.Commit(context.Background(), s, n); !errors.Is(err, journal.ErrInvalid) {
			t.Fatal("ready state bypassed controlled transition")
		}
	}
}

func TestHandoffParsingAndSnapshotCopiesDoNotGrantAuthority(t *testing.T) {
	for _, data := range [][]byte{nil, []byte(`{}`), []byte(`{"schema":1,"raw":"private"}`), make([]byte, 8193)} {
		if _, err := Parse(data); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid handoff accepted")
		}
	}
	c, _ := setup(t)
	g := initial(t, c)
	s := snapshot(t, g)
	s.Envelope.Admission.SessionID = strings.Repeat("f", 64)
	if snapshot(t, g).Envelope.Admission.SessionID == s.Envelope.Admission.SessionID {
		t.Fatal("caller changed internal admission through alias")
	}
	if _, err := BeginInitial(context.Background(), Config{Mode: "production", Repository: c.Repository}); !errors.Is(err, ErrInvalid) {
		t.Fatal("production mode accepted")
	}
}
