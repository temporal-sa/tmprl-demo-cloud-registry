package journal

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type memoryCell struct {
	mu      sync.Mutex
	data    []byte
	version Version
	calls   int
	outcome string
}

type confirmingCell struct {
	*memoryCell
	allow         bool
	confirmations int
	unresolved    bool
}

func (c *confirmingCell) ConfirmCommit(_ context.Context, _ Version, data []byte, digest string) error {
	c.confirmations++
	e, err := Decode(data)
	if err != nil || e.Digest != digest || !c.allow {
		return ErrUnknown
	}
	c.unresolved = false
	return nil
}

func (c *memoryCell) Read(context.Context) ([]byte, Version, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.data...), c.version, nil
}
func (c *memoryCell) CompareAndSwap(_ context.Context, v Version, data []byte, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if v != c.version {
		return ErrConflict
	}
	if c.outcome == "unknown-old" {
		return ErrUnknown
	}
	c.data = append([]byte(nil), data...)
	n, _ := strconv.Atoi(c.version.ResourceVersion)
	c.version.ResourceVersion = strconv.Itoa(n + 1)
	if c.outcome == "unknown-committed" {
		return ErrUnknown
	}
	return nil
}
func fixture(t *testing.T) (*Repository, *memoryCell, Snapshot) {
	t.Helper()
	e, err := Initial(rollout.Subject{Environment: "lab", Installation: "install", ProjectUID: "project-uid"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := e.JSON()
	c := &memoryCell{data: data, version: Version{"journal-uid", "1"}}
	r, err := New(c, e.Subject, c.version.UID)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r, c, s
}
func clone(e Envelope) Envelope {
	data, _ := json.Marshal(e)
	var next Envelope
	_ = json.Unmarshal(data, &next)
	return next
}
func reserve(e Envelope) Envelope {
	e = clone(e)
	e.Attempts = append(e.Attempts, Attempt{Number: 1, OperationID: "rollout-one", NamespaceName: "rgtest-candidate-one", State: Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("b", 64)})
	return e
}
func namespaceWrite() Write {
	return Write{ID: "create-namespace", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Preconditions: Preconditions{Kind: "Namespace", Action: "Create", Name: "rgtest-candidate-one", Fingerprint: strings.Repeat("a", 64), TargetRevision: "revision-one", ProviderOperationID: "rollout-one"}}
}
func commit(t *testing.T, r *Repository, s Snapshot, n Envelope) Snapshot {
	t.Helper()
	result, err := r.Commit(context.Background(), s, n)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDurableIntentPrecedesDispatchAndObservedUIDIsAtomic(t *testing.T) {
	r, c, s := fixture(t)
	s = commit(t, r, s, reserve(s.Envelope))
	n := clone(s.Envelope)
	n.Writes = append(n.Writes, namespaceWrite())
	s = commit(t, r, s, n)
	// A replacement repository reads the pending intent before any provider action.
	fresh, _ := New(c, s.Envelope.Subject, "journal-uid")
	reloaded, err := fresh.Load(context.Background())
	if err != nil || len(reloaded.Envelope.Writes) != 1 || reloaded.Envelope.Writes[0].State != Pending {
		t.Fatal("pending intent lost")
	}
	n = clone(s.Envelope)
	n.Writes[0].State = Committed
	n.Writes[0].ResultUID = "namespace-uid"
	n.Writes[0].ResultVersion = "3"
	if _, err = r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("observed namespace UID was not bound atomically")
	}
	n.Attempts[0].State = Observed
	n.Attempts[0].NamespaceUID = "namespace-uid"
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Attempts[0].State = Draining
	n.Attempts[0].DrainReason = DrainProjectFinalization
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Attempts[0].State = Consumed
	n.Attempts[0].CanonicalAbsent = true
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Attempts[0].State = Reserved
	n.Attempts[0].NamespaceUID = ""
	if _, err = r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("consumed namespace name was reused")
	}
}

func TestUnknownAcknowledgementResolvesOnlyExactCommit(t *testing.T) {
	for _, mode := range []string{"unknown-old", "unknown-committed"} {
		t.Run(mode, func(t *testing.T) {
			r, c, s := fixture(t)
			c.outcome = mode
			n := reserve(s.Envelope)
			written, err := r.Commit(context.Background(), s, n)
			if mode == "unknown-old" {
				if !errors.Is(err, ErrUnknown) {
					t.Fatal("old data was mistaken for final-not-committed")
				}
				if c.calls != 1 {
					t.Fatal("hidden write retry")
				}
				return
			}
			if err != nil || written.Envelope.Sequence != 2 {
				t.Fatal("exact committed receipt was not recovered")
			}
			_, err = r.Commit(context.Background(), s, n)
			if err != nil || c.calls != 1 {
				t.Fatal("replayed acknowledgement duplicated CAS")
			}
		})
	}
}

func TestReplayMustReleaseExactBackendUncertaintyBeforeAcknowledgement(t *testing.T) {
	_, base, s := fixture(t)
	base.outcome = "unknown-committed"
	cell := &confirmingCell{memoryCell: base, unresolved: true}
	repo, _ := New(cell, s.Envelope.Subject, "journal-uid")
	next := reserve(s.Envelope)
	if _, err := repo.Commit(context.Background(), s, next); !errors.Is(err, ErrUnknown) || !cell.unresolved {
		t.Fatal("visible commit bypassed backend uncertainty")
	}
	if _, err := repo.Commit(context.Background(), s, next); !errors.Is(err, ErrUnknown) || base.calls != 1 {
		t.Fatal("replay acknowledged without clearing gate or repeated CAS")
	}
	cell.allow = true
	written, err := repo.Commit(context.Background(), s, next)
	if err != nil || cell.unresolved || base.calls != 1 {
		t.Fatal("exact replay did not release gate")
	}
	base.outcome = ""
	next = clone(written.Envelope)
	next.WriterEpoch = 2
	if _, err = repo.Commit(context.Background(), written, next); err != nil || base.calls != 2 {
		t.Fatal("subsequent CAS blocked after proven recovery")
	}
}

func TestCompetingCASAndPinnedUIDFailClosed(t *testing.T) {
	r, c, s := fixture(t)
	next := reserve(s.Envelope)
	one := commit(t, r, s, next)
	other := reserve(s.Envelope)
	other.Attempts[0].NamespaceName = "rgtest-different"
	if _, err := r.Commit(context.Background(), s, other); !errors.Is(err, ErrConflict) {
		t.Fatal("stale CAS replaced accepted attempt")
	}
	c.version.UID = "replacement-uid"
	if _, err := r.Load(context.Background()); !errors.Is(err, ErrMissing) {
		t.Fatal("replacement journal adopted")
	}
	if _, err := r.Commit(context.Background(), one, clone(one.Envelope)); !errors.Is(err, ErrMissing) {
		t.Fatal("replacement journal written")
	}
}

func TestSubjectMalformedAndRawErrorsAreRejected(t *testing.T) {
	r, c, s := fixture(t)
	n := reserve(s.Envelope)
	n.Subject.ProjectUID = "foreign"
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) || c.calls != 0 {
		t.Fatal("foreign subject was rewritten")
	}
	for _, data := range [][]byte{append(c.data, []byte("{}")...), []byte(`{"schemaVersion":1,"private-provider-token":"sentinel"}`), []byte(strings.Repeat("x", MaxBytes+1))} {
		if _, err := Decode(data); !errors.Is(err, ErrInvalid) {
			t.Fatal("malformed data accepted")
		}
	}
	if safeError(errors.New("private-provider-token")) != ErrUnavailable {
		t.Fatal("raw backend cause exposed")
	}
}

func TestUnresolvedWriteBlocksEpochAndNewAttempt(t *testing.T) {
	r, _, s := fixture(t)
	s = commit(t, r, s, reserve(s.Envelope))
	n := clone(s.Envelope)
	n.Writes = []Write{namespaceWrite()}
	s = commit(t, r, s, n)
	for _, change := range []func(*Envelope){func(n *Envelope) { n.WriterEpoch++ }, func(n *Envelope) {
		n.Attempts = append(n.Attempts, Attempt{Number: 2, OperationID: "two", NamespaceName: "rgtest-two", State: Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("b", 64)})
	}} {
		n = clone(s.Envelope)
		change(&n)
		if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
			t.Fatal("pending write bypassed")
		}
	}
	n = clone(s.Envelope)
	n.Writes[0].State = Unknown
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Writes[0].State = Pending
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown provider write reset to pending")
	}
}

func TestNewEntriesRequireReservedAttemptsPendingWritesAndCurrentEpoch(t *testing.T) {
	r, _, s := fixture(t)
	n := reserve(s.Envelope)
	n.Attempts[0].State = Observed
	n.Attempts[0].NamespaceUID = "namespace-uid"
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("unproven imported attempt accepted")
	}
	s = commit(t, r, s, reserve(s.Envelope))
	n = clone(s.Envelope)
	n.Attempts[0].State, n.Attempts[0].NamespaceUID = Observed, "guessed-namespace-uid"
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("namespace UID imported without committed create evidence")
	}
	n = clone(s.Envelope)
	w1, w2 := namespaceWrite(), namespaceWrite()
	w2.ID = "duplicate-create"
	n.Writes = []Write{w1, w2}
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("two new create intents for same target accepted")
	}
	n = clone(s.Envelope)
	w := namespaceWrite()
	w.State = Committed
	w.ResultUID = "namespace-uid"
	w.ResultVersion = "2"
	n.Writes = []Write{w}
	n.Attempts[0].State = Observed
	n.Attempts[0].NamespaceUID = "namespace-uid"
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("provider result inserted without durable pending intent")
	}
	n = clone(s.Envelope)
	n.WriterEpoch = 2
	n.Writes = []Write{namespaceWrite()}
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("new write inherited stale epoch")
	}
}

func TestTerminalOutcomesAndPreconditionsAreImmutable(t *testing.T) {
	r, _, s := fixture(t)
	s = commit(t, r, s, reserve(s.Envelope))
	n := clone(s.Envelope)
	n.Writes = []Write{namespaceWrite()}
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Writes[0].State = Rejected
	s = commit(t, r, s, n)
	for _, change := range []func(*Envelope){func(n *Envelope) { n.Writes[0].State = Pending }, func(n *Envelope) { n.Writes[0].Preconditions.Fingerprint = strings.Repeat("f", 64) }, func(n *Envelope) { n.Writes = nil }, func(n *Envelope) { w := namespaceWrite(); w.ID = "another-create"; n.Writes = append(n.Writes, w) }} {
		n = clone(s.Envelope)
		change(&n)
		if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
			t.Fatal("terminal write or creation identity rewritten")
		}
	}
}

func TestProjectionRootSurvivesRecoveryAndArchive(t *testing.T) {
	r, _, s := fixture(t)
	s = commit(t, r, s, reserve(s.Envelope))
	p, _ := diagnostics.NewProjection(diagnostics.Identity{Environment: "lab", Installation: "install", ProjectUID: "project-uid", ProjectName: "example", OperationID: "rollout-one", ReconcileKey: "one", Generation: 1})
	at := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	f := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: "root-cause", Code: "CandidateNotReady", Category: diagnostics.Readiness, Message: "Candidate is not ready.", OccurredAt: at})
	p, _ = p.Apply(diagnostics.OperationEvent{Identity: p.Identity, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Step: "readiness", Failure: &f})
	n := clone(s.Envelope)
	n.Projection = &p
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Projection.Failure.Code = "ReplacementCause"
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("original cause changed")
	}
	n = clone(s.Envelope)
	n.Projection = nil
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("diagnosis disappeared without a new attempt")
	}
	n = clone(s.Envelope)
	n.Attempts = append(n.Attempts, Attempt{Number: 2, OperationID: "two", NamespaceName: "rgtest-two", State: Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("b", 64)})
	n.Projection = nil
	s = commit(t, r, s, n)
	if len(s.Envelope.Archive) != 1 || s.Envelope.Archive[0].Failure.Code != "CandidateNotReady" {
		t.Fatal("old diagnosis not archived")
	}
	n = clone(s.Envelope)
	n.Archive[0].Failure.Code = "Rewritten"
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("archived cause changed")
	}
}
