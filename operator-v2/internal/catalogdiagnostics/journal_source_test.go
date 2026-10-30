package catalogdiagnostics

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type desiredFunc func(context.Context, Target) (DesiredObservation, error)

func (f desiredFunc) ObserveDesired(c context.Context, t Target) (DesiredObservation, error) {
	return f(c, t)
}

type diagnosticCell struct {
	data    []byte
	version journal.Version
	reads   int
	err     error
}

func (c *diagnosticCell) Read(context.Context) ([]byte, journal.Version, error) {
	c.reads++
	return append([]byte(nil), c.data...), c.version, c.err
}
func (c *diagnosticCell) CompareAndSwap(_ context.Context, v journal.Version, b []byte, _ string) error {
	if c.version != v {
		return journal.ErrConflict
	}
	c.data = append([]byte(nil), b...)
	n, _ := strconv.Atoi(v.ResourceVersion)
	c.version.ResourceVersion = strconv.Itoa(n + 1)
	return nil
}

func journalFixture(t *testing.T) (*journal.Repository, *diagnosticCell) {
	t.Helper()
	e, err := journal.Initial(rollout.Subject{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID}, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := e.JSON()
	cell := &diagnosticCell{data: data, version: journal.Version{UID: "pinned-cell", ResourceVersion: "1"}}
	repo, err := journal.New(cell, e.Subject, cell.version.UID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next := saved.Envelope
	projection := snapshot(t).Operation
	next.Attempts = []journal.Attempt{{Number: 1, OperationID: projection.Identity.OperationID, NamespaceName: "candidate-one", Revision: projection.Identity.ReconcileKey, DesiredDigest: strings.Repeat("b", 64), State: journal.Reserved}}
	next.Projection = projection
	if _, err = repo.Commit(context.Background(), saved, next); err != nil {
		t.Fatal(err)
	}
	return repo, cell
}
func newJournalSource(t *testing.T, repo *journal.Repository, origin Origin, operation string, desired DesiredObserver) *JournalSource {
	t.Helper()
	s, err := NewJournalSource(target.Environment, target.Installation, []JournalBinding{{Target: target, Repository: repo, Origin: origin, OperationID: operation}}, desired, func() time.Time { return testNow })
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestJournalStatusReadsDurableFailureWithIndependentDesiredAndUnknownPersistenceTime(t *testing.T) {
	repo, _ := journalFixture(t)
	desiredCalls := 0
	s := newJournalSource(t, repo, Archived, "", desiredFunc(func(_ context.Context, scope Target) (DesiredObservation, error) {
		desiredCalls++
		if scope != target {
			t.Fatal("foreign desired query")
		}
		return DesiredObservation{Desired: diagnostics.DesiredState{ReconcileKey: "newer-independent-intent", Generation: 9}, Known: true, ObservedAt: testNow}, nil
	}))
	m := model(t, s)
	view, err := m.Read(context.Background(), grant(t), target)
	if err != nil {
		t.Fatal(err)
	}
	if desiredCalls != 1 || view.Operation == nil || view.Operation.Failure == nil || view.Operation.Failure.Code != "SmokeFailed" || view.Source.Origin != Archived || view.Source.RecordedAt != nil || view.Source.PersistenceTimeKnown || view.Freshness.State != diagnostics.FreshnessUnknown || !view.Partial || view.Operation.Availability.RouteState != diagnostics.RouteUnconfirmed {
		t.Fatalf("durable failure/freshness contract lost: %+v", view)
	}
	if !strings.Contains(view.Explanation, "persistence freshness is unknown") {
		t.Fatal("unknown persistence clock hidden")
	}
}

func TestJournalSourceAuthorizationPrecedesExistenceAndStorageFailureIsSafe(t *testing.T) {
	repo, cell := journalFixture(t)
	s := newJournalSource(t, repo, Materialized, "", desiredFunc(func(context.Context, Target) (DesiredObservation, error) {
		return DesiredObservation{}, errors.New("PRIVATE-desired")
	}))
	before := cell.reads
	if _, err := s.Read(context.Background(), ReadGrant{}); !errors.Is(err, ErrForbidden) || cell.reads != before {
		t.Fatal("ungranted source read")
	}
	foreign := target
	foreign.ProjectUID = "foreign"
	g, err := IssueReadGrant("verified", "policy-v1", foreign, testNow.Add(-time.Minute), testNow.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Read(context.Background(), g); !errors.Is(err, ErrNotFound) || cell.reads != before {
		t.Fatal("unknown project touched registered cell")
	}
	cell.err = errors.New("PRIVATE-storage")
	if _, err = s.Read(context.Background(), grant(t)); !errors.Is(err, ErrSourceUnavailable) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("unsafe storage error")
	}
	cell.err = nil
	cell.version.UID = "replacement-cell"
	if _, err = s.Read(context.Background(), grant(t)); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatal("replacement cell adopted or reported project absent")
	}
}

func TestJournalArchiveSelectionAndDesiredFailureRemainReadable(t *testing.T) {
	repo, _ := journalFixture(t)
	saved, err := repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next := saved.Envelope
	next.Attempts = append(append([]journal.Attempt(nil), next.Attempts...), journal.Attempt{Number: 2, OperationID: "op-2", NamespaceName: "candidate-two", Revision: "revision-two", DesiredDigest: strings.Repeat("c", 64), State: journal.Reserved})
	p, err := diagnostics.NewProjection(diagnostics.Identity{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID, ProjectName: "demo", OperationID: "op-2", ReconcileKey: "revision-two", Generation: 3})
	if err != nil {
		t.Fatal(err)
	}
	next.Projection = &p
	if _, err = repo.Commit(context.Background(), saved, next); err != nil {
		t.Fatal(err)
	}
	s := newJournalSource(t, repo, Materialized, "op-1", desiredFunc(func(context.Context, Target) (DesiredObservation, error) {
		return DesiredObservation{}, errors.New("PRIVATE-desired-source")
	}))
	view, err := model(t, s).Read(context.Background(), grant(t), target)
	if err != nil {
		t.Fatal(err)
	}
	if view.Source.Origin != Archived || view.Operation.Identity.OperationID != "op-1" || view.Operation.Failure.Code != "SmokeFailed" || view.Source.DesiredObservedAt != nil || !view.Partial || view.Freshness.State != diagnostics.FreshnessUnknown {
		t.Fatal("archive cause or independent desired error lost")
	}
	encoded, err := view.JSON()
	if err != nil || strings.Contains(string(encoded), "PRIVATE") {
		t.Fatal("unsafe desired error")
	}
}
