package catalogdiagnostics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
)

func historyCopy(e journal.Envelope) journal.Envelope {
	data, _ := json.Marshal(e)
	var copy journal.Envelope
	_ = json.Unmarshal(data, &copy)
	return copy
}

// Synthetic durable Cell, real Repository validation/CAS. No provider calls or
// credentials: these tests prove journal association and presentation only.
func historyFixture(t *testing.T, unresolved bool) (*journal.Repository, *diagnosticCell) {
	t.Helper()
	repo, cell := journalFixture(t)
	appendWrite := func(kind, name string, state journal.WriteState, action string) {
		saved, err := repo.Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		next := historyCopy(saved.Envelope)
		a := next.Attempts[0]
		id := "write-" + kind + "-" + name + "-" + action
		pre := journal.Preconditions{Kind: kind, Action: action, Name: name, TargetRevision: a.Revision, Fingerprint: strings.Repeat("d", 64), ProviderOperationID: id}
		if kind == "Namespace" {
			pre.ProviderOperationID = a.OperationID
		} else {
			pre.ParentUID = a.NamespaceUID
		}
		if action != "Create" {
			pre.UID, pre.Version = "PRIVATE-object", "PRIVATE-version"
		}
		write := journal.Write{ID: id, OperationID: a.OperationID, WriterEpoch: next.WriterEpoch, Preconditions: pre, State: journal.Pending}
		if kind == "ServiceAccount" {
			write.Input = journal.InputReference{Kind: "ServiceAccountRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: journal.ServiceAccountInputContract}
		}
		next.Writes = append(next.Writes, write)
		saved, err = repo.Commit(context.Background(), saved, next)
		if err != nil {
			t.Fatal(err)
		}
		if state == journal.Pending {
			return
		}
		next = historyCopy(saved.Envelope)
		w := &next.Writes[len(next.Writes)-1]
		w.State = state
		if state == journal.Committed {
			w.ResultUID, w.ResultVersion = "PRIVATE-"+kind+"-"+name, "PRIVATE-historical-version"
			if action != "Create" {
				w.ResultUID = pre.UID
			}
			if kind == "Namespace" {
				next.Attempts[0].State, next.Attempts[0].NamespaceUID = journal.Observed, w.ResultUID
			}
			if kind == "Service" {
				w.ResultSpecDigest, w.ResultSpecContract = strings.Repeat("e", 64), journal.ServiceSpecContract
			}
		}
		if _, err = repo.Commit(context.Background(), saved, next); err != nil {
			t.Fatal(err)
		}
	}
	appendWrite("Namespace", "candidate-one", journal.Committed, "Create")
	for _, kind := range createKinds[1:] {
		appendWrite(kind, "recorded", journal.Committed, "Create")
		appendWrite(kind, "rejected", journal.Rejected, "Create")
		if unresolved {
			appendWrite(kind, "pending", journal.Pending, "Create")
			appendWrite(kind, "unknown", journal.Unknown, "Create")
		}
	}
	appendWrite("ConfigMap", "recorded", journal.Committed, "Update")
	return repo, cell
}

func TestHistoricalCreatesUseOneSelectedAcknowledgedLoadAndAllFiniteKinds(t *testing.T) {
	repo, cell := historyFixture(t, true)
	s := newJournalSource(t, repo, Materialized, "", desiredFunc(func(context.Context, Target) (DesiredObservation, error) { return DesiredObservation{}, nil }))
	before := cell.reads
	view, err := model(t, s).Read(context.Background(), grant(t), target)
	if err != nil || view.CreateHistory == nil || cell.reads != before+1 {
		t.Fatal("history did not come from the single selected load", err)
	}
	if view.CreateHistory.Scope != historicalCreateScope || len(view.CreateHistory.Groups) != 13 {
		t.Fatal("finite kind/outcome groups lost")
	}
	for _, group := range view.CreateHistory.Groups {
		want := 1
		if group.Outcome == "OutcomeUnconfirmed" {
			want = 2 // Pending and Unknown do not assert dispatch or noncommit.
		}
		if group.Count != want {
			t.Fatal("wrong grouped count or non-Create action included")
		}
	}
	data, err := view.JSON()
	if err != nil || strings.Contains(string(data), "PRIVATE-") {
		t.Fatal("historical facts leaked private journal identity")
	}
	if view.Operation.Failure == nil || view.Operation.Failure.Code != "SmokeFailed" {
		t.Fatal("historical facts changed the original failure cause")
	}
	original := snapshot(t).Operation
	if view.Operation.Retry != original.Retry || view.Operation.Availability != original.Availability {
		t.Fatal("historical facts degraded or invented independent retry/route evidence")
	}
	var wire View
	if json.Unmarshal(data, &wire) != nil || !reflect.DeepEqual(wire.CreateHistory, view.CreateHistory) {
		t.Fatal("history did not survive actual wire serialization")
	}
	// Revocation after the source load and fact derivation still prevents return.
	g := grant(t)
	g.recheck = func(context.Context) error {
		if cell.reads > before+1 {
			return ErrForbidden
		}
		return nil
	}
	if _, err = model(t, s).Read(context.Background(), g, target); !errors.Is(err, ErrForbidden) {
		t.Fatal("history escaped final grant recheck")
	}
}

func TestHistoricalCreateArchiveSelectionDoesNotBorrowCurrentAttemptWrites(t *testing.T) {
	repo, _ := historyFixture(t, false)
	saved, _ := repo.Load(context.Background())
	next := historyCopy(saved.Envelope)
	old := next.Projection
	newProjection, _ := diagnostics.NewProjection(diagnostics.Identity{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID, ProjectName: "demo", OperationID: "op-new", Generation: 3, ReconcileKey: "different-reconcile"})
	next.Attempts = append(next.Attempts, journal.Attempt{Number: 2, OperationID: "op-new", NamespaceName: "candidate-new", Revision: "git-new", DesiredDigest: strings.Repeat("f", 64), State: journal.Reserved})
	next.Projection = &newProjection
	if _, err := repo.Commit(context.Background(), saved, next); err != nil {
		t.Fatal(err)
	}
	desired := desiredFunc(func(context.Context, Target) (DesiredObservation, error) { return DesiredObservation{}, nil })
	archived, err := model(t, newJournalSource(t, repo, Materialized, old.Identity.OperationID, desired)).Read(context.Background(), grant(t), target)
	if err != nil || archived.Source.Origin != Archived || archived.CreateHistory == nil || len(archived.CreateHistory.Groups) != 9 {
		t.Fatal("exact archived operation history lost", err)
	}
	current, err := model(t, newJournalSource(t, repo, Materialized, "", desired)).Read(context.Background(), grant(t), target)
	if err != nil || current.CreateHistory != nil {
		t.Fatal("current operation borrowed archived receipts", err)
	}
	// Defensive legacy association checks; these are intentionally not valid
	// Repository envelopes and cannot masquerade as native provenance.
	stored, _ := repo.Load(context.Background())
	differentRevision := historyCopy(stored.Envelope)
	differentRevision.Attempts[0].Revision = "git-commit-not-the-reconcile-key"
	for i := range differentRevision.Writes {
		if differentRevision.Writes[i].OperationID == old.Identity.OperationID {
			differentRevision.Writes[i].Preconditions.TargetRevision = differentRevision.Attempts[0].Revision
		}
	}
	if createHistoryFromJournal(differentRevision, old) == nil {
		t.Fatal("source revision was incorrectly equated with diagnostic reconcile key")
	}
	for name, mutate := range map[string]func(*journal.Envelope){
		"missing attempt":   func(e *journal.Envelope) { e.Attempts = e.Attempts[1:] },
		"duplicate attempt": func(e *journal.Envelope) { e.Attempts = append(e.Attempts, e.Attempts[0]) },
		"foreign subject":   func(e *journal.Envelope) { e.Subject.ProjectUID = "foreign" },
		"wrong revision":    func(e *journal.Envelope) { e.Writes[0].Preconditions.TargetRevision = "different" },
		"wrong parent":      func(e *journal.Envelope) { e.Writes[1].Preconditions.ParentUID = "foreign-parent" },
		"missing receipt":   func(e *journal.Envelope) { e.Writes[0].ResultVersion = "" },
	} {
		t.Run(name, func(t *testing.T) {
			e := historyCopy(stored.Envelope)
			mutate(&e)
			if createHistoryFromJournal(e, old) != nil {
				t.Fatal("ambiguous association supplied a partial plausible summary")
			}
		})
	}
}

func TestHistoricalCreateModelRejectsMismatchedPrivateFactsAndMalformedGroups(t *testing.T) {
	valid := func() Snapshot {
		s := snapshot(t)
		s.createHistory = &sourceCreateHistory{identity: s.Operation.Identity, groups: []CreateHistoryGroup{{Kind: "Service", Outcome: "CanonicalReceiptRecorded", Count: 1}}}
		return s
	}
	for name, mutate := range map[string]func(*Snapshot){
		"different operation":  func(s *Snapshot) { s.createHistory.identity.OperationID = "other" },
		"different generation": func(s *Snapshot) { s.createHistory.identity.Generation++ },
		"different reconcile":  func(s *Snapshot) { s.createHistory.identity.ReconcileKey = "other" },
		"different project":    func(s *Snapshot) { s.createHistory.identity.ProjectUID = "foreign" },
		"different name":       func(s *Snapshot) { s.createHistory.identity.ProjectName = "other" },
		"no operation":         func(s *Snapshot) { s.Operation = nil },
		"unknown kind":         func(s *Snapshot) { s.createHistory.groups[0].Kind = "PRIVATE-unknown" },
		"unknown outcome":      func(s *Snapshot) { s.createHistory.groups[0].Outcome = "AppliedAndHealthy" },
		"empty":                func(s *Snapshot) { s.createHistory.groups = nil },
		"zero":                 func(s *Snapshot) { s.createHistory.groups[0].Count = 0 },
		"negative":             func(s *Snapshot) { s.createHistory.groups[0].Count = -1 },
		"overflow":             func(s *Snapshot) { s.createHistory.groups[0].Count = journal.MaxWrites + 1 },
		"duplicate":            func(s *Snapshot) { s.createHistory.groups = append(s.createHistory.groups, s.createHistory.groups[0]) },
		"sum overflow": func(s *Snapshot) {
			s.createHistory.groups = []CreateHistoryGroup{{"Service", "Rejected", journal.MaxWrites}, {"ServiceAccount", "Rejected", 1}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := valid()
			mutate(&s)
			response := request(handler(t, model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil })), allowed(t)), http.MethodGet)
			if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), "PRIVATE") || strings.Contains(response.Body.String(), "createHistory") {
				t.Fatal("bad private association became an HTTP historical fact")
			}
		})
	}
	s := valid()
	view, err := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil })).Read(context.Background(), grant(t), target)
	if err != nil {
		t.Fatal(err)
	}
	s.createHistory.groups[0].Count = 9
	if view.CreateHistory.Groups[0].Count != 1 {
		t.Fatal("public view aliases private source facts")
	}
}

func TestHistoricalCreateAbsentWireCompatibilityAndFiniteFullBounds(t *testing.T) {
	s := snapshot(t)
	view, err := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil })).Read(context.Background(), grant(t), target)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := view.JSON()
	if bytes.Contains(before, []byte("createHistory")) {
		t.Fatal("legacy absent fact changed wire bytes")
	}
	// An independently decoded legacy value serializes byte-identically.
	var legacy View
	if json.Unmarshal(before, &legacy) != nil {
		t.Fatal("legacy decode failed")
	}
	after, _ := legacy.JSON()
	if !bytes.Equal(before, after) {
		t.Fatal("legacy wire changed with optional nil history")
	}
	fact := &sourceCreateHistory{identity: s.Operation.Identity}
	for _, kind := range createKinds {
		for _, outcome := range createOutcomes {
			fact.groups = append(fact.groups, CreateHistoryGroup{kind, outcome, 1})
		}
	}
	if full, err := fact.view(s.Operation.Identity); err != nil || len(full.Groups) != 15 {
		t.Fatal("finite fifteen-group shape refused")
	}
	fact.groups = []CreateHistoryGroup{{"Namespace", "OutcomeUnconfirmed", journal.MaxWrites}}
	if _, err := fact.view(s.Operation.Identity); err != nil {
		t.Fatal("exact total bound refused")
	}
}
