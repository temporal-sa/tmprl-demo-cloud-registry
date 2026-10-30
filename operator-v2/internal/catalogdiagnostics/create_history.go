package catalogdiagnostics

import (
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
)

const historicalCreateScope = "HistoricalCreateRecords"

var createKinds = [...]string{"Namespace", "ConfigMap", "Deployment", "Service", "ServiceAccount"}
var createOutcomes = [...]string{"CanonicalReceiptRecorded", "Rejected", "OutcomeUnconfirmed"}

// CreateHistoryView contains acknowledged historical journal facts only. It
// establishes neither present existence/health nor dispatch of an unknown write.
type CreateHistoryView struct {
	Scope  string               `json:"scope"`
	Groups []CreateHistoryGroup `json:"groups"`
}

type CreateHistoryGroup struct {
	Kind    string `json:"kind"`
	Outcome string `json:"outcome"`
	Count   int    `json:"count"`
}

// Only JournalSource constructs this private same-load association. The copied
// identity checks consistency with the selected projection; it is not separate
// evidence of historical generation/reconcile provenance or a mutation grant.
type sourceCreateHistory struct {
	identity diagnostics.Identity
	groups   []CreateHistoryGroup
}

func createKindKnown(kind string) bool {
	for _, known := range createKinds {
		if kind == known {
			return true
		}
	}
	return false
}

// Ambiguous/missing legacy associations omit the whole optional fact rather
// than count only the apparently convenient subset of a selected operation.
func createHistoryFromJournal(e journal.Envelope, p *diagnostics.OperationProjection) *sourceCreateHistory {
	if p == nil || len(e.Writes) > journal.MaxWrites || p.Identity.Environment != e.Subject.Environment || p.Identity.Installation != e.Subject.Installation || p.Identity.ProjectUID != e.Subject.ProjectUID {
		return nil
	}
	var attempt *journal.Attempt
	for i := range e.Attempts {
		if e.Attempts[i].OperationID == p.Identity.OperationID {
			if attempt != nil {
				return nil
			}
			attempt = &e.Attempts[i]
		}
	}
	if attempt == nil || attempt.Revision == "" {
		return nil
	}
	counts := make(map[[2]string]int)
	for _, w := range e.Writes {
		if w.OperationID != attempt.OperationID || w.Preconditions.Action != "Create" {
			continue
		}
		pre := w.Preconditions
		if !createKindKnown(pre.Kind) || pre.TargetRevision != attempt.Revision {
			return nil
		}
		if pre.Kind == "Namespace" {
			if pre.Name != attempt.NamespaceName || pre.ParentUID != "" {
				return nil
			}
		} else if attempt.NamespaceUID == "" || pre.ParentUID != attempt.NamespaceUID {
			return nil
		}
		var outcome string
		switch w.State {
		case journal.Committed:
			if w.ResultUID == "" || w.ResultVersion == "" {
				return nil
			}
			outcome = "CanonicalReceiptRecorded"
		case journal.Rejected:
			outcome = "Rejected"
		case journal.Pending, journal.Unknown:
			outcome = "OutcomeUnconfirmed"
		default:
			return nil
		}
		counts[[2]string{pre.Kind, outcome}]++
	}
	if len(counts) == 0 {
		return nil
	}
	fact := &sourceCreateHistory{identity: p.Identity}
	for _, kind := range createKinds {
		for _, outcome := range createOutcomes {
			if count := counts[[2]string{kind, outcome}]; count > 0 {
				fact.groups = append(fact.groups, CreateHistoryGroup{Kind: kind, Outcome: outcome, Count: count})
			}
		}
	}
	return fact
}

func (h *sourceCreateHistory) view(identity diagnostics.Identity) (*CreateHistoryView, error) {
	if h == nil {
		return nil, nil
	}
	if h.identity != identity || len(h.groups) == 0 || len(h.groups) > len(createKinds)*len(createOutcomes) {
		return nil, ErrInvalidSource
	}
	seen := make(map[[2]string]bool)
	total := 0
	for _, group := range h.groups {
		knownOutcome := false
		for _, outcome := range createOutcomes {
			knownOutcome = knownOutcome || group.Outcome == outcome
		}
		key := [2]string{group.Kind, group.Outcome}
		if !createKindKnown(group.Kind) || !knownOutcome || seen[key] || group.Count <= 0 || group.Count > journal.MaxWrites-total {
			return nil, ErrInvalidSource
		}
		seen[key] = true
		total += group.Count
	}
	return &CreateHistoryView{Scope: historicalCreateScope, Groups: append([]CreateHistoryGroup(nil), h.groups...)}, nil
}
