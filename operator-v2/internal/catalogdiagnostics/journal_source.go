package catalogdiagnostics

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type DesiredObservation struct {
	Desired    diagnostics.DesiredState
	Known      bool
	ObservedAt time.Time
}

// DesiredObserver independently observes the accepted project intent. It must
// not derive current desired state from the operation projection being read.
// Errors degrade desired freshness while preserving the durable failure cause.
type DesiredObserver interface {
	ObserveDesired(context.Context, Target) (DesiredObservation, error)
}

// JournalBinding is trusted bootstrap configuration. Repository pins actual
// cell UID and subject; registration is not name adoption or project discovery.
// Origin=Archived marks an administratively retired source. OperationID selects
// a retained operation if provided; otherwise the latest projection is read.
type JournalBinding struct {
	Target      Target
	Repository  *journal.Repository
	Origin      Origin
	OperationID string
}
type JournalSource struct {
	environment, installation string
	bindings                  map[Target]JournalBinding
	desired                   DesiredObserver
	now                       func() time.Time
}

func NewJournalSource(environment, installation string, bindings []JournalBinding, desired DesiredObserver, now func() time.Time) (*JournalSource, error) {
	if !scopeID.MatchString(environment) || !scopeID.MatchString(installation) || desired == nil || now == nil || len(bindings) > 4096 {
		return nil, ErrInvalidSource
	}
	s := &JournalSource{environment: environment, installation: installation, bindings: make(map[Target]JournalBinding, len(bindings)), desired: desired, now: now}
	for _, b := range bindings {
		if !b.Target.valid() || b.Target.Environment != environment || b.Target.Installation != installation || b.Repository == nil || (b.Origin != Materialized && b.Origin != Archived) || (b.OperationID != "" && !validReconcileKey(b.OperationID)) {
			return nil, ErrInvalidSource
		}
		if _, exists := s.bindings[b.Target]; exists {
			return nil, ErrInvalidSource
		}
		s.bindings[b.Target] = b
	}
	return s, nil
}

// Read has no workflow/client query or fallback resource discovery. Even direct
// callers need a live scoped grant before the registry existence lookup. Missing
// or replaced previously pinned storage is unavailable, not project-not-found.
func (s *JournalSource) Read(ctx context.Context, grant ReadGrant) (Snapshot, error) {
	target := grant.Target()
	if target.Environment != s.environment || target.Installation != s.installation {
		return Snapshot{}, ErrForbidden
	}
	if err := grant.authorize(ctx, target, s.now()); err != nil {
		return Snapshot{}, err
	}
	if ctx.Err() != nil {
		return Snapshot{}, ErrSourceUnavailable
	}
	b, ok := s.bindings[target]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	stored, err := b.Repository.Load(ctx)
	if err != nil {
		return Snapshot{}, ErrSourceUnavailable
	}
	if stored.Envelope.Subject != (rollout.Subject{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID}) {
		return Snapshot{}, ErrInvalidSource
	}
	operation := stored.Envelope.Projection
	origin := b.Origin
	if b.OperationID != "" && (operation == nil || operation.Identity.OperationID != b.OperationID) {
		operation = nil
		for i := range stored.Envelope.Archive {
			if stored.Envelope.Archive[i].Identity.OperationID == b.OperationID {
				operation = &stored.Envelope.Archive[i]
				origin = Archived
				break
			}
		}
		if operation == nil {
			return Snapshot{}, ErrNotFound
		}
	}
	result := Snapshot{Target: target, Origin: origin, Operation: operation}
	// Derive only from this exact acknowledged load and selected operation.
	// The final grant recheck below also encompasses this association work.
	result.createHistory = createHistoryFromJournal(stored.Envelope, operation)
	// Schema1 journal does not persist its storage commit time. Neither a fresh
	// GET nor event UpdatedAt establishes that clock. RecordedAt stays zero and
	// the read model explicitly reports unknown persistence freshness.
	if operation != nil {
		result.Partial = true
	}
	desired, err := s.desired.ObserveDesired(ctx, target)
	if err != nil {
		result.Partial = true
	} else {
		result.Desired, result.DesiredKnown, result.DesiredObservedAt = desired.Desired, desired.Known, desired.ObservedAt
	}
	if ctx.Err() != nil {
		return Snapshot{}, ErrSourceUnavailable
	}
	if err := grant.authorize(ctx, target, s.now()); err != nil {
		return Snapshot{}, err
	}
	return result, nil
}
