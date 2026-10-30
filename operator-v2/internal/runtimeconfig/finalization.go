package runtimeconfig

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
)

// runtimeFinalizationReady checks retained facts, never present-time name
// absence or an error string. Consumed known namespaces already carry the
// acknowledged cleanup protocol's canonical absence fact.
func runtimeFinalizationReady(e journal.Envelope) error {
	p := e.Projection
	if p == nil || p.State != diagnostics.Finalizing && p.State != diagnostics.Finalized {
		return failure(Invalid)
	}
	if p.Identity.Environment != e.Subject.Environment || p.Identity.Installation != e.Subject.Installation || p.Identity.ProjectUID != e.Subject.ProjectUID {
		return failure(SubjectMismatch)
	}
	for _, write := range e.Writes {
		if write.State == journal.Pending || write.State == journal.Unknown {
			return failure(Blocked)
		}
	}
	finalizing := false
	for _, attempt := range e.Attempts {
		if attempt.State != journal.Consumed || attempt.NamespaceUID != "" && !attempt.CanonicalAbsent {
			return failure(Blocked)
		}
		if attempt.DrainReason != journal.DrainProjectFinalization {
			continue
		}
		finalizing = true
		completed := false
		for _, recovery := range p.Recovery {
			if recovery.ID == retirementRecoveryID(attempt.OperationID) && recovery.Step == "namespace-cleanup" && recovery.State == diagnostics.OutcomeCompleted && len(recovery.RetainedResources) == 0 {
				completed = true
			}
		}
		if !completed {
			return failure(Blocked)
		}
	}
	if !finalizing {
		return failure(Blocked)
	}
	return nil
}

// CompleteRuntimeFinalization acknowledges completion for the finite local
// runtime's known namespaces. It does not stop payload processes, finalize a
// project CR, or establish cleanup of resources in other providers. No provider
// mutation is performed here. Normal writes remain disabled by the durable
// project-finalization reason, including after a clean process handoff.
func (w *Worker) CompleteRuntimeFinalization(ctx context.Context) error {
	if w == nil || w.config.Client == nil {
		return failure(Invalid)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.current(ctx); err != nil {
		return err
	}
	if err := runtimeFinalizationReady(w.snapshot.Envelope); err != nil {
		return err
	}
	p := w.snapshot.Envelope.Projection
	// Even an idempotent call has checked current admission and every retained
	// completion fact above. It does not append an invented progress event.
	if p.State == diagnostics.Finalized {
		return nil
	}
	if p.Sequence == ^uint64(0) {
		return failure(Invalid)
	}
	updated, err := p.Apply(diagnostics.OperationEvent{Identity: p.Identity, Sequence: p.Sequence + 1, OccurredAt: time.Now().UTC(), State: diagnostics.Finalized, Step: "runtime-finalized", Retry: &p.Retry, Availability: &p.Availability})
	if err != nil {
		return failure(Invalid)
	}
	next := clone(w.snapshot.Envelope)
	next.Projection = &updated
	return w.commit(ctx, next)
}
