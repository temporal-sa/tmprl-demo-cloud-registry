package runtimeconfig

import (
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

func validDiagnosticConfig(c DiagnosticConfig) bool {
	_, err := diagnostics.NewProjection(diagnostics.Identity{Environment: "configuration", Installation: "configuration", ProjectUID: "configuration", ProjectName: c.ProjectName, OperationID: "configuration", ReconcileKey: "configuration"})
	return err == nil
}

// No historical generation/key/name is derived from an input digest, resource
// name or current mutable authoring. The accepted desired intent supplies these
// facts only while its new namespace attempt is first recorded.
func acceptedDiagnosticIdentity(c DiagnosticConfig, desired rollout.Desired, attempt string) diagnostics.Identity {
	return diagnostics.Identity{Environment: desired.Subject.Environment, Installation: desired.Subject.Installation, ProjectUID: desired.Subject.ProjectUID, ProjectName: c.ProjectName, OperationID: attempt, ReconcileKey: desired.ReconcileKey, Generation: desired.Generation}
}
func (w *Worker) initialProjection(desired rollout.Desired, attempt string) (*diagnostics.OperationProjection, error) {
	if w.config.Diagnostics == nil {
		return nil, nil
	}
	p, err := newInitialProjection(*w.config.Diagnostics, desired, attempt, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return &p, nil
}
func newInitialProjection(c DiagnosticConfig, desired rollout.Desired, attempt string, at time.Time) (diagnostics.OperationProjection, error) {
	id := acceptedDiagnosticIdentity(c, desired, attempt)
	p, err := diagnostics.NewProjection(id)
	if err != nil || at.IsZero() {
		return diagnostics.OperationProjection{}, failure(Invalid)
	}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 1, OccurredAt: at, State: diagnostics.Running, Step: "runtime.namespace.create"})
	if err != nil {
		return diagnostics.OperationProjection{}, failure(Invalid)
	}
	return p, nil
}
func (w *Worker) preflightDiagnosticIdentity() error {
	if w.config.Diagnostics == nil || len(w.snapshot.Envelope.Attempts) == 0 {
		return nil
	}
	p := w.snapshot.Envelope.Projection
	last := w.snapshot.Envelope.Attempts[len(w.snapshot.Envelope.Attempts)-1]
	if p == nil || p.Sequence == 0 || p.Identity.OperationID != last.OperationID || p.Identity.Environment != w.snapshot.Envelope.Subject.Environment || p.Identity.Installation != w.snapshot.Envelope.Subject.Installation || p.Identity.ProjectUID != w.snapshot.Envelope.Subject.ProjectUID || p.Identity.ProjectName != w.config.Diagnostics.ProjectName {
		return failure(Invalid)
	}
	if _, err := p.JSON(); err != nil {
		return failure(Invalid)
	}
	return nil
}
