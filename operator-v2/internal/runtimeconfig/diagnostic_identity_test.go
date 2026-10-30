package runtimeconfig

import (
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"testing"
	"time"
)

func TestNewDiagnosticIdentityUsesAcceptedIntentWithoutInventedProgress(t *testing.T) {
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}
	desired := rollout.Desired{Subject: subject, Generation: 7, ReconcileKey: "accepted-key", Revision: "revision"}
	at := time.Date(2026, 10, 30, 5, 0, 0, 0, time.UTC)
	p, err := newInitialProjection(DiagnosticConfig{ProjectName: "demo"}, desired, "attempt-one", at)
	if err != nil || p.Identity.Generation != 7 || p.Identity.ReconcileKey != "accepted-key" || p.Identity.OperationID != "attempt-one" || p.Identity.ProjectName != "demo" || p.Sequence != 1 || p.State != diagnostics.Running || !p.ProgressedAt.Equal(at) || p.Availability.RouteState != diagnostics.RouteUnknown || p.Retry.Status != diagnostics.RetryUnknown {
		t.Fatal("initial accepted identity/progress differs", err)
	}
	desired.Generation = -1
	_, err = newInitialProjection(DiagnosticConfig{ProjectName: "demo"}, desired, "attempt-one", at)
	requireCode(t, err, Invalid)
}
func TestEnabledDiagnosticsCannotGuessHistoricalIdentity(t *testing.T) {
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}
	w := &Worker{config: Config{Diagnostics: &DiagnosticConfig{ProjectName: "demo"}}, snapshot: journal.Snapshot{Envelope: journal.Envelope{Subject: subject, Attempts: []journal.Attempt{{OperationID: "attempt-one"}}}}}
	requireCode(t, w.preflightDiagnosticIdentity(), Invalid)
	p, err := newInitialProjection(*w.config.Diagnostics, rollout.Desired{Subject: subject, Generation: 1, ReconcileKey: "key"}, "attempt-one", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	w.snapshot.Envelope.Projection = &p
	if err := w.preflightDiagnosticIdentity(); err != nil {
		t.Fatal(err)
	}
	p.Identity.OperationID = "foreign-operation"
	requireCode(t, w.preflightDiagnosticIdentity(), Invalid)
	w.config.Diagnostics = nil
	if err := w.preflightDiagnosticIdentity(); err != nil {
		t.Fatal("legacy prototype compatibility", err)
	}
}
