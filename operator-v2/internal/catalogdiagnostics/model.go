package catalogdiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

const MaxResponseBytes = 32 * 1024

type Origin string

const (
	Materialized Origin = "Materialized"
	Archived     Origin = "Archived"
)

// Snapshot comes from a materialized/archived durable source. Desired is an
// independently obtained observation, never copied from the operation simply
// to make it appear current. No worker must be available.
type Snapshot struct {
	Target            Target
	Origin            Origin
	Operation         *diagnostics.OperationProjection
	RecordedAt        time.Time
	Desired           diagnostics.DesiredState
	DesiredKnown      bool
	DesiredObservedAt time.Time
	Partial           bool
	createHistory     *sourceCreateHistory
}
type StatusSource interface {
	Read(context.Context, ReadGrant) (Snapshot, error)
}
type Config struct {
	Now                func() time.Time
	MaxProgressAge     time.Duration
	MaxDesiredAge      time.Duration
	MaxAvailabilityAge time.Duration
}
type Model struct {
	source StatusSource
	config Config
}

func NewModel(source StatusSource, config Config) (*Model, error) {
	if source == nil || config.Now == nil || config.MaxProgressAge <= 0 || config.MaxDesiredAge <= 0 || config.MaxAvailabilityAge <= 0 {
		return nil, errors.New("diagnostic model requires an explicit source, clock and freshness limits")
	}
	return &Model{source: source, config: config}, nil
}

type SourceView struct {
	Origin               Origin     `json:"origin"`
	RecordedAt           *time.Time `json:"recordedAt,omitempty"`
	DesiredObservedAt    *time.Time `json:"desiredObservedAt,omitempty"`
	ReadAt               time.Time  `json:"readAt"`
	PersistenceTimeKnown bool       `json:"persistenceTimeKnown"`
}
type View struct {
	SchemaVersion               int                              `json:"schemaVersion"`
	Target                      Target                           `json:"target"`
	Status                      string                           `json:"status"`
	Source                      SourceView                       `json:"source"`
	Freshness                   diagnostics.Freshness            `json:"freshness"`
	Operation                   *diagnostics.OperationProjection `json:"operation,omitempty"`
	Explanation                 string                           `json:"explanation"`
	EvidenceResolutionAvailable bool                             `json:"evidenceResolutionAvailable"`
	Partial                     bool                             `json:"partial,omitempty"`
	CreateHistory               *CreateHistoryView               `json:"createHistory,omitempty"`
}

// Read checks scope and expiry before crossing the source boundary. Raw source
// errors are discarded; only stable safe categories leave this method.
func (m *Model) Read(ctx context.Context, grant ReadGrant, target Target) (View, error) {
	now := m.config.Now()
	if err := grant.authorize(ctx, target, now); err != nil {
		return View{}, err
	}
	if ctx.Err() != nil {
		return View{}, ErrSourceUnavailable
	}
	snapshot, err := m.source.Read(ctx, grant)
	now = m.config.Now()
	if authErr := grant.authorize(ctx, target, now); authErr != nil {
		return View{}, authErr
	}
	if ctx.Err() != nil {
		return View{}, ErrSourceUnavailable
	}
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return View{}, ErrNotFound
		}
		return View{}, ErrSourceUnavailable
	}
	if snapshot.Target != target || (snapshot.Origin != Materialized && snapshot.Origin != Archived) {
		return View{}, ErrInvalidSource
	}
	if snapshot.RecordedAt.After(now) {
		return View{}, ErrInvalidSource
	}
	if snapshot.DesiredKnown && (!validReconcileKey(snapshot.Desired.ReconcileKey) || snapshot.Desired.Generation < 0) {
		return View{}, ErrInvalidSource
	}
	view := View{SchemaVersion: 1, Target: target, Status: "NotObserved", Source: SourceView{Origin: snapshot.Origin, ReadAt: now, PersistenceTimeKnown: !snapshot.RecordedAt.IsZero()}, Explanation: "No operation status has been recorded for this project.", Partial: snapshot.Partial}
	if !snapshot.RecordedAt.IsZero() {
		copy := snapshot.RecordedAt
		view.Source.RecordedAt = &copy
	}
	if !snapshot.DesiredObservedAt.IsZero() {
		copy := snapshot.DesiredObservedAt
		view.Source.DesiredObservedAt = &copy
	}
	if snapshot.Operation == nil {
		if snapshot.createHistory != nil {
			return View{}, ErrInvalidSource
		}
		view.Freshness = diagnostics.Freshness{State: diagnostics.FreshnessUnknown, EvaluatedAt: now}
		return view, nil
	}
	p := *snapshot.Operation
	switch p.State {
	case diagnostics.Pending, diagnostics.Running, diagnostics.Waiting, diagnostics.Retrying, diagnostics.Succeeded, diagnostics.Failed, diagnostics.Cancelled, diagnostics.Finalizing, diagnostics.Finalized:
	default:
		return View{}, ErrInvalidSource
	}
	if p.Identity.Environment != target.Environment || p.Identity.Installation != target.Installation || p.Identity.ProjectUID != target.ProjectUID {
		return View{}, ErrInvalidSource
	}
	if snapshot.RecordedAt.After(now) {
		return View{}, ErrInvalidSource
	}
	// This clones nested fields, enforces schema/size and redacts before a value
	// leaves the read-model boundary.
	safe, err := p.JSON()
	if err != nil {
		return View{}, ErrInvalidSource
	}
	// Decode into a zero value: unmarshalling over the shallow source copy can
	// reuse its slice backing arrays and mutate the source while stripping links.
	var cloned diagnostics.OperationProjection
	if json.Unmarshal(safe, &cloned) != nil {
		return View{}, ErrInvalidSource
	}
	p = cloned
	view.CreateHistory, err = snapshot.createHistory.view(p.Identity)
	if err != nil {
		return View{}, ErrInvalidSource
	}
	if p.Identity.Environment != target.Environment || p.Identity.Installation != target.Installation || p.Identity.ProjectUID != target.ProjectUID {
		return View{}, ErrInvalidSource
	}
	for i := range p.Evidence {
		p.Evidence[i].Locator = ""
	}
	confirmed := p.Availability.ConfirmedAt
	if snapshot.Origin == Archived || confirmed.IsZero() || confirmed.After(now) || now.Sub(confirmed) > m.config.MaxAvailabilityAge {
		p.Availability.RouteState = diagnostics.RouteUnconfirmed
	}
	desired := snapshot.Desired
	if !snapshot.DesiredKnown {
		desired = diagnostics.DesiredState{}
	}
	view.Freshness = p.AssessFreshness(desired, now, m.config.MaxProgressAge)
	if !snapshot.DesiredKnown || snapshot.DesiredObservedAt.IsZero() || snapshot.DesiredObservedAt.After(now) {
		view.Freshness.State = diagnostics.FreshnessUnknown
	} else if now.Sub(snapshot.DesiredObservedAt) > m.config.MaxDesiredAge {
		view.Freshness.State = diagnostics.FreshnessStale
	}
	if snapshot.RecordedAt.IsZero() {
		view.Freshness.State = diagnostics.FreshnessUnknown
	}
	view.Status = string(p.State)
	view.Operation = &p
	view.Explanation = diagnostics.Explain(p)
	view.Partial = snapshot.Partial || p.Partial || snapshot.RecordedAt.IsZero() || !snapshot.DesiredKnown
	if snapshot.RecordedAt.IsZero() {
		view.Explanation = "Persistence time was not recorded; persistence freshness is unknown. " + view.Explanation
	}
	if snapshot.Origin == Archived {
		view.Explanation = "Archived operation record; serving availability is unconfirmed. " + view.Explanation
	}
	encoded, err := view.JSON()
	if err != nil || len(encoded) > MaxResponseBytes {
		return View{}, ErrInvalidSource
	}
	return view, nil
}
func validReconcileKey(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == ':' || c == '/' || c == '@' || c == '+') {
			return false
		}
	}
	return true
}

// JSON serializes the already-safe Model.Read result. Do not construct views
// from arbitrary source/log strings: producers must sanitize known sensitive
// values before durable persistence; heuristic redaction cannot discover every
// application secret. Structured evidence resolution is deliberately separate.
func (v View) JSON() ([]byte, error) {
	type wire View
	data, err := json.Marshal(wire(v))
	if err != nil || len(data) > MaxResponseBytes {
		return nil, ErrInvalidSource
	}
	return data, nil
}
