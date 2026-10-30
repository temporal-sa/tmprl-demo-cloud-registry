package runtimefault

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

type Phase string

const (
	Admitted        Phase = "Admitted"
	BeforeAdmission Phase = "BeforeAdmission"
)

// Outcome is safe activity/history DATA. Its IDs, sequence and phase do not
// authorize storage, admission or provider replay, nor prove delivery. The
// configured boundary must pin accepted identity and persist this independently.
// BeforeAdmission cannot manufacture a Gate or modify a pinned HandoffReady
// journal; an independently authorized materialization sink is required.
type Outcome struct{ private *OutcomeView }
type OutcomeView struct {
	SchemaVersion int                  `json:"schemaVersion"`
	Identity      diagnostics.Identity `json:"identity"`
	EventID       string               `json:"eventID"`
	Sequence      uint64               `json:"sequence"`
	ObservedAt    time.Time            `json:"observedAt"`
	Phase         Phase                `json:"phase"`
	Fault         View                 `json:"fault"`
}

func NewOutcome(identity diagnostics.Identity, eventID string, sequence uint64, at time.Time, phase Phase, fault Fault) (Outcome, error) {
	if !validIdentity(identity) || !noUnsafeID(eventID) || sequence == 0 || at.IsZero() || !fault.Valid() || identity.OperationID != fault.private.OperationID || at.Before(fault.private.Failure.OccurredAt) || phase != Admitted && phase != BeforeAdmission {
		return Outcome{}, ErrContext
	}
	if phase == BeforeAdmission && (fault.private.JournalDelivery.State == Acknowledged || fault.private.Step != InputResolve && fault.private.Step != Observe && fault.private.Step != DispatchAuthorize) {
		return Outcome{}, ErrContext
	}
	v := OutcomeView{SchemaVersion: 1, Identity: identity, EventID: eventID, Sequence: sequence, ObservedAt: at.UTC(), Phase: phase, Fault: fault.View()}
	return Outcome{private: &v}, nil
}
func (o Outcome) View() OutcomeView {
	if o.private == nil {
		return OutcomeView{SchemaVersion: 1}
	}
	v := *o.private
	v.Fault = Fault{private: &v.Fault}.View()
	return v
}
func (o Outcome) MarshalJSON() ([]byte, error) {
	if o.private == nil {
		return nil, ErrContext
	}
	data, err := json.Marshal(o.View())
	if err != nil || len(data) > MaxJSONBytes {
		return nil, ErrContext
	}
	return data, nil
}
func (o Outcome) String() string             { data, _ := o.MarshalJSON(); return string(data) }
func (o Outcome) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, o.String()) }

// Event folds diagnostics ONLY. It has no execution callback. The actor must
// acknowledge this event/projection with its real outcome, handle exact duplicate
// identity/sequence safely, and repair delivery separately from provisioning.
// It deliberately leaves availability unchanged and retry UNKNOWN: neither an
// error class nor this envelope proves live routing or a scheduled retry.
func (o Outcome) Event() (diagnostics.OperationEvent, error) {
	if o.private == nil {
		return diagnostics.OperationEvent{}, ErrContext
	}
	v := o.View()
	f := v.Fault.Failure
	event := diagnostics.OperationEvent{SchemaVersion: diagnostics.SchemaVersion, Identity: v.Identity, Sequence: v.Sequence, OccurredAt: v.ObservedAt, State: diagnostics.Failed, Step: string(v.Fault.Step), Retry: &diagnostics.RetryInfo{Status: diagnostics.RetryUnknown}}
	if cleanupStep(v.Fault.Step) {
		event.State = diagnostics.Finalizing
		event.Recovery = []diagnostics.RecoveryOutcome{{ID: f.ID, Step: string(v.Fault.Step), State: diagnostics.OutcomeRetained, Failure: &f}}
	} else {
		event.Failure = &f
		if v.Fault.Step == ReceiptAcknowledge || v.Fault.Dispatch == DispatchUnknown || v.Fault.InputPersistence.State == DeliveryUnknown {
			event.State = diagnostics.Waiting
		}
	}
	return diagnostics.SafeEvent(event), nil
}
