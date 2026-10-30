package simulation

import (
	"context"
	"encoding/json"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"reflect"
)

// These methods model a single guarded in-memory writer. They are not proof of
// durable storage, a Kubernetes guard or real outstanding-request recovery.
func (w *World) AuthorizeAttempt(_ context.Context, r engine.Request) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, closed := w.supersessions[r.OperationID]; closed {
		return ErrConflict
	}
	if w.drainingOperations[r.OperationID] {
		return fault("AttemptDraining", "Normal execution is excluded while the previous attempt drains or awaits its durable acknowledgment.", diagnostics.Configuration)
	}
	if prior, exists := w.attemptBindings[r.OperationID]; exists && !reflect.DeepEqual(prior, r.Desired) {
		return ErrConflict
	}
	if w.unknownWrites[r.OperationID] {
		return fault("ProviderWriteUnresolved", "The exact previous provider write must be resolved before another execution step.", diagnostics.Configuration)
	}
	if w.currentOperation != "" && w.currentOperation != r.OperationID {
		if _, closed := w.supersessions[w.currentOperation]; !closed {
			return fault("PreviousAttemptNotDrained", "The previous candidate attempt has not been durably drained or retained.", diagnostics.Configuration)
		}
	}
	if w.currentOperation != r.OperationID {
		for _, unknown := range w.unknownWrites {
			if unknown {
				return fault("ProviderWriteUnresolved", "An unresolved provider write blocks the next attempt.", diagnostics.Configuration)
			}
		}
	}
	w.currentOperation = r.OperationID
	data, _ := json.Marshal(r.Desired)
	var desired rollout.Desired
	_ = json.Unmarshal(data, &desired)
	w.attemptBindings[r.OperationID] = desired
	w.candidate = w.candidates[r.OperationID]
	w.settled = w.settledByOperation[r.OperationID]
	w.failure = w.failureByOperation[r.OperationID]
	return nil
}
func (w *World) PrepareSupersession(_ context.Context, r engine.Request) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if prior, exists := w.attemptBindings[r.OperationID]; !exists || !reflect.DeepEqual(prior, r.Desired) {
		return ErrConflict
	}
	if w.currentOperation != "" && w.currentOperation != r.OperationID {
		return ErrConflict
	}
	if w.unknownWrites[r.OperationID] {
		return fault("ProviderWriteUnresolved", "The previous provider write must be independently resolved before draining or starting another attempt.", diagnostics.Configuration)
	}
	if w.currentOperation == "" {
		w.currentOperation = r.OperationID
	}
	if w.drainingOperations == nil {
		w.drainingOperations = map[string]bool{}
	}
	w.drainingOperations[r.OperationID] = true
	return nil
}
func (w *World) SettleSupersession(_ context.Context, outcome engine.SupersessionOutcome) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if outcome.OperationID != w.currentOperation || w.unknownWrites[outcome.OperationID] {
		return ErrConflict
	}
	if !w.drainingOperations[outcome.OperationID] {
		return ErrConflict
	}
	if w.FailSupersessionAck {
		return fault("SupersessionStorageUnavailable", "The previous attempt settlement was not durably acknowledged.", diagnostics.Publication)
	}
	if outcome.Candidate != nil {
		if w.candidate == nil || outcome.Candidate.Observation != w.candidate.observation || outcome.Candidate.Record == nil || w.records[w.candidate.observation.Ref.ProviderID] != *outcome.Candidate.Record {
			return ErrConflict
		}
	} else if w.candidate != nil {
		return ErrConflict
	}
	if outcome.Disposition == engine.AttemptActive && (w.active == nil || w.candidate == nil || w.active.ResourceID != w.candidate.observation.Ref.ProviderID || w.route == nil || w.route.observation.Revision != w.candidate.observation.Revision) {
		return ErrConflict
	}
	if prior, exists := w.supersessions[outcome.OperationID]; exists {
		if prior.Disposition != outcome.Disposition || !reflect.DeepEqual(prior.Desired, outcome.Desired) {
			return ErrConflict
		}
		return nil
	}
	data, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	var copy engine.SupersessionOutcome
	if err = json.Unmarshal(data, &copy); err != nil {
		return err
	}
	w.supersessions[outcome.OperationID] = copy
	w.trace = append(w.trace, "settle-supersession:"+outcome.OperationID)
	return nil
}
func (w *World) SetUnknownWrite(operation string, unknown bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.unknownWrites[operation] = unknown
}
func (w *World) Supersession(operation string) (engine.SupersessionOutcome, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	value, ok := w.supersessions[operation]
	data, _ := json.Marshal(value)
	var copy engine.SupersessionOutcome
	_ = json.Unmarshal(data, &copy)
	return copy, ok
}
func (w *World) CandidateForAttempt(operation string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.candidates[operation] != nil
}
