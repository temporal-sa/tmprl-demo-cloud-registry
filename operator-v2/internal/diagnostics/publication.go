package diagnostics

import (
	"bytes"
	"encoding/json"
	"errors"
)

var ErrInvalidPublicationRepair = errors.New("publication repair changes domain facts or is not a monotone sink update")
var ErrPublicationCapacity = errors.New("publication repair cannot fit without changing retained domain facts or sink reports")

// ValidatePublicationRepair recognizes only the existing bounded WithPublication
// operation. Domain identity/sequence/state/progress/cause/recovery/evidence/
// availability/retry/warnings/partial flags must have byteequal canonical wire.
// Old sinks cannot disappear or reorder. A successful report is monotone within
// its exact sink/source sequence; a newer source may have an uncertain outcome.
// This pure check has no provider callback, authority grant or ACK confirmation.
func ValidatePublicationRepair(previous, next OperationProjection) error {
	before, after := previous, next
	before.Publication = nil
	after.Publication = nil
	if !publicationWireEqual(before, after) || !validRepairSinks(previous) || !validRepairSinks(next) {
		return ErrInvalidPublicationRepair
	}
	if !publicationSerializationStable(next) {
		return errors.Join(ErrInvalidPublicationRepair, ErrPublicationCapacity)
	}
	present := map[string]bool{}
	for _, report := range next.Publication {
		present[report.Sink] = true
	}
	for _, report := range previous.Publication {
		if !present[report.Sink] {
			return ErrInvalidPublicationRepair
		}
	}
	candidate := previous
	for _, report := range next.Publication {
		var err error
		candidate, err = candidate.WithPublication(report)
		if err != nil {
			return ErrInvalidPublicationRepair
		}
	}
	if !publicationWireEqual(candidate, next) {
		return ErrInvalidPublicationRepair
	}
	return nil
}
func validRepairSinks(p OperationProjection) bool {
	if len(p.Publication) > 4 {
		return false
	}
	seen := map[string]bool{}
	for _, r := range p.Publication {
		if r.Sink == "" || len(r.Sink) > 64 || seen[r.Sink] || r.ObservedAt.IsZero() || r.SourceSequence > p.Sequence || r.State != DeliveryPending && r.State != DeliveryUnknown && r.State != DeliverySucceeded && r.State != DeliveryFailed {
			return false
		}
		seen[r.Sink] = true
	}
	return true
}
func publicationWireEqual(a, b OperationProjection) bool {
	// Bypass the projection's broad safe/truncating MarshalJSON normalization:
	// validation must not hide an attempted domain-field change. Nested failures
	// retain their standard canonical safe wire representation.
	type wire OperationProjection
	left, le := json.Marshal(wire(a))
	right, re := json.Marshal(wire(b))
	return le == nil && re == nil && bytes.Equal(left, right)
}

// General projection JSON may intentionally compact bounded summaries. A
// publication-only CAS cannot use that path to lose previously retained domain
// facts or sink rows: its complete candidate wire must survive unchanged.
func publicationSerializationStable(p OperationProjection) bool {
	type wire OperationProjection
	full, err := json.Marshal(wire(p))
	if err != nil || len(full) > MaxJSONBytes {
		return false
	}
	encoded, err := p.JSON()
	return err == nil && bytes.Equal(full, encoded)
}
