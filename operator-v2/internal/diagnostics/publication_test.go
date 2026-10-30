package diagnostics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestUnknownPublicationRepairPreservesAllDomainFacts(t *testing.T) {
	original := apply(t, projection(t), failedEvent())
	report := SinkHealth{Sink: "catalog", State: DeliveryUnknown, SourceSequence: original.Sequence, ObservedAt: observed.Add(time.Minute), Failure: &DomainFailure{ID: "delivery", Code: "JournalAckUnknown", Category: Publication, Message: "Delivery acknowledgement is unknown."}}
	unknown, err := original.WithPublication(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePublicationRepair(original, unknown); err != nil {
		t.Fatal(err)
	}
	if unknown.Sequence != original.Sequence || unknown.ProgressedAt != original.ProgressedAt || !reflect.DeepEqual(unknown.Failure, original.Failure) || unknown.Availability != original.Availability || !reflect.DeepEqual(unknown.Retry, original.Retry) {
		t.Fatal("delivery fabricated domain progress/outcome")
	}
	if !strings.Contains(Explain(unknown), "unknown acknowledgement independently") {
		t.Fatal("unknown acknowledgement not human-readable")
	}
	acknowledged, err := unknown.WithPublication(SinkHealth{Sink: "catalog", State: DeliverySucceeded, SourceSequence: original.Sequence, ObservedAt: observed.Add(2 * time.Minute)})
	if err != nil || ValidatePublicationRepair(unknown, acknowledged) != nil {
		t.Fatal("ACK repair rejected", err)
	}
	if len(original.Publication) != 0 || unknown.Publication[0].State != DeliveryUnknown || acknowledged.Publication[0].Failure != nil {
		t.Fatal("repair aliased prior values")
	}
	if _, err := acknowledged.WithPublication(report); !errors.Is(err, ErrStaleEvent) {
		t.Fatal("older sink report accepted")
	}
	report.ObservedAt = observed.Add(3 * time.Minute)
	if _, err := acknowledged.WithPublication(report); !errors.Is(err, ErrStaleEvent) {
		t.Fatal("same source ACK regressed to Unknown")
	}
	// A real newer domain event changes the delivery question. Old source ACK
	// does not establish delivery of the new source sequence.
	next := apply(t, acknowledged, OperationEvent{Identity: identity(), Sequence: 2, OccurredAt: observed.Add(3 * time.Minute), State: Running, Step: "new-real-progress"})
	report.SourceSequence = 2
	report.ObservedAt = observed.Add(4 * time.Minute)
	uncertain, err := next.WithPublication(report)
	if err != nil || ValidatePublicationRepair(next, uncertain) != nil {
		t.Fatal("new source uncertainty conflated with old ACK", err)
	}
}
func TestPublicationRepairRejectsDisappearingReorderedOrSmuggledFacts(t *testing.T) {
	p := apply(t, projection(t), failedEvent())
	var err error
	p, err = p.WithPublication(SinkHealth{Sink: "catalog", State: DeliveryUnknown, SourceSequence: 1, ObservedAt: observed.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.WithPublication(SinkHealth{Sink: "status", State: DeliveryFailed, SourceSequence: 1, ObservedAt: observed.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*OperationProjection){
		func(n *OperationProjection) { n.Publication = n.Publication[:1] },
		func(n *OperationProjection) { n.Publication[0], n.Publication[1] = n.Publication[1], n.Publication[0] },
		func(n *OperationProjection) { n.Publication = append(n.Publication, n.Publication[0]) },
		func(n *OperationProjection) { n.Publication[0].SourceSequence = 2 },
		func(n *OperationProjection) { n.Publication[0].ObservedAt = observed.Add(-time.Minute) },
		func(n *OperationProjection) { n.Publication[0].State = "not-a-state" },
		func(n *OperationProjection) { n.Step = "fake-progress" },
		func(n *OperationProjection) { n.ProgressedAt = observed.Add(time.Minute) },
		func(n *OperationProjection) { n.State = Succeeded },
		func(n *OperationProjection) { n.Sequence++ },
		func(n *OperationProjection) { n.Failure.Message = "replacement cause" },
		func(n *OperationProjection) { n.Availability.RouteState = RoutePromoted },
		func(n *OperationProjection) { n.Retry.Status = RetryScheduled },
		func(n *OperationProjection) { n.Recovery = []RecoveryOutcome{{ID: "new", State: OutcomeCompleted}} },
		func(n *OperationProjection) { n.Evidence = []EvidenceRef{{ID: "new"}} },
		func(n *OperationProjection) { n.Warnings = []DomainFailure{{ID: "new", Message: "new"}} },
		func(n *OperationProjection) { n.Partial = !n.Partial },
	} {
		wire, err := p.JSON()
		if err != nil {
			t.Fatal(err)
		}
		var n OperationProjection
		if json.Unmarshal(wire, &n) != nil {
			t.Fatal("clone")
		}
		change(&n)
		if err := ValidatePublicationRepair(p, n); !errors.Is(err, ErrInvalidPublicationRepair) {
			t.Fatal("invalid publication-only repair accepted")
		}
	}
	for i := 2; i < 4; i++ {
		p, err = p.WithPublication(SinkHealth{Sink: fmt.Sprintf("sink-%d", i), State: DeliveryUnknown, SourceSequence: 1, ObservedAt: observed.Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = p.WithPublication(SinkHealth{Sink: "overflow", State: DeliveryUnknown, SourceSequence: 1, ObservedAt: observed.Add(time.Minute)}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal("new sink silently discarded at bound")
	}
}

func TestPublicationRepairCannotCompactPreviouslyRetainedDomainFacts(t *testing.T) {
	p := apply(t, projection(t), failedEvent())
	// First materialize an intentionally bounded general projection. Its existing
	// partial marker does not permit a publication repair to lose more domain data.
	for i := 0; i < 8; i++ {
		p.Recovery = append(p.Recovery, RecoveryOutcome{ID: fmt.Sprintf("retained-%d", i), Step: "capture-evidence", State: OutcomeRetained, Failure: &DomainFailure{ID: fmt.Sprintf("failure-%d", i), Code: "EvidenceFailed", Category: Cleanup, Message: strings.Repeat("m", 768)}})
	}
	oldWire, err := p.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(oldWire, &p) != nil {
		t.Fatal("old bounded projection decode")
	}
	large := DomainFailure{ID: "delivery-big", Code: "PublicationUnknown", Category: Publication, Message: strings.Repeat("p", 768), NextAction: strings.Repeat("a", 512), Causes: []Cause{{Type: "safe-evidence", Message: strings.Repeat("c", 192)}, {Type: "safe-evidence", Message: strings.Repeat("d", 192)}}}
	next, err := p.WithPublication(SinkHealth{Sink: "catalog", State: DeliveryUnknown, SourceSequence: p.Sequence, ObservedAt: observed.Add(time.Minute), Failure: &large})
	if err != nil {
		t.Fatal(err)
	}
	type wire OperationProjection
	full, _ := json.Marshal(wire(next))
	compacted, err := next.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(full, compacted) {
		t.Fatal("regression fixture did not exercise aggregate compaction")
	}
	if err := ValidatePublicationRepair(p, next); !errors.Is(err, ErrInvalidPublicationRepair) || !errors.Is(err, ErrPublicationCapacity) {
		t.Fatal("publication repair could compact prior domain facts or lost capacity category")
	}
}
