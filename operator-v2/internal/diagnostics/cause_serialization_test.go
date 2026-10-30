package diagnostics

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAggregateCompactionRetainsExactBoundedOriginalCause(t *testing.T) {
	at := time.Date(2026, 10, 30, 5, 0, 0, 0, time.UTC)
	id := Identity{Environment: strings.Repeat("e", 128), Installation: strings.Repeat("i", 128), ProjectUID: strings.Repeat("u", 128), ProjectName: strings.Repeat("n", 128), OperationID: strings.Repeat("o", 128), ReconcileKey: strings.Repeat("k", 128), Generation: 1}
	p, err := NewProjection(id)
	if err != nil {
		t.Fatal(err)
	}
	failure := SafeFailure(DomainFailure{ID: "original", Code: "ReadinessFailed", Category: Readiness, Message: strings.Repeat("m", 768), NextAction: strings.Repeat("a", 512), OccurredAt: at, Provider: ProviderRef{Service: strings.Repeat("s", 64), Code: strings.Repeat("c", 96), RequestID: strings.Repeat("r", 128)}, Execution: ExecutionRef{Namespace: strings.Repeat("x", 64)}})
	original, _ := json.Marshal(failure)
	if len(failure.Message) != 768 || len(original) > MaxFailureBytes {
		t.Fatal("fixture original cause already compacted")
	}
	refs := make([]ResourceRef, 4)
	for i := range refs {
		refs[i] = ResourceRef{Provider: strings.Repeat("p", 48), Kind: strings.Repeat("t", 64), ID: strings.Repeat("v", 128)}
	}
	p, err = p.Apply(OperationEvent{Identity: id, Sequence: 1, OccurredAt: at, State: Failed, Step: strings.Repeat("d", 128), Failure: &failure, Recovery: []RecoveryOutcome{{ID: strings.Repeat("y", 96), Step: strings.Repeat("z", 128), State: OutcomeRetained, Failure: &failure, RetainedResources: refs}}, Availability: &Availability{ActiveRevision: strings.Repeat("h", 128), CandidateRevision: strings.Repeat("b", 128), RouteState: RouteActivePreserved, ConfirmedAt: at}})
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.WithPublication(SinkHealth{Sink: strings.Repeat("q", 64), State: DeliveryUnknown, SourceSequence: 1, ObservedAt: at, Failure: &failure})
	if err != nil {
		t.Fatal(err)
	}
	type wire OperationProjection
	full, _ := json.Marshal(wire(p))
	if len(full) <= MaxJSONBytes {
		t.Fatalf("fixture did not reach aggregate compaction: %d bytes", len(full))
	}
	encoded, err := p.JSON()
	if err != nil || len(encoded) > MaxJSONBytes {
		t.Fatal("bounded projection unavailable", err)
	}
	var decoded OperationProjection
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(decoded.Failure)
	if !bytes.Equal(original, after) {
		t.Fatal("aggregate serialization rewrote original cause")
	}
	if !decoded.Partial || decoded.Identity != id || decoded.Sequence != p.Sequence || !decoded.ProgressedAt.Equal(p.ProgressedAt) {
		t.Fatal("compaction lost explicit partial/progress identity")
	}
}
