package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

func publicationFixture(t *testing.T) (*Repository, Snapshot) {
	t.Helper()
	r, s := observedFixture(t)
	at := time.Date(2026, 10, 30, 5, 0, 0, 0, time.UTC)
	identity := diagnostics.Identity{Environment: s.Envelope.Subject.Environment, Installation: s.Envelope.Subject.Installation, ProjectUID: s.Envelope.Subject.ProjectUID, ProjectName: "example", OperationID: "rollout-one", ReconcileKey: "accepted-one", Generation: 1}
	p, err := diagnostics.NewProjection(identity)
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: identity, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Step: "readiness", Failure: &diagnostics.DomainFailure{ID: "root", Code: "ReadinessFailed", Category: diagnostics.Readiness, Message: "Candidate did not become ready."}, Retry: &diagnostics.RetryInfo{Status: diagnostics.RetryExhausted, Authoritative: true, ObservedAt: at}, Availability: &diagnostics.Availability{ActiveRevision: "active", CandidateRevision: "candidate", RouteState: diagnostics.RouteActivePreserved, ConfirmedAt: at}, Recovery: []diagnostics.RecoveryOutcome{{ID: "cleanup", State: diagnostics.OutcomeRetained}}, Evidence: []diagnostics.EvidenceRef{{ID: "evidence", State: "Captured"}}})
	if err != nil {
		t.Fatal(err)
	}
	next := clone(s.Envelope)
	next.Projection = &p
	s = commit(t, r, s, next)
	return r, s
}
func TestRepositoryPublicationOnlyRepairAdvancesStorageNotDomain(t *testing.T) {
	r, s := publicationFixture(t)
	before := clone(s.Envelope)
	p := *s.Envelope.Projection
	unknown, err := p.WithPublication(diagnostics.SinkHealth{Sink: "catalog", State: diagnostics.DeliveryUnknown, SourceSequence: p.Sequence, ObservedAt: p.ProgressedAt.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	next := clone(s.Envelope)
	next.Projection = &unknown
	s = commit(t, r, s, next)
	if s.Envelope.Sequence != before.Sequence+1 || s.Envelope.Projection.Sequence != p.Sequence || s.Envelope.Projection.ProgressedAt != p.ProgressedAt || !reflect.DeepEqual(s.Envelope.Attempts, before.Attempts) || !reflect.DeepEqual(s.Envelope.Writes, before.Writes) || !reflect.DeepEqual(s.Envelope.Projection.Failure, p.Failure) || s.Envelope.Projection.Availability != p.Availability || !reflect.DeepEqual(s.Envelope.Projection.Recovery, p.Recovery) || !reflect.DeepEqual(s.Envelope.Projection.Retry, p.Retry) {
		t.Fatal("publication repair changed operation/authority facts")
	}
	// A replacement source can read this persisted uncertainty after the writer
	// disappears; no live workflow or provider callback participates.
	reloaded, err := r.Load(context.Background())
	if err != nil || reloaded.Envelope.Projection.Publication[0].State != diagnostics.DeliveryUnknown {
		t.Fatal("unknown publication not durable", err)
	}
	ack, err := unknown.WithPublication(diagnostics.SinkHealth{Sink: "catalog", State: diagnostics.DeliverySucceeded, SourceSequence: p.Sequence, ObservedAt: p.ProgressedAt.Add(2 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	next = clone(s.Envelope)
	next.Projection = &ack
	s = commit(t, r, s, next)
	for _, change := range []func(*diagnostics.OperationProjection){
		func(n *diagnostics.OperationProjection) { n.Publication = nil },
		func(n *diagnostics.OperationProjection) {
			n.Publication[0].State = diagnostics.DeliveryUnknown
			n.Publication[0].ObservedAt = p.ProgressedAt.Add(3 * time.Minute)
		},
		func(n *diagnostics.OperationProjection) { n.Publication[0].SourceSequence++ },
		func(n *diagnostics.OperationProjection) { n.ProgressedAt = p.ProgressedAt.Add(time.Hour) },
		func(n *diagnostics.OperationProjection) { n.Step = "fake-provider-replay" },
		func(n *diagnostics.OperationProjection) { n.State = diagnostics.Succeeded },
		func(n *diagnostics.OperationProjection) { n.Failure.Message = "new root cause" },
		func(n *diagnostics.OperationProjection) { n.Availability.RouteState = diagnostics.RoutePromoted },
		func(n *diagnostics.OperationProjection) { n.Retry.Status = diagnostics.RetryScheduled },
		func(n *diagnostics.OperationProjection) { n.Recovery[0].State = diagnostics.OutcomeCompleted },
		func(n *diagnostics.OperationProjection) { n.Evidence[0].State = "Removed" },
	} {
		bad := clone(s.Envelope)
		change(bad.Projection)
		if _, err := r.Commit(context.Background(), s, bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("repository admitted non-publication repair", err)
		}
	}
}

func TestRepositoryDoesNotACKSinkSilentlyCompactedToOldHash(t *testing.T) {
	r, s := observedFixture(t)
	at := time.Date(2026, 10, 30, 5, 0, 0, 0, time.UTC)
	identity := diagnostics.Identity{Environment: s.Envelope.Subject.Environment, Installation: s.Envelope.Subject.Installation, ProjectUID: s.Envelope.Subject.ProjectUID, ProjectName: "example", OperationID: "rollout-one", ReconcileKey: "accepted-one", Generation: 1}
	large := func(id string) diagnostics.DomainFailure {
		return diagnostics.DomainFailure{ID: id, Code: "RecordedFailure", Category: diagnostics.Publication, Message: strings.Repeat("m", 768), NextAction: strings.Repeat("a", 512), Causes: []diagnostics.Cause{{Type: "evidence", Message: strings.Repeat("c", 192)}, {Type: "evidence", Message: strings.Repeat("d", 192)}}}
	}
	base, err := diagnostics.NewProjection(identity)
	if err != nil {
		t.Fatal(err)
	}
	root := large("root")
	recovery := large("recovery")
	base, err = base.Apply(diagnostics.OperationEvent{Identity: identity, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Step: "retained", Failure: &root, Recovery: []diagnostics.RecoveryOutcome{{ID: "cleanup", Step: strings.Repeat("s", 128), State: diagnostics.OutcomeRetained, Failure: &recovery}}, Partial: true})
	if err != nil {
		t.Fatal(err)
	}
	var old, candidate diagnostics.OperationProjection
	found := false
	type wire diagnostics.OperationProjection
	for width := 0; width <= 128; width++ {
		p := base
		p.Recovery[0].RetainedResources = nil
		for i := 0; i < 4; i++ {
			p.Recovery[0].RetainedResources = append(p.Recovery[0].RetainedResources, diagnostics.ResourceRef{Provider: strings.Repeat("p", 48), Kind: strings.Repeat("k", 64), ID: strings.Repeat("r", width)})
		}
		first := large("first-delivery")
		p, err = p.WithPublication(diagnostics.SinkHealth{Sink: "catalog", State: diagnostics.DeliveryUnknown, SourceSequence: 1, ObservedAt: at.Add(time.Minute), Failure: &first})
		if err != nil {
			t.Fatal(err)
		}
		full, _ := json.Marshal(wire(p))
		encoded, err := p.JSON()
		if err != nil || !bytes.Equal(full, encoded) {
			continue
		}
		second := large("second-delivery")
		next, e := p.WithPublication(diagnostics.SinkHealth{Sink: "status", State: diagnostics.DeliveryUnknown, SourceSequence: 1, ObservedAt: at.Add(2 * time.Minute), Failure: &second})
		if e != nil {
			t.Fatal(e)
		}
		nextEncoded, e := next.JSON()
		if e == nil && bytes.Equal(nextEncoded, encoded) {
			old, candidate, found = p, next, true
			break
		}
	}
	if !found {
		t.Fatal("regression fixture did not produce normalized-identical dropped sink")
	}
	n := clone(s.Envelope)
	n.Projection = &old
	s = commit(t, r, s, n)
	if jsonHash(old) != jsonHash(candidate) {
		t.Fatal("fixture failed to exercise equal normalized hashes")
	}
	n = clone(s.Envelope)
	n.Projection = &candidate
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) || !errors.Is(err, diagnostics.ErrPublicationCapacity) {
		t.Fatal("repository acknowledged a dropped new sink", err)
	}
	reloaded, err := r.Load(context.Background())
	if err != nil || len(reloaded.Envelope.Projection.Publication) != 1 {
		t.Fatal("failed repair altered stored projection", err)
	}
}
