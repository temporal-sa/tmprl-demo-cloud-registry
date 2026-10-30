package diagnostics

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

var observed = time.Date(2026, 10, 29, 14, 32, 0, 0, time.UTC)

func identity() Identity {
	return Identity{Environment: "local", Installation: "rowan-dev", ProjectUID: "uid-banking", ProjectName: "banking", OperationID: "op-123", ReconcileKey: "sha-abc", Generation: 12}
}

func projection(t *testing.T) OperationProjection {
	t.Helper()
	p, err := NewProjection(identity())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func failure() DomainFailure {
	return DomainFailure{ID: "failure-rollout", Code: "IMAGE_PULL_UNAUTHORIZED", Category: Readiness, Message: "Kubernetes reports ImagePullBackOff; image pull returned unauthorized.", Step: "candidate-readiness", OccurredAt: observed, Execution: ExecutionRef{Namespace: "local", WorkflowID: "project/banking", RunID: "run-1", ActivityID: "readiness-1", ScheduledEventID: 42}, Provider: ProviderRef{Service: "kubernetes", Code: "ImagePullBackOff"}, NextAction: "Verify the candidate image and pull-role access; then request a reconcile."}
}

func failedEvent() OperationEvent {
	f := failure()
	return OperationEvent{Identity: identity(), Sequence: 1, OccurredAt: observed, State: Failed, Step: "candidate-readiness", Failure: &f, Availability: &Availability{ActiveRevision: "git-healthy", CandidateRevision: "git-broken", RouteState: RouteActivePreserved, ConfirmedAt: observed}, Retry: &RetryInfo{Status: RetryExhausted, Authoritative: true, Attempt: 3, MaximumAttempts: 3, ObservedAt: observed}}
}

func apply(t *testing.T, p OperationProjection, event OperationEvent) OperationProjection {
	t.Helper()
	result, err := p.Apply(event)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFailureRecoveryAndPublicationRemainIndependent(t *testing.T) {
	p := apply(t, projection(t), failedEvent())
	root := *p.Failure
	cleanup := DomainFailure{ID: "cleanup-1", Code: "EVIDENCE_UPLOAD_DENIED", Category: Cleanup, Message: "Evidence upload denied."}
	p = apply(t, p, OperationEvent{Identity: identity(), Sequence: 2, OccurredAt: observed.Add(time.Minute), State: Failed, Step: "retain-candidate", Recovery: []RecoveryOutcome{{ID: "capture", Step: "capture evidence", State: OutcomeFailed, Failure: &cleanup, RetainedResources: []ResourceRef{{Provider: "kubernetes", Kind: "Namespace", ID: "candidate-banking"}}}}})
	if !reflect.DeepEqual(*p.Failure, root) {
		t.Fatal("cleanup replaced primary failure")
	}
	if p.Availability.ActiveRevision != "git-healthy" || p.Availability.RouteState != RouteActivePreserved {
		t.Fatal("cleanup clobbered independent serving state")
	}
	publication := DomainFailure{ID: "status-1", Code: "CATALOG_UNAVAILABLE", Category: Publication, Message: "Catalog write failed."}
	p, err := p.WithPublication(SinkHealth{Sink: "catalog", State: DeliveryFailed, SourceSequence: p.Sequence, ObservedAt: observed.Add(2 * time.Minute), Failure: &publication})
	if err != nil {
		t.Fatal(err)
	}
	before := p
	p, err = p.WithPublication(SinkHealth{Sink: "catalog", State: DeliverySucceeded, SourceSequence: p.Sequence, ObservedAt: observed.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if p.Sequence != before.Sequence || p.ProgressedAt != before.ProgressedAt || !reflect.DeepEqual(p.Failure, before.Failure) || !reflect.DeepEqual(p.Recovery, before.Recovery) || p.Availability != before.Availability {
		t.Fatal("publication repair changed operation facts")
	}
	if p.Publication[0].State != DeliverySucceeded || p.Publication[0].Failure != nil {
		t.Fatal("publication repair did not clear only its own failure")
	}
	if before.Publication[0].State != DeliveryFailed {
		t.Fatal("repair mutated an earlier projection")
	}
	text := Explain(p)
	for _, part := range []string{"IMAGE_PULL_UNAUTHORIZED", "ImagePullBackOff", "git-healthy remains routed", "capture evidence: Failed", "EVIDENCE_UPLOAD_DENIED", "Verify the candidate image", "Operation op-123"} {
		if !strings.Contains(text, part) {
			t.Errorf("explanation missing %q: %s", part, text)
		}
	}
	if strings.Contains(text, "after 0") {
		t.Fatal("invented retry attempt count")
	}
}

func TestStaleAndForeignObservationsCannotRegressProjection(t *testing.T) {
	p := apply(t, projection(t), failedEvent())
	before, _ := p.JSON()
	for _, mutate := range []func(*OperationEvent){
		func(e *OperationEvent) { e.Sequence = 1 },
		func(e *OperationEvent) { e.Sequence = 0 },
		func(e *OperationEvent) { e.Sequence = 2; e.OccurredAt = observed.Add(-time.Second) },
	} {
		e := failedEvent()
		mutate(&e)
		result, err := p.Apply(e)
		if !errors.Is(err, ErrStaleEvent) {
			t.Fatalf("got %v, want stale event", err)
		}
		after, _ := result.JSON()
		if string(after) != string(before) {
			t.Fatal("rejected event mutated projection")
		}
	}
	e := failedEvent()
	e.Sequence = 2
	e.Identity.Environment = "production"
	if _, err := p.Apply(e); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("got %v, want identity mismatch", err)
	}
	e = failedEvent()
	e.Sequence = 2
	e.Identity.ReconcileKey = "sha-other"
	if _, err := p.Apply(e); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("got %v, want intent mismatch", err)
	}
	if _, err := p.WithPublication(SinkHealth{Sink: "catalog", State: DeliverySucceeded, SourceSequence: 99, ObservedAt: observed}); !errors.Is(err, ErrFuturePublication) {
		t.Fatalf("got %v, want future publication rejection", err)
	}
}

func TestApplyDoesNotAliasEventsOrPriorProjections(t *testing.T) {
	e := failedEvent()
	e.Failure.Causes = []Cause{{Type: "Provider", Message: "original"}}
	e.Recovery = []RecoveryOutcome{{ID: "cleanup", Step: "delete candidate", State: OutcomeRunning, RetainedResources: []ResourceRef{{ID: "candidate"}}}}
	e.Evidence = []EvidenceRef{{ID: "pod-log", Locator: "evidence:one", State: "Captured"}}
	initial := projection(t)
	p := apply(t, initial, e)
	e.Failure.Causes[0].Message = "mutated"
	e.Recovery[0].RetainedResources[0].ID = "mutated"
	e.Evidence[0].Locator = "mutated"
	if p.Failure.Causes[0].Message != "original" || p.Recovery[0].RetainedResources[0].ID != "candidate" || p.Evidence[0].Locator != "evidence:one" {
		t.Fatal("projection retained aliases to producer data")
	}
	next := apply(t, p, OperationEvent{Identity: identity(), Sequence: 2, OccurredAt: observed.Add(time.Second), State: Failed, Step: "cleanup", Recovery: []RecoveryOutcome{{ID: "cleanup", Step: "delete candidate", State: OutcomeCompleted}}})
	next.Failure.Causes[0].Message = "other mutation"
	if p.Failure.Causes[0].Message != "original" || p.Recovery[0].State != OutcomeRunning || initial.Sequence != 0 {
		t.Fatal("Apply mutated previous projection")
	}
}

func TestRetryAndAvailabilityRequireAuthoritativeEvidence(t *testing.T) {
	next := observed.Add(time.Minute)
	cases := []struct {
		name   string
		retry  RetryInfo
		status RetryStatus
	}{
		{"category alone", RetryInfo{Status: RetryScheduled, Attempt: 1, NextAttemptAt: &next}, RetryUnknown},
		{"missing observation", RetryInfo{Status: RetryScheduled, Authoritative: true, NextAttemptAt: &next}, RetryUnknown},
		{"future observation", RetryInfo{Status: RetryScheduled, Authoritative: true, ObservedAt: next, NextAttemptAt: &next}, RetryUnknown},
		{"observed retry", RetryInfo{Status: RetryScheduled, Authoritative: true, Attempt: 2, ObservedAt: observed, NextAttemptAt: &next}, RetryScheduled},
		{"observed exhaustion", RetryInfo{Status: RetryExhausted, Authoritative: true, Attempt: 3, ObservedAt: observed, NextAttemptAt: &next}, RetryExhausted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := failedEvent()
			e.Retry = &c.retry
			p := apply(t, projection(t), e)
			if p.Retry.Status != c.status {
				t.Fatalf("got %s want %s", p.Retry.Status, c.status)
			}
			if c.status != RetryScheduled && p.Retry.NextAttemptAt != nil {
				t.Fatal("unknown/terminal retry leaked a next attempt")
			}
		})
	}
	e := failedEvent()
	e.Availability.ConfirmedAt = time.Time{}
	p := apply(t, projection(t), e)
	if p.Availability.RouteState != RouteUnknown || strings.Contains(Explain(p), "remains routed") {
		t.Fatal("unconfirmed availability presented as healthy")
	}
	e.Availability.ConfirmedAt = next
	p = apply(t, projection(t), e)
	if p.Availability.RouteState != RouteUnknown {
		t.Fatal("future route observation was accepted as current evidence")
	}
}

func TestFreshnessUsesIntentAndProgressNotProjectionTime(t *testing.T) {
	e := failedEvent()
	e.State = Running
	e.Failure = nil
	p := apply(t, projection(t), e)
	now := observed.Add(time.Hour)
	p, err := p.WithPublication(SinkHealth{Sink: "catalog", State: DeliverySucceeded, SourceSequence: 1, ObservedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	matching := DesiredState{ReconcileKey: identity().ReconcileKey, Generation: 12}
	if got := p.AssessFreshness(matching, now, time.Minute).State; got != FreshnessStale {
		t.Fatalf("recent projection hid stale progress: %s", got)
	}
	newer := DesiredState{ReconcileKey: "sha-new", Generation: 13}
	if got := p.AssessFreshness(newer, now, time.Minute).State; got != FreshnessReconciling {
		t.Fatalf("desired intent mismatch: %s", got)
	}
	p = apply(t, p, OperationEvent{Identity: identity(), Sequence: 2, OccurredAt: now, State: Failed, Step: "readiness"})
	if got := p.AssessFreshness(matching, now.Add(24*time.Hour), time.Minute).State; got != FreshnessCurrent {
		t.Fatalf("terminal observed failure should remain current for matching intent: %s", got)
	}
	if got := p.AssessFreshness(DesiredState{}, now, time.Minute).State; got != FreshnessUnknown {
		t.Fatalf("missing intent should be unknown: %s", got)
	}
}

func TestSafeProducerValuesAndAllWireViewsRedactCredentials(t *testing.T) {
	const known = "opaque-sentinel-9df83f"
	e := failedEvent()
	e.Failure.Message = "provider said " + known + "; password=\"multi word hidden\"; {\"api_key\":\"nested-hidden\"}; Authorization: Bearer header-hidden"
	e.Failure.Causes = []Cause{{Type: "Remote", Message: "token='quoted token with spaces'"}, {Type: "Key", Message: "sk-testcredential123456 AKIAABCDEFGHIJKLMNOP eyJabc.eyJdef.signature"}}
	e.Failure.NextAction = "Inspect https://alice:password@service.invalid/logs?X-Amz-Credential=query-hidden&token=more-hidden#fragment-hidden"
	e.Evidence = []EvidenceRef{{ID: "log", Locator: "https://service.invalid/logs?signed=secret-query", State: "Captured"}}
	e.Recovery = []RecoveryOutcome{{ID: "cleanup", Step: "delete", State: OutcomeFailed, Failure: &DomainFailure{ID: "cleanup", Code: "DELETE_DENIED", Message: "client_secret=" + known}}}
	safe := SafeEvent(e, known)
	// Producer construction itself must be safe, before JSON/history/logging.
	inMemory := fmt.Sprintf("%+v %+v %+v", *safe.Failure, safe.Failure.Causes, *safe.Recovery[0].Failure)
	data, err := json.Marshal(safe)
	if err != nil {
		t.Fatal(err)
	}
	p := apply(t, projection(t), safe)
	projectionData, err := p.JSON()
	if err != nil {
		t.Fatal(err)
	}
	views := []string{inMemory, string(data), string(projectionData), Explain(p)}
	for _, secret := range []string{known, "multi word hidden", "nested-hidden", "header-hidden", "quoted token with spaces", "sk-testcredential123456", "AKIAABCDEFGHIJKLMNOP", "eyJabc.eyJdef.signature", "query-hidden", "more-hidden", "fragment-hidden", "secret-query", "alice:password"} {
		for _, view := range views {
			if strings.Contains(view, secret) {
				t.Fatalf("secret %q survived in %s", secret, view)
			}
		}
	}
	if e.Failure.Message == safe.Failure.Message {
		t.Fatal("test did not exercise independent producer sanitization")
	}
	standalone, err := json.Marshal(DomainFailure{ID: "one", Code: "UNKNOWN", Message: "-----BEGIN PRIVATE KEY-----\nprivate material\n-----END PRIVATE KEY-----"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(standalone), "private material") {
		t.Fatal("private key survived standalone serialization")
	}
}

func TestByteBoundsAndPartialMarkersPreservePrimaryCause(t *testing.T) {
	for _, fill := range []string{"x", "<", "\x01", "界"} {
		t.Run(fmt.Sprintf("%x", fill), func(t *testing.T) {
			e := failedEvent()
			e.Failure.Message = strings.Repeat(fill, 20000)
			e.Failure.Causes = make([]Cause, 100)
			for i := range e.Failure.Causes {
				e.Failure.Causes[i] = Cause{Type: strings.Repeat(fill, 500), Message: strings.Repeat(fill, 2000)}
			}
			for i := 0; i < 100; i++ {
				f := failure()
				f.Message = strings.Repeat(fill, 10000)
				e.Recovery = append(e.Recovery, RecoveryOutcome{ID: fmt.Sprint(i), Step: strings.Repeat(fill, 1000), State: OutcomeFailed, Failure: &f})
				e.Evidence = append(e.Evidence, EvidenceRef{ID: fmt.Sprint(i), Locator: strings.Repeat(fill, 10000), State: "Captured"})
				e.Warnings = append(e.Warnings, f)
			}
			safe := SafeEvent(e)
			data, err := json.Marshal(safe)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > MaxJSONBytes || !safe.Partial {
				t.Fatalf("event bound/partial violated: %d %v", len(data), safe.Partial)
			}
			fdata, err := json.Marshal(*safe.Failure)
			if err != nil {
				t.Fatal(err)
			}
			if len(fdata) > MaxFailureBytes {
				t.Fatalf("failure bound violated: %d", len(fdata))
			}
			p := apply(t, projection(t), safe)
			data, err = p.JSON()
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > MaxJSONBytes || !p.Partial || p.Failure.Code != "IMAGE_PULL_UNAUTHORIZED" || p.Identity != identity() {
				t.Fatalf("projection lost primary identity/cause or byte bound: %d", len(data))
			}
			if len(Explain(p)) > MaxJSONBytes || !strings.Contains(Explain(p), "partial") {
				t.Fatal("human view lost bounds/partial marker")
			}
		})
	}
}

func TestInvalidAndUnknownSchemaDoNotBecomeValidHistory(t *testing.T) {
	e := failedEvent()
	e.SchemaVersion = 99
	if _, err := projection(t).Apply(e); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("future schema accepted: %v", err)
	}
	if _, err := json.Marshal(e); err == nil {
		t.Fatal("future event schema silently downgraded")
	}
	p := projection(t)
	p.SchemaVersion = 99
	if _, err := p.JSON(); err == nil {
		t.Fatal("future projection schema silently downgraded")
	}
	if !strings.Contains(Explain(p), "unavailable") {
		t.Fatal("unknown schema did not produce safe explanation")
	}
	for _, bad := range []Identity{{}, func() Identity { v := identity(); v.ProjectUID = strings.Repeat("x", 129); return v }(), func() Identity { v := identity(); v.Environment = "local\nprod"; return v }()} {
		if _, err := NewProjection(bad); err == nil {
			t.Fatal("invalid ownership identity accepted")
		}
	}
}

func TestHistoricalFailureAndOlderSinkReportAreExplicit(t *testing.T) {
	p := apply(t, projection(t), failedEvent())
	p, err := p.WithPublication(SinkHealth{Sink: "catalog", State: DeliverySucceeded, SourceSequence: 1, ObservedAt: observed.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.WithPublication(SinkHealth{Sink: "catalog", State: DeliveryFailed, SourceSequence: 1, ObservedAt: observed.Add(-time.Minute)}); !errors.Is(err, ErrStaleEvent) {
		t.Fatalf("older sink update accepted: %v", err)
	}
	p = apply(t, p, OperationEvent{Identity: identity(), Sequence: 2, OccurredAt: observed.Add(2 * time.Minute), State: Succeeded, Step: "promoted"})
	if !strings.Contains(Explain(p), "Previous failure [IMAGE_PULL_UNAUTHORIZED]") {
		t.Fatal("historical failure was presented as current cause")
	}
	if !strings.Contains(Explain(p), "older observation 1") {
		t.Fatal("stale sink was not exposed")
	}
}
