package runtimeconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

func TestMutationRejectionRequiresFiniteReviewedCodeOrIndependentNoDispatch(t *testing.T) {
	var typedNil *kube.Error
	requireCode(t, classify(typedNil), Unavailable)
	for _, cause := range []error{errors.New("raw provider failure"), typedNil, &kube.Error{}, &kube.Error{Code: kube.Code("future-provider-code")}, &kube.Error{Code: kube.UnknownWriteOutcome}} {
		if rejectedMutation(cause, false) {
			t.Fatal("unproven failure classified as known rejection")
		}
	}
	if !rejectedMutation(&kube.Error{Code: kube.ConditionalConflict}, false) {
		t.Fatal("reviewed conditional rejection lost")
	}
	if !rejectedMutation(errors.New("local writer check"), true) {
		t.Fatal("independent no-dispatch fact lost")
	}
	code, reason := localAuthorityFailure(&Error{Code: Code("RAW-UNKNOWN-ACTOR-CODE")})
	if code != Rejected || reason != runtimefault.AuthorityUnclassified {
		t.Fatal("arbitrary actor code propagated")
	}
	code, reason = localAuthorityFailure(failure(SubjectMismatch))
	if code != SubjectMismatch || reason != runtimefault.AuthorityMismatch {
		t.Fatal("known local dispatch denial lost")
	}
}

func TestFailureAllowsInspectionWithoutReopeningNormalMutation(t *testing.T) {
	w := &Worker{config: Config{Diagnostics: &DiagnosticConfig{ProjectName: "demo"}}, snapshot: journal.Snapshot{Envelope: journal.Envelope{Attempts: []journal.Attempt{{State: journal.Observed}}, Projection: &diagnostics.OperationProjection{Failure: &diagnostics.DomainFailure{ID: "retained-original"}}}}}
	requireCode(t, w.normalWorkAllowed(), Blocked)
	if err := w.normalObservationAllowed(); err != nil {
		t.Fatal("original failure prevented canonical inspection", err)
	}
	w.snapshot.Envelope.Attempts[0].State = journal.Draining
	requireCode(t, w.normalObservationAllowed(), Blocked)
	w.snapshot.Envelope.Attempts[0].State = journal.Consumed
	if err := w.normalWorkAllowed(); err != nil {
		t.Fatal("consumed attempt prevented distinct next operation", err)
	}
	for _, zero := range []*Worker{nil, {}} {
		_, err := zero.ObserveNamespace(context.Background(), rollout.Desired{}, "unaccepted")
		requireCode(t, err, Invalid)
	}
}

func TestPreAdmissionInputFaultKeepsCauseAndNeverNeedsGate(t *testing.T) {
	const marker = "PRIVATE-RUNTIME-INPUT-PATH-DO-NOT-SERIALIZE"
	w := &Worker{snapshot: journal.Snapshot{Envelope: journal.Envelope{Attempts: []journal.Attempt{{OperationID: "accepted", NamespaceName: "candidate-one"}}}}}
	target := runtimeinputs.Target{Parent: ownership.NamespaceParent{Name: "candidate-one", UID: "namespace-uid"}, AttemptID: "accepted", Name: "service", Fingerprint: strings.Repeat("a", 64)}
	ref := journal.InputReference{Kind: "ServiceRender", UID: "render-" + strings.Repeat("b", 64), Version: "1", Digest: strings.Repeat("c", 64), Contract: journal.ServiceInputContract}
	err := w.inputFailure(context.Background(), Unavailable, "Service", target, ref, runtimefault.InputResolve, fmt.Errorf("%w: %s", runtimeinputs.ErrUnproven, marker))
	var runtimeError *Error
	if !errors.As(err, &runtimeError) || runtimeError.Fault == nil {
		t.Fatal("missing safe input fault")
	}
	v := runtimeError.Fault.View()
	if v.Failure.Code != "AcceptedInputUnproven" || v.Step != runtimefault.InputResolve || v.Dispatch != runtimefault.NotDispatched || v.Target.NamespaceName != "candidate-one" || v.Input == nil || *v.Input != ref || v.JournalDelivery.State != runtimefault.NotAttempted || v.DiagnosticDelivery.State != runtimefault.NotAttempted {
		t.Fatal("pre-admission fault invented storage/provenance", v)
	}
	data, encodeErr := json.Marshal(runtimeError)
	if encodeErr != nil || strings.Contains(string(data), marker) || strings.Contains(fmt.Sprintf("%#v", runtimeError), marker) {
		t.Fatal("raw input error escaped runtime boundary", encodeErr)
	}
}

func TestFaultProjectionKeepsOriginalCleanupCauseAndSnapshotIndependent(t *testing.T) {
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}
	at := time.Now().UTC().Add(-time.Minute)
	p, err := newInitialProjection(DiagnosticConfig{ProjectName: "demo"}, rollout.Desired{Subject: subject, Generation: 1, ReconcileKey: "key"}, "current-operation", at)
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: p.Identity, Sequence: 2, OccurredAt: at.Add(time.Second), State: diagnostics.Failed, Step: "smoke", Failure: &diagnostics.DomainFailure{ID: "original-smoke", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Candidate smoke failed", OccurredAt: at.Add(time.Second)}, Retry: &diagnostics.RetryInfo{Status: diagnostics.RetryExhausted, Authoritative: true, Attempt: 3, MaximumAttempts: 3, ObservedAt: at.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{config: Config{Diagnostics: &DiagnosticConfig{ProjectName: "demo"}}, snapshot: journal.Snapshot{Envelope: journal.Envelope{Subject: subject, Projection: &p}}}
	write := journal.Write{ID: "delete-old-namespace", OperationID: "historical-attempt", Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Delete", Name: "candidate-old", UID: "canonical-old-namespace", Fingerprint: strings.Repeat("a", 64)}}
	f, err := runtimefault.MapProvider(w.faultContext(write, runtimefault.Delete, runtimefault.DispatchUnknown), &kube.Error{Code: kube.UnknownWriteOutcome, Recovery: "PRIVATE-PROVIDER-RESPONSE"})
	if err != nil {
		t.Fatal(err)
	}
	next := clone(w.snapshot.Envelope)
	if err := w.projectFault(&next, f); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(next.Projection.Failure, p.Failure) || next.Projection.State != p.State || next.Projection.Step != p.Step || !reflect.DeepEqual(next.Projection.Retry, p.Retry) || p.Retry.Status != diagnostics.RetryExhausted || next.Projection.Availability.RouteState != diagnostics.RouteUnknown || len(next.Projection.Recovery) != 1 || next.Projection.Recovery[0].Failure.Code != string(kube.UnknownWriteOutcome) {
		t.Fatal("cleanup replaced original operation failure")
	}
	if len(w.snapshot.Envelope.Projection.Recovery) != 0 || w.snapshot.Envelope.Projection.Sequence != 2 || f.View().JournalDelivery.State != runtimefault.NotAttempted {
		t.Fatal("unacknowledged composition mutated original snapshot/fault")
	}
	data, _ := next.Projection.JSON()
	if strings.Contains(string(data), "PRIVATE-PROVIDER-RESPONSE") {
		t.Fatal("raw recovery text escaped")
	}
}

func TestNewReadFaultIsMaterializedAsIndependentWarning(t *testing.T) {
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}
	at := time.Now().UTC().Add(-time.Minute)
	p, err := newInitialProjection(DiagnosticConfig{ProjectName: "demo"}, rollout.Desired{Subject: subject, Generation: 1, ReconcileKey: "key"}, "operation", at)
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: p.Identity, Sequence: 2, OccurredAt: at.Add(time.Second), State: diagnostics.Failed, Step: "smoke", Failure: &diagnostics.DomainFailure{ID: "first", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Original smoke failure", OccurredAt: at.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{config: Config{Diagnostics: &DiagnosticConfig{ProjectName: "demo"}}, snapshot: journal.Snapshot{Envelope: journal.Envelope{Projection: &p}}}
	ref := journal.InputReference{Kind: "ServiceRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: journal.ServiceInputContract}
	f, err := runtimefault.MapInput(runtimefault.Context{Step: runtimefault.InputResolve, Kind: runtimefault.Service, Purpose: "service", OperationID: "operation", WriteID: "create-service", Name: "backend", NamespaceUID: "namespace-uid", NamespaceName: "candidate", Fingerprint: strings.Repeat("c", 64), Input: &ref, OccurredAt: time.Now().UTC()}, runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	next := clone(w.snapshot.Envelope)
	if err := w.projectFault(&next, f); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(next.Projection.Failure, p.Failure) || next.Projection.State != p.State || next.Projection.Step != p.Step || len(next.Projection.Warnings) != 1 || next.Projection.Warnings[0].Code != "AcceptedInputUnproven" || !materializesFault(*next.Projection, f.View().Failure) {
		t.Fatal("new read failure disappeared behind original cause")
	}
	if len(p.Warnings) != 0 {
		t.Fatal("unacknowledged warning mutated original snapshot")
	}
	// The check inspects the final serialized bytes, not only in-memory fields.
	large := *next.Projection
	large.Recovery = nil
	for i := 0; i < 8; i++ {
		large.Recovery = append(large.Recovery, diagnostics.RecoveryOutcome{ID: fmt.Sprintf("retained-%d", i), State: diagnostics.OutcomeRetained, Failure: &diagnostics.DomainFailure{ID: fmt.Sprintf("failure-%d", i), Code: "Retained", Category: diagnostics.Cleanup, Message: strings.Repeat("m", 768)}})
	}
	if materializesFault(large, f.View().Failure) {
		t.Fatal("aggregate-discarded warning was acknowledged")
	}
}
