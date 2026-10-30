package runtimeconfig

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

func readinessWrite() journal.Write {
	return journal.Write{ID: "create-deployment", OperationID: "accepted", State: journal.Committed, ResultUID: "canonical-deployment", ResultVersion: "native-post-version", Input: journal.InputReference{Kind: "DeploymentRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: journal.DeploymentInputContract}, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create", Name: "backend", ParentUID: "namespace-uid", Fingerprint: strings.Repeat("c", 64)}}
}

func TestFailedReadinessWithSuccessfulReadKeepsCommittedCreationAndSafeCause(t *testing.T) {
	for _, code := range []string{"DeploymentReplicaFailure", "DeploymentProgressDeadlineExceeded"} {
		write := readinessWrite()
		w := &Worker{snapshot: journal.Snapshot{Envelope: journal.Envelope{Attempts: []journal.Attempt{{OperationID: "accepted", NamespaceName: "candidate-one"}}, Writes: []journal.Write{write}}}}
		observed := kube.DeploymentReadiness{State: "Failed", Code: code, Generation: 2, ObservedGeneration: 2, Desired: 1, Unavailable: 1}
		got, err := w.deploymentReadiness(context.Background(), write, observed)
		var cause *Error
		if !reflect.DeepEqual(got, observed) || !errors.As(err, &cause) || cause.Code != Unavailable || cause.Fault == nil {
			t.Fatal("nil API error suppressed known readiness failure")
		}
		v := cause.Fault.View()
		if v.Failure.Code != code || v.Failure.Category != diagnostics.Readiness || v.Failure.NextAction == "" || v.Dispatch != runtimefault.Applied || v.Readiness == nil || v.Readiness.ObservedGeneration != 2 || v.JournalDelivery.State != runtimefault.NotAttempted || v.DiagnosticDelivery.State != runtimefault.NotAttempted || v.Step != runtimefault.Observe {
			t.Fatal("known readiness/creation facts became mutation or persistence uncertainty")
		}
		if !reflect.DeepEqual(w.snapshot.Envelope.Writes, []journal.Write{write}) {
			t.Fatal("readiness failure changed native committed receipt")
		}
		raw, encodeErr := json.Marshal(cause.Fault)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if _, parseErr := runtimefault.ParseFault(raw); parseErr != nil {
			t.Fatal("worker readiness fault did not survive strict wire", parseErr)
		}
	}
}

func TestReadinessWaitingUnknownRetainedAndReadyAreNotCoerced(t *testing.T) {
	w := &Worker{}
	for _, state := range []string{"Ready", "Waiting", "Unknown", "Retained"} {
		original := kube.DeploymentReadiness{State: state, Code: "DeploymentReplicasPending", Generation: 2, ObservedGeneration: 1, Unavailable: 1}
		got, err := w.deploymentReadiness(context.Background(), readinessWrite(), original)
		if err != nil || !reflect.DeepEqual(got, original) {
			t.Fatal("nonfailure readiness was converted into failure or positive readiness")
		}
	}
	for _, original := range []kube.DeploymentReadiness{
		{State: "Failed", Code: "PRIVATE-FUTURE-REASON", Generation: 2, ObservedGeneration: 2},
		{State: "Failed", Code: "DeploymentReplicaFailure", Generation: 2, ObservedGeneration: 1},
		{State: "Failed", Code: "DeploymentReplicaFailure", Generation: 2, ObservedGeneration: 2, Replicas: -1},
	} {
		got, err := w.deploymentReadiness(context.Background(), readinessWrite(), original)
		requireCode(t, err, Uncertain)
		if got.State != "Unknown" || got.Code != "DeploymentReadinessUnproven" || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("unsupported status was promoted to a known failure or leaked raw code")
		}
	}
	write := readinessWrite()
	write.State = journal.Unknown
	got, err := w.deploymentReadiness(context.Background(), write, kube.DeploymentReadiness{State: "Failed", Code: "DeploymentReplicaFailure", Generation: 1, ObservedGeneration: 1})
	requireCode(t, err, Invalid)
	if got.State != "Unknown" {
		t.Fatal("unknown creation gained an Applied readiness claim")
	}
}

func TestReadinessProjectionPreservesFirstCauseAndIndependentDelivery(t *testing.T) {
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project"}
	at := time.Now().UTC().Add(-time.Minute)
	p, err := newInitialProjection(DiagnosticConfig{ProjectName: "demo"}, rollout.Desired{Subject: subject, Generation: 1, ReconcileKey: "key"}, "accepted", at)
	if err != nil {
		t.Fatal(err)
	}
	write := readinessWrite()
	w := &Worker{config: Config{Diagnostics: &DiagnosticConfig{ProjectName: "demo"}}, snapshot: journal.Snapshot{Envelope: journal.Envelope{Subject: subject, Projection: &p, Writes: []journal.Write{write}}}}
	_, observationErr := w.deploymentReadiness(context.Background(), write, kube.DeploymentReadiness{State: "Failed", Code: "DeploymentReplicaFailure", Generation: 2, ObservedGeneration: 2})
	var cause *Error
	if !errors.As(observationErr, &cause) || cause.Fault == nil {
		t.Fatal("readiness cause missing")
	}
	next := clone(w.snapshot.Envelope)
	if err := w.projectFault(&next, *cause.Fault); err != nil || next.Projection.Failure == nil || next.Projection.Failure.Code != "DeploymentReplicaFailure" || !reflect.DeepEqual(next.Writes, w.snapshot.Envelope.Writes) {
		t.Fatal("first readiness cause was not materialized independently of committed write", err)
	}
	w.snapshot.Envelope = next
	_, secondErr := w.deploymentReadiness(context.Background(), write, kube.DeploymentReadiness{State: "Failed", Code: "DeploymentProgressDeadlineExceeded", Generation: 2, ObservedGeneration: 2})
	if !errors.As(secondErr, &cause) || cause.Fault == nil {
		t.Fatal("secondary readiness cause missing")
	}
	later := clone(w.snapshot.Envelope)
	if err := w.projectFault(&later, *cause.Fault); err != nil || !reflect.DeepEqual(later.Projection.Failure, next.Projection.Failure) || len(later.Projection.Warnings) != 1 || later.Projection.Warnings[0].Code != "DeploymentProgressDeadlineExceeded" || later.Projection.Retry.Status != diagnostics.RetryUnknown || later.Projection.Availability.RouteState != diagnostics.RouteUnknown {
		t.Fatal("later readiness failure replaced original cause or invented health/retry", err)
	}
	unknown, err := cause.Fault.WithDelivery(runtimefault.Diagnostic, runtimefault.DeliveryUnknown, time.Now().UTC())
	if err != nil || unknown.View().Failure.Code != "DeploymentProgressDeadlineExceeded" || unknown.View().Dispatch != runtimefault.Applied || unknown.View().DiagnosticDelivery.State != runtimefault.DeliveryUnknown {
		t.Fatal("publication uncertainty replaced readiness observation")
	}
}
