//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"

	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

func TestPreparedNamespaceCleanupUsesFrozenCASAndPreservesForeignFinalizers(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	adapter, _ := typedClient(t)
	plan := managedNamespacePlan(t, "prepared-cleanup")
	bound := bootstrapManaged(t, adapter, plan)
	path := "/api/v1/namespaces/" + plan.Object.Name
	current := request(t, "GET", path, nil, 200)
	metadata(current)["finalizers"] = []string{provider.NamespaceLifecycleFinalizer, "operator-test.local/foreign-hold"}
	field(metadata(current), "annotations")["operator-test.local/preserve"] = "original"
	request(t, "PUT", path, current, 200)
	stale, err := bound.PrepareNamespaceDelete(context.Background(), plan.Writer, "prepared-delete-stale")
	if err != nil {
		t.Fatal(err)
	}
	current = request(t, "GET", path, nil, 200)
	if metadata(current)["deletionTimestamp"] != nil {
		t.Fatal("preparation sent DELETE")
	}
	field(metadata(current), "annotations")["operator-test.local/preserve"] = "raced"
	request(t, "PUT", path, current, 200)
	_, err = stale.Execute(context.Background())
	requireProviderCode(t, err, provider.ConditionalConflict)
	current = request(t, "GET", path, nil, 200)
	if metadata(current)["deletionTimestamp"] != nil {
		t.Fatal("stale ticket refreshed version and deleted namespace")
	}
	deletion, err := bound.PrepareNamespaceDelete(context.Background(), plan.Writer, "prepared-delete-current")
	if err != nil {
		t.Fatal(err)
	}
	result, err := deletion.Execute(context.Background())
	if err != nil || !result.Acknowledged || result.ConfirmedAbsent {
		t.Fatalf("unexpected DELETE acknowledgement: %+v %v", result, err)
	}
	current = request(t, "GET", path, nil, 200)
	if metadata(current)["deletionTimestamp"] == nil {
		t.Fatal("acknowledged DELETE did not begin namespace termination")
	}
	finalization, err := bound.PrepareOwnFinalizerRemoval(context.Background(), plan.Writer, "prepared-finalize-current")
	if err != nil {
		t.Fatal(err)
	}
	description := finalization.Description()
	if description.UID != bound.Parent().UID || description.ResourceVersion != text(metadata(current), "resourceVersion") || !description.Terminating {
		t.Fatal("finalizer ticket not pinned to terminating canonical UID/version")
	}
	result, err = finalization.Execute(context.Background())
	if err != nil || !result.Acknowledged || result.ConfirmedAbsent || result.UID != bound.Parent().UID || result.ResourceVersion == description.ResourceVersion {
		t.Fatalf("unexpected finalizer acknowledgement: %+v %v", result, err)
	}
	current = request(t, "GET", path, nil, 200)
	values, _ := metadata(current)["finalizers"].([]any)
	if len(values) != 1 || values[0] != "operator-test.local/foreign-hold" || field(metadata(current), "annotations")["operator-test.local/preserve"] != "raced" {
		t.Fatal("finalizer mutation stripped foreign metadata or finalizer")
	}
	// Only this fixture's explicit hold is released by its independent test
	// administrator. Provider/runtime never strip another actor's finalizer.
	metadata(current)["finalizers"] = []string{}
	request(t, "PUT", path, current, 200)
	wait(t, func() bool {
		response, err := client.Get(local.Server + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusNotFound
	})
	goal, err := bound.ObserveDrainingNamespace(context.Background())
	if err != nil || !goal.ConfirmedAbsent || goal.Acknowledged || goal.UID != "" || goal.ResourceVersion != "" {
		t.Fatalf("observation invented RPC/metadata: %+v %v", goal, err)
	}
	_, err = adapter.BootstrapNamespace(context.Background(), plan)
	requireProviderCode(t, err, provider.AttemptConsumed)
}

func TestPreparedCleanupUnknownResponseRemainsBlockedAfterCanonicalAbsence(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	plan := managedNamespacePlan(t, "prepared-unknown")
	path := "/api/v1/namespaces/" + plan.Object.Name
	options, attempts := acceptedButLostProxy(t, "PUT", path)
	adapter, err := provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	bound := bootstrapManaged(t, adapter, plan)
	deletion, err := bound.PrepareNamespaceDelete(context.Background(), plan.Writer, "prepared-before-unknown")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = deletion.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	ticket, err := bound.PrepareOwnFinalizerRemoval(context.Background(), plan.Writer, "prepared-unknown-finalize")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ticket.Execute(context.Background())
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	if attempts.Load() != 1 {
		t.Fatal("uncertain cleanup was automatically retried")
	}
	wait(t, func() bool {
		response, err := client.Get(local.Server + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusNotFound
	})
	_, err = bound.ObserveDrainingNamespace(context.Background())
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	state := adapter.Lifecycle(plan.Object.Name)
	if !state.Draining || len(state.Unresolved) != 1 || !state.Unresolved[0].Unknown || state.Unresolved[0].ExpectedVersion != ticket.Description().ResourceVersion || state.Unresolved[0].TargetFingerprint != ticket.Description().BodyDigest {
		t.Fatal("canonical absence erased exact unknown cleanup tombstone")
	}
}
