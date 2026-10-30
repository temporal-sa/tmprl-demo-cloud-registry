package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"
)

func localState(t *testing.T) *Client {
	t.Helper()
	client := &Client{}
	if err := client.lifecycle.observe("namespace-one", "uid-one", "attempt-one"); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestLocalLifecycleDrainsRunningWritersAndRefusesNewOnes(t *testing.T) {
	client := localState(t)
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "write-one"}, func() error { close(started); <-release; return nil })
	}()
	<-started
	drained := make(chan error, 1)
	go func() {
		drained <- client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one")
	}()
	deadline := time.Now().Add(time.Second)
	for !client.Lifecycle("namespace-one").Draining {
		if time.Now().After(deadline) {
			t.Fatal("draining did not start")
		}
		time.Sleep(time.Millisecond)
	}
	called := false
	err := client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "late-write"}, func() error { called = true; return nil })
	requireCode(t, err, LifecycleDraining)
	if called {
		t.Fatal("new callback started during drain")
	}
	select {
	case err := <-drained:
		t.Fatalf("drain finished before writer: %v", err)
	default:
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	if len(client.Lifecycle("namespace-one").Unresolved) != 0 {
		t.Fatal("completed operation retained an in-flight record")
	}
}

func TestUnknownWriteOutcomeRetainsTombstoneAndBlocksTeardownAndReuse(t *testing.T) {
	client := localState(t)
	err := client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "unknown-write", Action: "create-configmap", Name: "settings"}, func() error { return failure(UnknownWriteOutcome, errors.New("lost response")) })
	requireCode(t, err, UnknownWriteOutcome)
	snapshot := client.Lifecycle("namespace-one")
	if len(snapshot.Unresolved) != 1 || !snapshot.Unresolved[0].Unknown || snapshot.Unresolved[0].ID != "unknown-write" {
		t.Fatal("unknown write lost its operation tombstone")
	}
	err = client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one")
	requireCode(t, err, UnknownWriteOutcome)
	err = client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "retry-write"}, func() error { t.Fatal("unknown namespace reopened"); return nil })
	requireCode(t, err, UnknownWriteOutcome)
	err = client.lifecycle.reserve("namespace-one", "new-attempt")
	requireCode(t, err, AttemptConsumed)
}

func TestCanceledDrainDoesNotReopenNamespace(t *testing.T) {
	client := localState(t)
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "write"}, func() error { close(started); <-release; return nil })
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.lifecycle.drain(ctx, "namespace-one", "uid-one", "attempt-one")
	requireCode(t, err, LifecycleDraining)
	if !client.Lifecycle("namespace-one").Draining {
		t.Fatal("canceled drain reopened parent")
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one"); err != nil {
		t.Fatal(err)
	}
}

func TestPanicRetainsAmbiguousOperationBeforePropagating(t *testing.T) {
	client := localState(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("provider panic swallowed")
			}
		}()
		_ = client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "panicked-write"}, func() error { panic("provider panic") })
	}()
	state := client.Lifecycle("namespace-one")
	if len(state.Unresolved) != 1 || !state.Unresolved[0].Unknown {
		t.Fatal("panic lost ambiguous write state")
	}
	err := client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one")
	requireCode(t, err, UnknownWriteOutcome)
}

func TestObservedNamespaceNameNeverReservesAgain(t *testing.T) {
	client := localState(t)
	for _, attempt := range []string{"attempt-one", "different-attempt"} {
		requireCode(t, client.lifecycle.reserve("namespace-one", attempt), AttemptConsumed)
	}
	if err := client.lifecycle.reserve("fresh-name", "accepted-attempt-two"); err != nil {
		t.Fatal(err)
	}
}

func TestExactRecoveryClearsOnlyMatchingUnknownAndNeverReopensDrain(t *testing.T) {
	client := localState(t)
	started, release := make(chan struct{}, 2), make(chan struct{})
	results := make(chan error, 2)
	operations := []OperationTombstone{{ID: "update-one", Action: "update-configmap", Name: "one", UID: "child-one", ExpectedVersion: "rv-one", TargetRevision: "rev-one", TargetFingerprint: "fingerprint-one", WriterEpoch: 1}, {ID: "update-two", Action: "update-configmap", Name: "two", UID: "child-two", ExpectedVersion: "rv-two", TargetRevision: "rev-two", TargetFingerprint: "fingerprint-two", WriterEpoch: 1}}
	for _, operation := range operations {
		go func(op OperationTombstone) {
			results <- client.lifecycle.run("namespace-one", "uid-one", "attempt-one", op, func() error { started <- struct{}{}; <-release; return failure(UnknownWriteOutcome, nil) })
		}(operation)
	}
	<-started
	<-started
	close(release)
	requireCode(t, <-results, UnknownWriteOutcome)
	requireCode(t, <-results, UnknownWriteOutcome)
	requireCode(t, client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one"), UnknownWriteOutcome)
	wrong := operations[0]
	wrong.TargetFingerprint = "other-target"
	requireCode(t, client.lifecycle.exactUnknown("namespace-one", "uid-one", "attempt-one", wrong, true), UnknownWriteOutcome)
	if err := client.lifecycle.exactUnknown("namespace-one", "uid-one", "attempt-one", operations[0], true); err != nil {
		t.Fatal(err)
	}
	state := client.Lifecycle("namespace-one")
	if len(state.Unresolved) != 1 || state.Unresolved[0].ID != operations[1].ID || !state.Draining {
		t.Fatal("recovery removed unrelated unknown or reopened drain")
	}
	requireCode(t, client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one"), UnknownWriteOutcome)
	if err := client.lifecycle.exactUnknown("namespace-one", "uid-one", "attempt-one", operations[1], true); err != nil {
		t.Fatal(err)
	}
	if err := client.lifecycle.drain(context.Background(), "namespace-one", "uid-one", "attempt-one"); err != nil {
		t.Fatal(err)
	}
	requireCode(t, client.lifecycle.run("namespace-one", "uid-one", "attempt-one", OperationTombstone{ID: "late"}, func() error { t.Fatal("drain reopened"); return nil }), LifecycleDraining)
}
