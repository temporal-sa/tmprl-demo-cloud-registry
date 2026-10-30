package journal

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDeploymentIntentRequiresObservedExactNamespaceAndFiniteKind(t *testing.T) {
	r, _, saved := fixture(t)
	saved = commit(t, r, saved, reserve(saved.Envelope))
	next := clone(saved.Envelope)
	next.Writes = []Write{namespaceWrite()}
	saved = commit(t, r, saved, next)
	next = clone(saved.Envelope)
	next.Attempts[0].State = Observed
	next.Attempts[0].NamespaceUID = "namespace-uid"
	next.Writes[0].State = Committed
	next.Writes[0].ResultUID = "namespace-uid"
	next.Writes[0].ResultVersion = "2"
	saved = commit(t, r, saved, next)
	write := Write{ID: "create-deployment", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Preconditions: Preconditions{Kind: "Deployment", Action: "Create", Name: "app", ParentUID: "namespace-uid", TargetRevision: "revision-one", ProviderOperationID: "apply-app", Fingerprint: strings.Repeat("d", 64)}}
	for _, change := range []func(*Write){func(w *Write) { w.Preconditions.Kind = "Arbitrary" }, func(w *Write) { w.Preconditions.ParentUID = "other" }, func(w *Write) { w.Preconditions.ParentUID = "" }, func(w *Write) { w.Preconditions.TargetRevision = "other" }} {
		bad := write
		change(&bad)
		next = clone(saved.Envelope)
		next.Writes = append(next.Writes, bad)
		if _, err := r.Commit(context.Background(), saved, next); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid deployment journal accepted: %v", err)
		}
	}
	next = clone(saved.Envelope)
	next.Writes = append(next.Writes, write)
	saved = commit(t, r, saved, next)
	next = clone(saved.Envelope)
	next.Writes[1].State = Committed
	next.Writes[1].ResultUID = "deployment-uid"
	next.Writes[1].ResultVersion = "3"
	saved = commit(t, r, saved, next)
	if saved.Envelope.Writes[1].ResultUID != "deployment-uid" {
		t.Fatal("deployment canonical UID lost")
	}
}

func TestPrivateDeploymentInputReferenceIsFiniteAndImmutable(t *testing.T) {
	r, saved := observedFixture(t)
	reference := InputReference{Kind: "DeploymentRender", UID: "render-" + strings.Repeat("b", 64), Version: "1", Digest: strings.Repeat("a", 64), Contract: DeploymentInputContract}
	write := Write{ID: "retained-deployment", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Input: reference, Preconditions: Preconditions{Kind: "Deployment", Action: "Create", Name: "app", ParentUID: "namespace-uid", TargetRevision: "revision-one", ProviderOperationID: "apply-app", Fingerprint: strings.Repeat("d", 64)}}
	for _, change := range []func(*Write){
		func(w *Write) { w.Input.Kind = "RawBlob" },
		func(w *Write) { w.Input.Digest = "sha256:" + w.Input.Digest },
		func(w *Write) { w.Input.UID = "" },
		func(w *Write) { w.Input.UID = "x/../../private" },
		func(w *Write) { w.Input.UID = "https://host/path" },
		func(w *Write) { w.Input.Contract = "" },
		func(w *Write) { w.Input.Contract = "future-contract" },
		func(w *Write) { w.Input.Version = "2" },
		func(w *Write) { w.Preconditions.Kind = "ConfigMap" },
		func(w *Write) { w.Preconditions.Kind = "Service" },
	} {
		bad := write
		change(&bad)
		n := clone(saved.Envelope)
		n.Writes = append(n.Writes, bad)
		if _, err := r.Commit(context.Background(), saved, n); !errors.Is(err, ErrInvalid) {
			t.Fatal("unbounded or wrong-kind input reference accepted", err)
		}
	}
	n := clone(saved.Envelope)
	n.Writes = append(n.Writes, write)
	saved = commit(t, r, saved, n)
	for _, replacement := range []InputReference{{}, {Kind: "DeploymentRender", UID: "render-" + strings.Repeat("c", 64), Version: "1", Digest: reference.Digest, Contract: reference.Contract}} {
		n = clone(saved.Envelope)
		n.Writes[1].Input = replacement
		if _, err := r.Commit(context.Background(), saved, n); !errors.Is(err, ErrInvalid) {
			t.Fatal("pending input reference changed after acknowledgement", err)
		}
	}
	n = clone(saved.Envelope)
	n.Writes[1].State, n.Writes[1].ResultUID, n.Writes[1].ResultVersion = Committed, "deployment-uid", "3"
	saved = commit(t, r, saved, n)
	if saved.Envelope.Writes[1].Input != reference {
		t.Fatal("input reference did not survive commit and durable decoding")
	}
}
