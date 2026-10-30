package journal

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestServiceCreateRequiresImmutableVersionedAllocatedSpecReceipt(t *testing.T) {
	r, s := observedFixture(t)
	w := Write{ID: "create-service", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Preconditions: Preconditions{Kind: "Service", Action: "Create", Name: "backend", ParentUID: "namespace-uid", TargetRevision: "revision-one", ProviderOperationID: "apply-service", Fingerprint: strings.Repeat("a", 64)}}
	for _, change := range []func(*Write){
		func(v *Write) {
			v.ResultSpecDigest = strings.Repeat("b", 64)
			v.ResultSpecContract = ServiceSpecContract
		},
		func(v *Write) {
			v.State = Unknown
			v.ResultSpecDigest = strings.Repeat("b", 64)
			v.ResultSpecContract = ServiceSpecContract
		},
		func(v *Write) {
			v.Preconditions.Action = "Update"
			v.Preconditions.UID = "service-uid"
			v.Preconditions.Version = "3"
		},
	} {
		bad := w
		change(&bad)
		n := clone(s.Envelope)
		n.Writes = append(n.Writes, bad)
		if _, e := r.Commit(context.Background(), s, n); !errors.Is(e, ErrInvalid) {
			t.Fatal("unsupported/premature Service result accepted", e)
		}
	}
	n := clone(s.Envelope)
	n.Writes = append(n.Writes, w)
	s = commit(t, r, s, n)
	for _, change := range []func(*Write){
		func(v *Write) {},
		func(v *Write) { v.ResultSpecDigest = strings.Repeat("b", 64) },
		func(v *Write) { v.ResultSpecContract = ServiceSpecContract },
		func(v *Write) {
			v.ResultSpecDigest = strings.Repeat("b", 64)
			v.ResultSpecContract = "future-unsupported"
		},
	} {
		n = clone(s.Envelope)
		v := &n.Writes[1]
		v.State = Committed
		v.ResultUID = "service-uid"
		v.ResultVersion = "3"
		change(v)
		if _, e := r.Commit(context.Background(), s, n); !errors.Is(e, ErrInvalid) {
			t.Fatal("missing/future Service allocation receipt learned from metadata", e)
		}
	}
	n = clone(s.Envelope)
	n.Writes[1].State = Committed
	n.Writes[1].ResultUID = "service-uid"
	n.Writes[1].ResultVersion = "3"
	n.Writes[1].ResultSpecDigest = strings.Repeat("b", 64)
	n.Writes[1].ResultSpecContract = ServiceSpecContract
	s = commit(t, r, s, n)
	for _, change := range []func(*Write){func(v *Write) { v.ResultSpecDigest = strings.Repeat("c", 64) }, func(v *Write) { v.ResultSpecContract = "" }} {
		n = clone(s.Envelope)
		change(&n.Writes[1])
		if _, e := r.Commit(context.Background(), s, n); !errors.Is(e, ErrInvalid) {
			t.Fatal("creation receipt changed after commitment", e)
		}
	}
}

func TestPrivateInputFamiliesMatchWriteKindAndRemainImmutable(t *testing.T) {
	r, s := observedFixture(t)
	ref := InputReference{Kind: "ServiceRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: ServiceInputContract}
	w := Write{ID: "private-service", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Input: ref, Preconditions: Preconditions{Kind: "Service", Action: "Create", Name: "backend", ParentUID: "namespace-uid", TargetRevision: "revision-one", ProviderOperationID: "apply-service", Fingerprint: strings.Repeat("c", 64)}}
	for _, change := range []func(*Write){func(v *Write) { v.Preconditions.Kind = "Deployment" }, func(v *Write) { v.Input.Contract = DeploymentInputContract }, func(v *Write) { v.Input.Kind = "DeploymentRender"; v.Input.Contract = DeploymentInputContract }} {
		bad := w
		change(&bad)
		n := clone(s.Envelope)
		n.Writes = append(n.Writes, bad)
		if _, e := r.Commit(context.Background(), s, n); !errors.Is(e, ErrInvalid) {
			t.Fatal("cross-family input accepted", e)
		}
	}
	n := clone(s.Envelope)
	n.Writes = append(n.Writes, w)
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Writes[1].Input.Digest = strings.Repeat("d", 64)
	if _, e := r.Commit(context.Background(), s, n); !errors.Is(e, ErrInvalid) {
		t.Fatal("Pending private Service input changed", e)
	}
}
