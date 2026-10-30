package journal

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestServiceAccountRequiresExactOriginalInputAndCanonicalCreateReceipt(t *testing.T) {
	r, s := observedFixture(t)
	ref := InputReference{Kind: "ServiceAccountRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: ServiceAccountInputContract}
	w := Write{ID: "create-sa", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Input: ref, Preconditions: Preconditions{Kind: "ServiceAccount", Action: "Create", Name: "backend", ParentUID: "namespace-uid", TargetRevision: "revision-one", ProviderOperationID: "apply-sa", Fingerprint: strings.Repeat("c", 64)}}
	for _, change := range []func(*Write){
		func(v *Write) { v.Input = InputReference{} },
		func(v *Write) { v.Input.Kind = "ServiceRender"; v.Input.Contract = ServiceInputContract },
		func(v *Write) {
			v.Preconditions.Action = "Update"
			v.Preconditions.UID = "sa-uid"
			v.Preconditions.Version = "3"
		},
		func(v *Write) {
			v.Preconditions.Action = "Delete"
			v.Preconditions.UID = "sa-uid"
			v.Preconditions.Version = "3"
		},
		func(v *Write) { v.ResultUID = "sa-uid"; v.ResultVersion = "3" },
	} {
		bad := w
		change(&bad)
		next := clone(s.Envelope)
		next.Writes = append(next.Writes, bad)
		if _, err := r.Commit(context.Background(), s, next); !errors.Is(err, ErrInvalid) {
			t.Fatal("unproven SA write accepted", err)
		}
	}
	next := clone(s.Envelope)
	next.Writes = append(next.Writes, w)
	s = commit(t, r, s, next)
	for _, change := range []func(*Write){func(v *Write) { v.Input.Digest = strings.Repeat("d", 64) }, func(v *Write) { v.State = Committed; v.ResultUID = "sa-uid" }, func(v *Write) { v.State = Committed; v.ResultVersion = "3" }} {
		next = clone(s.Envelope)
		change(&next.Writes[1])
		if _, err := r.Commit(context.Background(), s, next); !errors.Is(err, ErrInvalid) {
			t.Fatal("changed input/incomplete receipt accepted", err)
		}
	}
	next = clone(s.Envelope)
	next.Writes[1].State = Committed
	next.Writes[1].ResultUID = "sa-uid"
	next.Writes[1].ResultVersion = "3"
	s = commit(t, r, s, next)
	next = clone(s.Envelope)
	next.Writes[1].ResultVersion = "4"
	if _, err := r.Commit(context.Background(), s, next); !errors.Is(err, ErrInvalid) {
		t.Fatal("historical creation receipt changed", err)
	}
}
