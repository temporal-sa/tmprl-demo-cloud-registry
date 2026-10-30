package deploymentjournal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/childjournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/serviceaccountjournal"
)

// Reuse the independently admitted real Repository/Gate fixture. The typed
// facades must not authorize one another's acknowledged journal entries.
func TestServiceAccountGuardRequiresItsRetainedInputAndCannotCrossKinds(t *testing.T) {
	for _, kind := range []string{"Deployment", "Service", "ServiceAccount"} {
		c, _, r := fixture(t)
		ref := journal.InputReference{Kind: "ServiceAccountRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: journal.ServiceAccountInputContract}
		cfg := serviceaccountjournal.Config{Gate: c.Gate, Owner: c.Owner, Parent: c.Parent, WriterID: c.WriterID, Input: ref}
		missing := cfg
		missing.Input = journal.InputReference{}
		if _, err := serviceaccountjournal.New(context.Background(), missing); !errors.Is(err, childjournal.ErrUnverified) {
			t.Fatal("SA guard without original input admitted", err)
		}
		g, err := serviceaccountjournal.New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		d, err := New(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		next := pending(snapshot(t, c.Gate), r)
		next.Writes[1].Preconditions.Kind = kind
		if kind == "ServiceAccount" {
			next.Writes[1].Input = ref
		}
		save(t, c.Gate, next)
		request := kube.ServiceAccountWriteIntent{Owner: r.Owner, Parent: r.Parent, OperationID: r.OperationID, ProviderOperationID: r.ProviderOperationID, Name: r.Name, Revision: r.Revision, Fingerprint: r.Fingerprint, WriteID: r.WriteID, WriterID: r.WriterID, WriterEpoch: r.WriterEpoch}
		err = g.VerifyServiceAccountIntent(context.Background(), request)
		if kind == "ServiceAccount" && err != nil || kind != "ServiceAccount" && !errors.Is(err, childjournal.ErrUnverified) {
			t.Fatal("SA guard crossed acknowledged kind/input boundary", err)
		}
		if err = d.VerifyDeploymentIntent(context.Background(), r); kind == "Deployment" && err != nil || kind != "Deployment" && !errors.Is(err, childjournal.ErrUnverified) {
			t.Fatal("Deployment guard crossed SA boundary", err)
		}
		if kind != "ServiceAccount" {
			continue
		}
		for _, change := range []func(*kube.ServiceAccountWriteIntent){func(v *kube.ServiceAccountWriteIntent) { v.Parent.UID = "recreated" }, func(v *kube.ServiceAccountWriteIntent) { v.WriterEpoch++ }, func(v *kube.ServiceAccountWriteIntent) { v.Fingerprint = strings.Repeat("d", 64) }} {
			bad := request
			change(&bad)
			if err = g.VerifyServiceAccountIntent(context.Background(), bad); !errors.Is(err, childjournal.ErrUnverified) {
				t.Fatal("changed SA write admitted", err)
			}
		}
		next = snapshot(t, c.Gate)
		next.Writes[1].State = journal.Committed
		next.Writes[1].ResultUID = "sa-uid"
		next.Writes[1].ResultVersion = "3"
		save(t, c.Gate, next)
		if err = g.VerifyServiceAccountIntent(context.Background(), request); !errors.Is(err, childjournal.ErrUnverified) {
			t.Fatal("committed SA authorized another POST", err)
		}
	}
}
