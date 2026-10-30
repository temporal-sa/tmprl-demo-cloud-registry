// Package serviceaccountjournal binds the finite ServiceAccount create to an
// admitted actor's exact acknowledged Pending write and private original input.
// The actor must serialize/drain its callback; this read is not an RPC fence.
package serviceaccountjournal

import (
	"context"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/childjournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

var ErrUnverified = childjournal.ErrUnverified

type Config struct {
	Gate     *admission.Gate
	Owner    ownership.Owner
	Parent   ownership.NamespaceParent
	WriterID string
	Input    journal.InputReference
}

type Guard struct{ core *childjournal.Guard }

func New(ctx context.Context, c Config) (*Guard, error) {
	core, err := childjournal.New(ctx, childjournal.Config{Kind: childjournal.ServiceAccount, Gate: c.Gate, Owner: c.Owner, Parent: c.Parent, WriterID: c.WriterID, Input: c.Input})
	if err != nil {
		return nil, err
	}
	return &Guard{core: core}, nil
}

func (g *Guard) VerifyServiceAccountIntent(ctx context.Context, r kube.ServiceAccountWriteIntent) error {
	if g == nil || g.core == nil {
		return ErrUnverified
	}
	return g.core.Verify(ctx, childjournal.Intent{Owner: r.Owner, Parent: r.Parent, OperationID: r.OperationID, ProviderOperationID: r.ProviderOperationID, Name: r.Name, Revision: r.Revision, Fingerprint: r.Fingerprint, WriteID: r.WriteID, WriterID: r.WriterID, WriterEpoch: r.WriterEpoch})
}

var _ kube.ServiceAccountIntentVerifier = (*Guard)(nil)
