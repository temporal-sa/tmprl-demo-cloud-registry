// Package servicejournal connects a typed Service create to its exact active
// acknowledged Pending journal record. The actor serializes the full callback;
// this metadata check alone is not a lease, provider receipt or RPC fence.
package servicejournal

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
	core, e := childjournal.New(ctx, childjournal.Config{Kind: childjournal.Service, Gate: c.Gate, Owner: c.Owner, Parent: c.Parent, WriterID: c.WriterID, Input: c.Input})
	if e != nil {
		return nil, e
	}
	return &Guard{core: core}, nil
}
func (g *Guard) VerifyServiceIntent(ctx context.Context, r kube.ServiceWriteIntent) error {
	if g == nil || g.core == nil {
		return ErrUnverified
	}
	return g.core.Verify(ctx, childjournal.Intent{Owner: r.Owner, Parent: r.Parent, OperationID: r.OperationID, ProviderOperationID: r.ProviderOperationID, Name: r.Name, Revision: r.Revision, Fingerprint: r.Fingerprint, WriteID: r.WriteID, WriterID: r.WriterID, WriterEpoch: r.WriterEpoch})
}

var _ kube.ServiceIntentVerifier = (*Guard)(nil)
