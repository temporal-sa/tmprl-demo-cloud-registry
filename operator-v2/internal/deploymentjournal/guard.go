// Package deploymentjournal connects typed Deployment dispatch to the exact
// acknowledged Pending write in an admitted actor's private journal. The actor
// must serialize/drain its full callback; verification is not an RPC fence.
package deploymentjournal

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
	core, e := childjournal.New(ctx, childjournal.Config{Kind: childjournal.Deployment, Gate: c.Gate, Owner: c.Owner, Parent: c.Parent, WriterID: c.WriterID, Input: c.Input})
	if e != nil {
		return nil, e
	}
	return &Guard{core: core}, nil
}
func (g *Guard) VerifyDeploymentIntent(ctx context.Context, r kube.DeploymentWriteIntent) error {
	if g == nil || g.core == nil {
		return ErrUnverified
	}
	return g.core.Verify(ctx, childjournal.Intent{Owner: r.Owner, Parent: r.Parent, OperationID: r.OperationID, ProviderOperationID: r.ProviderOperationID, Name: r.Name, Revision: r.Revision, Fingerprint: r.Fingerprint, WriteID: r.WriteID, WriterID: r.WriterID, WriterEpoch: r.WriterEpoch})
}

var _ kube.DeploymentIntentVerifier = (*Guard)(nil)
