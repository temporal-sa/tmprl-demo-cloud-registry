// Package childjournal verifies the finite Deployment, Service and ServiceAccount create
// capabilities against an actor's exact acknowledged journal write. Callers
// must serialize/drain the complete callback; this read is not an RPC fence.
package childjournal

import (
	"context"
	"errors"
	"regexp"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

var ErrUnverified = errors.New("namespaced create requires an exact active acknowledged pending journal intent")
var stableID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)

type Kind string

const (
	Deployment     Kind = "Deployment"
	Service        Kind = "Service"
	ServiceAccount Kind = "ServiceAccount"
)

type Config struct {
	Kind     Kind
	Gate     *admission.Gate
	Owner    ownership.Owner
	Parent   ownership.NamespaceParent
	WriterID string
	Input    journal.InputReference
}
type Intent struct {
	Owner                                                                            ownership.Owner
	Parent                                                                           ownership.NamespaceParent
	OperationID, ProviderOperationID, Name, Revision, Fingerprint, WriteID, WriterID string
	WriterEpoch                                                                      uint64
}
type Guard struct {
	config  Config
	subject rollout.Subject
	cellUID string
	session journal.Admission
}

func New(ctx context.Context, c Config) (*Guard, error) {
	if c.Kind != Deployment && c.Kind != Service && c.Kind != ServiceAccount || c.Gate == nil || c.Owner.Lifetime != ownership.Disposable || c.Owner.RunID == "" || c.Parent.Owner != c.Owner || c.Parent.UID == "" || c.Parent.Name == "" || c.Parent.ClusterID == "" || c.Parent.CreationOperationID == "" || c.Parent.CreationIdempotencyKey == "" || !stableID.MatchString(c.WriterID) {
		return nil, ErrUnverified
	}
	if c.Kind == ServiceAccount && !c.Input.MatchesKind("ServiceAccount") {
		return nil, ErrUnverified
	}
	if c.Input != (journal.InputReference{}) && !c.Input.MatchesKind(string(c.Kind)) {
		return nil, ErrUnverified
	}
	s, e := c.Gate.Snapshot(ctx)
	subject := rollout.Subject{Environment: c.Owner.EnvironmentID, Installation: c.Owner.InstallationID, ProjectUID: c.Owner.ProjectUID}
	if e != nil || s.Envelope.Subject != subject || s.Envelope.Admission == nil || s.Envelope.Admission.State != journal.AdmissionActive || s.Version.UID == "" || !matchesAttempt(s.Envelope, c.Parent, c.Parent.CreationOperationID, "") {
		return nil, ErrUnverified
	}
	return &Guard{config: c, subject: subject, cellUID: s.Version.UID, session: *s.Envelope.Admission}, nil
}
func matchesAttempt(e journal.Envelope, p ownership.NamespaceParent, operation, revision string) bool {
	if len(e.Attempts) == 0 {
		return false
	}
	a := e.Attempts[len(e.Attempts)-1]
	return a.OperationID == operation && a.State == journal.Observed && a.NamespaceUID == p.UID && a.NamespaceName == p.Name && (revision == "" || a.Revision == revision)
}
func (g *Guard) Verify(ctx context.Context, r Intent) error {
	if g == nil || g.config.Gate == nil || ctx.Err() != nil {
		return ErrUnverified
	}
	c := g.config
	if r.Owner != c.Owner || r.Parent != c.Parent || r.WriterID != c.WriterID || r.OperationID != c.Parent.CreationOperationID || r.WriteID == "" {
		return ErrUnverified
	}
	s, e := c.Gate.Snapshot(ctx)
	if e != nil || s.Version.UID != g.cellUID || s.Envelope.Subject != g.subject || s.Envelope.Admission == nil || *s.Envelope.Admission != g.session || s.Envelope.WriterEpoch != r.WriterEpoch || !matchesAttempt(s.Envelope, c.Parent, r.OperationID, r.Revision) {
		return ErrUnverified
	}
	for _, w := range s.Envelope.Writes {
		if w.ID != r.WriteID && (w.State == journal.Pending || w.State == journal.Unknown) {
			return ErrUnverified
		}
	}
	for _, w := range s.Envelope.Writes {
		if w.ID != r.WriteID {
			continue
		}
		p := w.Preconditions
		if w.State != journal.Pending || w.Input != c.Input || w.OperationID != r.OperationID || w.WriterEpoch != r.WriterEpoch || p.Kind != string(c.Kind) || p.Action != "Create" || p.Name != r.Name || p.ParentUID != r.Parent.UID || p.TargetRevision != r.Revision || p.ProviderOperationID != r.ProviderOperationID || p.Fingerprint != r.Fingerprint || p.UID != "" || p.Version != "" || w.ResultUID != "" || w.ResultVersion != "" || w.ConfirmedAbsent || w.ResultSpecDigest != "" || w.ResultSpecContract != "" {
			return ErrUnverified
		}
		return nil
	}
	return ErrUnverified
}
