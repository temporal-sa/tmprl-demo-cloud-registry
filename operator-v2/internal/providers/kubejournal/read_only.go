package kubejournal

import (
	"context"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

// ReadBinding carries no mutation or writer capability.
type ReadBinding interface {
	Context() ownership.NamespacedContext
	ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error)
}
type ReadConfig struct {
	Namespace ReadBinding
	Pinned    ownership.NamespacedRecord
	Subject   rollout.Subject
}
type ReadOnlyCell struct{ reader *Cell }

func validateReadAssignment(namespace ReadBinding, record ownership.NamespacedRecord, subject rollout.Subject) error {
	if namespace == nil {
		return journal.ErrInvalid
	}
	ctx := namespace.Context()
	if subject.Environment != ctx.Owner().EnvironmentID || subject.Installation != ctx.Owner().InstallationID || ctx.ParentPurpose() != "namespace" || ctx.Parent().UID == "" || record.Parent != ctx.Parent() || record.Record.Ref.Scope != ctx.Scope() || record.Record.Owner != ctx.Owner() || record.Record.Ref.Purpose != "configmap" || record.Record.Ref.Name != Name(subject) || record.Record.Ref.ProviderID == "" || record.Record.CreationOperationID == "" || record.Record.CreationIdempotencyKey == "" || record.Record.Revision != Revision {
		return journal.ErrInvalid
	}
	_, err := journal.Initial(subject, 1)
	return err
}

// NewReadOnly reuses the exact mutable-cell assignment and live read validator,
// but never constructs an invented Writer or exposes an update capability.
func NewReadOnly(config ReadConfig) (*ReadOnlyCell, error) {
	if err := validateReadAssignment(config.Namespace, config.Pinned, config.Subject); err != nil {
		return nil, err
	}
	return &ReadOnlyCell{reader: &Cell{config: Config{Namespace: readOnlyBinding{config.Namespace}, Pinned: config.Pinned, Subject: config.Subject}}}, nil
}
func (c *ReadOnlyCell) Read(ctx context.Context) ([]byte, journal.Version, error) {
	if c == nil || c.reader == nil {
		return nil, journal.Version{}, journal.ErrInvalid
	}
	return c.reader.Read(ctx)
}
func (*ReadOnlyCell) CompareAndSwap(context.Context, journal.Version, []byte, string) error {
	return journal.ErrInvalid
}

// The internal adapter only satisfies Cell's existing implementation type. Its
// mutation methods unconditionally deny; none call the supplied read binding.
type readOnlyBinding struct{ ReadBinding }

func (readOnlyBinding) UpdateConfigMap(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	return ownership.NamespacedRecord{}, journal.ErrInvalid
}
func (readOnlyBinding) ConfirmConfigMapUpdate(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	return ownership.NamespacedRecord{}, journal.ErrInvalid
}
func (readOnlyBinding) HasUnknownConfigMapUpdate(kube.ConfigMapUpdate) (bool, error) {
	return false, journal.ErrInvalid
}

var _ journal.Cell = (*ReadOnlyCell)(nil)
