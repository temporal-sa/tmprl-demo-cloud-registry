// Package kubejournal implements a single pinned control-cell CAS backend. It
// does not bootstrap/recreate cells, delete namespaces, or establish distributed
// writer fencing. The supplied Kubernetes adapter supports local serialized
// mode only; the durable facts here are one prerequisite for a later mode.
package kubejournal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const DataKey = "journal.json"
const Revision = "journal-schema-1"
const cellLabel = "registry.tmprl-demo.cloud/journal-cell"

type Binding interface {
	Context() ownership.NamespacedContext
	ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error)
	UpdateConfigMap(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error)
	ConfirmConfigMapUpdate(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error)
	HasUnknownConfigMapUpdate(kube.ConfigMapUpdate) (bool, error)
}
type Config struct {
	Namespace Binding
	// Pinned is bootstrap provenance independently stored outside this cell.
	// Mutable payload versions are authorized by this exact cell identity and
	// Subject assignment; they are not adopted as general resource ownership.
	Pinned  ownership.NamespacedRecord
	Subject rollout.Subject
	Writer  func() ownership.Writer
}
type Cell struct{ config Config }

// Identity contains read-only assignment metadata; it is not a transferable
// mutation permission or a substitute for independently pinned bootstrap proof.
type Identity struct {
	Subject       rollout.Subject
	ControlOwner  ownership.Owner
	ControlParent ownership.NamespaceParent
	Scope         ownership.Scope
	CellName      string
	CellUID       string
}

func (c *Cell) Identity() Identity {
	return Identity{Subject: c.config.Subject, ControlOwner: c.config.Namespace.Context().Owner(), ControlParent: c.config.Pinned.Parent, Scope: c.config.Pinned.Record.Ref.Scope, CellName: c.config.Pinned.Record.Ref.Name, CellUID: c.config.Pinned.Record.Ref.ProviderID}
}

func Name(subject rollout.Subject) string {
	data, _ := json.Marshal(subject)
	hash := sha256.Sum256(data)
	return "project-journal-" + hex.EncodeToString(hash[:16])
}

// BootstrapObject returns a desired object for a separate explicit bootstrap
// capability. It authorizes no create and contains no credential values.
func BootstrapObject(namespace string, initial journal.Envelope) (*corev1.ConfigMap, error) {
	data, err := initial.JSON()
	if err != nil || namespace == "" {
		return nil, journal.ErrInvalid
	}
	return desired(namespace, initial.Subject, data), nil
}
func desired(namespace string, subject rollout.Subject, data []byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: Name(subject), Namespace: namespace, Labels: map[string]string{cellLabel: "true"}}, Data: map[string]string{DataKey: string(data)}}
}

func New(config Config) (*Cell, error) {
	if config.Namespace == nil || config.Writer == nil {
		return nil, journal.ErrInvalid
	}
	if err := validateReadAssignment(config.Namespace, config.Pinned, config.Subject); err != nil {
		return nil, journal.ErrInvalid
	}
	if w := config.Writer(); w.Owner != config.Namespace.Context().Owner() || w.Epoch == 0 {
		return nil, journal.ErrInvalid
	}
	return &Cell{config}, nil
}

func classify(err error) error {
	if apierrors.IsNotFound(err) {
		return journal.ErrMissing
	}
	var provider *kube.Error
	if errors.As(err, &provider) {
		switch provider.Code {
		case kube.ConditionalConflict:
			return journal.ErrConflict
		case kube.UnknownWriteOutcome:
			return journal.ErrUnknown
		case kube.OwnershipRejected, kube.AttemptConsumed, kube.ParentTerminating:
			return journal.ErrMissing
		}
	}
	return journal.ErrUnavailable
}

func (c *Cell) read(ctx context.Context) (kube.ConfigMapSnapshot, []byte, error) {
	snapshot, err := c.config.Namespace.ObserveConfigMap(ctx, c.config.Pinned)
	if err != nil {
		return kube.ConfigMapSnapshot{}, nil, classify(err)
	}
	object, observed := snapshot.Object, snapshot.Observation.Observation
	pinned := c.config.Pinned.Record
	if object == nil || object.UID == "" || string(object.UID) != pinned.Ref.ProviderID || object.DeletionTimestamp != nil || object.ResourceVersion == "" || object.Namespace != c.config.Namespace.Context().NamespaceName() || object.Name != pinned.Ref.Name || observed.Ref != pinned.Ref || observed.Owner != pinned.Owner || observed.CreationIdempotencyKey != pinned.CreationIdempotencyKey || object.Annotations[kube.CreationOperationAnnotation] != pinned.CreationOperationID || observed.Revision != Revision || snapshot.Observation.ParentUID != c.config.Pinned.Parent.UID {
		return kube.ConfigMapSnapshot{}, nil, journal.ErrMissing
	}
	data := []byte(object.Data[DataKey])
	e, err := journal.Decode(data)
	if err != nil || e.Subject != c.config.Subject || len(object.Data) != 1 || len(object.BinaryData) != 0 || object.Immutable != nil || !reflect.DeepEqual(object.Labels, map[string]string{cellLabel: "true"}) {
		return kube.ConfigMapSnapshot{}, nil, journal.ErrInvalid
	}
	fingerprint, err := kube.ConfigMapFingerprint(desired(object.Namespace, c.config.Subject, data))
	if err != nil || fingerprint != observed.SpecFingerprint {
		return kube.ConfigMapSnapshot{}, nil, journal.ErrInvalid
	}
	return snapshot, data, nil
}

func (c *Cell) Read(ctx context.Context) ([]byte, journal.Version, error) {
	s, data, err := c.read(ctx)
	if err != nil {
		return nil, journal.Version{}, err
	}
	return data, journal.Version{UID: string(s.Object.UID), ResourceVersion: s.Object.ResourceVersion}, nil
}

func (c *Cell) CompareAndSwap(ctx context.Context, expected journal.Version, data []byte, commitID string) error {
	e, err := journal.Decode(data)
	if err != nil || e.Subject != c.config.Subject || e.Digest != commitID || expected.UID != c.config.Pinned.Record.Ref.ProviderID || expected.ResourceVersion == "" {
		return journal.ErrInvalid
	}
	current, _, err := c.read(ctx)
	if err != nil {
		return err
	}
	if current.Object.ResourceVersion != expected.ResourceVersion {
		return journal.ErrConflict
	}
	object := desired(current.Object.Namespace, c.config.Subject, data)
	fingerprint, err := kube.ConfigMapFingerprint(object)
	if err != nil {
		return journal.ErrInvalid
	}
	intent, err := c.config.Namespace.Context().Intent("configmap", object.Name, Revision, "journal-"+commitID, fingerprint)
	if err != nil {
		return journal.ErrInvalid
	}
	// This dedicated cell capability owns all payload versions of one previously
	// pinned UID. It never imports another object's revision/fingerprint as a
	// resource ledger. Re-authorize exact current data plus provider CAS.
	record := c.config.Pinned
	record.Record.Revision = current.Observation.Observation.Revision
	record.Record.SpecFingerprint = current.Observation.Observation.SpecFingerprint
	record.Record.WriterEpoch = current.Observation.Observation.WriterEpoch
	request := kube.ConfigMapUpdate{ConfigMapMutation: kube.ConfigMapMutation{OperationID: intent.Intent.OperationID, Record: record, ExpectedVersion: expected.ResourceVersion, Writer: c.config.Writer()}, Desired: object, Intent: intent}
	_, err = c.config.Namespace.UpdateConfigMap(ctx, request)
	if err == nil {
		return nil
	}
	if errors.Is(classify(err), journal.ErrUnknown) {
		// Only this same client may clear its exact committed write tombstone.
		// Old/absent data does not make the delayed write safely noncommitted.
		if _, confirmed := c.config.Namespace.ConfirmConfigMapUpdate(ctx, request); confirmed == nil {
			return nil
		}
		return journal.ErrUnknown
	}
	return classify(err)
}

var _ journal.Cell = (*Cell)(nil)

func (c *Cell) ConfirmCommit(ctx context.Context, expected journal.Version, data []byte, commitID string) error {
	e, err := journal.Decode(data)
	if err != nil || e.Subject != c.config.Subject || e.Digest != commitID || expected.UID != c.config.Pinned.Record.Ref.ProviderID || expected.ResourceVersion == "" {
		return journal.ErrInvalid
	}
	current, stored, err := c.read(ctx)
	if err != nil {
		return err
	}
	observed, err := journal.Decode(stored)
	if err != nil || observed.Digest != commitID || current.Object.ResourceVersion == expected.ResourceVersion {
		return journal.ErrUnknown
	}
	object := desired(current.Object.Namespace, c.config.Subject, data)
	fingerprint, err := kube.ConfigMapFingerprint(object)
	if err != nil {
		return journal.ErrInvalid
	}
	intent, err := c.config.Namespace.Context().Intent("configmap", object.Name, Revision, "journal-"+commitID, fingerprint)
	if err != nil {
		return journal.ErrInvalid
	}
	request := kube.ConfigMapUpdate{ConfigMapMutation: kube.ConfigMapMutation{OperationID: intent.Intent.OperationID, Record: c.config.Pinned, ExpectedVersion: expected.ResourceVersion, Writer: c.config.Writer()}, Desired: object, Intent: intent}
	unknown, err := c.config.Namespace.HasUnknownConfigMapUpdate(request)
	if err != nil {
		return journal.ErrUnknown
	}
	if !unknown {
		return nil
	}
	if _, err := c.config.Namespace.ConfirmConfigMapUpdate(ctx, request); err != nil {
		return journal.ErrUnknown
	}
	return nil
}

var _ journal.CommitConfirmer = (*Cell)(nil)
