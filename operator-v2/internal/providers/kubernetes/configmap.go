package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const OperationAnnotation = "registry.tmprl-demo.cloud/operation"
const NamespaceLifecycleFinalizer = "registry.tmprl-demo.cloud/namespace-lifecycle"

type ConfigMapSnapshot struct {
	Object      *corev1.ConfigMap
	Observation ownership.NamespacedObservation
}
type ConfigMapMutation struct {
	OperationID     string
	Record          ownership.NamespacedRecord
	ExpectedVersion string
	Writer          ownership.Writer
}
type ConfigMapUpdate struct {
	ConfigMapMutation
	Desired *corev1.ConfigMap
	Intent  ownership.NamespacedIntent
}
type DeletionResult struct {
	Requested       bool
	ConfirmedAbsent bool
	Retained        bool
	Finalizers      []string
}

// ConfigMapFingerprint pins the complete pre-stamping desired object. It uses
// the same JSON digest contract as the pure renderer, not a live object's
// server-populated metadata. The durable journal must retain the mutation intent.
func ConfigMapFingerprint(object *corev1.ConfigMap) (string, error) {
	if object == nil {
		return "", failure(InvalidConfiguration, nil)
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return "", failure(InvalidConfiguration, nil)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (b *NamespaceBinding) validateChildRecord(record ownership.NamespacedRecord) error {
	if record.Parent != b.bound.Parent() || record.Record.Ref.Scope != b.bound.Scope() || record.Record.Ref.Purpose != "configmap" || record.Record.Ref.ProviderID == "" || record.Record.Ref.Name == "" {
		return failure(OwnershipRejected, nil)
	}
	return nil
}

func (b *NamespaceBinding) childObservation(object *corev1.ConfigMap) (ownership.NamespacedObservation, error) {
	if object.Namespace != b.bound.NamespaceName() {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	marker := object.Annotations[ownership.ParentNamespaceUIDAnnotation]
	if marker == "" || marker != b.bound.Parent().UID || len(object.OwnerReferences) != 1 {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	ref := object.OwnerReferences[0]
	if ref.APIVersion != "v1" || ref.Kind != "Namespace" || ref.Name != b.bound.NamespaceName() || string(ref.UID) != marker {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	observed, err := b.client.observeMetadata(object, b.bound.NamespaceName())
	if err != nil {
		return ownership.NamespacedObservation{}, err
	}
	if observed.Ref.Purpose != "configmap" || observed.Owner != b.bound.Owner() {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	return ownership.NamespacedObservation{Observation: observed, ParentUID: marker}, nil
}

func (b *NamespaceBinding) observeConfigMap(ctx context.Context, record ownership.NamespacedRecord) (ConfigMapSnapshot, error) {
	if err := b.validateChildRecord(record); err != nil {
		return ConfigMapSnapshot{}, err
	}
	request, cancel := context.WithTimeout(ctx, b.client.timeout)
	defer cancel()
	object, err := b.client.core.ConfigMaps(b.bound.NamespaceName()).Get(request, record.Record.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return ConfigMapSnapshot{}, failure(ChildUnavailable, err)
	}
	observed, err := b.childObservation(object)
	if err != nil {
		return ConfigMapSnapshot{}, err
	}
	if observed.Observation.Ref != record.Record.Ref || observed.Observation.Owner != record.Record.Owner || object.Annotations[CreationOperationAnnotation] != record.Record.CreationOperationID || observed.Observation.CreationIdempotencyKey != record.Record.CreationIdempotencyKey {
		return ConfigMapSnapshot{}, failure(OwnershipRejected, nil)
	}
	return ConfigMapSnapshot{Object: object.DeepCopy(), Observation: observed}, nil
}

// ObserveConfigMap permits independent recovery reads without pretending that
// annotations alone authorize another write. It never recreates a missing child.
func (b *NamespaceBinding) ObserveConfigMap(ctx context.Context, record ownership.NamespacedRecord) (ConfigMapSnapshot, error) {
	if _, err := b.Refresh(ctx); err != nil {
		return ConfigMapSnapshot{}, err
	}
	return b.observeConfigMap(ctx, record)
}

func (b *NamespaceBinding) authorizeConfigMap(ctx context.Context, request ConfigMapMutation, action ownership.Action) (ConfigMapSnapshot, ownership.Observation, error) {
	parent, err := b.Refresh(ctx)
	if err != nil {
		return ConfigMapSnapshot{}, ownership.Observation{}, err
	}
	snapshot, err := b.observeConfigMap(ctx, request.Record)
	if err != nil {
		return ConfigMapSnapshot{}, ownership.Observation{}, err
	}
	if request.ExpectedVersion == "" || snapshot.Object.ResourceVersion != request.ExpectedVersion {
		return ConfigMapSnapshot{}, ownership.Observation{}, failure(ConditionalConflict, nil)
	}
	_, err = b.bound.Authorize(ownership.NamespacedMutationRequest{Action: action, Record: request.Record, Observed: snapshot.Observation, ParentObserved: parent, ExpectedRevision: request.Record.Record.Revision, Writer: request.Writer, Now: time.Now()})
	if err != nil {
		return ConfigMapSnapshot{}, ownership.Observation{}, failure(OwnershipRejected, err)
	}
	return snapshot, parent, nil
}

func (b *NamespaceBinding) validateDesired(object *corev1.ConfigMap, intent ownership.NamespacedIntent) error {
	if object == nil || object.APIVersion != "v1" || object.Kind != "ConfigMap" || object.Namespace != b.bound.NamespaceName() || object.Name != intent.Intent.Name || intent.Intent.Purpose != "configmap" || object.UID != "" || object.ResourceVersion != "" || object.GenerateName != "" || object.DeletionTimestamp != nil || !object.CreationTimestamp.IsZero() || len(object.OwnerReferences) > 0 || len(object.Finalizers) > 0 {
		return failure(InvalidConfiguration, nil)
	}
	fingerprint, err := ConfigMapFingerprint(object)
	if err != nil || fingerprint != intent.Intent.SpecFingerprint {
		return failure(OwnershipRejected, err)
	}
	expected := CreationAnnotations(intent.Intent, ownership.Writer{})
	for _, key := range []string{PurposeAnnotation, CreationOperationAnnotation, CreationKeyAnnotation, FingerprintAnnotation, WriterEpochAnnotation, AttemptAnnotation} {
		if _, exists := object.Annotations[key]; exists {
			return failure(OwnershipRejected, nil)
		}
	}
	for key, value := range map[string]string{OwnerAnnotation: expected[OwnerAnnotation], RevisionAnnotation: intent.Intent.Revision, OperationAnnotation: intent.Intent.OperationID, ownership.ParentNamespaceUIDAnnotation: b.bound.Parent().UID} {
		if actual, exists := object.Annotations[key]; exists && actual != value {
			return failure(OwnershipRejected, nil)
		}
	}
	return nil
}

func (b *NamespaceBinding) stampConfigMap(object *corev1.ConfigMap, intent ownership.NamespacedIntent, writer ownership.Writer, created *ownership.Record) {
	if object.Annotations == nil {
		object.Annotations = map[string]string{}
	}
	for key, value := range CreationAnnotations(intent.Intent, writer) {
		object.Annotations[key] = value
	}
	if created != nil {
		object.Annotations[CreationOperationAnnotation] = created.CreationOperationID
		object.Annotations[CreationKeyAnnotation] = created.CreationIdempotencyKey
	}
	object.Annotations[OperationAnnotation] = intent.Intent.OperationID
	object.Annotations[ownership.ParentNamespaceUIDAnnotation] = b.bound.Parent().UID
	object.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: b.bound.NamespaceName(), UID: types.UID(b.bound.Parent().UID)}}
}

func mutationError(err error) error {
	if apierrors.IsConflict(err) {
		return failure(ConditionalConflict, err)
	}
	if apierrors.IsNotFound(err) {
		return failure(ChildUnavailable, err)
	}
	if apierrors.IsAlreadyExists(err) || apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsBadRequest(err) {
		return failure(OwnershipRejected, err)
	}
	return failure(UnknownWriteOutcome, err)
}

func sameData(left, right *corev1.ConfigMap) bool {
	encode := func(value *corev1.ConfigMap) []byte {
		result, _ := json.Marshal(struct {
			Data       map[string]string `json:"data,omitempty"`
			BinaryData map[string][]byte `json:"binaryData,omitempty"`
			Immutable  *bool             `json:"immutable,omitempty"`
		}{value.Data, value.BinaryData, value.Immutable})
		return result
	}
	return reflect.DeepEqual(encode(left), encode(right))
}

// UpdateConfigMap is a whole-desired ConfigMap replacement with an exact
// canonical child UID/resourceVersion condition. It preserves server identity,
// creation provenance and existing finalizers. Journal commit/recovery is not
// implemented here; an unknown outcome retains a local unresolved tombstone.
func (b *NamespaceBinding) UpdateConfigMap(ctx context.Context, request ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	var result ownership.NamespacedRecord
	if err := b.validateDesired(request.Desired, request.Intent); err != nil {
		return result, err
	}
	if request.OperationID != request.Intent.Intent.OperationID {
		return result, failure(OwnershipRejected, nil)
	}
	if request.Intent.Intent.Name != request.Record.Record.Ref.Name {
		return result, failure(OwnershipRejected, nil)
	}
	op := updateTombstone(request)
	err := b.client.lifecycle.run(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, op, func() error {
		current, parent, err := b.authorizeConfigMap(ctx, request.ConfigMapMutation, ownership.Mutate)
		if err != nil {
			return err
		}
		if _, err := b.bound.AuthorizeCreate(request.Intent, parent, request.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		object := request.Desired.DeepCopy()
		object.UID = current.Object.UID
		object.ResourceVersion = request.ExpectedVersion
		object.CreationTimestamp = current.Object.CreationTimestamp
		object.Finalizers = append([]string(nil), current.Object.Finalizers...)
		b.stampConfigMap(object, request.Intent, request.Writer, &request.Record.Record)
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		written, err := b.client.updateConfigMap(call, object)
		if err != nil {
			return mutationError(err)
		}
		observed, err := b.childObservation(written)
		if err != nil || written.UID != current.Object.UID || written.Name != current.Object.Name || !sameData(object, written) || observed.Observation.Revision != request.Intent.Intent.Revision || observed.Observation.SpecFingerprint != request.Intent.Intent.SpecFingerprint || observed.Observation.WriterEpoch != request.Writer.Epoch || observed.Observation.CreationIdempotencyKey != request.Record.Record.CreationIdempotencyKey || written.Annotations[CreationOperationAnnotation] != request.Record.Record.CreationOperationID || written.Annotations[OperationAnnotation] != request.OperationID {
			return failure(UnknownWriteOutcome, err)
		}
		result = request.Record
		result.Record.Revision = request.Intent.Intent.Revision
		result.Record.SpecFingerprint = request.Intent.Intent.SpecFingerprint
		result.Record.WriterEpoch = request.Writer.Epoch
		return nil
	})
	return result, err
}

func (b *NamespaceBinding) DeleteConfigMap(ctx context.Context, request ConfigMapMutation) (DeletionResult, error) {
	result := DeletionResult{Retained: true}
	op := OperationTombstone{ID: request.OperationID, Action: "delete-configmap", Name: request.Record.Record.Ref.Name, UID: request.Record.Record.Ref.ProviderID, ExpectedVersion: request.ExpectedVersion}
	err := b.client.lifecycle.run(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, op, func() error {
		current, _, err := b.authorizeConfigMap(ctx, request, ownership.Cleanup)
		if err != nil {
			return err
		}
		uid, version := current.Object.UID, request.ExpectedVersion
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		if err := b.client.deleteConfigMap(call, b.bound.NamespaceName(), current.Object.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}); err != nil {
			return mutationError(err)
		}
		result.Requested = true
		object, err := b.client.core.ConfigMaps(b.bound.NamespaceName()).Get(call, current.Object.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			result.ConfirmedAbsent = true
			result.Retained = false
			return nil
		}
		if err != nil {
			return failure(UnknownWriteOutcome, err)
		}
		if object.UID != uid {
			return failure(UnknownWriteOutcome, nil)
		}
		result.Finalizers = append([]string(nil), object.Finalizers...)
		// A still-observed UID is retained even if it has no finalizer. Only
		// independently confirmed absence reports that the child is gone.
		return nil
	})
	return result, err
}
