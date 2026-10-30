package kubernetes

import (
	"context"
	"reflect"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

func updateTombstone(request ConfigMapUpdate) OperationTombstone {
	return OperationTombstone{ID: request.OperationID, Action: "update-configmap", Name: request.Record.Record.Ref.Name, UID: request.Record.Record.Ref.ProviderID, ExpectedVersion: request.ExpectedVersion, TargetRevision: request.Intent.Intent.Revision, TargetFingerprint: request.Intent.Intent.SpecFingerprint, WriterEpoch: request.Writer.Epoch}
}

// HasUnknownConfigMapUpdate inspects only this client's local exact-operation
// bookkeeping. False does not prove a provider write never committed. A caller
// acknowledging a commit must independently verify its exact provider result.
func (b *NamespaceBinding) HasUnknownConfigMapUpdate(request ConfigMapUpdate) (bool, error) {
	if err := b.validateDesired(request.Desired, request.Intent); err != nil {
		return false, err
	}
	if request.OperationID != request.Intent.Intent.OperationID || request.Intent.Parent != b.bound.Parent() || request.Intent.Intent.Scope != b.bound.Scope() || request.Intent.Intent.Owner != b.bound.Owner() || request.Writer.Owner != b.bound.Owner() || request.Intent.Intent.Name != request.Record.Record.Ref.Name || request.ExpectedVersion == "" {
		return false, failure(OwnershipRejected, nil)
	}
	expected := updateTombstone(request)
	l := &b.client.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	state := l.names[b.parent.Ref.Name]
	if state == nil || state.uid != b.parent.Ref.ProviderID || state.attempt != b.attemptID {
		return false, failure(AttemptConsumed, nil)
	}
	actual, exists := state.operations[expected.ID]
	if !exists {
		return false, nil
	}
	expected.Unknown = true
	if !actual.Unknown || actual != expected {
		return false, failure(UnknownWriteOutcome, nil)
	}
	return true, nil
}

func (l *localLifecycle) exactUnknown(name, uid, attempt string, expected OperationTombstone, clear bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	state := l.names[name]
	if state == nil || state.uid != uid || state.attempt != attempt {
		return failure(AttemptConsumed, nil)
	}
	expected.Unknown = true
	actual, exists := state.operations[expected.ID]
	if !exists || actual != expected || !actual.Unknown || state.writers != 0 {
		return failure(UnknownWriteOutcome, nil)
	}
	if clear {
		delete(state.operations, expected.ID)
		state.unknown = false
		for _, remaining := range state.operations {
			if remaining.Unknown {
				state.unknown = true
			}
		}
	}
	return nil
}

// ConfirmConfigMapUpdate resolves only a specific update which this same Client
// recorded as uncertain. The caller retains the original trusted mutation intent.
// It performs no write and makes no NotCommitted inference: old data, absence,
// replacement or mismatched evidence keep the tombstone. A successful read
// proves a committed snapshot; a later writer still needs a fresh UID/RV CAS.
// Resolution does not reopen a draining namespace or provide restart durability.
func (b *NamespaceBinding) ConfirmConfigMapUpdate(ctx context.Context, request ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	var result ownership.NamespacedRecord
	if err := b.validateDesired(request.Desired, request.Intent); err != nil {
		return result, err
	}
	if request.OperationID != request.Intent.Intent.OperationID || request.Intent.Parent != b.bound.Parent() || request.Intent.Intent.Scope != b.bound.Scope() || request.Intent.Intent.Owner != b.bound.Owner() || request.Writer.Owner != b.bound.Owner() || request.Intent.Intent.Name != request.Record.Record.Ref.Name || request.ExpectedVersion == "" {
		return result, failure(OwnershipRejected, nil)
	}
	tombstone := updateTombstone(request)
	if err := b.client.lifecycle.exactUnknown(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, tombstone, false); err != nil {
		return result, err
	}
	if _, err := b.Refresh(ctx); err != nil {
		return result, err
	}
	current, err := b.observeConfigMap(ctx, request.Record)
	if err != nil {
		return result, err
	}
	observed, object := current.Observation.Observation, current.Object
	expected := request.Desired.DeepCopy()
	b.stampConfigMap(expected, request.Intent, request.Writer, &request.Record.Record)
	if object.DeletionTimestamp != nil || object.ResourceVersion == request.ExpectedVersion || object.ResourceVersion == "" || object.Annotations[OperationAnnotation] != request.OperationID || observed.Revision != request.Intent.Intent.Revision || observed.SpecFingerprint != request.Intent.Intent.SpecFingerprint || observed.WriterEpoch != request.Writer.Epoch || !sameData(object, expected) || !reflect.DeepEqual(object.Labels, expected.Labels) || !reflect.DeepEqual(object.Annotations, expected.Annotations) {
		return result, failure(UnknownWriteOutcome, nil)
	}
	if err := b.client.lifecycle.exactUnknown(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, tombstone, true); err != nil {
		return result, err
	}
	result = request.Record
	result.Record.Revision = request.Intent.Intent.Revision
	result.Record.SpecFingerprint = request.Intent.Intent.SpecFingerprint
	result.Record.WriterEpoch = request.Writer.Epoch
	return result, nil
}
