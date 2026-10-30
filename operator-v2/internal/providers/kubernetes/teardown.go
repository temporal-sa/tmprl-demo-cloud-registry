package kubernetes

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// DrainAndDeleteNamespace never reopens a draining namespace, even on failure.
// It confirms canonical UID absence before reporting completion. It removes
// only our metadata finalizer, after managed callbacks drain with no uncertain
// results. The registry is local memory; process restart/multiple writers need
// the separately planned durable coordinator, so production remains disabled.
func (b *NamespaceBinding) DrainAndDeleteNamespace(ctx context.Context, writer ownership.Writer, operationID string) (DeletionResult, error) {
	result := DeletionResult{Retained: true}
	if err := b.client.lifecycle.drain(ctx, b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID); err != nil {
		return result, err
	}
	op := OperationTombstone{ID: operationID, Action: "delete-namespace", Name: b.parent.Ref.Name, UID: b.parent.Ref.ProviderID}
	err := b.client.lifecycle.runCleanup(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, op, func() error {
		observed, namespace, err := b.refreshState(ctx, true)
		if apierrors.IsNotFound(err) {
			result = DeletionResult{ConfirmedAbsent: true}
			return nil
		}
		if err != nil {
			return err
		}
		authorize := func(current ownership.Observation) error {
			_, err := b.base.Authorize(ownership.MutationRequest{Action: ownership.Cleanup, Record: b.parent, Observed: current, ExpectedRevision: b.parent.Revision, Writer: writer, Now: time.Now(), Fencing: ownership.ProviderConditional})
			if err != nil {
				return failure(OwnershipRejected, err)
			}
			return nil
		}
		if err := authorize(observed); err != nil {
			return err
		}
		if namespace.DeletionTimestamp == nil {
			uid, version := types.UID(b.parent.Ref.ProviderID), namespace.ResourceVersion
			call, cancel := context.WithTimeout(ctx, b.client.timeout)
			err := b.client.deleteNamespace(call, namespace.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
			cancel()
			if apierrors.IsNotFound(err) {
				result = DeletionResult{Requested: true, ConfirmedAbsent: true}
				return nil
			}
			if err != nil {
				return mutationError(err)
			}
		}
		result.Requested = true
		for {
			current, object, err := b.refreshState(ctx, true)
			if apierrors.IsNotFound(err) {
				result = DeletionResult{Requested: true, ConfirmedAbsent: true}
				return nil
			}
			if err != nil {
				return err
			}
			if contains(object.Finalizers, NamespaceLifecycleFinalizer) {
				if err := authorize(current); err != nil {
					return err
				}
				updated := object.DeepCopy()
				updated.Finalizers = nil
				for _, finalizer := range object.Finalizers {
					if finalizer != NamespaceLifecycleFinalizer {
						updated.Finalizers = append(updated.Finalizers, finalizer)
					}
				}
				// UID/resourceVersion are retained from this independent read.
				call, cancel := context.WithTimeout(ctx, b.client.timeout)
				_, err := b.client.updateNamespace(call, updated)
				cancel()
				if apierrors.IsNotFound(err) {
					result = DeletionResult{Requested: true, ConfirmedAbsent: true}
					return nil
				}
				if err != nil {
					return mutationError(err)
				}
			}
			result.Finalizers = nil
			for _, finalizer := range object.Finalizers {
				if finalizer != NamespaceLifecycleFinalizer {
					result.Finalizers = append(result.Finalizers, finalizer)
				}
			}
			for _, finalizer := range object.Spec.Finalizers {
				if finalizer != "kubernetes" {
					result.Finalizers = append(result.Finalizers, string(finalizer))
				}
			}
			if len(result.Finalizers) > 0 {
				return failure(NamespaceRetained, nil)
			}
			select {
			case <-ctx.Done():
				return failure(NamespaceRetained, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
	return result, err
}
