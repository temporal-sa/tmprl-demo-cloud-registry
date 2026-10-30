package kubernetes

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ConfigMapCreate struct {
	Desired *corev1.ConfigMap
	Intent  ownership.NamespacedIntent
	Writer  ownership.Writer
}

// CreateConfigMap holds the managed parent lifecycle gate through dispatch and
// response validation. A Namespace pre-read is not atomic cross-resource CAS:
// safety requires our finalizer, no managed name reuse, and all managed writers
// using this same local Client. An administrator forcibly removing the finalizer
// or another process bypassing this gate is outside this deliberately local mode.
func (b *NamespaceBinding) CreateConfigMap(ctx context.Context, request ConfigMapCreate) (ownership.NamespacedRecord, error) {
	var result ownership.NamespacedRecord
	if err := b.validateDesired(request.Desired, request.Intent); err != nil {
		return result, err
	}
	op := OperationTombstone{ID: request.Intent.Intent.OperationID, Action: "create-configmap", Name: request.Desired.Name}
	err := b.client.lifecycle.run(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, op, func() error {
		parent, namespace, err := b.refreshState(ctx, false)
		if err != nil {
			return err
		}
		if !contains(namespace.Finalizers, NamespaceLifecycleFinalizer) || b.attemptID != b.parent.CreationOperationID {
			return failure(ParentUnprotected, nil)
		}
		if _, err := b.bound.AuthorizeCreate(request.Intent, parent, request.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		object := request.Desired.DeepCopy()
		b.stampConfigMap(object, request.Intent, request.Writer, nil)
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		created, err := b.client.createConfigMap(call, object)
		conflicted := apierrors.IsAlreadyExists(err)
		if conflicted {
			created, err = b.client.core.ConfigMaps(b.bound.NamespaceName()).Get(call, object.Name, metav1.GetOptions{})
		}
		if err != nil {
			return mutationError(err)
		}
		observed, err := b.childObservation(created)
		if err != nil || created.Name != object.Name || created.Annotations[CreationOperationAnnotation] != request.Intent.Intent.OperationID || created.Annotations[OperationAnnotation] != request.Intent.Intent.OperationID || !sameData(created, object) {
			if conflicted {
				return failure(OwnershipRejected, err)
			}
			return failure(UnknownWriteOutcome, err)
		}
		result, err = b.bound.BindCreated(request.Intent, parent, observed, request.Writer, time.Now())
		if err != nil {
			if conflicted {
				return failure(OwnershipRejected, err)
			}
			return failure(UnknownWriteOutcome, err)
		}
		return nil
	})
	return result, err
}
