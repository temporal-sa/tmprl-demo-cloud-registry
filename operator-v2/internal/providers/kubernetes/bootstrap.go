package kubernetes

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type NamespaceBootstrap struct {
	Context ownership.Context
	Intent  ownership.Intent
	Object  *corev1.Namespace
	Writer  ownership.Writer
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (c *Client) validateBootstrap(request NamespaceBootstrap) error {
	object, intent := request.Object, request.Intent
	scope, err := request.Context.Scope(ownership.Kubernetes)
	if err != nil || scope.ClusterID != c.identity.ID || scope.Namespace != "" || scope != intent.Scope || intent.Purpose != "runtime-namespace" {
		return failure(OwnershipRejected, err)
	}
	if _, err := request.Context.AuthorizeCreate(intent, request.Writer, time.Now()); err != nil {
		return failure(OwnershipRejected, err)
	}
	if object == nil || object.APIVersion != "v1" || object.Kind != "Namespace" || object.Name != intent.Name || object.Namespace != "" || object.GenerateName != "" || object.UID != "" || object.ResourceVersion != "" || !object.CreationTimestamp.IsZero() || object.DeletionTimestamp != nil || len(object.OwnerReferences) > 0 || len(object.Finalizers) > 0 || len(object.Spec.Finalizers) > 0 || object.Status.Phase != "" {
		return failure(InvalidConfiguration, nil)
	}
	expected := CreationAnnotations(intent, request.Writer)
	for _, key := range []string{PurposeAnnotation, CreationOperationAnnotation, CreationKeyAnnotation, FingerprintAnnotation, WriterEpochAnnotation, AttemptAnnotation} {
		if _, exists := object.Annotations[key]; exists {
			return failure(OwnershipRejected, nil)
		}
	}
	for key, value := range map[string]string{OwnerAnnotation: expected[OwnerAnnotation], RevisionAnnotation: intent.Revision, OperationAnnotation: intent.OperationID} {
		if actual, exists := object.Annotations[key]; exists && actual != value {
			return failure(OwnershipRejected, nil)
		}
	}
	return nil
}

func (c *Client) bindBootstrap(ctx context.Context, request NamespaceBootstrap, object *corev1.Namespace) (*NamespaceBinding, error) {
	if object == nil || object.UID == "" {
		return nil, failure(UnknownWriteOutcome, nil)
	}
	// UID observation consumes the name even if subsequent provenance checking
	// fails. This does not grant ownership of an unverified conflicting object.
	if err := c.lifecycle.observe(request.Intent.Name, string(object.UID), request.Intent.OperationID); err != nil {
		return nil, err
	}
	if object.Name != request.Intent.Name || object.Namespace != "" || object.DeletionTimestamp != nil || !contains(object.Finalizers, NamespaceLifecycleFinalizer) || object.Annotations[AttemptAnnotation] != request.Intent.OperationID || object.Annotations[CreationOperationAnnotation] != request.Intent.OperationID {
		return nil, failure(OwnershipRejected, nil)
	}
	observed, err := c.observeMetadata(object, "")
	if err != nil {
		return nil, err
	}
	parent, err := request.Context.BindCreated(request.Intent, observed, request.Writer, time.Now())
	if err != nil {
		return nil, failure(OwnershipRejected, err)
	}
	return c.BindNamespace(ctx, request.Context, parent, request.Intent.OperationID)
}

// BootstrapNamespace is separate from child reconciliation. OperationID is the
// accepted namespace-attempt identity. A live matching UID can be recovered;
// an observed name which is now absent, draining, replaced or uncertain is never
// POSTed again. This memory is local-process only and must become durable before
// any production/multi-process mode can be enabled.
func (c *Client) BootstrapNamespace(ctx context.Context, request NamespaceBootstrap) (*NamespaceBinding, error) {
	if c == nil || c.core == nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	if err := c.validateBootstrap(request); err != nil {
		return nil, err
	}
	if err := c.verifyCluster(ctx); err != nil {
		return nil, err
	}
	state := c.Lifecycle(request.Intent.Name)
	if state.Attempt != "" && state.Attempt != request.Intent.OperationID {
		return nil, failure(AttemptConsumed, nil)
	}
	for _, operation := range state.Unresolved {
		if operation.Unknown {
			return nil, failure(UnknownWriteOutcome, nil)
		}
	}
	if state.Draining {
		return nil, failure(AttemptConsumed, nil)
	}
	if state.UID != "" {
		call, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		current, err := c.core.Namespaces().Get(call, request.Intent.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, failure(AttemptConsumed, nil)
		}
		if err != nil {
			return nil, failure(ParentUnavailable, err)
		}
		if string(current.UID) != state.UID {
			return nil, failure(AttemptConsumed, nil)
		}
		return c.bindBootstrap(ctx, request, current)
	}
	if err := c.lifecycle.reserve(request.Intent.Name, request.Intent.OperationID); err != nil {
		return nil, err
	}
	var result *NamespaceBinding
	err := c.lifecycle.run(request.Intent.Name, "", request.Intent.OperationID, OperationTombstone{ID: request.Intent.OperationID, Action: "create-namespace", Name: request.Intent.Name}, func() error {
		if _, err := request.Context.AuthorizeCreate(request.Intent, request.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		object := request.Object.DeepCopy()
		if object.Annotations == nil {
			object.Annotations = map[string]string{}
		}
		for key, value := range CreationAnnotations(request.Intent, request.Writer) {
			object.Annotations[key] = value
		}
		object.Annotations[OperationAnnotation] = request.Intent.OperationID
		object.Annotations[AttemptAnnotation] = request.Intent.OperationID
		object.Finalizers = []string{NamespaceLifecycleFinalizer}
		call, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		created, err := c.createNamespace(call, object)
		conflicted := apierrors.IsAlreadyExists(err)
		if conflicted {
			created, err = c.core.Namespaces().Get(call, object.Name, metav1.GetOptions{})
		}
		if err != nil {
			return mutationError(err)
		}
		result, err = c.bindBootstrap(ctx, request, created)
		if err != nil && !conflicted {
			// The API accepted this write, but canonical outcome validation did
			// not finish. Never downgrade that uncertainty to a retryable refusal.
			return failure(UnknownWriteOutcome, err)
		}
		return err
	})
	return result, err
}
