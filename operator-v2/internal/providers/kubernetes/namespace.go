package kubernetes

import (
	"context"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type NamespaceBinding struct {
	client    *Client
	base      ownership.Context
	parent    ownership.Record
	bound     ownership.NamespacedContext
	attemptID string
}

func (b *NamespaceBinding) Context() ownership.NamespacedContext { return b.bound }
func (b *NamespaceBinding) Parent() ownership.NamespaceParent    { return b.bound.Parent() }

func (c *Client) BindNamespace(ctx context.Context, base ownership.Context, parent ownership.Record, attemptID string) (*NamespaceBinding, error) {
	if c == nil || c.core == nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	scope, err := base.Scope(ownership.Kubernetes)
	if err != nil || scope.Namespace != "" || scope.ClusterID != c.identity.ID || parent.Ref.Scope != scope || attemptID == "" {
		return nil, failure(OwnershipRejected, err)
	}
	current, raw, err := c.readParent(ctx, parent)
	if err != nil {
		return nil, err
	}
	if raw.Annotations[AttemptAnnotation] != attemptID {
		return nil, failure(OwnershipRejected, nil)
	}
	bound, err := base.BindNamespace(parent, current)
	if err != nil {
		return nil, failure(OwnershipRejected, err)
	}
	if err := c.lifecycle.observe(parent.Ref.Name, parent.Ref.ProviderID, attemptID); err != nil {
		return nil, err
	}
	return &NamespaceBinding{client: c, base: base, parent: parent, bound: bound, attemptID: attemptID}, nil
}

func (c *Client) readParent(ctx context.Context, parent ownership.Record) (ownership.Observation, *corev1.Namespace, error) {
	return c.readParentState(ctx, parent, false)
}

func (c *Client) readParentState(ctx context.Context, parent ownership.Record, allowTerminating bool) (ownership.Observation, *corev1.Namespace, error) {
	if err := c.verifyCluster(ctx); err != nil {
		return ownership.Observation{}, nil, err
	}
	request, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	namespace, err := c.core.Namespaces().Get(request, parent.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return ownership.Observation{}, nil, failure(ParentUnavailable, err)
	}
	if !allowTerminating && (namespace.DeletionTimestamp != nil || namespace.Status.Phase == corev1.NamespaceTerminating) {
		return ownership.Observation{}, nil, failure(ParentTerminating, nil)
	}
	if namespace.Status.Phase != corev1.NamespaceActive && !(allowTerminating && namespace.Status.Phase == corev1.NamespaceTerminating) {
		return ownership.Observation{}, nil, failure(ParentUnavailable, nil)
	}
	if namespace.Annotations[CreationOperationAnnotation] != parent.CreationOperationID {
		return ownership.Observation{}, nil, failure(OwnershipRejected, nil)
	}
	observation, err := c.observeMetadata(namespace, "")
	return observation, namespace, err
}

func (b *NamespaceBinding) Refresh(ctx context.Context) (ownership.Observation, error) {
	current, _, err := b.refreshState(ctx, false)
	return current, err
}

func (b *NamespaceBinding) refreshState(ctx context.Context, allowTerminating bool) (ownership.Observation, *corev1.Namespace, error) {
	current, raw, err := b.client.readParentState(ctx, b.parent, allowTerminating)
	if err != nil {
		return ownership.Observation{}, nil, err
	}
	if raw.Annotations[AttemptAnnotation] != b.attemptID {
		return ownership.Observation{}, nil, failure(OwnershipRejected, nil)
	}
	if _, err := b.base.BindNamespace(b.parent, current); err != nil {
		return ownership.Observation{}, nil, failure(OwnershipRejected, err)
	}
	return current, raw, nil
}
