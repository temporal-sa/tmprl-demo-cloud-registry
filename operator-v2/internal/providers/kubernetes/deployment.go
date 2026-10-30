package kubernetes

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
)

func (b *NamespaceBinding) freezeDeployment(object *appsv1.Deployment, intent ownership.NamespacedIntent) (*appsv1.Deployment, *appsv1.Deployment, error) {
	if b == nil || b.client == nil || b.client.apps == nil || object == nil {
		return nil, nil, failure(InvalidConfiguration, nil)
	}
	// Freeze before validation, verifier callbacks or any queued lifecycle work.
	d := object.DeepCopy()
	if d.APIVersion != "apps/v1" || d.Kind != "Deployment" || d.Namespace != b.bound.NamespaceName() || d.Name != intent.Intent.Name || intent.Intent.Purpose != "deployment" || intent.Intent.Revision != b.bound.ParentRevision() || intent.Parent != b.bound.Parent() {
		return nil, nil, failure(OwnershipRejected, nil)
	}
	allowed := metav1.ObjectMeta{Name: d.Name, Namespace: d.Namespace, Labels: d.Labels, Annotations: d.Annotations}
	if !sameDeploymentJSON(allowed, d.ObjectMeta) || !sameDeploymentJSON(d.Status, appsv1.DeploymentStatus{}) {
		return nil, nil, failure(InvalidConfiguration, nil)
	}
	fingerprint, err := DeploymentFingerprint(d)
	if err != nil || fingerprint != intent.Intent.SpecFingerprint {
		return nil, nil, failure(OwnershipRejected, nil)
	}
	expected := CreationAnnotations(intent.Intent, ownership.Writer{})
	for _, key := range []string{PurposeAnnotation, CreationOperationAnnotation, CreationKeyAnnotation, FingerprintAnnotation, WriterEpochAnnotation, AttemptAnnotation, "deployment.kubernetes.io/revision"} {
		if _, exists := d.Annotations[key]; exists {
			return nil, nil, failure(OwnershipRejected, nil)
		}
	}
	for key, value := range map[string]string{OwnerAnnotation: expected[OwnerAnnotation], RevisionAnnotation: intent.Intent.Revision, OperationAnnotation: intent.Intent.OperationID, ownership.ParentNamespaceUIDAnnotation: b.bound.Parent().UID} {
		if actual, exists := d.Annotations[key]; exists && actual != value {
			return nil, nil, failure(OwnershipRejected, nil)
		}
	}
	normalized, err := deploymentBaseline(d)
	if err != nil {
		return nil, nil, err
	}
	return d, normalized, nil
}

func (b *NamespaceBinding) stampDeployment(d *appsv1.Deployment, intent ownership.NamespacedIntent, writer ownership.Writer) {
	if d.Annotations == nil {
		d.Annotations = map[string]string{}
	}
	for k, v := range CreationAnnotations(intent.Intent, writer) {
		d.Annotations[k] = v
	}
	d.Annotations[OperationAnnotation] = intent.Intent.OperationID
	d.Annotations[ownership.ParentNamespaceUIDAnnotation] = b.bound.Parent().UID
	d.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: b.bound.NamespaceName(), UID: types.UID(b.bound.Parent().UID)}}
}

func (b *NamespaceBinding) deploymentObservation(d *appsv1.Deployment) (ownership.NamespacedObservation, error) {
	if d == nil || d.Namespace != b.bound.NamespaceName() || d.Annotations[ownership.ParentNamespaceUIDAnnotation] != b.bound.Parent().UID || len(d.OwnerReferences) != 1 {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	r := d.OwnerReferences[0]
	if r.APIVersion != "v1" || r.Kind != "Namespace" || r.Name != b.bound.NamespaceName() || string(r.UID) != b.bound.Parent().UID || r.Controller != nil || r.BlockOwnerDeletion != nil {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	o, err := b.client.observeMetadata(d, b.bound.NamespaceName())
	if err != nil {
		return ownership.NamespacedObservation{}, err
	}
	if o.Ref.Purpose != "deployment" || o.Owner != b.bound.Owner() {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	return ownership.NamespacedObservation{Observation: o, ParentUID: b.bound.Parent().UID}, nil
}

func (b *NamespaceBinding) deploymentWrite(request DeploymentCreate) DeploymentWriteIntent {
	return DeploymentWriteIntent{Owner: b.bound.Owner(), Parent: b.bound.Parent(), OperationID: b.attemptID, ProviderOperationID: request.Intent.Intent.OperationID, Name: request.Desired.Name, Revision: request.Intent.Intent.Revision, Fingerprint: request.Intent.Intent.SpecFingerprint, WriteID: request.WriteID, WriterID: request.Writer.ID, WriterEpoch: request.Writer.Epoch}
}
func deploymentTombstone(request DeploymentCreate) OperationTombstone {
	return OperationTombstone{ID: request.Intent.Intent.OperationID, Action: "create-deployment", Name: request.Desired.Name, TargetRevision: request.Intent.Intent.Revision, TargetFingerprint: request.Intent.Intent.SpecFingerprint, WriterEpoch: request.Writer.Epoch}
}

// CreateDeployment never adopts by name or retries a mutation in the SDK. The
// exact desired body and independent pending journal verification are required.
// Gate safety is limited to this Client's local serialized parent lifecycle;
// forcible finalizer removal/bypass writers/distributed execution are excluded.
func (b *NamespaceBinding) CreateDeployment(ctx context.Context, request DeploymentCreate) (ownership.NamespacedRecord, error) {
	receipt, err := b.CreateDeploymentWithReceipt(ctx, request)
	return receipt.Record, err
}

// CreateDeploymentWithReceipt dispatches the same one POST as CreateDeployment,
// returning its exact validated native resourceVersion without a follow-up GET.
// The historical receipt establishes neither current health nor CAS authority.
func (b *NamespaceBinding) CreateDeploymentWithReceipt(ctx context.Context, request DeploymentCreate) (DeploymentCreation, error) {
	var result DeploymentCreation
	frozen, expected, err := b.freezeDeployment(request.Desired, request.Intent)
	if err != nil {
		return result, err
	}
	request.Desired = frozen
	if request.Verifier == nil || request.WriteID == "" {
		return result, deploymentFailure(DeploymentIntentUnverified)
	}
	err = b.client.lifecycle.run(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, deploymentTombstone(request), func() error {
		parent, namespace, err := b.refreshState(ctx, false)
		if err != nil {
			return err
		}
		if !contains(namespace.Finalizers, NamespaceLifecycleFinalizer) || b.attemptID != b.parent.CreationOperationID {
			return failure(ParentUnprotected, nil)
		}
		if _, err = b.bound.AuthorizeCreate(request.Intent, parent, request.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		b.stampDeployment(frozen, request.Intent, request.Writer)
		b.stampDeployment(expected, request.Intent, request.Writer)
		if err = request.Verifier.VerifyDeploymentIntent(ctx, b.deploymentWrite(request)); err != nil {
			return deploymentFailure(DeploymentIntentUnverified)
		}
		// The verifier may wait on durable state. Do not dispatch an expired writer.
		if _, err = b.bound.AuthorizeCreate(request.Intent, parent, request.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		created, err := b.client.createDeployment(call, frozen)
		if err != nil {
			return mutationError(err)
		} // AlreadyExists is rejected; no GET/adoption fallback.
		observation, err := b.deploymentObservation(created)
		if err != nil || !deploymentMetadataMatches(created, expected) || !sameDeploymentJSON(created.Spec, expected.Spec) || created.DeletionTimestamp != nil {
			return failure(UnknownWriteOutcome, nil)
		}
		result.Record, err = b.bound.BindCreated(request.Intent, parent, observation, request.Writer, time.Now())
		if err != nil {
			return failure(UnknownWriteOutcome, nil)
		}
		result.CreationResourceVersion = created.ResourceVersion
		return nil
	})
	if err != nil {
		return DeploymentCreation{}, err
	}
	return result, nil
}

func (c *Client) createDeployment(ctx context.Context, object *appsv1.Deployment) (*appsv1.Deployment, error) {
	result := &appsv1.Deployment{}
	err := c.apps.RESTClient().Post().Namespace(object.Namespace).Resource("deployments").VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(object).MaxRetries(0).Do(ctx).Into(result)
	return result, err
}

// ObserveDeployment accepts the original desired body, not just its copied
// fingerprint annotation. It verifies exact parent/UID/provenance/revision and
// the full defaulted spec before returning any readiness view. A read does not
// clear unknown create tombstones, adopt missing objects, or grant another write.
func (b *NamespaceBinding) ObserveDeployment(ctx context.Context, request DeploymentRead) (DeploymentSnapshot, error) {
	_, expected, err := b.freezeDeployment(request.Desired, request.Intent)
	if err != nil {
		return DeploymentSnapshot{}, err
	}
	r := request.Record
	if r.Parent != b.bound.Parent() || r.Record.Ref.Scope != b.bound.Scope() || r.Record.Ref.Purpose != "deployment" || r.Record.Ref.ProviderID == "" || r.Record.Ref.Name != expected.Name || r.Record.Owner != b.bound.Owner() || r.Record.CreationOperationID != request.Intent.Intent.OperationID || r.Record.CreationIdempotencyKey != request.Intent.Intent.IdempotencyKey || r.Record.Revision != request.Intent.Intent.Revision || r.Record.SpecFingerprint != request.Intent.Intent.SpecFingerprint || r.Record.WriterEpoch == 0 {
		return DeploymentSnapshot{}, failure(OwnershipRejected, nil)
	}
	if _, err = b.Refresh(ctx); err != nil {
		return DeploymentSnapshot{}, err
	}
	call, cancel := context.WithTimeout(ctx, b.client.timeout)
	defer cancel()
	actual, err := b.client.apps.Deployments(b.bound.NamespaceName()).Get(call, r.Record.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return DeploymentSnapshot{}, failure(ChildUnavailable, err)
	}
	observation, err := b.deploymentObservation(actual)
	if err != nil {
		return DeploymentSnapshot{}, err
	}
	o := observation.Observation
	if o.Ref != r.Record.Ref || o.Owner != r.Record.Owner || o.CreationIdempotencyKey != r.Record.CreationIdempotencyKey || o.Revision != r.Record.Revision || o.SpecFingerprint != r.Record.SpecFingerprint || o.WriterEpoch != r.Record.WriterEpoch {
		return DeploymentSnapshot{}, failure(OwnershipRejected, nil)
	}
	b.stampDeployment(expected, request.Intent, ownership.Writer{Epoch: r.Record.WriterEpoch})
	if !deploymentMetadataMatches(actual, expected) || !sameDeploymentJSON(actual.Spec, expected.Spec) {
		return DeploymentSnapshot{}, deploymentFailure(DeploymentSpecMismatch)
	}
	view := DeploymentView{Version: "deployment-view-v1", Namespace: b.bound.NamespaceName(), NamespaceUID: b.bound.Parent().UID, Name: actual.Name, UID: string(actual.UID), ResourceVersion: actual.ResourceVersion, Revision: o.Revision, Fingerprint: o.SpecFingerprint, Readiness: deploymentReadiness(actual)}
	return DeploymentSnapshot{&deploymentSnapshotData{object: actual.DeepCopy(), observation: observation, view: view}}, nil
}

// No confirm-create capability exists yet: ambiguous creates retain the exact
// tombstone and namespace. A future journal-backed receipt recovery must prove
// that local operation and full provider outcome, never adopt by name alone.
