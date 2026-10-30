package kubernetes

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
)

func (b *NamespaceBinding) freezeService(object *corev1.Service, intent ownership.NamespacedIntent) (*corev1.Service, *corev1.Service, error) {
	if b == nil || b.client == nil || b.client.core == nil || object == nil {
		return nil, nil, failure(InvalidConfiguration, nil)
	}
	// Freeze before validation, verifier callbacks or any queued lifecycle work.
	d := object.DeepCopy()
	if d.APIVersion != "v1" || d.Kind != "Service" || d.Namespace != b.bound.NamespaceName() || d.Name != intent.Intent.Name || intent.Intent.Purpose != "service" || intent.Intent.Revision != b.bound.ParentRevision() || intent.Parent != b.bound.Parent() {
		return nil, nil, failure(OwnershipRejected, nil)
	}
	allowed := metav1.ObjectMeta{Name: d.Name, Namespace: d.Namespace, Labels: d.Labels, Annotations: d.Annotations}
	if !sameDeploymentJSON(allowed, d.ObjectMeta) || !sameDeploymentJSON(d.Status, corev1.ServiceStatus{}) {
		return nil, nil, failure(InvalidConfiguration, nil)
	}
	fingerprint, err := ServiceFingerprint(d)
	if err != nil || fingerprint != intent.Intent.SpecFingerprint {
		return nil, nil, failure(OwnershipRejected, nil)
	}
	expected := CreationAnnotations(intent.Intent, ownership.Writer{})
	for _, key := range []string{PurposeAnnotation, CreationOperationAnnotation, CreationKeyAnnotation, FingerprintAnnotation, WriterEpochAnnotation, AttemptAnnotation} {
		if _, exists := d.Annotations[key]; exists {
			return nil, nil, failure(OwnershipRejected, nil)
		}
	}
	for key, value := range map[string]string{OwnerAnnotation: expected[OwnerAnnotation], RevisionAnnotation: intent.Intent.Revision, OperationAnnotation: intent.Intent.OperationID, ownership.ParentNamespaceUIDAnnotation: b.bound.Parent().UID} {
		if actual, exists := d.Annotations[key]; exists && actual != value {
			return nil, nil, failure(OwnershipRejected, nil)
		}
	}
	normalized, err := serviceBaseline(d)
	if err != nil {
		return nil, nil, err
	}
	return d, normalized, nil
}

func (b *NamespaceBinding) stampService(d *corev1.Service, intent ownership.NamespacedIntent, writer ownership.Writer) {
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

func (b *NamespaceBinding) serviceObservation(d *corev1.Service) (ownership.NamespacedObservation, error) {
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
	if o.Ref.Purpose != "service" || o.Owner != b.bound.Owner() {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	return ownership.NamespacedObservation{Observation: o, ParentUID: b.bound.Parent().UID}, nil
}

func (b *NamespaceBinding) serviceWrite(request ServiceCreate) ServiceWriteIntent {
	return ServiceWriteIntent{Owner: b.bound.Owner(), Parent: b.bound.Parent(), OperationID: b.attemptID, ProviderOperationID: request.Intent.Intent.OperationID, Name: request.Desired.Name, Revision: request.Intent.Intent.Revision, Fingerprint: request.Intent.Intent.SpecFingerprint, WriteID: request.WriteID, WriterID: request.Writer.ID, WriterEpoch: request.Writer.Epoch}
}
func serviceTombstone(request ServiceCreate) OperationTombstone {
	return OperationTombstone{ID: request.Intent.Intent.OperationID, Action: "create-service", Name: request.Desired.Name, TargetRevision: request.Intent.Intent.Revision, TargetFingerprint: request.Intent.Intent.SpecFingerprint, WriterEpoch: request.Writer.Epoch}
}

// CreateService never adopts by name or retries a mutation in the SDK. The
// exact desired body and independent pending journal verification are required.
// Gate safety is limited to this Client's local serialized parent lifecycle;
// forcible finalizer removal/bypass writers/distributed execution are excluded.
func (b *NamespaceBinding) CreateService(ctx context.Context, request ServiceCreate) (ServiceRecord, error) {
	var result ServiceRecord
	frozen, expected, err := b.freezeService(request.Desired, request.Intent)
	if err != nil {
		return result, err
	}
	request.Desired = frozen
	if request.Verifier == nil || request.WriteID == "" {
		return result, serviceFailure(ServiceIntentUnverified)
	}
	err = b.client.lifecycle.run(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, serviceTombstone(request), func() error {
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
		b.stampService(frozen, request.Intent, request.Writer)
		b.stampService(expected, request.Intent, request.Writer)
		if err = request.Verifier.VerifyServiceIntent(ctx, b.serviceWrite(request)); err != nil {
			return serviceFailure(ServiceIntentUnverified)
		}
		// The verifier may wait on durable state. Do not dispatch an expired writer.
		if _, err = b.bound.AuthorizeCreate(request.Intent, parent, request.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		created, err := b.client.createService(call, frozen)
		if err != nil {
			return mutationError(err)
		} // AlreadyExists is rejected; no GET/adoption fallback.
		observation, err := b.serviceObservation(created)
		if err != nil || !serviceMetadataMatches(created, expected) || !serviceAllocatedMatches(created, expected) || created.DeletionTimestamp != nil {
			return failure(UnknownWriteOutcome, nil)
		}
		result.Resource, err = b.bound.BindCreated(request.Intent, parent, observation, request.Writer, time.Now())
		if err != nil {
			return failure(UnknownWriteOutcome, nil)
		}
		result.CreationResourceVersion = created.ResourceVersion
		result.SpecContract = ServiceSpecContract
		result.AllocatedSpecDigest, err = serviceDigest(created.Spec)
		if err != nil {
			return failure(UnknownWriteOutcome, nil)
		}
		return nil
	})
	if err != nil {
		return ServiceRecord{}, err
	}
	return result, nil
}

func (c *Client) createService(ctx context.Context, object *corev1.Service) (*corev1.Service, error) {
	result := &corev1.Service{}
	err := c.core.RESTClient().Post().Namespace(object.Namespace).Resource("services").VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(object).MaxRetries(0).Do(ctx).Into(result)
	return result, err
}

// ObserveService accepts the original desired body, not just its copied
// fingerprint annotation. It verifies exact parent/UID/provenance/revision and
// the full defaulted spec and allocation receipt before returning a safe view. A read does not
// clear unknown create tombstones, adopt missing objects, or grant another write.
func (b *NamespaceBinding) ObserveService(ctx context.Context, request ServiceRead) (ServiceSnapshot, error) {
	_, expected, err := b.freezeService(request.Desired, request.Intent)
	if err != nil {
		return ServiceSnapshot{}, err
	}
	receipt := request.Record
	if receipt.SpecContract != ServiceSpecContract || !serviceReceiptDigest.MatchString(receipt.AllocatedSpecDigest) {
		return ServiceSnapshot{}, serviceFailure(ServiceIntentUnverified)
	}
	r := receipt.Resource
	if r.Parent != b.bound.Parent() || r.Record.Ref.Scope != b.bound.Scope() || r.Record.Ref.Purpose != "service" || r.Record.Ref.ProviderID == "" || r.Record.Ref.Name != expected.Name || r.Record.Owner != b.bound.Owner() || r.Record.CreationOperationID != request.Intent.Intent.OperationID || r.Record.CreationIdempotencyKey != request.Intent.Intent.IdempotencyKey || r.Record.Revision != request.Intent.Intent.Revision || r.Record.SpecFingerprint != request.Intent.Intent.SpecFingerprint || r.Record.WriterEpoch == 0 {
		return ServiceSnapshot{}, failure(OwnershipRejected, nil)
	}
	if _, err = b.Refresh(ctx); err != nil {
		return ServiceSnapshot{}, err
	}
	call, cancel := context.WithTimeout(ctx, b.client.timeout)
	defer cancel()
	actual, err := b.client.core.Services(b.bound.NamespaceName()).Get(call, r.Record.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return ServiceSnapshot{}, failure(ChildUnavailable, err)
	}
	observation, err := b.serviceObservation(actual)
	if err != nil {
		return ServiceSnapshot{}, err
	}
	o := observation.Observation
	if o.Ref != r.Record.Ref || o.Owner != r.Record.Owner || o.CreationIdempotencyKey != r.Record.CreationIdempotencyKey || o.Revision != r.Record.Revision || o.SpecFingerprint != r.Record.SpecFingerprint || o.WriterEpoch != r.Record.WriterEpoch {
		return ServiceSnapshot{}, failure(OwnershipRejected, nil)
	}
	b.stampService(expected, request.Intent, ownership.Writer{Epoch: r.Record.WriterEpoch})
	if !serviceMetadataMatches(actual, expected) || !serviceAllocatedMatches(actual, expected) {
		return ServiceSnapshot{}, serviceFailure(ServiceSpecMismatch)
	}
	digest, err := serviceDigest(actual.Spec)
	if err != nil || digest != receipt.AllocatedSpecDigest {
		return ServiceSnapshot{}, serviceFailure(ServiceSpecMismatch)
	}
	view := ServiceView{Version: "service-view-v1", Namespace: b.bound.NamespaceName(), NamespaceUID: b.bound.Parent().UID, Name: actual.Name, UID: string(actual.UID), ResourceVersion: actual.ResourceVersion, Revision: o.Revision, Fingerprint: o.SpecFingerprint, SpecContract: ServiceSpecContract, AllocatedSpecDigest: digest, ObservedAllocated: true, BackendReadiness: "NotEvaluated"}
	return ServiceSnapshot{&serviceSnapshotData{object: actual.DeepCopy(), observation: observation, view: view}}, nil
}

// No confirm-create capability exists yet: ambiguous creates retain the exact
// tombstone and namespace. A future journal-backed receipt recovery must prove
// that local operation and full provider outcome, never adopt by name alone.
