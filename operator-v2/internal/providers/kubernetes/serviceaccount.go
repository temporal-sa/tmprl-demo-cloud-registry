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

func (b *NamespaceBinding) freezeServiceAccount(object *corev1.ServiceAccount, intent ownership.NamespacedIntent) (*corev1.ServiceAccount, error) {
	if b == nil || b.client == nil || b.client.core == nil || object == nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	d := object.DeepCopy()
	if d.APIVersion != "v1" || d.Kind != "ServiceAccount" || d.Namespace != b.bound.NamespaceName() || d.Name != intent.Intent.Name || intent.Intent.Purpose != "service-account" || intent.Intent.Revision != b.bound.ParentRevision() || intent.Parent != b.bound.Parent() {
		return nil, failure(OwnershipRejected, nil)
	}
	allowed := metav1.ObjectMeta{Name: d.Name, Namespace: d.Namespace, Labels: d.Labels, Annotations: d.Annotations}
	if !sameDeploymentJSON(allowed, d.ObjectMeta) {
		return nil, serviceAccountFailure(ServiceAccountUnsupportedSpec)
	}
	fingerprint, err := ServiceAccountFingerprint(d)
	if err != nil || fingerprint != intent.Intent.SpecFingerprint {
		return nil, failure(OwnershipRejected, nil)
	}
	expected := map[string]string{OwnerAnnotation: CreationAnnotations(intent.Intent, ownership.Writer{})[OwnerAnnotation], RevisionAnnotation: intent.Intent.Revision, OperationAnnotation: intent.Intent.OperationID, ownership.ParentNamespaceUIDAnnotation: b.bound.Parent().UID}
	for key, value := range d.Annotations {
		if want, ok := expected[key]; !ok || want != value {
			return nil, serviceAccountFailure(ServiceAccountUnsupportedSpec)
		}
	}
	return serviceAccountBaseline(d)
}

func (b *NamespaceBinding) stampServiceAccount(d *corev1.ServiceAccount, intent ownership.NamespacedIntent, writer ownership.Writer) {
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

func (b *NamespaceBinding) serviceAccountObservation(d *corev1.ServiceAccount) (ownership.NamespacedObservation, error) {
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
	if o.Ref.Purpose != "service-account" || o.Owner != b.bound.Owner() {
		return ownership.NamespacedObservation{}, failure(OwnershipRejected, nil)
	}
	return ownership.NamespacedObservation{Observation: o, ParentUID: b.bound.Parent().UID}, nil
}

func (b *NamespaceBinding) serviceAccountWrite(r ServiceAccountCreate) ServiceAccountWriteIntent {
	return ServiceAccountWriteIntent{Owner: b.bound.Owner(), Parent: b.bound.Parent(), OperationID: b.attemptID, ProviderOperationID: r.Intent.Intent.OperationID, Name: r.Desired.Name, Revision: r.Intent.Intent.Revision, Fingerprint: r.Intent.Intent.SpecFingerprint, WriteID: r.WriteID, WriterID: r.Writer.ID, WriterEpoch: r.Writer.Epoch}
}

// CreateServiceAccount performs one exact guarded POST, never adoption, SDK
// mutation retry or a follow-up GET to discover receipt identity/version. Only
// this Client's held local parent lifecycle is enforced; distributed execution
// and bypass/finalizer-removal writers are outside its capability.
func (b *NamespaceBinding) CreateServiceAccount(ctx context.Context, r ServiceAccountCreate) (ServiceAccountCreation, error) {
	var result ServiceAccountCreation
	frozen, err := b.freezeServiceAccount(r.Desired, r.Intent)
	if err != nil {
		return result, err
	}
	r.Desired = frozen
	if r.Verifier == nil || r.WriteID == "" {
		return result, serviceAccountFailure(ServiceAccountIntentUnverified)
	}
	tombstone := OperationTombstone{ID: r.Intent.Intent.OperationID, Action: "create-serviceaccount", Name: frozen.Name, TargetRevision: r.Intent.Intent.Revision, TargetFingerprint: r.Intent.Intent.SpecFingerprint, WriterEpoch: r.Writer.Epoch}
	err = b.client.lifecycle.run(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, tombstone, func() error {
		parent, namespace, err := b.refreshState(ctx, false)
		if err != nil {
			return err
		}
		if !contains(namespace.Finalizers, NamespaceLifecycleFinalizer) || b.attemptID != b.parent.CreationOperationID {
			return failure(ParentUnprotected, nil)
		}
		if _, err = b.bound.AuthorizeCreate(r.Intent, parent, r.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		b.stampServiceAccount(frozen, r.Intent, r.Writer)
		if err = r.Verifier.VerifyServiceAccountIntent(ctx, b.serviceAccountWrite(r)); err != nil {
			return serviceAccountFailure(ServiceAccountIntentUnverified)
		}
		if _, err = b.bound.AuthorizeCreate(r.Intent, parent, r.Writer, time.Now()); err != nil {
			return failure(OwnershipRejected, err)
		}
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		created := &corev1.ServiceAccount{}
		err = b.client.core.RESTClient().Post().Namespace(frozen.Namespace).Resource("serviceaccounts").VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(frozen).MaxRetries(0).Do(call).Into(created)
		if err != nil {
			return mutationError(err)
		}
		observation, err := b.serviceAccountObservation(created)
		if err != nil || !serviceAccountMetadataMatches(created, frozen) || !serviceAccountPolicyMatches(created, frozen) {
			return failure(UnknownWriteOutcome, nil)
		}
		result.Record, err = b.bound.BindCreated(r.Intent, parent, observation, r.Writer, time.Now())
		if err != nil {
			return failure(UnknownWriteOutcome, nil)
		}
		result.CreationResourceVersion = created.ResourceVersion
		return nil
	})
	if err != nil {
		return ServiceAccountCreation{}, err
	}
	return result, nil
}

// ObserveServiceAccount checks an already pinned canonical UID against its
// original body. It never adopts a name, learns creation receipt fields, clears
// unresolved tombstones or certifies future Pod admission/credential absence.
func (b *NamespaceBinding) ObserveServiceAccount(ctx context.Context, r ServiceAccountRead) (ServiceAccountSnapshot, error) {
	expected, err := b.freezeServiceAccount(r.Desired, r.Intent)
	if err != nil {
		return ServiceAccountSnapshot{}, err
	}
	record := r.Record
	if record.Parent != b.bound.Parent() || record.Record.Ref.Scope != b.bound.Scope() || record.Record.Ref.Purpose != "service-account" || record.Record.Ref.ProviderID == "" || record.Record.Ref.Name != expected.Name || record.Record.Owner != b.bound.Owner() || record.Record.CreationOperationID != r.Intent.Intent.OperationID || record.Record.CreationIdempotencyKey != r.Intent.Intent.IdempotencyKey || record.Record.Revision != r.Intent.Intent.Revision || record.Record.SpecFingerprint != r.Intent.Intent.SpecFingerprint || record.Record.WriterEpoch == 0 {
		return ServiceAccountSnapshot{}, failure(OwnershipRejected, nil)
	}
	if _, err = b.Refresh(ctx); err != nil {
		return ServiceAccountSnapshot{}, err
	}
	call, cancel := context.WithTimeout(ctx, b.client.timeout)
	defer cancel()
	actual, err := b.client.core.ServiceAccounts(b.bound.NamespaceName()).Get(call, record.Record.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return ServiceAccountSnapshot{}, failure(ChildUnavailable, err)
	}
	observation, err := b.serviceAccountObservation(actual)
	if err != nil {
		return ServiceAccountSnapshot{}, err
	}
	o := observation.Observation
	if o.Ref != record.Record.Ref || o.Owner != record.Record.Owner || o.CreationIdempotencyKey != record.Record.CreationIdempotencyKey || o.Revision != record.Record.Revision || o.SpecFingerprint != record.Record.SpecFingerprint || o.WriterEpoch != record.Record.WriterEpoch {
		return ServiceAccountSnapshot{}, failure(OwnershipRejected, nil)
	}
	b.stampServiceAccount(expected, r.Intent, ownership.Writer{Epoch: record.Record.WriterEpoch})
	if !serviceAccountMetadataMatches(actual, expected) || !serviceAccountPolicyMatches(actual, expected) {
		return ServiceAccountSnapshot{}, serviceAccountFailure(ServiceAccountSpecMismatch)
	}
	view := ServiceAccountView{Version: "serviceaccount-view-v1", Namespace: b.bound.NamespaceName(), NamespaceUID: b.bound.Parent().UID, Name: actual.Name, UID: string(actual.UID), ResourceVersion: actual.ResourceVersion, Revision: o.Revision, Fingerprint: o.SpecFingerprint, ObservedMinimalPolicy: true}
	return ServiceAccountSnapshot{&serviceAccountSnapshotData{object: actual.DeepCopy(), observation: observation, view: view}}, nil
}
