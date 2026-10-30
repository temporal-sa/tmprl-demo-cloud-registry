package runtimeconfig

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func writeID(kind, attempt, name string) string {
	encoded, _ := json.Marshal([]string{kind, attempt, name})
	sum := sha256.Sum256(encoded)
	return kind + "-" + hex.EncodeToString(sum[:])
}
func metadataMatches(observed kube.ConfigMapSnapshot, write journal.Write) bool {
	o, p := observed.Observation.Observation, write.Preconditions
	return observed.Object != nil && observed.Object.DeletionTimestamp == nil && o.Ref.ProviderID == write.ResultUID && o.Revision == p.TargetRevision && o.SpecFingerprint == p.Fingerprint && o.WriterEpoch == write.WriterEpoch && observed.Object.Annotations[kube.OperationAnnotation] == p.ProviderOperationID
}

// Every enabled desired object already carries these renderer annotations.
// Knowing that pre-stamp shape lets recovery independently hash the complete
// actual body without trusting a copied fingerprint annotation as evidence.
func renderedShape(object *corev1.ConfigMap, intent ownership.NamespacedIntent, writer ownership.Writer) bool {
	expected := kube.CreationAnnotations(intent.Intent, writer)
	for key, value := range map[string]string{kube.OwnerAnnotation: expected[kube.OwnerAnnotation], ownership.ParentNamespaceUIDAnnotation: intent.Parent.UID, kube.RevisionAnnotation: intent.Intent.Revision, kube.OperationAnnotation: intent.Intent.OperationID} {
		if actual, exists := object.Annotations[key]; !exists || actual != value {
			return false
		}
	}
	return true
}
func bodyMatchesStored(snapshot kube.ConfigMapSnapshot, fingerprint string) bool {
	if snapshot.Object == nil || snapshot.Object.Name != "platform-project-metadata" || snapshot.Object.DeletionTimestamp != nil {
		return false
	}
	object := snapshot.Object.DeepCopy()
	annotations := make(map[string]string, len(object.Annotations))
	for key, value := range object.Annotations {
		annotations[key] = value
	}
	for _, key := range []string{kube.PurposeAnnotation, kube.CreationOperationAnnotation, kube.CreationKeyAnnotation, kube.FingerprintAnnotation, kube.WriterEpochAnnotation} {
		delete(annotations, key)
	}
	object.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
	object.ObjectMeta = metav1.ObjectMeta{Name: object.Name, Namespace: object.Namespace, Labels: object.Labels, Annotations: annotations}
	actual, err := kube.ConfigMapFingerprint(object)
	return err == nil && actual == fingerprint
}
func (w *Worker) configRecord(binding *kube.NamespaceBinding, write journal.Write) (ownership.NamespacedRecord, error) {
	p := write.Preconditions
	if p.Kind != "ConfigMap" || p.Action != "Create" || p.ParentUID != binding.Parent().UID || write.State != journal.Committed || write.WriterEpoch != w.snapshot.Envelope.WriterEpoch {
		return ownership.NamespacedRecord{}, failure(Changed)
	}
	intent, err := binding.Context().Intent("configmap", p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
	if err != nil {
		return ownership.NamespacedRecord{}, failure(Invalid)
	}
	return ownership.NamespacedRecord{Parent: binding.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Intent.Scope, Purpose: "configmap", Name: p.Name, ProviderID: write.ResultUID}, Owner: intent.Intent.Owner, CreationOperationID: intent.Intent.OperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: p.TargetRevision, SpecFingerprint: p.Fingerprint, WriterEpoch: write.WriterEpoch}}, nil
}
func fullDesiredMatches(snapshot kube.ConfigMapSnapshot, desired *corev1.ConfigMap, intent ownership.NamespacedIntent, writer ownership.Writer) bool {
	if snapshot.Object == nil {
		return false
	}
	expected := desired.DeepCopy()
	if expected.Annotations == nil {
		expected.Annotations = map[string]string{}
	}
	for key, value := range kube.CreationAnnotations(intent.Intent, writer) {
		expected.Annotations[key] = value
	}
	expected.Annotations[kube.OperationAnnotation] = intent.Intent.OperationID
	expected.Annotations[ownership.ParentNamespaceUIDAnnotation] = intent.Parent.UID
	expected.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: intent.Parent.Name, UID: types.UID(intent.Parent.UID)}}
	actual := snapshot.Object
	return actual.DeletionTimestamp == nil && actual.Name == expected.Name && actual.Namespace == expected.Namespace && reflect.DeepEqual(actual.Data, expected.Data) && reflect.DeepEqual(actual.BinaryData, expected.BinaryData) && reflect.DeepEqual(actual.Immutable, expected.Immutable) && reflect.DeepEqual(actual.Labels, expected.Labels) && reflect.DeepEqual(actual.Annotations, expected.Annotations) && reflect.DeepEqual(actual.OwnerReferences, expected.OwnerReferences)
}

// ApplyConfigMap applies exactly the renderer's project metadata ConfigMap.
// Other bundle kinds and ConfigMap purposes/names are not enabled in this slice.
// A committed replay observes the canonical UID and desired contents; it does
// not POST again or adopt a same-name replacement, even with copied metadata.
func (s *Session) ApplyConfigMap(ctx context.Context, desired renderer.DesiredObject) (Receipt, error) {
	w := s.worker
	w.mu.Lock()
	defer w.mu.Unlock()
	var receipt Receipt
	if err := w.current(ctx); err != nil {
		return receipt, err
	}
	if err := w.normalWorkAllowed(); err != nil {
		return receipt, err
	}
	object, ok := desired.Object.(*corev1.ConfigMap)
	if !ok || object == nil || object.Name != "platform-project-metadata" || desired.Intent.Parent != s.binding.Parent() || desired.Intent.Intent.Owner != w.config.Project.Owner() || desired.Intent.Intent.Scope != s.binding.Context().Scope() {
		return receipt, failure(Invalid)
	}
	fingerprint, err := kube.ConfigMapFingerprint(object)
	if err != nil || fingerprint != desired.Intent.Intent.SpecFingerprint {
		return receipt, failure(Invalid)
	}
	var attempt *journal.Attempt
	for _, a := range w.snapshot.Envelope.Attempts {
		if a.OperationID == s.attempt {
			copy := a
			attempt = &copy
		}
	}
	if attempt == nil || attempt.State != journal.Observed || attempt.NamespaceUID != s.binding.Parent().UID {
		return receipt, failure(Consumed)
	}
	if desired.Intent.Intent.Revision != attempt.Revision || desired.Intent.Intent.OperationID != attempt.OperationID || desired.Intent.Intent.Purpose != "configmap" {
		return receipt, failure(Changed)
	}
	pending := journal.Write{ID: writeID("configmap", attempt.OperationID, object.Name), OperationID: attempt.OperationID, WriterEpoch: w.snapshot.Envelope.WriterEpoch, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "ConfigMap", Action: "Create", Name: object.Name, ParentUID: attempt.NamespaceUID, Fingerprint: fingerprint, TargetRevision: attempt.Revision, ProviderOperationID: desired.Intent.Intent.OperationID}}
	writer, err := w.dispatchWriter()
	if err != nil {
		return receipt, err
	}
	if !renderedShape(object, desired.Intent, writer) {
		return receipt, failure(Invalid)
	}
	for _, write := range w.snapshot.Envelope.Writes {
		if write.ID != pending.ID {
			continue
		}
		if write.Preconditions != pending.Preconditions || write.OperationID != pending.OperationID || write.State != journal.Committed {
			return receipt, failure(Changed)
		}
		record, err := w.configRecord(s.binding, write)
		if err != nil {
			return receipt, err
		}
		snapshot, err := s.binding.ObserveConfigMap(ctx, record)
		if err != nil {
			return receipt, classify(err)
		}
		if !metadataMatches(snapshot, write) || !fullDesiredMatches(snapshot, object, desired.Intent, writer) {
			return receipt, failure(Changed)
		}
		return Receipt{Subject: w.config.Cell.Identity().Subject, AttemptID: attempt.OperationID, Record: record, ResourceVersion: snapshot.Object.ResourceVersion, JournalVersion: w.snapshot.Version}, nil
	}
	next := clone(w.snapshot.Envelope)
	next.Writes = append(next.Writes, pending)
	index := len(next.Writes) - 1
	if err := w.commit(ctx, next); err != nil {
		return receipt, err
	}
	writer, err = w.dispatchWriter()
	if err != nil {
		return receipt, w.retainUndispatched(ctx, index, err)
	}
	record, err := s.binding.CreateConfigMap(ctx, kube.ConfigMapCreate{Desired: object, Intent: desired.Intent, Writer: writer})
	if err != nil {
		return receipt, w.retainFailure(ctx, index, err)
	}
	snapshot, err := s.binding.ObserveConfigMap(ctx, record)
	if err != nil || !fullDesiredMatches(snapshot, object, desired.Intent, writer) {
		return receipt, w.retainObservationFailure(ctx, index, err)
	}
	if err := w.finishWrite(ctx, index, journal.Committed, record.Record.Ref.ProviderID, snapshot.Object.ResourceVersion, false); err != nil {
		return receipt, err
	}
	return Receipt{Subject: w.config.Cell.Identity().Subject, AttemptID: attempt.OperationID, Record: record, ResourceVersion: snapshot.Object.ResourceVersion, JournalVersion: w.snapshot.Version}, nil
}
