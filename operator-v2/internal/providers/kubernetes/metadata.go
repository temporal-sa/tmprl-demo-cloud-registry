package kubernetes

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	OwnerAnnotation             = "registry.tmprl-demo.cloud/owner"
	PurposeAnnotation           = "registry.tmprl-demo.cloud/purpose"
	CreationOperationAnnotation = "registry.tmprl-demo.cloud/creation-operation"
	CreationKeyAnnotation       = "registry.tmprl-demo.cloud/creation-key"
	RevisionAnnotation          = "registry.tmprl-demo.cloud/revision"
	FingerprintAnnotation       = "registry.tmprl-demo.cloud/spec-fingerprint"
	WriterEpochAnnotation       = "registry.tmprl-demo.cloud/writer-epoch"
	AttemptAnnotation           = "registry.tmprl-demo.cloud/namespace-attempt"
)

// ClassifyScope is independent of a project-supplied Namespace field. Initial
// capabilities intentionally support only reviewed core/v1 Namespace/ConfigMap;
// extensions, alternate API versions and additional resource kinds are closed.
func ClassifyScope(gvk schema.GroupVersionKind) (namespaced bool, err error) {
	if gvk.Group != "" || gvk.Version != "v1" {
		return false, failure(UnsupportedKind, nil)
	}
	switch gvk.Kind {
	case "Namespace":
		return false, nil
	case "ConfigMap":
		return true, nil
	default:
		return false, failure(UnsupportedKind, nil)
	}
}

// CreationAnnotations is metadata encoding, not authorization. Bootstrap and
// child APIs must still authorize the intent and enforce their lifecycle gate.
func CreationAnnotations(intent ownership.Intent, writer ownership.Writer) map[string]string {
	owner, _ := json.Marshal(intent.Owner)
	return map[string]string{OwnerAnnotation: string(owner), PurposeAnnotation: intent.Purpose, CreationOperationAnnotation: intent.OperationID, CreationKeyAnnotation: intent.IdempotencyKey, RevisionAnnotation: intent.Revision, FingerprintAnnotation: intent.SpecFingerprint, WriterEpochAnnotation: strconv.FormatUint(writer.Epoch, 10)}
}

func (c *Client) observeMetadata(meta metav1.Object, namespace string) (ownership.Observation, error) {
	annotations := meta.GetAnnotations()
	var owner ownership.Owner
	decoder := json.NewDecoder(bytes.NewBufferString(annotations[OwnerAnnotation]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&owner); err != nil {
		return ownership.Observation{}, failure(OwnershipRejected, nil)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ownership.Observation{}, failure(OwnershipRejected, nil)
	}
	epoch, err := strconv.ParseUint(annotations[WriterEpochAnnotation], 10, 64)
	if err != nil || epoch == 0 || meta.GetUID() == "" || meta.GetResourceVersion() == "" || annotations[PurposeAnnotation] == "" || annotations[CreationOperationAnnotation] == "" || annotations[CreationKeyAnnotation] == "" || annotations[RevisionAnnotation] == "" || annotations[FingerprintAnnotation] == "" {
		return ownership.Observation{}, failure(OwnershipRejected, nil)
	}
	return ownership.Observation{Ref: ownership.ResourceRef{Scope: ownership.Scope{Provider: ownership.Kubernetes, ClusterID: c.identity.ID, Namespace: namespace}, Purpose: annotations[PurposeAnnotation], Name: meta.GetName(), ProviderID: string(meta.GetUID())}, Owner: owner, CreationIdempotencyKey: annotations[CreationKeyAnnotation], Revision: annotations[RevisionAnnotation], SpecFingerprint: annotations[FingerprintAnnotation], WriterEpoch: epoch, Version: meta.GetResourceVersion()}, nil
}
