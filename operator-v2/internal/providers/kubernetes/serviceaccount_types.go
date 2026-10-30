package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
)

const (
	ServiceAccountUnsupportedSpec  Code = "kubernetes_serviceaccount_unsupported_spec"
	ServiceAccountSpecMismatch     Code = "kubernetes_serviceaccount_spec_mismatch"
	ServiceAccountIntentUnverified Code = "kubernetes_serviceaccount_intent_unverified"
)

func serviceAccountFailure(code Code) error {
	return &Error{Code: code, Recovery: map[Code]string{
		ServiceAccountUnsupportedSpec:  "Use the reviewed explicit automount-disabled ServiceAccount baseline without secret references, identity annotations or finalizers.",
		ServiceAccountSpecMismatch:     "Retain the namespace and compare the canonical ServiceAccount against its original accepted body; copied fingerprint annotations cannot prove the live policy.",
		ServiceAccountIntentUnverified: "Independently verify the exact acknowledged active Pending ServiceAccount create before dispatch.",
	}[code]}
}

// ServiceAccountWriteIntent is metadata constructed by the provider, not proof
// of its own persistence. The actor must serialize the whole callback; a check
// alone does not fence distributed or previously dispatched provider writes.
type ServiceAccountWriteIntent struct {
	Owner                                          ownership.Owner
	Parent                                         ownership.NamespaceParent
	OperationID, ProviderOperationID               string
	Name, Revision, Fingerprint, WriteID, WriterID string
	WriterEpoch                                    uint64
}
type ServiceAccountIntentVerifier interface {
	VerifyServiceAccountIntent(context.Context, ServiceAccountWriteIntent) error
}

// Requests are private execution config, not workflow history. The caller must
// not mutate Desired during its call-entry copy; later callbacks cannot change
// the frozen dispatch. Synthetic verifiers prove no real journal admission.
type ServiceAccountCreate struct {
	Desired  *corev1.ServiceAccount
	Intent   ownership.NamespacedIntent
	Writer   ownership.Writer
	WriteID  string
	Verifier ServiceAccountIntentVerifier
}

func (ServiceAccountCreate) String() string               { return "ServiceAccountCreate{execution-only}" }
func (r ServiceAccountCreate) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (ServiceAccountCreate) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

// CreationResourceVersion is the original validated POST version, not current
// CAS authority, durable ACK or a statement about future Pod admission. Persist
// this canonical receipt before any subsequent observation.
type ServiceAccountCreation struct {
	Record                  ownership.NamespacedRecord
	CreationResourceVersion string
}
type ServiceAccountRead struct {
	Desired *corev1.ServiceAccount
	Intent  ownership.NamespacedIntent
	Record  ownership.NamespacedRecord
}

func (ServiceAccountRead) String() string               { return "ServiceAccountRead{execution-only}" }
func (r ServiceAccountRead) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (ServiceAccountRead) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

func ServiceAccountFingerprint(object *corev1.ServiceAccount) (string, error) {
	if object == nil {
		return "", failure(InvalidConfiguration, nil)
	}
	return serviceDigest(object)
}

type ServiceAccountView struct {
	Version                                                                    string `json:"version"`
	Namespace, NamespaceUID, Name, UID, ResourceVersion, Revision, Fingerprint string
	ObservedMinimalPolicy                                                      bool `json:"observedMinimalPolicy"`
}

// The opaque snapshot never serializes raw annotations, Secret names or SDK
// bodies. A policy observation covers this read only, not credential absence,
// future lifetime, Pod payload, execution identity or readiness.
type ServiceAccountSnapshot struct{ private *serviceAccountSnapshotData }
type serviceAccountSnapshotData struct {
	object      *corev1.ServiceAccount
	observation ownership.NamespacedObservation
	view        ServiceAccountView
}

func (s ServiceAccountSnapshot) View() ServiceAccountView {
	if s.private == nil {
		return ServiceAccountView{Version: "serviceaccount-view-v1"}
	}
	return s.private.view
}
func (s ServiceAccountSnapshot) Observation() ownership.NamespacedObservation {
	if s.private == nil {
		return ownership.NamespacedObservation{}
	}
	return s.private.observation
}

// ObjectForProvider returns an explicit private copy, never a durable DTO.
func (s ServiceAccountSnapshot) ObjectForProvider() *corev1.ServiceAccount {
	if s.private == nil {
		return nil
	}
	return s.private.object.DeepCopy()
}
func (s ServiceAccountSnapshot) MarshalJSON() ([]byte, error) { return json.Marshal(s.View()) }
func (s ServiceAccountSnapshot) String() string               { data, _ := s.MarshalJSON(); return string(data) }
func (s ServiceAccountSnapshot) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, s.String()) }
