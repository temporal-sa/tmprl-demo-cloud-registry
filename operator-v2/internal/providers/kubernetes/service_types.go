package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
)

const (
	ServiceUnsupportedSpec  Code = "kubernetes_service_unsupported_spec"
	ServiceSpecMismatch     Code = "kubernetes_service_spec_mismatch"
	ServiceIntentUnverified Code = "kubernetes_service_intent_unverified"
	ServiceSpecContract          = "clusterip-service-v1-k8s1.37"
)

func serviceFailure(code Code) error {
	return &Error{Code: code, Recovery: map[Code]string{
		ServiceUnsupportedSpec:  "Use the reviewed selector-based single-stack ClusterIP TCP Service baseline; other features require a verified capability.",
		ServiceSpecMismatch:     "Retain the owned Service and compare the original desired body and durable verified creation receipt; never learn allocation provenance from a current GET.",
		ServiceIntentUnverified: "Verify the exact acknowledged active Pending Service intent and retain its validated canonical UID and allocated-spec receipt before reuse.",
	}[code]}
}

// ServiceWriteIntent is safe metadata, not evidence of persistence. A real
// verifier must independently read the private active admission/journal and
// match its exact acknowledged Pending write. The actor serializes the whole
// callback; this point-in-time check is not a distributed RPC fence.
type ServiceWriteIntent struct {
	Owner                                          ownership.Owner
	Parent                                         ownership.NamespaceParent
	OperationID, ProviderOperationID               string
	Name, Revision, Fingerprint, WriteID, WriterID string
	WriterEpoch                                    uint64
}
type ServiceIntentVerifier interface {
	VerifyServiceIntent(context.Context, ServiceWriteIntent) error
}
type ServiceCreate struct {
	Desired  *corev1.Service
	Intent   ownership.NamespacedIntent
	Writer   ownership.Writer
	WriteID  string
	Verifier ServiceIntentVerifier
}

func (ServiceCreate) String() string               { return "ServiceCreate{execution-only}" }
func (r ServiceCreate) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (ServiceCreate) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

// ServiceRecord is an integrity receipt from the exact validated successful
// POST. Real composition must independently persist/pin it with the canonical
// UID in the acknowledged journal. Supplied hashes do not authenticate callers.
// Missing receipts must never be reconstructed from a GET. Contract/digest are
// immutable once committed. The digest includes ALL defaulted spec fields and
// the allocated address, but no raw addresses need enter durable DTOs.
type ServiceRecord struct {
	Resource            ownership.NamespacedRecord
	SpecContract        string
	AllocatedSpecDigest string
	// Historical native POST evidence, not a current CAS precondition, durable
	// journal acknowledgement or current availability observation.
	CreationResourceVersion string
}
type ServiceRead struct {
	Desired *corev1.Service
	Intent  ownership.NamespacedIntent
	Record  ServiceRecord
}

func (ServiceRead) String() string               { return "ServiceRead{execution-only}" }
func (r ServiceRead) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (ServiceRead) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }
func ServiceFingerprint(object *corev1.Service) (string, error) {
	if object == nil {
		return "", failure(InvalidConfiguration, nil)
	}
	return serviceDigest(object)
}
func serviceDigest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil || len(data) > 512*1024 {
		return "", failure(InvalidConfiguration, nil)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type ServiceView struct {
	Version                                                                    string `json:"version"`
	Namespace, NamespaceUID, Name, UID, ResourceVersion, Revision, Fingerprint string
	SpecContract, AllocatedSpecDigest                                          string
	ObservedAllocated                                                          bool   `json:"observedAllocated"`
	BackendReadiness                                                           string `json:"backendReadiness"`
}

// Snapshot has no automatic raw selector, address, annotations or provider
// status output. Allocation observation is NOT endpoint/DNS/traffic readiness.
type ServiceSnapshot struct{ private *serviceSnapshotData }
type serviceSnapshotData struct {
	object      *corev1.Service
	observation ownership.NamespacedObservation
	view        ServiceView
}

func (s ServiceSnapshot) View() ServiceView {
	if s.private == nil {
		return ServiceView{Version: "service-view-v1", BackendReadiness: "NotEvaluated"}
	}
	return s.private.view
}
func (s ServiceSnapshot) Observation() ownership.NamespacedObservation {
	if s.private == nil {
		return ownership.NamespacedObservation{}
	}
	return s.private.observation
}
func (s ServiceSnapshot) ObjectForProvider() *corev1.Service {
	if s.private == nil {
		return nil
	}
	return s.private.object.DeepCopy()
}
func (s ServiceSnapshot) MarshalJSON() ([]byte, error) { return json.Marshal(s.View()) }
func (s ServiceSnapshot) String() string               { data, _ := s.MarshalJSON(); return string(data) }
func (s ServiceSnapshot) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, s.String()) }
