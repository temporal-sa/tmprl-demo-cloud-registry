package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	appsv1 "k8s.io/api/apps/v1"
)

const (
	DeploymentUnsupportedSpec  Code = "kubernetes_deployment_unsupported_spec"
	DeploymentSpecMismatch     Code = "kubernetes_deployment_spec_mismatch"
	DeploymentIntentUnverified Code = "kubernetes_deployment_intent_unverified"
)

func deploymentFailure(code Code) error {
	return &Error{Code: code, Recovery: map[Code]string{
		DeploymentUnsupportedSpec:  "Use the reviewed single-container Deployment baseline; unsupported fields or server defaults require a new verified capability.",
		DeploymentSpecMismatch:     "Retain the owned Deployment and reconcile its full desired spec; copied intent annotations do not prove the live body matches.",
		DeploymentIntentUnverified: "Persist and independently verify the exact pending Deployment write against the active admitted journal before dispatch.",
	}[code]}
}

// DeploymentWriteIntent contains only metadata. It is constructed from a frozen
// body and verified binding, never accepted as evidence of its own persistence.
type DeploymentWriteIntent struct {
	Owner                                          ownership.Owner
	Parent                                         ownership.NamespaceParent
	OperationID                                    string
	ProviderOperationID                            string
	Name, Revision, Fingerprint, WriteID, WriterID string
	WriterEpoch                                    uint64
}

// DeploymentIntentVerifier is an injected trusted capability seam, NOT proof
// that any DTO was persisted. Real composition must check an independently
// pinned Repository, active admission and its exact ACKed Pending write. It must
// reject foreign subject/parent/attempt/revision/epoch or changed write fields.
// Synthetic verifiers are valid only in explicitly labeled unit/proof fixtures.
type DeploymentIntentVerifier interface {
	VerifyDeploymentIntent(context.Context, DeploymentWriteIntent) error
}

// DeploymentCreate is execution-only. Callers must not mutate Desired during
// the call-entry copy. Subsequent caller changes cannot change dispatch. Raw
// bodies must never enter workflow history, diagnostics, logs or journal DTOs.
type DeploymentCreate struct {
	Desired  *appsv1.Deployment
	Intent   ownership.NamespacedIntent
	Writer   ownership.Writer
	WriteID  string
	Verifier DeploymentIntentVerifier
}

func (DeploymentCreate) String() string               { return "DeploymentCreate{execution-only}" }
func (r DeploymentCreate) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (DeploymentCreate) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

func DeploymentFingerprint(object *appsv1.Deployment) (string, error) {
	if object == nil {
		return "", failure(InvalidConfiguration, nil)
	}
	data, err := json.Marshal(object)
	if err != nil || len(data) > 512*1024 {
		return "", failure(InvalidConfiguration, nil)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// DeploymentCreation contains only the exact validated successful POST receipt.
// CreationResourceVersion is HISTORICAL response evidence; it is not a current
// CAS precondition, readiness observation, durable ACK or grant of mutation
// authority. Persist it with the original Pending operation before later reads.
type DeploymentCreation struct {
	Record                  ownership.NamespacedRecord
	CreationResourceVersion string
}

type DeploymentRead struct {
	Desired *appsv1.Deployment
	Intent  ownership.NamespacedIntent
	Record  ownership.NamespacedRecord
}

func (DeploymentRead) String() string               { return "DeploymentRead{execution-only}" }
func (r DeploymentRead) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (DeploymentRead) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

// Ready describes CURRENT Deployment availability at the observed generation,
// not descendant drain or teardown completion. An omitted optional terminating
// count is explicitly unknown and does not invalidate available new replicas in
// this create-only immutable per-attempt namespace. A positive count waits.
type DeploymentReadiness struct {
	State                                                     string `json:"state"`
	Code                                                      string `json:"code"`
	Generation                                                int64  `json:"generation"`
	ObservedGeneration                                        int64  `json:"observedGeneration"`
	Desired, Replicas, Updated, Ready, Available, Unavailable int32
	Terminating                                               int32 `json:"terminating"`
	TerminatingKnown                                          bool  `json:"terminatingKnown"`
	CollisionCount                                            int32 `json:"collisionCount"`
	CollisionKnown                                            bool  `json:"collisionKnown"`
}
type DeploymentView struct {
	Version                                                                    string `json:"version"`
	Namespace, NamespaceUID, Name, UID, ResourceVersion, Revision, Fingerprint string
	Readiness                                                                  DeploymentReadiness `json:"readiness"`
}

// DeploymentSnapshot protects the complete object behind an opaque handle.
// Default fmt/JSON expose only safe bounded metadata/readiness, never authored
// env, pod spec, condition reason/message, admission annotations or provider text.
type DeploymentSnapshot struct{ private *deploymentSnapshotData }
type deploymentSnapshotData struct {
	object      *appsv1.Deployment
	observation ownership.NamespacedObservation
	view        DeploymentView
}

func (s DeploymentSnapshot) View() DeploymentView {
	if s.private == nil {
		return DeploymentView{Version: "deployment-view-v1", Readiness: DeploymentReadiness{State: "Unknown", Code: "DeploymentUnverified"}}
	}
	return s.private.view
}
func (s DeploymentSnapshot) Observation() ownership.NamespacedObservation {
	if s.private == nil {
		return ownership.NamespacedObservation{}
	}
	return s.private.observation
}

// ObjectForProvider is an explicit private inspection boundary. Returned copy
// contains authored config and raw provider status; never serialize it durably.
func (s DeploymentSnapshot) ObjectForProvider() *appsv1.Deployment {
	if s.private == nil {
		return nil
	}
	return s.private.object.DeepCopy()
}
func (s DeploymentSnapshot) MarshalJSON() ([]byte, error) { return json.Marshal(s.View()) }
func (s DeploymentSnapshot) String() string               { data, _ := s.MarshalJSON(); return string(data) }
func (s DeploymentSnapshot) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, s.String()) }
