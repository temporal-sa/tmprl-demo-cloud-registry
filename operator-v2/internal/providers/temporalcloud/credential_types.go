package temporalcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/grpc"
)

type CredentialTransport interface {
	CreateServiceAccount(context.Context, *cloudservice.CreateServiceAccountRequest, ...grpc.CallOption) (*cloudservice.CreateServiceAccountResponse, error)
	GetServiceAccount(context.Context, *cloudservice.GetServiceAccountRequest, ...grpc.CallOption) (*cloudservice.GetServiceAccountResponse, error)
	CreateApiKey(context.Context, *cloudservice.CreateApiKeyRequest, ...grpc.CallOption) (*cloudservice.CreateApiKeyResponse, error)
	GetApiKey(context.Context, *cloudservice.GetApiKeyRequest, ...grpc.CallOption) (*cloudservice.GetApiKeyResponse, error)
	GetApiKeys(context.Context, *cloudservice.GetApiKeysRequest, ...grpc.CallOption) (*cloudservice.GetApiKeysResponse, error)
	DeleteApiKey(context.Context, *cloudservice.DeleteApiKeyRequest, ...grpc.CallOption) (*cloudservice.DeleteApiKeyResponse, error)
}

type CredentialCapabilities struct {
	CreateScopedServiceAccounts bool
	StageAPIKeys                bool
	VerifyCandidates            bool
	RevokeOwnedCandidates       bool
}

type CredentialConfig struct {
	FailurePolicy                 CredentialFailurePolicy
	Capabilities                  CredentialCapabilities
	MaximumKeyTTL                 time.Duration
	MaximumRecoveryPages          int
	StoreID                       string
	PreprovisionedServiceAccounts []ScopedServiceAccountBinding
}

type ScopedServiceAccountBinding struct {
	NamespaceID            string
	NamespaceIncarnationID string
	Owner                  ownership.Owner
	Reference              ServiceAccountRef
}

type CredentialIntent struct {
	// Zero preserves the original compensation-enabled intent contract.
	FailurePolicy          CredentialFailurePolicy `json:",omitempty"`
	SlotID                 string
	StageID                string
	Owner                  ownership.Owner
	NamespaceID            string
	NamespaceIncarnationID string
	OperationKey           string
	AttemptID              string
	ExpiresAt              time.Time
	ServiceAccountName     string
	ServiceAccountMarker   string
	KeyDisplayName         string
	KeyMarker              string
	CandidateID            string
}

type CredentialFailurePolicy string

// DisposableRetainOnFailure permits create-and-stage without revocation
// authority for a preprovisioned disposable namespace. Interrupted writes stay
// retained for separately reviewed recovery; no compensation, verification,
// promotion, interrupted create replay or replacement stage is authorized.
const DisposableRetainOnFailure CredentialFailurePolicy = "disposable_retain_on_failure"

type ServiceAccountRef struct {
	ID        string
	Name      string
	CreatedAt time.Time
	// VersionDigest hashes exact native provider version bytes. Native opaque
	// versions never belong in durable credential metadata.
	VersionDigest string
	ProvenanceID  string
}

type KeyRef struct {
	ID                string
	ServiceAccountID  string
	CreatedAt         time.Time
	VersionDigest     string
	CreationReceiptID string
	Verified          bool
	State             string
}

type SecretVersionRef struct {
	StoreID     string
	CandidateID string
	// VersionID must equal CandidateWrite.WriteID, fixed before key material
	// enters storage (Secrets Manager ClientRequestToken is suitable).
	VersionID              string
	WriteID                string
	Owner                  ownership.Owner
	NamespaceIncarnationID string
	ServiceAccountID       string
	KeyID                  string
	Encrypted              bool
	Candidate              bool
	Active                 bool
}

type CandidateWrite struct {
	Intent  CredentialIntent
	Key     KeyRef
	WriteID string
}

type WriteResolution string

const (
	WriteCommitted WriteResolution = "Committed"
	// FinalNotCommitted is a strong store guarantee: no delayed write for
	// this idempotency token can commit afterward. Otherwise return Unknown.
	WriteFinalNotCommitted WriteResolution = "FinalNotCommitted"
	WriteUnknown           WriteResolution = "Unknown"
)

type CandidateResolution struct {
	Outcome   WriteResolution
	Reference SecretVersionRef
}

// SecureCandidateStore is an activity/provider-only seam. Implementations must
// encrypt material and atomically reject active/foreign version deletion. A
// plain eventually-consistent absence read is not FinalNotCommitted evidence.
type SecureCandidateStore interface {
	PutCandidate(context.Context, CandidateWrite, KeyMaterial) (SecretVersionRef, error)
	ResolveWrite(context.Context, CandidateWrite) (CandidateResolution, error)
	DeleteCandidate(context.Context, SecretVersionRef) error
}

type CanaryAttestation struct {
	NamespaceIncarnationID string
	KeyID                  string
	VersionID              string
	Passed                 bool
	VerifiedAt             time.Time
}

// CredentialCanary resolves the version inside the secure activity boundary.
// It never returns tokens, headers, raw responses or provider error messages.
// Each call must perform a fresh check and timestamp it using the same trusted
// clock as the namespace adapter; cached or future attestations are rejected.
type CredentialCanary interface {
	Check(context.Context, SecretVersionRef) (CanaryAttestation, error)
}

type CredentialPhase string

const (
	CredentialRecorded          CredentialPhase = "IntentRecorded"
	CredentialSARequested       CredentialPhase = "ServiceAccountCreateRequested"
	CredentialSAKnown           CredentialPhase = "ServiceAccountKnown"
	CredentialSAReady           CredentialPhase = "ServiceAccountReady"
	CredentialKeyRequested      CredentialPhase = "APIKeyCreateRequested"
	CredentialStoreRequested    CredentialPhase = "CandidateWriteRequested"
	CredentialStaged            CredentialPhase = "CandidateStaged"
	CredentialVerified          CredentialPhase = "CandidateVerified"
	CredentialCompensating      CredentialPhase = "CompensationRequested"
	CredentialRevocationPending CredentialPhase = "KeyRevocationPending"
	CredentialRevoked           CredentialPhase = "KeyRevoked"
	CredentialAborted           CredentialPhase = "Aborted"
)

// CredentialState contains safe references only and is suitable for durable
// activity/workflow payloads. It intentionally has no SDK response or material.
type CredentialState struct {
	Version            string
	Intent             CredentialIntent
	Phase              CredentialPhase
	ServiceAccount     ServiceAccountRef
	Key                KeyRef
	Candidate          SecretVersionRef
	SAReceipt          Receipt
	KeyReceipt         Receipt
	DeleteReceipt      Receipt
	CandidateWriteID   string
	FailureCode        CredentialCode
	CleanupFailureCode CredentialCode
}

// CredentialJournal must be linearizable and enforce owner/slot CAS. A slot is
// one namespace incarnation, preventing another attempt from bypassing an
// unresolved candidate. Unknown journal outcomes must return an error rather
// than a stale state. Retrying a successful CAS must preserve its safe result.
// Previous versions must remain available as immutable diagnostic evidence.
type CredentialJournal interface {
	Load(context.Context, string) (CredentialState, bool, error)
	CompareAndSwap(context.Context, string, string, CredentialState) (CredentialState, error)
}

type StageRequest struct {
	Namespace WriteRequest
	AttemptID string
	ExpiresAt time.Time
}

type CredentialCode string

const (
	CredentialInvalid         CredentialCode = "CredentialInvalidRequest"
	CredentialDisabled        CredentialCode = "CredentialCapabilityDisabled"
	CredentialOwnership       CredentialCode = "CredentialOwnershipConflict"
	CredentialAttemptConflict CredentialCode = "CredentialAttemptConflict"
	CredentialJournalUnknown  CredentialCode = "CredentialJournalOutcomeUnknown"
	CredentialProviderFailure CredentialCode = "CredentialProviderFailure"
	CredentialSAUnproven      CredentialCode = "CredentialServiceAccountOutcomeUnproven"
	CredentialKeyUnproven     CredentialCode = "CredentialKeyOutcomeUnproven"
	CredentialMaterialLost    CredentialCode = "CredentialOneTimeMaterialLost"
	CredentialStoreUnknown    CredentialCode = "CredentialStoreOutcomeUnknown"
	CredentialStoreRejected   CredentialCode = "CredentialStoreRejected"
	CredentialCanaryFailed    CredentialCode = "CredentialCanaryFailed"
	CredentialCleanupBlocked  CredentialCode = "CredentialCleanupBlocked"
	CredentialVersionChanged  CredentialCode = "CredentialProviderVersionChanged"
	CredentialRetained        CredentialCode = "CredentialRetainedRecoveryRequired"
)

type CredentialError struct {
	Code         CredentialCode
	Retryable    bool
	ProviderCode string
	State        CredentialState
}

func (e *CredentialError) Error() string { return string(e.Code) }
func (e *CredentialError) Diagnostic(id string, at time.Time) diagnostics.DomainFailure {
	message := "Credential staging stopped before promotion."
	next := "Inspect the safe stage references and verify the exact namespace binding."
	switch e.Code {
	case CredentialStoreUnknown:
		message = "Candidate storage has an unknown outcome; its key is retained."
		next = "Resolve the exact candidate write ID before revoking the key or starting another attempt."
	case CredentialMaterialLost:
		message = "The one-time API key response cannot be recovered."
		next = "Confirm revocation of the verified owned key, then use a new durable attempt."
	case CredentialKeyUnproven, CredentialSAUnproven:
		message = "Ownership of an interrupted credential create has not been proven."
		next = "Resolve the recorded provider operation; do not adopt or revoke an identity guessed by name."
	case CredentialCanaryFailed:
		message = "The candidate credential failed its canary and was never promoted."
		next = "Inspect original failure and compensation state; retain the current active credential."
	case CredentialCleanupBlocked:
		message = "Credential cleanup lacks safe inactive-version or ownership evidence."
		next = "Resolve candidate version ownership and active state before cleanup."
	case CredentialVersionChanged:
		message = "The owned key's provider version changed; conditional revocation is blocked."
		next = "Review the exact key incarnation and persisted version binding before accepting a new recovery step."
	case CredentialRetained:
		message = "The disposable credential stage is retained and requires an explicit recovery decision."
		next = "Review recorded create and storage receipts; do not replay, revoke, replace or promote this stage."
	case CredentialDisabled:
		message = "The requested credential capability is disabled."
		next = "Use the reviewed scoped fixture configuration; do not broaden account administration."
	case CredentialJournalUnknown:
		message = "The safe credential journal update has an unknown outcome."
		next = "Reload the exact namespace slot and version before another provider operation."
	}
	category := diagnostics.Configuration
	if e.ProviderCode == "PermissionDenied" || e.ProviderCode == "Unauthenticated" {
		category = diagnostics.Authentication
		message = "The scoped Temporal Cloud credential principal was rejected."
		next = "Repair only the reviewed namespace-scoped principal's permissions or API key."
	}
	return diagnostics.SafeFailure(diagnostics.DomainFailure{ID: id, Code: string(e.Code), Category: category, Message: message, Step: "CredentialStaging", OccurredAt: at, NextAction: next, Provider: diagnostics.ProviderRef{Service: "temporal-cloud", Code: e.ProviderCode}})
}

// KeyMaterial is usable only synchronously inside PutCandidate. It deliberately
// fails serialization and redacts formatting. Use does not prevent a trusted
// store implementation from deliberately copying bytes into encrypted storage.
type KeyMaterial struct{ state *materialState }
type materialState struct {
	mu    sync.Mutex
	bytes []byte
	live  bool
}

func newMaterial(token string) KeyMaterial {
	return KeyMaterial{&materialState{bytes: []byte(token), live: true}}
}
func (KeyMaterial) String() string               { return "KeyMaterial{redacted}" }
func (KeyMaterial) GoString() string             { return "KeyMaterial{redacted}" }
func (m KeyMaterial) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, m.String()) }
func (KeyMaterial) MarshalJSON() ([]byte, error) {
	return nil, errors.New("credential material must not be serialized")
}
func (KeyMaterial) MarshalText() ([]byte, error) {
	return nil, errors.New("credential material must not be serialized")
}
func (m KeyMaterial) Use(fn func([]byte) error) error {
	if m.state == nil {
		return errors.New("credential material is no longer available")
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	if !m.state.live {
		return errors.New("credential material is no longer available")
	}
	return fn(m.state.bytes)
}
func (m KeyMaterial) close() {
	if m.state != nil {
		m.state.mu.Lock()
		defer m.state.mu.Unlock()
		clear(m.state.bytes)
		m.state.bytes = nil
		m.state.live = false
	}
}

func (m KeyMaterial) empty() bool {
	if m.state == nil {
		return true
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	return len(m.state.bytes) == 0
}
func (m KeyMaterial) appearsIn(value string) bool {
	if m.state == nil {
		return false
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	return len(m.state.bytes) > 0 && bytes.Contains([]byte(value), m.state.bytes)
}

func stateFingerprint(state CredentialState) string { state.Version = ""; return fingerprint(state) }
func sameIntent(a, b CredentialIntent) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
