package temporalcloud

import (
	"context"
	"encoding/json"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CredentialManager struct {
	namespaces     *Adapter
	transport      CredentialTransport
	config         CredentialConfig
	preprovisioned map[string]ScopedServiceAccountBinding
	journal        CredentialJournal
	store          SecureCandidateStore
	canary         CredentialCanary
}

func NewCredentialManager(config CredentialConfig, namespaces *Adapter, transport CredentialTransport, journal CredentialJournal, store SecureCandidateStore, canary CredentialCanary) (*CredentialManager, error) {
	if namespaces == nil || transport == nil || journal == nil || store == nil || !text(config.StoreID) || config.MaximumKeyTTL <= 0 || config.MaximumKeyTTL > 30*24*time.Hour || config.MaximumRecoveryPages < 1 || config.MaximumRecoveryPages > 20 {
		return nil, &CredentialError{Code: CredentialInvalid}
	}
	retain := config.FailurePolicy == DisposableRetainOnFailure
	if config.FailurePolicy != "" && !retain {
		return nil, &CredentialError{Code: CredentialInvalid}
	}
	if retain && (namespaces.owner.Owner().Lifetime != ownership.Disposable || config.MaximumKeyTTL > time.Hour || config.Capabilities.CreateScopedServiceAccounts || config.Capabilities.RevokeOwnedCandidates || config.Capabilities.VerifyCandidates) {
		return nil, &CredentialError{Code: CredentialInvalid}
	}
	if config.Capabilities.StageAPIKeys && ((!config.Capabilities.RevokeOwnedCandidates && !retain) || namespaces.guard == nil) {
		return nil, &CredentialError{Code: CredentialInvalid}
	}
	if config.Capabilities.VerifyCandidates && canary == nil {
		return nil, &CredentialError{Code: CredentialInvalid}
	}
	m := &CredentialManager{namespaces: namespaces, transport: transport, config: config, journal: journal, store: store, canary: canary, preprovisioned: map[string]ScopedServiceAccountBinding{}}
	m.config.PreprovisionedServiceAccounts = nil
	for _, binding := range config.PreprovisionedServiceAccounts {
		ns, ok := namespaces.bindings[binding.NamespaceID]
		if !ok || binding.Owner != namespaces.owner.Owner() || binding.NamespaceIncarnationID != ns.Record.Ref.ProviderID || !text(binding.Reference.ID) || binding.Reference.CreatedAt.IsZero() || !text(binding.Reference.ProvenanceID) || !digestPattern.MatchString(binding.Reference.VersionDigest) {
			return nil, &CredentialError{Code: CredentialOwnership}
		}
		if _, exists := m.preprovisioned[binding.NamespaceID]; exists {
			return nil, &CredentialError{Code: CredentialInvalid}
		}
		m.preprovisioned[binding.NamespaceID] = binding
	}
	if retain && len(m.preprovisioned) != len(namespaces.bindings) {
		return nil, &CredentialError{Code: CredentialInvalid}
	}
	return m, nil
}

func (m *CredentialManager) Prepare(request StageRequest) (CredentialIntent, error) {
	a := m.namespaces
	binding, ok := a.bindings[request.Namespace.NamespaceID]
	if !ok || request.Namespace.ExpectedIncarnationID != binding.Record.Ref.ProviderID || request.Namespace.ExpectedRevision != binding.Record.Revision || !text(request.Namespace.OperationKey) || !text(request.AttemptID) || request.ExpiresAt.IsZero() || request.ExpiresAt.Sub(a.now()) > m.config.MaximumKeyTTL {
		return CredentialIntent{}, &CredentialError{Code: CredentialInvalid}
	}
	slot := fingerprint(struct {
		Owner       any
		Incarnation string
	}{a.owner.Owner(), binding.Record.Ref.ProviderID})
	stage := fingerprint(struct {
		Slot, Operation, Attempt string
		Expiry                   time.Time
		FailurePolicy            CredentialFailurePolicy `json:",omitempty"`
	}{slot, request.Namespace.OperationKey, request.AttemptID, request.ExpiresAt.UTC(), m.config.FailurePolicy})
	saName, err := a.owner.Name(a.scope.Provider, "service-account", slot, 63)
	if err != nil {
		return CredentialIntent{}, &CredentialError{Code: CredentialInvalid}
	}
	keyName, err := a.owner.Name(a.scope.Provider, "api-key", stage, 63)
	if err != nil {
		return CredentialIntent{}, &CredentialError{Code: CredentialInvalid}
	}
	candidateID, err := a.owner.Path(a.scope.Provider, "api-key-candidate", stage)
	if err != nil {
		return CredentialIntent{}, &CredentialError{Code: CredentialInvalid}
	}
	return CredentialIntent{FailurePolicy: m.config.FailurePolicy, SlotID: slot, StageID: stage, Owner: a.owner.Owner(), NamespaceID: binding.NamespaceID, NamespaceIncarnationID: binding.Record.Ref.ProviderID, OperationKey: request.Namespace.OperationKey, AttemptID: request.AttemptID, ExpiresAt: request.ExpiresAt.UTC(), ServiceAccountName: saName, ServiceAccountMarker: "registry-sa:" + slot, KeyDisplayName: keyName, KeyMarker: "registry-key:" + stage, CandidateID: candidateID}, nil
}

// Advance runs only inside an activity/provider boundary. It returns safe
// references and performs bounded transitions, never raw SDK responses/tokens.
// The caller must continue pending phases durably and impose an overall budget.
func (m *CredentialManager) Advance(ctx context.Context, request StageRequest) (CredentialState, error) {
	if !m.config.Capabilities.StageAPIKeys {
		return CredentialState{}, &CredentialError{Code: CredentialDisabled}
	}
	intent, err := m.Prepare(request)
	if err != nil {
		return CredentialState{}, err
	}
	var result CredentialState
	err = m.namespaces.mutate(ctx, request.Namespace, func(call context.Context, _ *namespace.Namespace) error {
		state, found, err := m.journal.Load(call, intent.SlotID)
		if err != nil {
			return m.error(CredentialJournalUnknown, state, true)
		}
		if !found {
			if !intent.ExpiresAt.After(m.namespaces.now()) {
				return m.error(CredentialInvalid, state, false)
			}
			result, err = m.save(call, CredentialState{}, CredentialState{Intent: intent, Phase: CredentialRecorded})
			return err
		}
		if !text(state.Version) || !credentialVersionsValid(state) || state.Intent.SlotID != intent.SlotID || state.Intent.Owner != intent.Owner || state.Intent.NamespaceIncarnationID != intent.NamespaceIncarnationID {
			return m.error(CredentialOwnership, state, false)
		}
		if !sameIntent(state.Intent, intent) {
			if state.Phase != CredentialAborted || !intent.ExpiresAt.After(m.namespaces.now()) {
				return m.error(CredentialAttemptConflict, state, false)
			}
			next := CredentialState{Intent: intent, Phase: CredentialSAKnown, ServiceAccount: state.ServiceAccount, SAReceipt: state.SAReceipt}
			result, err = m.save(call, state, next)
			return err
		}
		result = state
		result, err = m.advance(call, state)
		return err
	})
	if err != nil {
		if typed, ok := err.(*CredentialError); ok {
			return typed.State, typed
		}
		return result, m.error(CredentialOwnership, result, false)
	}
	return result, nil
}

func (m *CredentialManager) advance(ctx context.Context, state CredentialState) (CredentialState, error) {
	if m.config.FailurePolicy == DisposableRetainOnFailure {
		switch state.Phase {
		case CredentialRecorded, CredentialSAReady:
		case CredentialStaged:
			return state, nil
		default:
			return state, m.error(CredentialRetained, state, false)
		}
	}
	switch state.Phase {
	case CredentialRecorded:
		if fixture, ok := m.preprovisioned[state.Intent.NamespaceID]; ok {
			state.ServiceAccount = fixture.Reference
			ref, err := m.observeServiceAccount(ctx, state)
			if err != nil {
				return state, err
			}
			next := state
			next.ServiceAccount = ref
			next.Phase = CredentialSAReady
			return m.save(ctx, state, next)
		}
		if !m.config.Capabilities.CreateScopedServiceAccounts {
			return state, m.error(CredentialDisabled, state, false)
		}
		request := m.serviceAccountRequest(state)
		next := state
		next.SAReceipt = m.namespaces.receipt(state.Intent.NamespaceID, "service-account-create", state.Intent.StageID, fingerprint(request.Spec))
		next.Phase = CredentialSARequested
		saved, err := m.save(ctx, state, next)
		if err != nil {
			return saved, err
		}
		return m.createServiceAccount(ctx, saved)
	case CredentialSARequested:
		// Service-account create has no one-time secret. Replaying the exact
		// persisted async ID is idempotent; an unrecoverable ID remains unproven.
		return m.createServiceAccount(ctx, state)
	case CredentialSAKnown:
		ref, err := m.observeServiceAccount(ctx, state)
		if err != nil {
			return state, err
		}
		next := state
		next.ServiceAccount = ref
		next.Phase = CredentialSAReady
		return m.save(ctx, state, next)
	case CredentialSAReady:
		if !state.Intent.ExpiresAt.After(m.namespaces.now()) {
			return state, m.error(CredentialInvalid, state, false)
		}
		if _, err := m.observeServiceAccount(ctx, state); err != nil {
			return state, err
		}
		request := m.keyRequest(state)
		next := state
		next.KeyReceipt = m.namespaces.receipt(state.Intent.NamespaceID, "api-key-create", state.Intent.StageID, fingerprint(request.Spec))
		next.Phase = CredentialKeyRequested
		saved, err := m.save(ctx, state, next)
		if err != nil {
			return saved, err
		}
		return m.createAndStoreKey(ctx, saved)
	case CredentialKeyRequested:
		return m.recoverLostKey(ctx, state)
	case CredentialStoreRequested:
		return m.resolveStore(ctx, state)
	case CredentialStaged:
		return m.verifyCandidate(ctx, state)
	case CredentialCompensating:
		return m.revokeCandidate(ctx, state)
	case CredentialRevocationPending:
		return m.confirmRevocation(ctx, state)
	case CredentialRevoked:
		return m.removeCandidate(ctx, state)
	case CredentialVerified, CredentialAborted:
		return state, nil
	default:
		return state, m.error(CredentialInvalid, state, false)
	}
}

func (m *CredentialManager) serviceAccountRequest(state CredentialState) *cloudservice.CreateServiceAccountRequest {
	return &cloudservice.CreateServiceAccountRequest{Spec: &identity.ServiceAccountSpec{Name: state.Intent.ServiceAccountName, Description: state.Intent.ServiceAccountMarker,
		NamespaceScopedAccess: &identity.NamespaceScopedAccess{Namespace: state.Intent.NamespaceID, Access: &identity.NamespaceAccess{Permission: identity.NamespaceAccess_PERMISSION_WRITE}}}, AsyncOperationId: state.SAReceipt.ID}
}

func (m *CredentialManager) createServiceAccount(ctx context.Context, state CredentialState) (CredentialState, error) {
	if !m.config.Capabilities.CreateScopedServiceAccounts {
		return state, m.error(CredentialDisabled, state, false)
	}
	request := m.serviceAccountRequest(state)
	if state.SAReceipt != m.namespaces.receipt(state.Intent.NamespaceID, "service-account-create", state.Intent.StageID, fingerprint(request.Spec)) {
		return state, m.error(CredentialOwnership, state, false)
	}
	response, err := m.transport.CreateServiceAccount(ctx, request)
	if err != nil {
		return state, m.providerFailure(err, state, CredentialSAUnproven)
	}
	if response == nil || !text(response.ServiceAccountId) || response.AsyncOperation == nil || response.AsyncOperation.Id != request.AsyncOperationId {
		return state, m.error(CredentialSAUnproven, state, false)
	}
	next := state
	next.ServiceAccount = ServiceAccountRef{ID: response.ServiceAccountId, Name: state.Intent.ServiceAccountName, ProvenanceID: request.AsyncOperationId}
	next.Phase = CredentialSAKnown
	return m.save(ctx, state, next)
}

func (m *CredentialManager) observeServiceAccount(ctx context.Context, state CredentialState) (ServiceAccountRef, error) {
	ref := state.ServiceAccount
	if !text(ref.ID) || ref.Name != state.Intent.ServiceAccountName || !text(ref.ProvenanceID) {
		return ref, m.error(CredentialOwnership, state, false)
	}
	response, err := m.transport.GetServiceAccount(ctx, &cloudservice.GetServiceAccountRequest{ServiceAccountId: ref.ID})
	if err != nil {
		return ref, m.providerFailure(err, state, CredentialProviderFailure)
	}
	if response == nil || response.ServiceAccount == nil {
		return ref, m.error(CredentialSAUnproven, state, false)
	}
	actual := response.ServiceAccount
	if actual.Id != ref.ID || actual.Spec == nil || actual.Spec.Name != ref.Name || actual.Spec.Description != state.Intent.ServiceAccountMarker || actual.Spec.Access != nil || actual.Spec.ProjectScopedAccess != nil || actual.Spec.NamespaceScopedAccess == nil || actual.Spec.NamespaceScopedAccess.Namespace != state.Intent.NamespaceID || actual.Spec.NamespaceScopedAccess.Access.GetPermission() != identity.NamespaceAccess_PERMISSION_WRITE || actual.CreatedTime == nil || actual.CreatedTime.CheckValid() != nil || !text(actual.ResourceVersion) {
		return ref, m.error(CredentialOwnership, state, false)
	}
	if !ref.CreatedAt.IsZero() && !ref.CreatedAt.Equal(actual.CreatedTime.AsTime()) {
		return ref, m.error(CredentialOwnership, state, false)
	}
	if actual.State != resource.ResourceState_RESOURCE_STATE_ACTIVE {
		return ref, m.error(CredentialProviderFailure, state, true)
	}
	ref.CreatedAt = actual.CreatedTime.AsTime()
	ref.VersionDigest = ProviderVersionDigest(actual.ResourceVersion)
	return ref, nil
}

func (m *CredentialManager) keyRequest(state CredentialState) *cloudservice.CreateApiKeyRequest {
	return &cloudservice.CreateApiKeyRequest{Spec: &identity.ApiKeySpec{OwnerId: state.ServiceAccount.ID, OwnerType: identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT, DisplayName: state.Intent.KeyDisplayName, Description: state.Intent.KeyMarker, ExpiryTime: timestamppb.New(state.Intent.ExpiresAt)}, AsyncOperationId: state.KeyReceipt.ID}
}

func (m *CredentialManager) createAndStoreKey(ctx context.Context, state CredentialState) (CredentialState, error) {
	request := m.keyRequest(state)
	response, err := m.transport.CreateApiKey(ctx, request)
	if err != nil {
		return state, m.providerFailure(err, state, CredentialKeyUnproven)
	}
	var material KeyMaterial
	if response != nil {
		material = newMaterial(response.Token)
		response.Token = ""
		defer material.close()
	}
	if response == nil || !text(response.KeyId) || response.AsyncOperation == nil || response.AsyncOperation.Id != request.AsyncOperationId {
		return state, m.error(CredentialKeyUnproven, state, false)
	}
	if material.appearsIn(response.KeyId) {
		return state, m.error(CredentialKeyUnproven, state, false)
	}
	next := state
	next.Key = KeyRef{ID: response.KeyId, ServiceAccountID: state.ServiceAccount.ID, CreationReceiptID: request.AsyncOperationId}
	verified, err := m.observeKey(ctx, next)
	if err != nil {
		if m.config.FailurePolicy == DisposableRetainOnFailure {
			return state, m.error(CredentialRetained, state, false)
		}
		next.Phase = CredentialCompensating
		failure := m.error(CredentialMaterialLost, next, false)
		if typed, ok := err.(*CredentialError); ok {
			failure = typed
		}
		next.FailureCode = failure.Code
		saved, saveErr := m.saveWithMaterial(ctx, state, next, material)
		if saveErr != nil {
			return saved, saveErr
		}
		return saved, &CredentialError{Code: failure.Code, Retryable: failure.Retryable, ProviderCode: failure.ProviderCode, State: saved}
	}
	next.Key = verified
	if material.empty() {
		if m.config.FailurePolicy == DisposableRetainOnFailure {
			return state, m.error(CredentialRetained, state, false)
		}
		next.Phase = CredentialCompensating
		next.FailureCode = CredentialMaterialLost
		saved, saveErr := m.saveWithMaterial(ctx, state, next, material)
		if saveErr != nil {
			return saved, saveErr
		}
		return saved, m.error(CredentialMaterialLost, saved, false)
	}
	next.Phase = CredentialStoreRequested
	next.CandidateWriteID = fingerprint(struct{ Stage, Key string }{next.Intent.StageID, next.Key.ID})
	saved, err := m.saveWithMaterial(ctx, state, next, material)
	if err != nil {
		return saved, err
	}
	write := m.candidateWrite(saved)
	reference, storeErr := m.store.PutCandidate(ctx, write, material)
	if storeErr != nil {
		if m.config.FailurePolicy == DisposableRetainOnFailure {
			return saved, m.error(CredentialStoreUnknown, saved, false)
		}
		return m.resolveStoreWithMaterial(ctx, saved, material)
	}
	if !m.validCandidate(write, reference) {
		return saved, m.error(CredentialStoreUnknown, saved, false)
	}
	next = saved
	next.Candidate = reference
	next.Phase = CredentialStaged
	return m.saveWithMaterial(ctx, saved, next, material)
}

func (m *CredentialManager) observeKey(ctx context.Context, state CredentialState) (KeyRef, error) {
	observed, err := m.observeKeyExecution(ctx, state, false)
	if err != nil {
		return state.Key, err
	}
	return observed.private.reference, nil
}

func (m *CredentialManager) observeKeyExecution(ctx context.Context, state CredentialState, conditional bool) (keyObservation, error) {
	ref := state.Key
	if !text(ref.ID) || ref.ServiceAccountID != state.ServiceAccount.ID || ref.CreationReceiptID != state.KeyReceipt.ID {
		return keyObservation{}, m.error(CredentialOwnership, state, false)
	}
	response, err := m.transport.GetApiKey(ctx, &cloudservice.GetApiKeyRequest{KeyId: ref.ID})
	if err != nil {
		return keyObservation{}, m.providerFailure(err, state, CredentialProviderFailure)
	}
	if response == nil || response.ApiKey == nil || response.ApiKey.Id != ref.ID || !m.keyMatches(state, response.ApiKey) || !text(response.ApiKey.ResourceVersion) || response.ApiKey.CreatedTime == nil || response.ApiKey.CreatedTime.CheckValid() != nil {
		return keyObservation{}, m.error(CredentialOwnership, state, false)
	}
	actual := response.ApiKey
	if !ref.CreatedAt.IsZero() && !ref.CreatedAt.Equal(actual.CreatedTime.AsTime()) {
		return keyObservation{}, m.error(CredentialOwnership, state, false)
	}
	ref.CreatedAt = actual.CreatedTime.AsTime()
	digest := ProviderVersionDigest(actual.ResourceVersion)
	if digest == "" {
		return keyObservation{}, m.error(CredentialOwnership, state, false)
	}
	if conditional && ref.VersionDigest != "" && ref.VersionDigest != digest {
		return keyObservation{}, m.error(CredentialVersionChanged, state, false)
	}
	ref.VersionDigest = digest
	ref.Verified = true
	ref.State = actual.State.String()
	return keyObservation{private: &keyObservationData{reference: ref, nativeVersion: actual.ResourceVersion}}, nil
}

func (m *CredentialManager) keyMatches(state CredentialState, key *identity.ApiKey) bool {
	return key != nil && key.Spec != nil && key.Spec.OwnerId == state.ServiceAccount.ID && key.Spec.OwnerType == identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT && key.Spec.DisplayName == state.Intent.KeyDisplayName && key.Spec.Description == state.Intent.KeyMarker && key.Spec.ExpiryTime != nil && key.Spec.ExpiryTime.CheckValid() == nil && key.Spec.ExpiryTime.AsTime().Equal(state.Intent.ExpiresAt)
}

func (m *CredentialManager) candidateWrite(state CredentialState) CandidateWrite {
	return CandidateWrite{Intent: state.Intent, Key: state.Key, WriteID: fingerprint(struct{ Stage, Key string }{state.Intent.StageID, state.Key.ID})}
}

func (m *CredentialManager) validCandidate(write CandidateWrite, ref SecretVersionRef) bool {
	return ref.StoreID == m.config.StoreID && ref.CandidateID == write.Intent.CandidateID && ref.VersionID == write.WriteID && digestPattern.MatchString(ref.VersionID) && ref.WriteID == write.WriteID && ref.Owner == write.Intent.Owner && ref.NamespaceIncarnationID == write.Intent.NamespaceIncarnationID && ref.ServiceAccountID == write.Key.ServiceAccountID && ref.KeyID == write.Key.ID && ref.Encrypted && ref.Candidate && !ref.Active
}

func (m *CredentialManager) save(ctx context.Context, previous, next CredentialState) (CredentialState, error) {
	return m.saveWithMaterial(ctx, previous, next, KeyMaterial{})
}

// Reject material echoed in ANY state field before CAS, including opaque
// provider versions. Journal-returned versions are checked before exposing them.
// A journal backend is still trusted not to generate/store secret data itself.
func (m *CredentialManager) saveWithMaterial(ctx context.Context, previous, next CredentialState, material KeyMaterial) (CredentialState, error) {
	if !credentialVersionsValid(next) {
		return previous, m.error(CredentialOwnership, previous, false)
	}
	encoded, _ := json.Marshal(next)
	if material.appearsIn(string(encoded)) {
		return previous, m.error(CredentialKeyUnproven, previous, false)
	}
	next.Version = ""
	saved, err := m.journal.CompareAndSwap(ctx, next.Intent.SlotID, previous.Version, next)
	encoded, _ = json.Marshal(saved)
	if material.appearsIn(string(encoded)) {
		return previous, m.error(CredentialJournalUnknown, previous, false)
	}
	if err != nil || !credentialVersionsValid(saved) || !text(saved.Version) || stateFingerprint(saved) != stateFingerprint(next) {
		return next, m.error(CredentialJournalUnknown, next, true)
	}
	return saved, nil
}

func (m *CredentialManager) error(code CredentialCode, state CredentialState, retry bool) *CredentialError {
	return &CredentialError{Code: code, Retryable: retry, State: state}
}
func (m *CredentialManager) providerFailure(err error, state CredentialState, fallback CredentialCode) *CredentialError {
	classified := providerError(err, Receipt{}, false)
	return &CredentialError{Code: fallback, ProviderCode: classified.ProviderCode, Retryable: classified.Retryable, State: state}
}

func (m *CredentialManager) resolveStore(ctx context.Context, state CredentialState) (CredentialState, error) {
	return m.resolveStoreWithMaterial(ctx, state, KeyMaterial{})
}
func (m *CredentialManager) resolveStoreWithMaterial(ctx context.Context, state CredentialState, material KeyMaterial) (CredentialState, error) {
	write := m.candidateWrite(state)
	resolution, err := m.store.ResolveWrite(ctx, write)
	if err != nil || resolution.Outcome == WriteUnknown {
		return state, m.error(CredentialStoreUnknown, state, true)
	}
	switch resolution.Outcome {
	case WriteCommitted:
		if !m.validCandidate(write, resolution.Reference) {
			return state, m.error(CredentialCleanupBlocked, state, false)
		}
		next := state
		next.Candidate = resolution.Reference
		next.Phase = CredentialStaged
		return m.saveWithMaterial(ctx, state, next, material)
	case WriteFinalNotCommitted:
		next := state
		next.Phase = CredentialCompensating
		if next.FailureCode == "" {
			next.FailureCode = CredentialStoreRejected
		}
		saved, saveErr := m.saveWithMaterial(ctx, state, next, material)
		if saveErr != nil {
			return saved, saveErr
		}
		return saved, m.error(CredentialStoreRejected, saved, false)
	default:
		return state, m.error(CredentialStoreUnknown, state, false)
	}
}

func (m *CredentialManager) verifyCandidate(ctx context.Context, state CredentialState) (CredentialState, error) {
	if !m.config.Capabilities.VerifyCandidates {
		return state, m.error(CredentialDisabled, state, false)
	}
	if _, err := m.observeServiceAccount(ctx, state); err != nil {
		return state, err
	}
	key, err := m.observeKey(ctx, state)
	if err != nil {
		return state, err
	}
	if key.State != resource.ResourceState_RESOURCE_STATE_ACTIVE.String() {
		return state, m.error(CredentialProviderFailure, state, true)
	}
	resolution, err := m.store.ResolveWrite(ctx, m.candidateWrite(state))
	if err != nil || resolution.Outcome != WriteCommitted {
		return state, m.error(CredentialStoreUnknown, state, true)
	}
	if !m.validCandidate(m.candidateWrite(state), resolution.Reference) || resolution.Reference.VersionID != state.Candidate.VersionID {
		return state, m.error(CredentialCleanupBlocked, state, false)
	}
	startedAt := m.namespaces.now()
	attestation, err := m.canary.Check(ctx, state.Candidate)
	completedAt := m.namespaces.now()
	next := state
	// Persist the independently observed ACTIVE incarnation/version, including
	// on canary failure. Deletion later must still fetch and match this exact
	// digest; compensation never silently substitutes a newer native version.
	next.Key = key
	if err != nil || !attestation.Passed || attestation.NamespaceIncarnationID != state.Intent.NamespaceIncarnationID || attestation.KeyID != state.Key.ID || attestation.VersionID != state.Candidate.VersionID || attestation.VerifiedAt.IsZero() || attestation.VerifiedAt.Before(startedAt) || attestation.VerifiedAt.After(completedAt) || attestation.VerifiedAt.Before(key.CreatedAt) || !state.Intent.ExpiresAt.After(completedAt) {
		next.Phase = CredentialCompensating
		next.FailureCode = CredentialCanaryFailed
		saved, saveErr := m.save(ctx, state, next)
		if saveErr != nil {
			return saved, saveErr
		}
		return saved, m.error(CredentialCanaryFailed, saved, false)
	}
	next.Phase = CredentialVerified
	return m.save(ctx, state, next)
}

// No activation or active-secret overwrite exists here. Full rotation requires
// a separate coordinated promotion/retirement protocol.
