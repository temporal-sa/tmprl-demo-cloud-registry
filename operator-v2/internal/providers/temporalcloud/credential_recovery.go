package temporalcloud

import (
	"context"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (m *CredentialManager) recoverLostKey(ctx context.Context, state CredentialState) (CredentialState, error) {
	if _, err := m.observeServiceAccount(ctx, state); err != nil {
		return state, err
	}
	expected := m.keyRequest(state)
	if state.KeyReceipt != m.namespaces.receipt(state.Intent.NamespaceID, "api-key-create", state.Intent.StageID, fingerprint(expected.Spec)) {
		return state, m.error(CredentialOwnership, state, false)
	}
	current, err := m.namespaces.pollRaw(ctx, state.KeyReceipt)
	if err != nil {
		if typed, ok := err.(*Error); ok && typed.Code == NamespaceMissing {
			return state, m.error(CredentialKeyUnproven, state, true)
		}
		return state, m.providerFailure(err, state, CredentialKeyUnproven)
	}
	if current.State == operation.AsyncOperation_STATE_PENDING || current.State == operation.AsyncOperation_STATE_IN_PROGRESS {
		return state, nil
	}
	actual := &cloudservice.CreateApiKeyRequest{}
	if current.State != operation.AsyncOperation_STATE_FULFILLED || current.OperationInput == nil || current.OperationInput.UnmarshalTo(actual) != nil || !proto.Equal(actual, expected) || current.StartedTime == nil || current.FinishedTime == nil || current.StartedTime.CheckValid() != nil || current.FinishedTime.CheckValid() != nil {
		return state, m.error(CredentialKeyUnproven, state, false)
	}
	var match *identity.ApiKey
	pageToken := ""
	seen := map[string]bool{}
	for page := 0; page < m.config.MaximumRecoveryPages; page++ {
		response, err := m.transport.GetApiKeys(ctx, &cloudservice.GetApiKeysRequest{OwnerId: state.ServiceAccount.ID, OwnerType: identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT, PageSize: 100, PageToken: pageToken})
		if err != nil {
			return state, m.providerFailure(err, state, CredentialKeyUnproven)
		}
		if response == nil {
			return state, m.error(CredentialKeyUnproven, state, false)
		}
		for _, key := range response.ApiKeys {
			if key == nil || key.Spec == nil || key.Spec.OwnerId != state.ServiceAccount.ID || key.Spec.OwnerType != identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT {
				return state, m.error(CredentialKeyUnproven, state, false)
			}
			if !m.keyMatches(state, key) {
				continue
			}
			if !text(key.Id) || !text(key.ResourceVersion) || key.CreatedTime == nil || key.CreatedTime.CheckValid() != nil || key.CreatedTime.AsTime().Before(current.StartedTime.AsTime()) || key.CreatedTime.AsTime().After(current.FinishedTime.AsTime()) {
				return state, m.error(CredentialKeyUnproven, state, false)
			}
			if match != nil {
				return state, m.error(CredentialKeyUnproven, state, false)
			}
			match = key
		}
		if response.NextPageToken == "" {
			if match == nil {
				return state, m.error(CredentialKeyUnproven, state, true)
			}
			next := state
			next.Key = KeyRef{ID: match.Id, ServiceAccountID: state.ServiceAccount.ID, CreatedAt: match.CreatedTime.AsTime(), VersionDigest: ProviderVersionDigest(match.ResourceVersion), CreationReceiptID: state.KeyReceipt.ID, Verified: true}
			next.Phase = CredentialCompensating
			next.FailureCode = CredentialMaterialLost
			return m.save(ctx, state, next)
		}
		if seen[response.NextPageToken] {
			return state, m.error(CredentialKeyUnproven, state, false)
		}
		seen[response.NextPageToken] = true
		pageToken = response.NextPageToken
	}
	return state, m.error(CredentialKeyUnproven, state, false)
}

func (m *CredentialManager) revokeCandidate(ctx context.Context, state CredentialState) (CredentialState, error) {
	if !m.config.Capabilities.RevokeOwnedCandidates {
		return state, m.error(CredentialDisabled, state, false)
	}
	if _, err := m.observeServiceAccount(ctx, state); err != nil {
		return state, err
	}
	if state.Candidate.VersionID != "" {
		resolution, err := m.store.ResolveWrite(ctx, m.candidateWrite(state))
		if err != nil || resolution.Outcome != WriteCommitted || !m.validCandidate(m.candidateWrite(state), resolution.Reference) || resolution.Reference.VersionID != state.Candidate.VersionID {
			return m.cleanupFailure(ctx, state, CredentialCleanupBlocked)
		}
	}
	observed, err := m.observeKeyExecution(ctx, state, true)
	if err != nil {
		if typed, ok := err.(*CredentialError); ok && typed.ProviderCode == "NotFound" && state.Key.Verified && !state.Key.CreatedAt.IsZero() {
			next := state
			next.Phase = CredentialRevoked
			return m.save(ctx, state, next)
		}
		if typed, ok := err.(*CredentialError); ok && typed.Code == CredentialVersionChanged {
			return m.cleanupFailure(ctx, state, CredentialVersionChanged)
		}
		return state, err
	}
	key := observed.private.reference
	nativeVersion := observed.private.nativeVersion
	if key.State == resource.ResourceState_RESOURCE_STATE_DELETED.String() {
		next := state
		next.Key = key
		next.Phase = CredentialRevoked
		return m.save(ctx, state, next)
	}
	next := state
	next.Key = key
	next.DeleteReceipt = m.namespaces.receipt(state.Intent.NamespaceID, "api-key-delete", state.Intent.StageID, fingerprint(struct{ ID, Version string }{key.ID, nativeVersion}))
	next.Phase = CredentialRevocationPending
	saved, err := m.save(ctx, state, next)
	if err != nil {
		return saved, err
	}
	// The exact independently observed native version stays in this activity;
	// the journal contains only its digest and a hash of the exact request.
	if ProviderVersionDigest(nativeVersion) != saved.Key.VersionDigest {
		return saved, m.error(CredentialOwnership, saved, false)
	}
	response, err := m.transport.DeleteApiKey(ctx, &cloudservice.DeleteApiKeyRequest{KeyId: key.ID, ResourceVersion: nativeVersion, AsyncOperationId: saved.DeleteReceipt.ID})
	if err != nil {
		classified := providerError(err, saved.DeleteReceipt, true)
		next := saved
		next.CleanupFailureCode = CredentialProviderFailure
		if !classified.RecoveryRequired {
			next.Phase = CredentialCompensating
		}
		recorded, saveErr := m.save(ctx, saved, next)
		if saveErr != nil {
			return recorded, saveErr
		}
		return recorded, &CredentialError{Code: CredentialProviderFailure, Retryable: classified.Retryable, ProviderCode: classified.ProviderCode, State: recorded}
	}
	if response == nil || response.AsyncOperation == nil || response.AsyncOperation.Id != saved.DeleteReceipt.ID {
		return m.cleanupFailure(ctx, saved, CredentialKeyUnproven)
	}
	return saved, nil
}

func (m *CredentialManager) confirmRevocation(ctx context.Context, state CredentialState) (CredentialState, error) {
	if !state.Key.Verified || !digestPattern.MatchString(state.Key.VersionDigest) || !digestPattern.MatchString(state.DeleteReceipt.InputFingerprint) || state.DeleteReceipt != m.namespaces.receipt(state.Intent.NamespaceID, "api-key-delete", state.Intent.StageID, state.DeleteReceipt.InputFingerprint) {
		return state, m.error(CredentialOwnership, state, false)
	}
	current, err := m.namespaces.pollRaw(ctx, state.DeleteReceipt)
	if err != nil {
		if typed, ok := err.(*Error); ok && typed.Code == NamespaceMissing {
			return state, m.error(CredentialKeyUnproven, state, true)
		}
		return state, m.providerFailure(err, state, CredentialKeyUnproven)
	}
	if current.State == operation.AsyncOperation_STATE_PENDING || current.State == operation.AsyncOperation_STATE_IN_PROGRESS {
		return state, nil
	}
	if current.State != operation.AsyncOperation_STATE_FULFILLED {
		return m.cleanupFailure(ctx, state, CredentialProviderFailure)
	}
	// The object may already be absent. Recover the exact native precondition
	// only from the matching echoed request, never by substituting its digest
	// or a fresh live version into a different delete fingerprint.
	expected := &cloudservice.DeleteApiKeyRequest{}
	if current.OperationInput == nil || current.OperationInput.UnmarshalTo(expected) != nil || expected.KeyId != state.Key.ID || expected.AsyncOperationId != state.DeleteReceipt.ID || ProviderVersionDigest(expected.ResourceVersion) != state.Key.VersionDigest || state.DeleteReceipt.InputFingerprint != fingerprint(struct{ ID, Version string }{expected.KeyId, expected.ResourceVersion}) {
		return state, m.error(CredentialKeyUnproven, state, false)
	}
	response, err := m.transport.GetApiKey(ctx, &cloudservice.GetApiKeyRequest{KeyId: state.Key.ID})
	absent := status.Code(err) == codes.NotFound
	if err != nil && !absent {
		return state, m.providerFailure(err, state, CredentialProviderFailure)
	}
	if !absent {
		if response == nil || response.ApiKey == nil || response.ApiKey.Id != state.Key.ID {
			return state, m.error(CredentialOwnership, state, false)
		}
		if response.ApiKey.CreatedTime == nil || !response.ApiKey.CreatedTime.AsTime().Equal(state.Key.CreatedAt) {
			return state, m.error(CredentialOwnership, state, false)
		}
		if response.ApiKey.State != resource.ResourceState_RESOURCE_STATE_DELETED {
			if !m.keyMatches(state, response.ApiKey) {
				return state, m.error(CredentialOwnership, state, false)
			}
			return state, nil
		}
	}
	next := state
	next.Phase = CredentialRevoked
	next.CleanupFailureCode = ""
	return m.save(ctx, state, next)
}

func (m *CredentialManager) removeCandidate(ctx context.Context, state CredentialState) (CredentialState, error) {
	if state.Candidate.VersionID != "" {
		// Store implementation must enforce the owner/version/non-active
		// condition atomically, not merely trust the stale Active flag here.
		if !m.validCandidate(m.candidateWrite(state), state.Candidate) {
			return m.cleanupFailure(ctx, state, CredentialCleanupBlocked)
		}
		if err := m.store.DeleteCandidate(ctx, state.Candidate); err != nil {
			return m.cleanupFailure(ctx, state, CredentialCleanupBlocked)
		}
	}
	next := state
	next.Phase = CredentialAborted
	next.CleanupFailureCode = ""
	return m.save(ctx, state, next)
}

func (m *CredentialManager) cleanupFailure(ctx context.Context, state CredentialState, code CredentialCode) (CredentialState, error) {
	next := state
	next.CleanupFailureCode = code
	saved, err := m.save(ctx, state, next)
	if err != nil {
		return saved, err
	}
	return saved, m.error(code, saved, true)
}
