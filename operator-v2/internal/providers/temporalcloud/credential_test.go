package temporalcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const sentinelToken = "LIVE-SENTINEL-TEMPORAL-KEY-DO-NOT-STORE-IN-HISTORY"

type fakeCredentialTransport struct {
	*fakeTransport
	now              time.Time
	sa               *identity.ServiceAccount
	keys             map[string]*identity.ApiKey
	ops              map[string]*operation.AsyncOperation
	saRequests       []*cloudservice.CreateServiceAccountRequest
	keyRequests      []*cloudservice.CreateApiKeyRequest
	keyLists         []*cloudservice.GetApiKeysRequest
	deletes          []*cloudservice.DeleteApiKeyRequest
	lostSAResponse   bool
	lostKeyResponse  bool
	foreignKey       bool
	duplicateLostKey bool
	tokenAsID        bool
	tokenAsVersion   bool
}

func (f *fakeCredentialTransport) op(id string, request proto.Message) *operation.AsyncOperation {
	input, _ := anypb.New(request)
	return &operation.AsyncOperation{Id: id, State: operation.AsyncOperation_STATE_FULFILLED, OperationInput: input, StartedTime: timestamppb.New(f.now.Add(-time.Second)), FinishedTime: timestamppb.New(f.now.Add(time.Second))}
}
func (f *fakeCredentialTransport) CreateServiceAccount(ctx context.Context, r *cloudservice.CreateServiceAccountRequest, _ ...grpc.CallOption) (*cloudservice.CreateServiceAccountResponse, error) {
	f.note(ctx, "CreateServiceAccount")
	f.saRequests = append(f.saRequests, proto.Clone(r).(*cloudservice.CreateServiceAccountRequest))
	if f.sa == nil {
		f.sa = &identity.ServiceAccount{Id: "scoped-service-account-id", Spec: proto.Clone(r.Spec).(*identity.ServiceAccountSpec), CreatedTime: timestamppb.New(f.now), ResourceVersion: "sa-version-one", State: resource.ResourceState_RESOURCE_STATE_ACTIVE}
	}
	f.ops[r.AsyncOperationId] = f.op(r.AsyncOperationId, r)
	if f.lostSAResponse {
		return nil, status.Error(codes.Unavailable, sentinelToken)
	}
	return &cloudservice.CreateServiceAccountResponse{ServiceAccountId: f.sa.Id, AsyncOperation: f.ops[r.AsyncOperationId]}, nil
}
func (f *fakeCredentialTransport) GetServiceAccount(ctx context.Context, r *cloudservice.GetServiceAccountRequest, _ ...grpc.CallOption) (*cloudservice.GetServiceAccountResponse, error) {
	f.note(ctx, "GetServiceAccount")
	if f.sa == nil || r.ServiceAccountId != f.sa.Id {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return &cloudservice.GetServiceAccountResponse{ServiceAccount: proto.Clone(f.sa).(*identity.ServiceAccount)}, nil
}
func (f *fakeCredentialTransport) CreateApiKey(ctx context.Context, r *cloudservice.CreateApiKeyRequest, _ ...grpc.CallOption) (*cloudservice.CreateApiKeyResponse, error) {
	f.note(ctx, "CreateApiKey")
	f.keyRequests = append(f.keyRequests, proto.Clone(r).(*cloudservice.CreateApiKeyRequest))
	id := "owned-key-" + strconv.Itoa(len(f.keyRequests))
	if f.tokenAsID {
		id = sentinelToken
	}
	spec := proto.Clone(r.Spec).(*identity.ApiKeySpec)
	if f.foreignKey {
		spec.OwnerId = "foreign-service-account"
	}
	f.keys[id] = &identity.ApiKey{Id: id, Spec: spec, ResourceVersion: "key-version-one", CreatedTime: timestamppb.New(f.now), State: resource.ResourceState_RESOURCE_STATE_ACTIVE}
	if f.tokenAsVersion {
		f.keys[id].ResourceVersion = sentinelToken
	}
	f.ops[r.AsyncOperationId] = f.op(r.AsyncOperationId, r)
	if f.duplicateLostKey {
		copy := proto.Clone(f.keys[id]).(*identity.ApiKey)
		copy.Id = "ambiguous-duplicate-key"
		f.keys[copy.Id] = copy
	}
	if f.lostKeyResponse {
		return nil, status.Error(codes.Unavailable, sentinelToken)
	}
	return &cloudservice.CreateApiKeyResponse{KeyId: id, Token: sentinelToken, AsyncOperation: f.ops[r.AsyncOperationId]}, nil
}
func (f *fakeCredentialTransport) GetApiKey(ctx context.Context, r *cloudservice.GetApiKeyRequest, _ ...grpc.CallOption) (*cloudservice.GetApiKeyResponse, error) {
	f.note(ctx, "GetApiKey")
	key := f.keys[r.KeyId]
	if key == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return &cloudservice.GetApiKeyResponse{ApiKey: proto.Clone(key).(*identity.ApiKey)}, nil
}
func (f *fakeCredentialTransport) GetApiKeys(ctx context.Context, r *cloudservice.GetApiKeysRequest, _ ...grpc.CallOption) (*cloudservice.GetApiKeysResponse, error) {
	f.note(ctx, "GetApiKeys")
	f.keyLists = append(f.keyLists, proto.Clone(r).(*cloudservice.GetApiKeysRequest))
	if r.OwnerId == "" || r.OwnerType != identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT {
		return nil, status.Error(codes.PermissionDenied, "unfiltered list forbidden")
	}
	response := &cloudservice.GetApiKeysResponse{}
	for _, key := range f.keys {
		if key.Spec.OwnerId == r.OwnerId {
			response.ApiKeys = append(response.ApiKeys, proto.Clone(key).(*identity.ApiKey))
		}
	}
	return response, nil
}
func (f *fakeCredentialTransport) DeleteApiKey(ctx context.Context, r *cloudservice.DeleteApiKeyRequest, _ ...grpc.CallOption) (*cloudservice.DeleteApiKeyResponse, error) {
	f.note(ctx, "DeleteApiKey")
	f.deletes = append(f.deletes, proto.Clone(r).(*cloudservice.DeleteApiKeyRequest))
	key := f.keys[r.KeyId]
	if key == nil || key.ResourceVersion != r.ResourceVersion {
		return nil, status.Error(codes.Aborted, "stale")
	}
	f.ops[r.AsyncOperationId] = pending(r.AsyncOperationId)
	f.ops[r.AsyncOperationId].OperationInput, _ = anypb.New(r)
	return &cloudservice.DeleteApiKeyResponse{AsyncOperation: f.ops[r.AsyncOperationId]}, nil
}

type fakeCredentialJournal struct {
	states           map[string]CredentialState
	history          []CredentialState
	sequence         int
	failPhase        CredentialPhase
	commitFailure    bool
	echoVersionPhase CredentialPhase
}

func (j *fakeCredentialJournal) Load(_ context.Context, slot string) (CredentialState, bool, error) {
	state, found := j.states[slot]
	return state, found, nil
}
func (j *fakeCredentialJournal) CompareAndSwap(_ context.Context, slot, version string, next CredentialState) (CredentialState, error) {
	current := j.states[slot]
	if current.Version != version {
		return CredentialState{}, errors.New("CAS conflict " + sentinelToken)
	}
	fail := j.failPhase == next.Phase
	if fail && !j.commitFailure {
		j.failPhase = ""
		return CredentialState{}, errors.New(sentinelToken)
	}
	j.sequence++
	next.Version = "journal-version-" + strconv.Itoa(j.sequence)
	j.states[slot] = next
	j.history = append(j.history, next)
	if fail {
		j.failPhase = ""
		return CredentialState{}, errors.New(sentinelToken)
	}
	if j.echoVersionPhase == next.Phase {
		next.Version = sentinelToken
	}
	return next, nil
}

type fakeCandidateStore struct {
	mode          string
	references    map[string]SecretVersionRef
	puts, deletes int
	material      KeyMaterial
	active        bool
}

func (s *fakeCandidateStore) PutCandidate(_ context.Context, write CandidateWrite, material KeyMaterial) (SecretVersionRef, error) {
	s.puts++
	s.material = material
	var saw bool
	if err := material.Use(func(token []byte) error { saw = string(token) == sentinelToken; return nil }); err != nil || !saw {
		return SecretVersionRef{}, errors.New("missing material")
	}
	if s.mode == "unknown" || s.mode == "rejected" {
		return SecretVersionRef{}, errors.New(sentinelToken)
	}
	ref := SecretVersionRef{StoreID: "sandbox-encrypted-store", CandidateID: write.Intent.CandidateID, VersionID: write.WriteID, WriteID: write.WriteID, Owner: write.Intent.Owner, NamespaceIncarnationID: write.Intent.NamespaceIncarnationID, ServiceAccountID: write.Key.ServiceAccountID, KeyID: write.Key.ID, Encrypted: true, Candidate: true}
	if s.mode == "leaky-reference" {
		ref.VersionID = sentinelToken
	}
	s.references[write.WriteID] = ref
	if s.mode == "commit-response-lost" {
		return SecretVersionRef{}, errors.New(sentinelToken)
	}
	return ref, nil
}
func (s *fakeCandidateStore) ResolveWrite(_ context.Context, write CandidateWrite) (CandidateResolution, error) {
	if s.mode == "unknown" {
		return CandidateResolution{Outcome: WriteUnknown}, nil
	}
	if s.mode == "rejected" {
		return CandidateResolution{Outcome: WriteFinalNotCommitted}, nil
	}
	ref, found := s.references[write.WriteID]
	if !found {
		return CandidateResolution{Outcome: WriteUnknown}, nil
	}
	ref.Active = s.active
	return CandidateResolution{Outcome: WriteCommitted, Reference: ref}, nil
}
func (s *fakeCandidateStore) DeleteCandidate(_ context.Context, ref SecretVersionRef) error {
	if s.active {
		return errors.New("active version " + sentinelToken)
	}
	current, found := s.references[ref.WriteID]
	if !found || current != ref {
		return errors.New("foreign version")
	}
	s.deletes++
	delete(s.references, ref.WriteID)
	return nil
}

type fakeCredentialCanary struct {
	afterCheck func()
	now        time.Time
	fail       bool
	calls      int
}

func (c *fakeCredentialCanary) Check(_ context.Context, ref SecretVersionRef) (CanaryAttestation, error) {
	c.calls++
	if c.afterCheck != nil {
		c.afterCheck()
	}
	if c.fail {
		return CanaryAttestation{}, errors.New(sentinelToken)
	}
	return CanaryAttestation{NamespaceIncarnationID: ref.NamespaceIncarnationID, KeyID: ref.KeyID, VersionID: ref.VersionID, Passed: true, VerifiedAt: c.now}, nil
}

type credentialFixture struct {
	base      *testFixture
	manager   *CredentialManager
	transport *fakeCredentialTransport
	journal   *fakeCredentialJournal
	store     *fakeCandidateStore
	canary    *fakeCredentialCanary
	request   StageRequest
}

func credentialsFixture(t *testing.T) *credentialFixture {
	t.Helper()
	base := fixture(t)
	transport := &fakeCredentialTransport{fakeTransport: base.fake, now: base.now, keys: map[string]*identity.ApiKey{}, ops: map[string]*operation.AsyncOperation{}}
	base.fake.onOperation = func(r *cloudservice.GetAsyncOperationRequest) (*cloudservice.GetAsyncOperationResponse, error) {
		op := transport.ops[r.AsyncOperationId]
		if op == nil {
			return nil, status.Error(codes.NotFound, "absent operation")
		}
		return &cloudservice.GetAsyncOperationResponse{AsyncOperation: proto.Clone(op).(*operation.AsyncOperation)}, nil
	}
	journal := &fakeCredentialJournal{states: map[string]CredentialState{}}
	store := &fakeCandidateStore{references: map[string]SecretVersionRef{}}
	canary := &fakeCredentialCanary{now: base.now}
	cfg := CredentialConfig{Capabilities: CredentialCapabilities{CreateScopedServiceAccounts: true, StageAPIKeys: true, VerifyCandidates: true, RevokeOwnedCandidates: true}, MaximumKeyTTL: 30 * 24 * time.Hour, MaximumRecoveryPages: 2, StoreID: "sandbox-encrypted-store"}
	manager, err := NewCredentialManager(cfg, base.adapter, transport, journal, store, canary)
	if err != nil {
		t.Fatal(err)
	}
	return &credentialFixture{base, manager, transport, journal, store, canary, StageRequest{Namespace: base.write(), AttemptID: "durable-attempt-one", ExpiresAt: base.now.Add(24 * time.Hour)}}
}
func (f *credentialFixture) advance(t *testing.T) CredentialState {
	t.Helper()
	state, err := f.manager.Advance(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func (f *credentialFixture) staged(t *testing.T) CredentialState {
	t.Helper()
	var state CredentialState
	for step := 0; step < 8; step++ {
		state = f.advance(t)
		if state.Phase == CredentialStaged {
			return state
		}
	}
	t.Fatal("candidate did not stage")
	return state
}
func credentialError(t *testing.T, err error, code CredentialCode) *CredentialError {
	t.Helper()
	var typed *CredentialError
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("got %v; want %s", err, code)
	}
	return typed
}
func assertNoMaterial(t *testing.T, values ...any) {
	t.Helper()
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), sentinelToken) {
			t.Fatal("one-time key entered serializable state")
		}
		if strings.Contains(fmt.Sprintf("%#v", value), sentinelToken) {
			t.Fatal("one-time key entered formatted state")
		}
	}
}

func TestScopedCredentialStagingReturnsOnlySafeReferences(t *testing.T) {
	f := credentialsFixture(t)
	staged := f.staged(t)
	if staged.Candidate.VersionID == "" || !staged.Key.Verified || staged.Phase != CredentialStaged {
		t.Fatalf("bad staged result: %+v", staged)
	}
	sa := f.transport.saRequests[0].Spec
	if sa.Access != nil || sa.ProjectScopedAccess != nil || sa.NamespaceScopedAccess.Namespace != f.base.binding.NamespaceID || sa.NamespaceScopedAccess.Access.Permission != identity.NamespaceAccess_PERMISSION_WRITE {
		t.Fatal("service account gained broad authority")
	}
	key := f.transport.keyRequests[0].Spec
	if key.OwnerId != f.transport.sa.Id || key.OwnerType != identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT {
		t.Fatal("key owner does not match verified scoped SA")
	}
	verified := f.advance(t)
	if verified.Phase != CredentialVerified || f.canary.calls != 1 {
		t.Fatal("candidate not verified")
	}
	if err := f.store.material.Use(func([]byte) error { return nil }); err == nil {
		t.Fatal("material escaped synchronous store lifetime")
	}
	if _, err := json.Marshal(f.store.material); err == nil {
		t.Fatal("material unexpectedly serializable")
	}
	assertNoMaterial(t, verified, f.journal.history, f.transport.keyRequests)
	if f.base.fake.unbounded {
		t.Fatal("credential RPC lacks deadline")
	}
}

func TestUnknownStoreOutcomeRetainsKeyAndBlocksAnotherAttempt(t *testing.T) {
	f := credentialsFixture(t)
	f.store.mode = "unknown"
	f.advance(t)
	f.advance(t)
	f.advance(t)
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialStoreUnknown)
	if state.Phase != CredentialStoreRequested || len(f.transport.deletes) != 0 || len(f.transport.keyRequests) != 1 {
		t.Fatal("unknown store outcome revoked or recreated key")
	}
	request := f.request
	request.AttemptID = "another-attempt"
	_, err = f.manager.Advance(context.Background(), request)
	credentialError(t, err, CredentialAttemptConflict)
	assertNoMaterial(t, state, err, f.journal.history)
}

func TestCommittedLostStoreResponseResolvesExactCandidate(t *testing.T) {
	f := credentialsFixture(t)
	f.store.mode = "commit-response-lost"
	state := f.staged(t)
	if state.Candidate.KeyID != state.Key.ID || len(f.transport.deletes) != 0 || f.store.puts != 1 {
		t.Fatal("committed write was mistaken for absent write")
	}
	assertNoMaterial(t, state, f.journal.history)
}

func TestCanaryFailureCompensatesOnlyOwnedInactiveKey(t *testing.T) {
	f := credentialsFixture(t)
	f.staged(t)
	f.canary.fail = true
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialCanaryFailed)
	if state.FailureCode != CredentialCanaryFailed || state.Phase != CredentialCompensating {
		t.Fatal("canary failure lost original cause")
	}
	state = f.advance(t)
	if state.Phase != CredentialRevocationPending || len(f.transport.deletes) != 1 || f.store.deletes != 0 {
		t.Fatal("revocation request reported cleanup complete")
	}
	request := f.transport.deletes[0]
	if request.KeyId != state.Key.ID || ProviderVersionDigest(request.ResourceVersion) != state.Key.VersionDigest {
		t.Fatal("cleanup lacked exact key/version")
	}
	state = f.advance(t)
	if state.Phase != CredentialRevocationPending || len(f.transport.deletes) != 1 {
		t.Fatal("pending revoke was retried blindly")
	}
	f.transport.ops[request.AsyncOperationId].State = operation.AsyncOperation_STATE_FULFILLED
	delete(f.transport.keys, request.KeyId)
	state = f.advance(t)
	if state.Phase != CredentialRevoked {
		t.Fatal("fulfilled revocation not recognized")
	}
	state = f.advance(t)
	if state.Phase != CredentialAborted || state.FailureCode != CredentialCanaryFailed || f.store.deletes != 1 {
		t.Fatal("cleanup replaced failure or removed wrong candidate")
	}
	assertNoMaterial(t, state, err, f.journal.history)
}

func TestActiveCandidateBlocksCompensation(t *testing.T) {
	f := credentialsFixture(t)
	f.staged(t)
	f.canary.fail = true
	_, _ = f.manager.Advance(context.Background(), f.request)
	f.store.active = true
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialCleanupBlocked)
	if state.FailureCode != CredentialCanaryFailed || state.CleanupFailureCode != CredentialCleanupBlocked || len(f.transport.deletes) != 0 {
		t.Fatal("cleanup changed an active or unknown version")
	}
}

func TestLostKeyResponseRequiresReceiptAndUniqueOwnedMetadata(t *testing.T) {
	f := credentialsFixture(t)
	f.transport.lostKeyResponse = true
	f.advance(t)
	f.advance(t)
	f.advance(t)
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialKeyUnproven)
	if state.Phase != CredentialKeyRequested || f.store.puts != 0 {
		t.Fatal("lost token was fabricated or stored")
	}
	state = f.advance(t)
	if state.Phase != CredentialCompensating || state.FailureCode != CredentialMaterialLost || !state.Key.Verified || len(f.transport.keyRequests) != 1 {
		t.Fatal("lost key was recreated or guessed")
	}
	if len(f.transport.keyLists) != 1 || f.transport.keyLists[0].OwnerId != state.ServiceAccount.ID {
		t.Fatal("recovery used unfiltered identity access")
	}
	f.advance(t)
	if len(f.transport.deletes) != 1 || f.transport.deletes[0].KeyId != state.Key.ID {
		t.Fatal("verified orphan key not targeted exactly")
	}
	assertNoMaterial(t, state, err, f.journal.history)
}

func TestAmbiguousLostKeyRecoveryNeverRevokesGuessedKeys(t *testing.T) {
	f := credentialsFixture(t)
	f.transport.lostKeyResponse = true
	f.transport.duplicateLostKey = true
	f.advance(t)
	f.advance(t)
	f.advance(t)
	_, _ = f.manager.Advance(context.Background(), f.request)
	_, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialKeyUnproven)
	if len(f.transport.deletes) != 0 || f.store.puts != 0 {
		t.Fatal("ambiguous display/provenance match caused mutation")
	}
}

func TestLostServiceAccountResponseReusesOnlyExactCreateRequest(t *testing.T) {
	f := credentialsFixture(t)
	f.transport.lostSAResponse = true
	f.advance(t)
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialSAUnproven)
	if state.Phase != CredentialSARequested {
		t.Fatal("safe SA intent not persisted before create")
	}
	f.transport.lostSAResponse = false
	state = f.advance(t)
	if state.Phase != CredentialSAKnown || len(f.transport.saRequests) != 2 || !proto.Equal(f.transport.saRequests[0], f.transport.saRequests[1]) {
		t.Fatal("SA retry changed owner/request/async identity")
	}
}

func TestForeignServiceAccountOrKeyCannotBeUsedOrCompensated(t *testing.T) {
	f := credentialsFixture(t)
	f.advance(t)
	f.advance(t)
	f.transport.sa.Spec.Access = &identity.Access{}
	_, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialOwnership)
	if len(f.transport.keyRequests) != 0 {
		t.Fatal("account-scoped SA minted a runtime key")
	}
	f = credentialsFixture(t)
	f.transport.foreignKey = true
	f.advance(t)
	f.advance(t)
	f.advance(t)
	_, _ = f.manager.Advance(context.Background(), f.request)
	_, err = f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialOwnership)
	if len(f.transport.deletes) != 0 || f.store.puts != 0 {
		t.Fatal("foreign key was staged or revoked")
	}
}

func TestJournalFailureAfterCreateRetainsSafeRecoveryAndNoMaterial(t *testing.T) {
	f := credentialsFixture(t)
	f.advance(t)
	f.advance(t)
	f.advance(t)
	f.journal.failPhase = CredentialStoreRequested
	f.journal.commitFailure = true
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialJournalUnknown)
	if state.Key.ID == "" || f.store.puts != 0 {
		t.Fatal("failed journal crossed into untracked secret write")
	}
	f.store.mode = "rejected"
	state, err = f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialStoreRejected)
	if state.Phase != CredentialCompensating || len(f.transport.keyRequests) != 1 {
		t.Fatal("recovery recreated a one-time token")
	}
	assertNoMaterial(t, state, err, f.journal.history)
}

func TestDisabledAndInvalidCredentialRequestsMakeNoCredentialCalls(t *testing.T) {
	f := credentialsFixture(t)
	f.manager.config.Capabilities = CredentialCapabilities{}
	_, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialDisabled)
	if len(f.base.fake.calls) != 0 {
		t.Fatal("disabled credentials capability made a provider call")
	}
	f = credentialsFixture(t)
	request := f.request
	request.Namespace.NamespaceID = "production.acct1"
	_, err = f.manager.Advance(context.Background(), request)
	credentialError(t, err, CredentialInvalid)
	if len(f.base.fake.calls) != 0 {
		t.Fatal("foreign namespace made a provider call")
	}
	request = f.request
	request.ExpiresAt = f.base.now.Add(31 * 24 * time.Hour)
	_, err = f.manager.Advance(context.Background(), request)
	credentialError(t, err, CredentialInvalid)
}

func TestActivationDelayDoesNotRevokeCandidateOrRunCanary(t *testing.T) {
	f := credentialsFixture(t)
	state := f.staged(t)
	f.transport.keys[state.Key.ID].State = resource.ResourceState_RESOURCE_STATE_ACTIVATING
	_, err := f.manager.Advance(context.Background(), f.request)
	typed := credentialError(t, err, CredentialProviderFailure)
	if !typed.Retryable || f.canary.calls != 0 || len(f.transport.deletes) != 0 {
		t.Fatal("activation delay was treated as a failed runtime credential")
	}
	f.transport.keys[state.Key.ID].State = resource.ResourceState_RESOURCE_STATE_ACTIVE
	if f.advance(t).Phase != CredentialVerified {
		t.Fatal("ready candidate did not resume")
	}
}

func TestProviderOrStoreCannotEchoMaterialIntoSafeReferenceFields(t *testing.T) {
	f := credentialsFixture(t)
	f.transport.tokenAsID = true
	f.advance(t)
	f.advance(t)
	f.advance(t)
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialKeyUnproven)
	assertNoMaterial(t, state, err, f.journal.history)
	f = credentialsFixture(t)
	f.store.mode = "leaky-reference"
	f.advance(t)
	f.advance(t)
	f.advance(t)
	state, err = f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialStoreUnknown)
	if state.Candidate.VersionID != "" {
		t.Fatal("untrusted version entered durable state")
	}
	assertNoMaterial(t, state, err, f.journal.history)
}

func TestVerifiedCandidateCannotBeReplacedWithoutRotationProtocol(t *testing.T) {
	f := credentialsFixture(t)
	f.staged(t)
	f.advance(t)
	next := f.request
	next.AttemptID = "attempt-two"
	_, err := f.manager.Advance(context.Background(), next)
	credentialError(t, err, CredentialAttemptConflict)
	if len(f.transport.keyRequests) != 1 || f.store.puts != 1 {
		t.Fatal("new attempt overwrote a verified candidate")
	}
}

func TestCompensationIsIdempotentForPreviouslyVerifiedAbsentKey(t *testing.T) {
	f := credentialsFixture(t)
	staged := f.staged(t)
	f.canary.fail = true
	_, _ = f.manager.Advance(context.Background(), f.request)
	delete(f.transport.keys, staged.Key.ID)
	state := f.advance(t)
	if state.Phase != CredentialRevoked || len(f.transport.deletes) != 0 {
		t.Fatal("verified absence was not treated as already revoked")
	}
	state = f.advance(t)
	if state.Phase != CredentialAborted || state.FailureCode != CredentialCanaryFailed {
		t.Fatal("idempotent compensation lost original failure")
	}
}

func TestMaterialEchoInOpaqueVersionOnlyPersistsDigest(t *testing.T) {
	f := credentialsFixture(t)
	f.transport.tokenAsVersion = true
	f.advance(t)
	f.advance(t)
	f.advance(t)
	state, err := f.manager.Advance(context.Background(), f.request)
	if err != nil || state.Phase != CredentialStaged || state.Key.VersionDigest != ProviderVersionDigest(sentinelToken) {
		t.Fatal("opaque native version was not safely digested")
	}
	assertNoMaterial(t, state, err, f.journal.history)
}
func TestMaterialEchoInJournalReturnedVersionIsNeverReturned(t *testing.T) {
	for _, phase := range []CredentialPhase{CredentialStoreRequested, CredentialStaged} {
		t.Run(string(phase), func(t *testing.T) {
			f := credentialsFixture(t)
			f.journal.echoVersionPhase = phase
			f.advance(t)
			f.advance(t)
			f.advance(t)
			state, err := f.manager.Advance(context.Background(), f.request)
			credentialError(t, err, CredentialJournalUnknown)
			assertNoMaterial(t, state, err, f.journal.history)
		})
	}
}
func TestCanaryMustAttestDuringCallBeforeExpiry(t *testing.T) {
	for _, delta := range []time.Duration{-time.Nanosecond, time.Nanosecond} {
		t.Run(delta.String(), func(t *testing.T) {
			f := credentialsFixture(t)
			f.staged(t)
			f.canary.now = f.base.now.Add(delta)
			state, err := f.manager.Advance(context.Background(), f.request)
			credentialError(t, err, CredentialCanaryFailed)
			if state.Phase != CredentialCompensating {
				t.Fatal("stale/future attestation accepted")
			}
		})
	}
	f := credentialsFixture(t)
	f.staged(t)
	// The fresh canary call crosses the exact key expiry boundary.
	f.canary.afterCheck = func() { f.manager.namespaces.now = func() time.Time { return f.request.ExpiresAt } }
	f.canary.now = f.request.ExpiresAt
	state, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialCanaryFailed)
	if state.Phase != CredentialCompensating {
		t.Fatal("expired candidate verified")
	}
}
