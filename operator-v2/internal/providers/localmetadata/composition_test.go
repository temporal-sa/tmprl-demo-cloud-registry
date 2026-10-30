package localmetadata

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awstypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudidentity "go.temporal.io/cloud-sdk/api/identity/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const compositionMaterial = "COMPOSITION-ONE-TIME-MATERIAL-NO-DURABLE-COPY-7b23"

// All transports/guards/canary below are owned synchronous test doubles. They
// provide neither real provider permissions/encryption nor active-reference,
// distributed writer, runtime drain or production canary guarantees.
type compositionCloud struct {
	tc.Transport
	tc.CredentialTransport
	config                                                                                        Config
	key                                                                                           *cloudidentity.ApiKey
	ops                                                                                           map[string]*operation.AsyncOperation
	creates, deletes, lists                                                                       int
	lostCreate, ambiguousCreate, missingInput, tokenVersion, saTokenVersion, initialKeyActivating bool
	emptyToken, badKeyObservation                                                                 bool
}

func (c *compositionCloud) GetNamespace(_ context.Context, r *cloudservice.GetNamespaceRequest, _ ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error) {
	if r.Namespace != c.config.NamespaceID {
		return nil, status.Error(codes.PermissionDenied, compositionMaterial)
	}
	name := strings.TrimSuffix(r.Namespace, ".testaccount")
	return &cloudservice.GetNamespaceResponse{Namespace: &namespace.Namespace{Namespace: r.Namespace, Spec: &namespace.NamespaceSpec{Name: name, Replicas: []*namespace.ReplicaSpec{{Region: "aws-us-east-2"}}, RetentionDays: 1, ApiKeyAuth: &namespace.ApiKeyAuthSpec{Enabled: true}}, CreatedTime: timestamppb.New(c.config.NamespaceCreatedAt), ResourceVersion: "namespace-v1", State: resource.ResourceState_RESOURCE_STATE_ACTIVE}}, nil
}
func (c *compositionCloud) GetServiceAccount(_ context.Context, r *cloudservice.GetServiceAccountRequest, _ ...grpc.CallOption) (*cloudservice.GetServiceAccountResponse, error) {
	sa := c.config.PreprovisionedServiceAccount
	if r.ServiceAccountId != sa.ID {
		return nil, status.Error(codes.PermissionDenied, compositionMaterial)
	}
	version := "sa-v1"
	if c.saTokenVersion {
		version = compositionMaterial
	}
	return &cloudservice.GetServiceAccountResponse{ServiceAccount: &cloudidentity.ServiceAccount{Id: sa.ID, Spec: &cloudidentity.ServiceAccountSpec{Name: sa.Name, Description: "registry-sa:" + digest(struct {
		Owner       any
		Incarnation string
	}{c.config.Ownership.Owner(), c.config.NamespaceIncarnationID}), NamespaceScopedAccess: &cloudidentity.NamespaceScopedAccess{Namespace: c.config.NamespaceID, Access: &cloudidentity.NamespaceAccess{Permission: cloudidentity.NamespaceAccess_PERMISSION_WRITE}}}, CreatedTime: timestamppb.New(sa.CreatedAt), ResourceVersion: version, State: resource.ResourceState_RESOURCE_STATE_ACTIVE}}, nil
}
func (c *compositionCloud) fulfilled(id string, request proto.Message) *operation.AsyncOperation {
	input, _ := anypb.New(request)
	if c.missingInput {
		input = nil
	}
	return &operation.AsyncOperation{Id: id, State: operation.AsyncOperation_STATE_FULFILLED, OperationInput: input, StartedTime: timestamppb.New(testNow.Add(-time.Second)), FinishedTime: timestamppb.New(testNow.Add(time.Second))}
}
func (c *compositionCloud) CreateApiKey(_ context.Context, r *cloudservice.CreateApiKeyRequest, _ ...grpc.CallOption) (*cloudservice.CreateApiKeyResponse, error) {
	c.creates++
	if r.Spec.OwnerId != c.config.PreprovisionedServiceAccount.ID || r.Spec.OwnerType != cloudidentity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT {
		return nil, status.Error(codes.PermissionDenied, compositionMaterial)
	}
	version := "key-v1"
	if c.tokenVersion {
		version = compositionMaterial
	}
	c.key = &cloudidentity.ApiKey{Id: "owned-composition-key", Spec: proto.Clone(r.Spec).(*cloudidentity.ApiKeySpec), CreatedTime: timestamppb.New(testNow), ResourceVersion: version, State: resource.ResourceState_RESOURCE_STATE_ACTIVE}
	if c.initialKeyActivating {
		c.key.State = resource.ResourceState_RESOURCE_STATE_ACTIVATING
	}
	c.ops[r.AsyncOperationId] = c.fulfilled(r.AsyncOperationId, r)
	if c.lostCreate {
		return nil, status.Error(codes.Unavailable, compositionMaterial)
	}
	token := compositionMaterial
	if c.emptyToken {
		token = ""
	}
	return &cloudservice.CreateApiKeyResponse{KeyId: c.key.Id, Token: token, AsyncOperation: proto.Clone(c.ops[r.AsyncOperationId]).(*operation.AsyncOperation)}, nil
}
func (c *compositionCloud) GetApiKey(_ context.Context, r *cloudservice.GetApiKeyRequest, _ ...grpc.CallOption) (*cloudservice.GetApiKeyResponse, error) {
	if c.key == nil || r.KeyId != c.key.Id {
		return nil, status.Error(codes.NotFound, compositionMaterial)
	}
	key := proto.Clone(c.key).(*cloudidentity.ApiKey)
	if c.badKeyObservation {
		key.Spec.OwnerId = "foreign-sa-observation"
	}
	return &cloudservice.GetApiKeyResponse{ApiKey: key}, nil
}
func (c *compositionCloud) GetApiKeys(_ context.Context, r *cloudservice.GetApiKeysRequest, _ ...grpc.CallOption) (*cloudservice.GetApiKeysResponse, error) {
	c.lists++
	if r.OwnerId != c.config.PreprovisionedServiceAccount.ID || r.OwnerType != cloudidentity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT || r.PageToken != "" || r.PageSize != 100 {
		return nil, status.Error(codes.PermissionDenied, compositionMaterial)
	}
	out := &cloudservice.GetApiKeysResponse{}
	if c.key != nil {
		out.ApiKeys = append(out.ApiKeys, proto.Clone(c.key).(*cloudidentity.ApiKey))
		if c.ambiguousCreate {
			other := proto.Clone(c.key).(*cloudidentity.ApiKey)
			other.Id = "different-owned-matching-key"
			out.ApiKeys = append(out.ApiKeys, other)
		}
	}
	return out, nil
}
func (c *compositionCloud) GetAsyncOperation(_ context.Context, r *cloudservice.GetAsyncOperationRequest, _ ...grpc.CallOption) (*cloudservice.GetAsyncOperationResponse, error) {
	op := c.ops[r.AsyncOperationId]
	if op == nil {
		return nil, status.Error(codes.NotFound, compositionMaterial)
	}
	return &cloudservice.GetAsyncOperationResponse{AsyncOperation: proto.Clone(op).(*operation.AsyncOperation)}, nil
}
func (c *compositionCloud) DeleteApiKey(_ context.Context, r *cloudservice.DeleteApiKeyRequest, _ ...grpc.CallOption) (*cloudservice.DeleteApiKeyResponse, error) {
	if c.key == nil || r.KeyId != c.key.Id || r.ResourceVersion != c.key.ResourceVersion {
		return nil, status.Error(codes.PermissionDenied, compositionMaterial)
	}
	c.deletes++
	c.key.State = resource.ResourceState_RESOURCE_STATE_DELETED
	c.ops[r.AsyncOperationId] = c.fulfilled(r.AsyncOperationId, r)
	return &cloudservice.DeleteApiKeyResponse{AsyncOperation: proto.Clone(c.ops[r.AsyncOperationId]).(*operation.AsyncOperation)}, nil
}

type compositionAWS struct {
	config               Config
	kms                  string
	metadata             *secretsmanager.DescribeSecretOutput
	material             []byte
	creates              int
	lostCreate, wrongKMS bool
	validatedPayload     bool
}

func (a *compositionAWS) CreateSecret(_ context.Context, r *secretsmanager.CreateSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	a.creates++
	a.validatedPayload = bytes.Equal(r.SecretBinary, []byte(compositionMaterial)) && r.SecretString == nil && aws.ToString(r.KmsKeyId) == a.kms && hex64.MatchString(aws.ToString(r.ClientRequestToken))
	a.material = append([]byte(nil), r.SecretBinary...)
	if !a.validatedPayload {
		return nil, errors.New(compositionMaterial)
	}
	scope, _ := a.config.Ownership.Scope(ownership.AWS)
	arn := "arn:" + scope.Partition + ":secretsmanager:" + scope.Region + ":" + scope.AccountID + ":secret:" + aws.ToString(r.Name) + "-AbC123"
	born := testNow.Add(time.Second)
	key := a.kms
	if a.wrongKMS {
		key = "arn:aws:kms:us-west-2:123456789012:key/22222222-2222-4222-8222-222222222222"
	}
	a.metadata = &secretsmanager.DescribeSecretOutput{ARN: aws.String(arn), Name: aws.String(aws.ToString(r.Name)), KmsKeyId: aws.String(key), CreatedDate: &born, Tags: append([]awstypes.Tag(nil), r.Tags...), VersionIdsToStages: map[string][]string{aws.ToString(r.ClientRequestToken): {"AWSCURRENT"}}}
	if a.lostCreate {
		return nil, errors.New(compositionMaterial)
	}
	return &secretsmanager.CreateSecretOutput{ARN: aws.String(arn), Name: aws.String(aws.ToString(r.Name)), VersionId: aws.String(aws.ToString(r.ClientRequestToken))}, nil
}
func (a *compositionAWS) DescribeSecret(_ context.Context, r *secretsmanager.DescribeSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	if a.metadata == nil {
		return nil, &awstypes.ResourceNotFoundException{Message: aws.String(compositionMaterial)}
	}
	if aws.ToString(r.SecretId) != aws.ToString(a.metadata.ARN) && aws.ToString(r.SecretId) != aws.ToString(a.metadata.Name) {
		return nil, errors.New(compositionMaterial)
	}
	copy := *a.metadata
	copy.Tags = append([]awstypes.Tag(nil), a.metadata.Tags...)
	copy.VersionIdsToStages = map[string][]string{}
	for id, stages := range a.metadata.VersionIdsToStages {
		copy.VersionIdsToStages[id] = append([]string(nil), stages...)
	}
	return &copy, nil
}

type compositionNamespaceGuard struct{ config Config }

func (g compositionNamespaceGuard) Run(ctx context.Context, id string, w ownership.Writer, fn func(context.Context) error) error {
	if id != g.config.NamespaceID || w.Owner != g.config.Ownership.Owner() || w.Epoch != 1 || !w.ExpiresAt.After(testNow) {
		return errors.New(compositionMaterial)
	}
	return fn(ctx)
}

type compositionCandidateGuard struct {
	config Config
	active bool
}

func (g *compositionCandidateGuard) Run(ctx context.Context, w tc.CandidateWrite, fn func(context.Context) error) error {
	if g.active || w.Intent.Owner != g.config.Ownership.Owner() || w.Intent.NamespaceID != g.config.NamespaceID || w.Intent.NamespaceIncarnationID != g.config.NamespaceIncarnationID {
		return errors.New(compositionMaterial)
	}
	return fn(ctx)
}

type compositionCanary struct {
	fail  bool
	cloud *compositionCloud
	aws   *compositionAWS
	calls int
}

func (c *compositionCanary) Check(_ context.Context, r tc.SecretVersionRef) (tc.CanaryAttestation, error) {
	c.calls++
	if c.fail || c.aws.metadata == nil || c.cloud.key == nil || !bytes.Equal(c.aws.material, []byte(compositionMaterial)) || r.KeyID != c.cloud.key.Id || len(c.aws.metadata.VersionIdsToStages[r.VersionID]) != 1 {
		return tc.CanaryAttestation{}, errors.New(compositionMaterial)
	}
	return tc.CanaryAttestation{NamespaceIncarnationID: r.NamespaceIncarnationID, KeyID: r.KeyID, VersionID: r.VersionID, Passed: true, VerifiedAt: testNow}, nil
}

type composition struct {
	backend *Store
	binding Binding
	root    *os.Root
	cloud   *compositionCloud
	aws     *compositionAWS
	guard   *compositionCandidateGuard
	canary  *compositionCanary
	manager *tc.CredentialManager
	request tc.StageRequest
}

func composed(t *testing.T) *composition {
	t.Helper()
	backend, binding, root, _ := testStore(t, 128)
	cfg := backend.config
	c := &composition{backend: backend, binding: binding, root: root, cloud: &compositionCloud{config: cfg, ops: map[string]*operation.AsyncOperation{}}}
	c.aws = &compositionAWS{config: cfg, kms: "arn:aws:kms:us-west-2:123456789012:key/11111111-1111-4111-8111-111111111111"}
	c.guard = &compositionCandidateGuard{config: cfg}
	c.canary = &compositionCanary{cloud: c.cloud, aws: c.aws}
	t.Cleanup(func() { clear(c.aws.material) })
	c.rebuild(t)
	return c
}
func (c *composition) rebuild(t *testing.T) {
	t.Helper()
	cfg := c.backend.config
	o := cfg.Ownership
	i, err := o.Intent(ownership.Temporal, "namespace", "metadata-target", "fixture-v1", "namespace-create", "fixture-fingerprint", 39)
	if err != nil {
		t.Fatal(err)
	}
	w := ownership.Writer{ID: "fixture-writer", Owner: o.Owner(), Epoch: 1, ExpiresAt: testNow.Add(time.Hour)}
	record, err := o.BindCreated(i, ownership.Observation{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: i.Name, ProviderID: cfg.NamespaceIncarnationID}, Owner: o.Owner(), CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: 1, Version: "namespace-v1"}, w, testNow)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := tc.New(tc.Config{Ownership: o, Bindings: []tc.Binding{{NamespaceID: cfg.NamespaceID, CreatedAt: cfg.NamespaceCreatedAt, ProvenanceID: "fixture-create-provenance", Record: record}}, AllowedRegions: []string{"aws-us-east-2"}, MaximumRetentionDays: 7, CallTimeout: time.Second, Guard: compositionNamespaceGuard{cfg}, Now: func() time.Time { return testNow }}, c.cloud)
	if err != nil {
		t.Fatal(err)
	}
	store, err := awsstore.New(awsstore.Config{Ownership: o, StoreID: cfg.StoreID, KMS: awsstore.KMSBinding{ARN: c.aws.kms, ProvenanceID: "fixture-kms-create", Owner: o.Owner()}, Namespaces: []awsstore.NamespaceBinding{{ID: cfg.NamespaceID, IncarnationID: cfg.NamespaceIncarnationID, Owner: o.Owner()}}, AllowCandidateCreate: true, CallTimeout: time.Second}, c.aws, c.backend.CredentialJournal(), c.backend.OutcomeLedger(), c.guard)
	if err != nil {
		t.Fatal(err)
	}
	c.manager, err = tc.NewCredentialManager(tc.CredentialConfig{Capabilities: tc.CredentialCapabilities{StageAPIKeys: true, VerifyCandidates: true, RevokeOwnedCandidates: true}, MaximumKeyTTL: 24 * time.Hour, MaximumRecoveryPages: 1, StoreID: cfg.StoreID, PreprovisionedServiceAccounts: []tc.ScopedServiceAccountBinding{{NamespaceID: cfg.NamespaceID, NamespaceIncarnationID: cfg.NamespaceIncarnationID, Owner: o.Owner(), Reference: cfg.PreprovisionedServiceAccount}}}, adapter, c.cloud, c.backend.CredentialJournal(), store, c.canary)
	if err != nil {
		t.Fatal(err)
	}
	c.request = tc.StageRequest{Namespace: tc.WriteRequest{NamespaceID: cfg.NamespaceID, ExpectedIncarnationID: cfg.NamespaceIncarnationID, ExpectedRevision: record.Revision, ExpectedVersion: "namespace-v1", OperationKey: "fixed-stage-operation", Writer: w}, AttemptID: "attempt-one", ExpiresAt: testNow.Add(time.Hour)}
}
func (c *composition) restart(t *testing.T) {
	t.Helper()
	next, err := Open(c.root, c.binding, c.backend.config)
	if err != nil {
		t.Fatal(err)
	}
	c.backend = next
	c.rebuild(t)
}
func (c *composition) advance(t *testing.T) tc.CredentialState {
	t.Helper()
	state, err := c.manager.Advance(ctx, c.request)
	if err != nil {
		t.Fatal("composed Advance failed: ", err)
	}
	return state
}
func (c *composition) privacy(t *testing.T) {
	t.Helper()
	f, err := c.root.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := f.ReadDir(300)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := c.root.ReadFile(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(compositionMaterial)) {
			t.Fatal("one-time material entered durable metadata")
		}
	}
}
func compositionCode(t *testing.T, err error, want tc.CredentialCode) {
	t.Helper()
	e, ok := err.(*tc.CredentialError)
	if !ok || e.Code != want || strings.Contains(err.Error(), compositionMaterial) {
		t.Fatal("unexpected unsafe composition diagnostic")
	}
}

func TestCompositionSuccessfulStageVerifyAndRestart(t *testing.T) {
	c := composed(t)
	c.advance(t)
	c.advance(t)
	state := c.advance(t)
	if state.Phase != tc.CredentialStaged || c.cloud.creates != 1 || c.aws.creates != 1 || !c.aws.validatedPayload {
		t.Fatal("actual adapters did not produce one exact staged candidate")
	}
	c.restart(t)
	state = c.advance(t)
	if state.Phase != tc.CredentialVerified || c.canary.calls != 1 || c.cloud.deletes != 0 || c.cloud.creates != 1 || c.aws.creates != 1 {
		t.Fatal("restart replayed create or skipped candidate canary")
	}
	c.privacy(t)
}

func TestCompositionLostAWSCreateReplyRetainsKeyAndBlocksNewAttempt(t *testing.T) {
	c := composed(t)
	c.aws.lostCreate = true
	c.advance(t)
	c.advance(t)
	state, err := c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialStoreUnknown)
	if state.Phase != tc.CredentialStoreRequested || c.aws.metadata == nil {
		t.Fatal("lost create outcome not retained")
	}
	c.restart(t)
	_, err = c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialStoreUnknown)
	other := c.request
	other.AttemptID = "attempt-two"
	_, err = c.manager.Advance(ctx, other)
	compositionCode(t, err, tc.CredentialAttemptConflict)
	if c.cloud.creates != 1 || c.aws.creates != 1 || c.cloud.deletes != 0 || c.canary.calls != 0 {
		t.Fatal("unknown write caused recreate/revoke/verify")
	}
	c.privacy(t)
}

func TestCompositionLostKeyReplyRequiresExactOwnedRecovery(t *testing.T) {
	c := composed(t)
	c.cloud.lostCreate = true
	c.advance(t)
	c.advance(t)
	_, err := c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialKeyUnproven)
	c.restart(t)
	state := c.advance(t)
	if state.Phase != tc.CredentialCompensating || state.FailureCode != tc.CredentialMaterialLost || c.cloud.lists != 1 || c.aws.creates != 0 {
		t.Fatal("lost key was recreated or storage fabricated")
	}
	c.advance(t)
	c.cloud.key = nil
	c.restart(t)
	c.advance(t)
	state = c.advance(t)
	if state.Phase != tc.CredentialAborted || c.cloud.deletes != 1 || c.cloud.creates != 1 {
		t.Fatal("verified owned orphan did not follow exact revocation path")
	}
	c.privacy(t)
}

func TestCompositionAmbiguousOrUnechoedKeyNeverRevoked(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "ambiguous", true: "missing-input"}[missing], func(t *testing.T) {
			c := composed(t)
			c.cloud.lostCreate = true
			c.cloud.ambiguousCreate = !missing
			c.cloud.missingInput = missing
			c.advance(t)
			c.advance(t)
			_, _ = c.manager.Advance(ctx, c.request)
			c.restart(t)
			_, err := c.manager.Advance(ctx, c.request)
			compositionCode(t, err, tc.CredentialKeyUnproven)
			if c.cloud.deletes != 0 || c.aws.creates != 0 || c.cloud.creates != 1 {
				t.Fatal("unproven ownership caused mutation")
			}
			c.privacy(t)
		})
	}
}

func TestCompositionOpaqueVersionTokenEchoAfterRestartDoesNotPersist(t *testing.T) {
	c := composed(t)
	c.cloud.tokenVersion = true
	c.advance(t)
	c.advance(t)
	state := c.advance(t)
	if state.Phase != tc.CredentialStaged || state.Key.VersionDigest != tc.ProviderVersionDigest(compositionMaterial) {
		t.Fatal("native opaque version was not safely digested")
	}
	c.privacy(t)
	c.restart(t)
	state = c.advance(t)
	if state.Phase != tc.CredentialVerified {
		t.Fatal("digest-only metadata did not resume")
	}
	c.privacy(t)
}

func TestCompositionServiceAccountNativeVersionOnlyPersistsDigest(t *testing.T) {
	c := composed(t)
	c.cloud.saTokenVersion = true
	c.advance(t)
	state := c.advance(t)
	if state.ServiceAccount.VersionDigest != tc.ProviderVersionDigest(compositionMaterial) {
		t.Fatal("service-account version was not safely digested")
	}
	c.privacy(t)
	c.restart(t)
	state = c.advance(t)
	if state.Phase != tc.CredentialStaged {
		t.Fatal("digest-only service account failed to resume")
	}
	c.privacy(t)
}

func TestCompositionCASUncertaintyPreservesProviderRestartBarriers(t *testing.T) {
	for _, target := range []struct {
		kind, phase     string
		expectedCreates int
	}{{"journal", string(tc.CredentialStoreRequested), 0}, {"outcome", string(awsstore.CreateReceipted), 1}, {"outcome", string(awsstore.MetadataVerified), 1}, {"journal", string(tc.CredentialStaged), 1}} {
		for _, point := range []string{"after-intent", "after-commit"} {
			t.Run(target.phase+"/"+point, func(t *testing.T) {
				c := composed(t)
				c.advance(t)
				c.advance(t)
				fired := false
				c.backend.fault = func(at string) error {
					if fired || at != point {
						return nil
					}
					v, err := c.backend.scan(ctx, true)
					if err != nil {
						t.Fatal("owned fault could not inspect its safe metadata boundary")
					}
					kind, phase := "", ""
					if v.pending != nil {
						kind = v.pending.Kind
						if v.pending.Journal != nil {
							phase = string(v.pending.Journal.Phase)
						}
						if v.pending.Outcome != nil {
							phase = string(v.pending.Outcome.Phase)
						}
					} else if len(v.history) > 0 {
						r := v.history[len(v.history)-1]
						kind = r.Kind
						if r.Journal != nil {
							phase = string(r.Journal.Phase)
						}
						if r.Outcome != nil {
							phase = string(r.Outcome.Phase)
						}
					}
					if kind == target.kind && phase == target.phase {
						fired = true
						return Uncertain
					}
					return nil
				}
				_, err := c.manager.Advance(ctx, c.request)
				if !fired || err == nil || c.cloud.creates != 1 || c.aws.creates != target.expectedCreates {
					t.Fatal("fault did not retain exact before-dispatch boundary")
				}
				pending, pendingErr := c.backend.Pending(ctx)
				if pendingErr != nil || pending.Digest == "" {
					t.Fatal("uncertain metadata intent lost")
				}
				c.privacy(t)
				c.restart(t)
				state, err := c.manager.Advance(ctx, c.request)
				if point == "after-intent" {
					compositionCode(t, err, tc.CredentialJournalUnknown)
					if err = c.backend.Recover(ctx, pending.Digest); err != nil {
						t.Fatal("exact metadata recovery failed")
					}
					state, err = c.manager.Advance(ctx, c.request)
				}
				if target.expectedCreates == 0 {
					compositionCode(t, err, tc.CredentialStoreUnknown)
					if state.Phase != tc.CredentialStoreRequested {
						t.Fatal("lost one-time material fabricated storage")
					}
				} else if err != nil || (state.Phase != tc.CredentialStaged && state.Phase != tc.CredentialVerified) {
					t.Fatal("known candidate receipt did not recover exact staged version")
				}
				if c.cloud.creates != 1 || c.aws.creates != target.expectedCreates || c.cloud.deletes != 0 {
					t.Fatal("uncertainty replayed provider creation or revoked key")
				}
				c.privacy(t)
			})
		}
	}
}

func TestCompositionWrongKMSOrActiveGuardNeverCompensatesUnknownStore(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "wrong-KMS", true: "fake-active-guard"}[active], func(t *testing.T) {
			c := composed(t)
			c.guard.active = active
			c.aws.wrongKMS = !active
			c.advance(t)
			c.advance(t)
			_, err := c.manager.Advance(ctx, c.request)
			compositionCode(t, err, tc.CredentialStoreUnknown)
			c.restart(t)
			_, err = c.manager.Advance(ctx, c.request)
			compositionCode(t, err, tc.CredentialStoreUnknown)
			if c.cloud.deletes != 0 || c.cloud.creates != 1 || c.canary.calls != 0 {
				t.Fatal("unproven storage became cleanup/verify authority")
			}
			c.privacy(t)
		})
	}
}

func TestCompositionUnsupportedCandidateDeleteRetainsOriginalFailure(t *testing.T) {
	c := composed(t)
	c.advance(t)
	c.advance(t)
	c.advance(t)
	c.canary.fail = true
	_, err := c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialCanaryFailed)
	c.advance(t)
	c.cloud.key = nil
	c.restart(t)
	state := c.advance(t)
	if state.Phase != tc.CredentialRevoked || c.cloud.deletes != 1 {
		t.Fatal("exact fulfilled deletion did not recover after restart")
	}
	state, err = c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialCleanupBlocked)
	if state.Phase != tc.CredentialRevoked || state.FailureCode != tc.CredentialCanaryFailed || state.Candidate.VersionID == "" || c.aws.metadata == nil {
		t.Fatal("unsupported store deletion was reported as completed cleanup")
	}
	c.privacy(t)
}

func TestCompositionInitialActivationDelayStagesMaterialWithoutPrematureCanary(t *testing.T) {
	c := composed(t)
	c.cloud.initialKeyActivating = true
	c.advance(t)
	c.advance(t)
	state := c.advance(t)
	if state.Phase != tc.CredentialStaged || c.aws.creates != 1 {
		t.Fatal("activation delay discarded available one-time material")
	}
	_, err := c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialProviderFailure)
	if c.canary.calls != 0 || c.cloud.deletes != 0 {
		t.Fatal("activation delay authorized canary or revocation")
	}
	c.cloud.key.State = resource.ResourceState_RESOURCE_STATE_ACTIVE
	if c.advance(t).Phase != tc.CredentialVerified {
		t.Fatal("activation did not resume verification")
	}
	c.privacy(t)
}

func TestCompositionVersionDriftCleanupCauseSurvivesReopen(t *testing.T) {
	c := composed(t)
	c.advance(t)
	c.advance(t)
	staged := c.advance(t)
	c.canary.fail = true
	_, err := c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialCanaryFailed)
	c.cloud.key.ResourceVersion = "independently-changed-native-version"
	state, err := c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialVersionChanged)
	if state.FailureCode != tc.CredentialCanaryFailed || state.CleanupFailureCode != tc.CredentialVersionChanged || state.Key.VersionDigest != staged.Key.VersionDigest || c.cloud.deletes != 0 {
		t.Fatal("cleanup drift replaced original failure or version precondition")
	}
	c.restart(t)
	retained, found, err := c.backend.CredentialJournal().Load(ctx, staged.Intent.SlotID)
	if err != nil || !found || retained.Phase != tc.CredentialCompensating || retained.FailureCode != tc.CredentialCanaryFailed || retained.CleanupFailureCode != tc.CredentialVersionChanged {
		t.Fatal("reopen lost original failure or proven cleanup blocker")
	}
	_, err = c.manager.Advance(ctx, c.request)
	compositionCode(t, err, tc.CredentialVersionChanged)
	if c.cloud.deletes != 0 {
		t.Fatal("reopen substituted a fresh native version")
	}
	c.privacy(t)
}
