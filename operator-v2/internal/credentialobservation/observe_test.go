package credentialobservation

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awstypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	enums "go.temporal.io/api/enums/v1"
	namespaceinfo "go.temporal.io/api/namespace/v1"
	workflow "go.temporal.io/api/workflowservice/v1"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const sentinel = "OBSERVATION-ONLY-SYNTHETIC-KEY-MUST-NOT-ESCAPE-6f23"

var at = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
var bg = context.Background()

type records struct {
	state   tc.CredentialState
	outcome awsstore.Outcome
	reads   int
	change  bool
	fail    bool
}

func (r *records) Load(_ context.Context, key string) (tc.CredentialState, bool, error) {
	r.reads++
	if r.fail {
		return tc.CredentialState{}, false, errors.New(sentinel)
	}
	v := r.state
	if r.change && r.reads > 1 {
		v.Version = hash("changed")
	}
	return v, key == v.Intent.SlotID, nil
}

type outcomeReader struct{ records *records }

func (r outcomeReader) Load(_ context.Context, key string) (awsstore.Outcome, bool, error) {
	return r.records.outcome, key == r.records.outcome.Write.WriteID, nil
}

type guard struct{ duplicate, deny bool }

func (g *guard) Run(ctx context.Context, _ tc.SecretVersionRef, fn func(context.Context) error) error {
	if g.deny {
		return errors.New(sentinel)
	}
	e := fn(ctx)
	if g.duplicate {
		_ = fn(ctx)
	}
	return e
}

type secrets struct {
	metadata         *secretsmanager.DescribeSecretOutput
	value            *secretsmanager.GetSecretValueOutput
	reads, describes int
	fail             bool
	input            *secretsmanager.GetSecretValueInput
	lastBytes        []byte
	noRetries        bool
}

func (s *secrets) DescribeSecret(_ context.Context, r *secretsmanager.DescribeSecretInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	s.describes++
	if aws.ToString(r.SecretId) != aws.ToString(s.metadata.ARN) {
		return nil, errors.New(sentinel)
	}
	if s.fail {
		return nil, errors.New(sentinel)
	}
	return s.metadata, nil
}
func (s *secrets) GetSecretValue(_ context.Context, r *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	s.reads++
	s.input = r
	o := secretsmanager.Options{}
	for _, fn := range opts {
		fn(&o)
	}
	s.noRetries = o.Retryer != nil && o.Retryer.MaxAttempts() == 1
	if s.fail {
		return nil, errors.New(sentinel)
	}
	value := *s.value
	value.SecretBinary = append([]byte(nil), s.value.SecretBinary...)
	s.lastBytes = value.SecretBinary
	return &value, nil
}

type fakeProbes struct {
	cloudReads
	dataReads
	identity                                         *cloud.GetCurrentIdentityResponse
	namespace                                        *namespace.Namespace
	data                                             *workflow.DescribeNamespaceResponse
	identityCalls, namespaceCalls, dataCalls, closed int
	fail, changedBirth                               bool
	afterData                                        func()
}

func (p *fakeProbes) GetCurrentIdentity(context.Context, *cloud.GetCurrentIdentityRequest, ...grpc.CallOption) (*cloud.GetCurrentIdentityResponse, error) {
	p.identityCalls++
	if p.fail {
		return nil, errors.New(sentinel)
	}
	return proto.Clone(p.identity).(*cloud.GetCurrentIdentityResponse), nil
}
func (p *fakeProbes) GetNamespace(_ context.Context, r *cloud.GetNamespaceRequest, _ ...grpc.CallOption) (*cloud.GetNamespaceResponse, error) {
	p.namespaceCalls++
	if r.Namespace != p.namespace.Namespace {
		return nil, errors.New(sentinel)
	}
	n := proto.Clone(p.namespace).(*namespace.Namespace)
	if p.changedBirth && p.namespaceCalls > 1 {
		n.CreatedTime = timestamppb.New(n.CreatedTime.AsTime().Add(time.Second))
	}
	return &cloud.GetNamespaceResponse{Namespace: n}, nil
}
func (p *fakeProbes) DescribeNamespace(_ context.Context, r *workflow.DescribeNamespaceRequest, _ ...grpc.CallOption) (*workflow.DescribeNamespaceResponse, error) {
	p.dataCalls++
	if r.Namespace != p.namespace.Namespace || r.WeakConsistency || r.Id != "" {
		return nil, errors.New(sentinel)
	}
	if p.afterData != nil {
		p.afterData()
	}
	return proto.Clone(p.data).(*workflow.DescribeNamespaceResponse), nil
}

type fixture struct {
	observer *Observer
	config   Config
	records  *records
	secrets  *secrets
	probes   *fakeProbes
	guard    *guard
	ref      tc.SecretVersionRef
	key      *material
}

func must[T any](v T, e error) T {
	if e != nil {
		panic(e)
	}
	return v
}
func ca(t *testing.T) []byte {
	t.Helper()
	key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-offline-test-ca"}, NotBefore: at.Add(-time.Hour), NotAfter: at.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der := must(x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key))
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func setup(t *testing.T) *fixture {
	t.Helper()
	o := must(ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "observation-tests", ProjectUID: "project-one", Lifetime: ownership.Disposable, RunID: "run-one"}, ResourceRoot: "rv2", Scopes: []ownership.Scope{{Provider: ownership.Temporal, AccountID: "testacct", Namespace: "fixture"}, {Provider: ownership.AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-2"}}}))
	i := must(o.Intent(ownership.Temporal, "namespace", "primary", "fixture-v1", "create-namespace", "namespace-fingerprint", 39))
	id := i.Name + ".testacct"
	born := at.Add(-time.Hour)
	w := ownership.Writer{ID: "fixture-writer", Owner: o.Owner(), Epoch: 1, ExpiresAt: at.Add(time.Hour)}
	r := must(o.BindCreated(i, ownership.Observation{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: i.Name, ProviderID: tc.IncarnationID(id, born)}, Owner: o.Owner(), CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: 1, Version: "namespace-v1"}, w, at))
	b := tc.Binding{NamespaceID: id, CreatedAt: born, ProvenanceID: "fixture-namespace-create", Record: r}
	slot := hash(struct {
		Owner       any
		Incarnation string
	}{o.Owner(), r.Ref.ProviderID})
	sa := tc.ServiceAccountRef{ID: "owned-sa", Name: must(o.Name(ownership.Temporal, "service-account", slot, 63)), CreatedAt: at.Add(-5 * time.Minute), VersionDigest: tc.ProviderVersionDigest("sa-v1"), ProvenanceID: "fixture-sa-create"}
	records := &records{}
	// Actual pure Prepare derives the accepted stage/name contract; no provider
	// method is called by these explicit embedded no-I/O transport interfaces.
	noIO := struct {
		tc.Transport
		tc.CredentialTransport
		tc.SecureCandidateStore
	}{}
	adapter := must(tc.New(tc.Config{Ownership: o, Bindings: []tc.Binding{b}, CallTimeout: time.Second, Now: func() time.Time { return at }}, noIO))
	manager := must(tc.NewCredentialManager(tc.CredentialConfig{FailurePolicy: tc.DisposableRetainOnFailure, MaximumKeyTTL: time.Hour, MaximumRecoveryPages: 1, StoreID: "owned-store", PreprovisionedServiceAccounts: []tc.ScopedServiceAccountBinding{{NamespaceID: id, NamespaceIncarnationID: r.Ref.ProviderID, Owner: o.Owner(), Reference: sa}}}, adapter, noIO, journalOnly{}, noIO, nil))
	intent := must(manager.Prepare(tc.StageRequest{Namespace: tc.WriteRequest{NamespaceID: id, ExpectedIncarnationID: r.Ref.ProviderID, ExpectedRevision: r.Revision, OperationKey: "stage-op", Writer: w}, AttemptID: "attempt-one", ExpiresAt: at.Add(time.Hour)}))
	key := tc.KeyRef{ID: "owned-key", ServiceAccountID: sa.ID, CreatedAt: at.Add(-time.Minute), VersionDigest: tc.ProviderVersionDigest("key-v1"), CreationReceiptID: "owned-key-create-receipt", Verified: true, State: resource.ResourceState_RESOURCE_STATE_ACTIVE.String()}
	write := hash(struct{ Stage, Key string }{intent.StageID, key.ID})
	ref := tc.SecretVersionRef{StoreID: "owned-store", CandidateID: intent.CandidateID, VersionID: write, WriteID: write, Owner: o.Owner(), NamespaceIncarnationID: r.Ref.ProviderID, ServiceAccountID: sa.ID, KeyID: key.ID, Encrypted: true, Candidate: true}
	records.state = tc.CredentialState{Version: hash("journal-version"), Intent: intent, Phase: tc.CredentialStaged, ServiceAccount: sa, Key: key, Candidate: ref, CandidateWriteID: write, KeyReceipt: tc.Receipt{ID: key.CreationReceiptID, NamespaceID: id, Kind: "api-key-create", OperationKey: intent.StageID, InputFingerprint: hash("key-spec")}}
	k := key
	k.VersionDigest = ""
	k.State = ""
	arn := "arn:aws:secretsmanager:us-west-2:123456789012:secret:" + intent.CandidateID + "-AbC123"
	records.outcome = awsstore.Outcome{Version: hash("outcome-version"), Write: tc.CandidateWrite{Intent: intent, Key: k, WriteID: write}, Phase: awsstore.MetadataVerified, ARN: arn, CreatedAt: at.Add(-30 * time.Second)}
	cert := ca(t)
	cfg := Config{Ownership: o, Namespace: b, ProjectID: "owned-project-id", StoreID: ref.StoreID, KMSARN: "arn:aws:kms:us-west-2:123456789012:key/11111111-1111-4111-8111-111111111111", TrustBindingID: "reviewed-fixture-trust", Cloud: Endpoint{Address: "cloud.example.test:443", ServerName: "cloud.example.test", RootCAPEM: cert}, Data: Endpoint{Address: "data.example.test:443", ServerName: "data.example.test", RootCAPEM: cert}, Timeout: time.Second, Now: func() time.Time { return at }}
	tags := []awstypes.Tag{}
	for k, v := range map[string]string{"registry:owner": hash(intent.Owner), "registry:stage": intent.StageID, "registry:namespace": hash(intent.NamespaceIncarnationID), "registry:key": hash(struct{ SA, Key string }{sa.ID, key.ID}), "registry:write": write} {
		tags = append(tags, awstypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	secrets := &secrets{metadata: &secretsmanager.DescribeSecretOutput{ARN: aws.String(arn), Name: aws.String(intent.CandidateID), KmsKeyId: aws.String(cfg.KMSARN), CreatedDate: aws.Time(records.outcome.CreatedAt), Tags: tags, VersionIdsToStages: map[string][]string{write: {"AWSCURRENT"}}}, value: &secretsmanager.GetSecretValueOutput{ARN: aws.String(arn), Name: aws.String(intent.CandidateID), VersionId: aws.String(write), CreatedDate: aws.Time(at.Add(-29 * time.Second)), SecretBinary: []byte(sentinel)}}
	fake := &fakeProbes{identity: &cloud.GetCurrentIdentityResponse{Principal: &cloud.GetCurrentIdentityResponse_ServiceAccount{ServiceAccount: &identity.ServiceAccount{Id: sa.ID, CreatedTime: timestamppb.New(sa.CreatedAt), ResourceVersion: "sa-current", State: resource.ResourceState_RESOURCE_STATE_ACTIVE, Spec: &identity.ServiceAccountSpec{Name: sa.Name, Description: intent.ServiceAccountMarker, NamespaceScopedAccess: &identity.NamespaceScopedAccess{Namespace: id, Access: &identity.NamespaceAccess{Permission: identity.NamespaceAccess_PERMISSION_WRITE}}}}}, PrincipalApiKey: &identity.ApiKey{Id: key.ID, CreatedTime: timestamppb.New(key.CreatedAt), ResourceVersion: "key-current", State: resource.ResourceState_RESOURCE_STATE_ACTIVE, Spec: &identity.ApiKeySpec{OwnerId: sa.ID, OwnerType: identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT, DisplayName: intent.KeyDisplayName, Description: intent.KeyMarker, ExpiryTime: timestamppb.New(intent.ExpiresAt)}}}, namespace: &namespace.Namespace{Namespace: id, CreatedTime: timestamppb.New(born), ResourceVersion: "namespace-current", State: resource.ResourceState_RESOURCE_STATE_ACTIVE, ProjectId: cfg.ProjectID, Spec: &namespace.NamespaceSpec{Name: i.Name, ApiKeyAuth: &namespace.ApiKeyAuthSpec{Enabled: true}}}, data: &workflow.DescribeNamespaceResponse{NamespaceInfo: &namespaceinfo.NamespaceInfo{Name: id, Id: "observed-data-plane-id", State: enums.NAMESPACE_STATE_REGISTERED}}}
	f := &fixture{config: cfg, records: records, secrets: secrets, probes: fake, guard: &guard{}, ref: ref}
	f.observer = must(configured(cfg, records, outcomeReader{records}, secrets, f.guard, func(_ Config, key *material, _ time.Time) (*probes, error) {
		f.key = key
		return &probes{cloud: fake, data: fake, close: func() { fake.closed++ }}, nil
	}))
	return f
}

type journalOnly struct{}

func (journalOnly) Load(context.Context, string) (tc.CredentialState, bool, error) {
	panic("Prepare must not read")
}
func (journalOnly) CompareAndSwap(context.Context, string, string, tc.CredentialState) (tc.CredentialState, error) {
	panic("Prepare must not write")
}

func checkError(t *testing.T, e error, code Code) {
	t.Helper()
	v, ok := e.(*Error)
	if !ok || v.Code != code || strings.Contains(e.Error(), sentinel) {
		t.Fatalf("unexpected diagnostic: %v", e)
	}
}
func TestExactVersionObservationHasFiniteEvidenceAndClosesMaterial(t *testing.T) {
	f := setup(t)
	proof, e := f.observer.Observe(bg, f.ref)
	if e != nil {
		t.Fatal(e)
	}
	if proof.Schema != 1 || proof.Contract != Contract || proof.KeyID != f.ref.KeyID || proof.VersionID != f.ref.WriteID || proof.DataPlaneIDHash != hash(f.probes.data.NamespaceInfo.Id) || f.probes.namespaceCalls != 2 || f.probes.dataCalls != 1 || f.probes.identityCalls != 1 || f.probes.closed != 1 {
		t.Fatal("incomplete finite observation")
	}
	if f.secrets.input.VersionStage != nil || aws.ToString(f.secrets.input.VersionId) != f.ref.WriteID || aws.ToString(f.secrets.input.SecretId) != f.records.outcome.ARN || !f.secrets.noRetries {
		t.Fatal("secret fetch was not exact/attempt bounded")
	}
	if f.key == nil || !f.key.private.closed || len(f.key.private.bytes) != 0 || !bytes.Equal(f.secrets.lastBytes, make([]byte, len(f.secrets.lastBytes))) {
		t.Fatal("activity material not cleared")
	}
	raw, _ := json.Marshal(proof)
	if bytes.Contains(raw, []byte(sentinel)) {
		t.Fatal("material escaped result")
	}
	if f.records.state.Phase != tc.CredentialStaged {
		t.Fatal("observation changed credential phase")
	}
}

func TestUnusedVersionDateDoesNotChangeExactResourceVersionAuthority(t *testing.T) {
	for _, date := range []*time.Time{nil, aws.Time(time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC))} {
		f := setup(t)
		f.secrets.value.CreatedDate = date
		proof, e := f.observer.Observe(bg, f.ref)
		if e != nil {
			t.Fatal("unused descriptive version date changed authority")
		}
		if proof.ObservedAt != at || proof.VersionID != f.ref.WriteID || aws.ToString(f.secrets.input.SecretId) != f.records.outcome.ARN || aws.ToString(f.secrets.input.VersionId) != f.ref.WriteID {
			t.Fatal("version date influenced exact binding or observation clock")
		}
		f = setup(t)
		f.secrets.value.CreatedDate = date
		f.secrets.metadata.CreatedDate = aws.Time(at)
		if _, e = f.observer.Observe(bg, f.ref); e == nil || f.secrets.reads != 0 {
			t.Fatal("ignored version date weakened independent resource birth")
		}
	}
}

func TestMetadataAuthorityFailuresNeverFetchMaterial(t *testing.T) {
	for name, change := range map[string]func(*fixture){
		"unknown": func(f *fixture) { f.records.fail = true }, "phase": func(f *fixture) { f.records.state.Phase = tc.CredentialCompensating }, "write": func(f *fixture) { f.ref.WriteID = hash("foreign") }, "key": func(f *fixture) { f.records.state.Key.ID = "foreign" }, "policy": func(f *fixture) { f.records.state.Intent.FailurePolicy = "" }, "active": func(f *fixture) { f.ref.Active = true }, "receipt": func(f *fixture) { f.records.outcome.Phase = awsstore.CreateRequested }, "owner": func(f *fixture) { f.records.state.Intent.Owner.RunID = "foreign" }, "raw-version": func(f *fixture) { f.records.state.Version = sentinel },
	} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			change(f)
			if _, e := f.observer.Observe(bg, f.ref); e == nil {
				t.Fatal("foreign/unknown metadata admitted")
			}
			if f.secrets.reads != 0 || f.probes.identityCalls != 0 {
				t.Fatal("unproven metadata exposed material")
			}
		})
	}
}

func TestSecretIdentityAndPayloadNegativeCases(t *testing.T) {
	for name, change := range map[string]func(*fixture){
		"birth": func(f *fixture) { f.secrets.metadata.CreatedDate = aws.Time(at) }, "kms": func(f *fixture) { f.secrets.metadata.KmsKeyId = aws.String("foreign") }, "tags": func(f *fixture) { f.secrets.metadata.Tags = nil }, "deleted": func(f *fixture) { f.secrets.metadata.DeletedDate = aws.Time(at) }, "version": func(f *fixture) { f.secrets.value.VersionId = aws.String(hash("other")) }, "arn": func(f *fixture) { f.secrets.value.ARN = aws.String("foreign") }, "name": func(f *fixture) { f.secrets.value.Name = aws.String("foreign") }, "empty": func(f *fixture) { f.secrets.value.SecretBinary = nil }, "large": func(f *fixture) { f.secrets.value.SecretBinary = bytes.Repeat([]byte("x"), 65537) }, "string": func(f *fixture) { f.secrets.value.SecretString = aws.String(sentinel) }, "header-injection": func(f *fixture) { f.secrets.value.SecretBinary = []byte(sentinel + "\n") }, "unknown": func(f *fixture) { f.secrets.fail = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			change(f)
			if _, e := f.observer.Observe(bg, f.ref); e == nil {
				t.Fatal("invalid secret admitted")
			}
			if f.probes.identityCalls != 0 {
				t.Fatal("invalid secret reached candidate connection")
			}
		})
	}
}

func TestCandidateIdentityNamespaceAndFreshnessFailuresRetainStage(t *testing.T) {
	for name, change := range map[string]func(*fixture){
		"user": func(f *fixture) {
			f.probes.identity.Principal = &cloud.GetCurrentIdentityResponse_User{User: &identity.User{Id: "admin"}}
		}, "missing-key": func(f *fixture) { f.probes.identity.PrincipalApiKey = nil }, "wrong-key": func(f *fixture) { f.probes.identity.PrincipalApiKey.Id = "wrong" }, "disabled": func(f *fixture) { f.probes.identity.PrincipalApiKey.Spec.Disabled = true }, "inactive": func(f *fixture) {
			f.probes.identity.PrincipalApiKey.State = resource.ResourceState_RESOURCE_STATE_ACTIVATING
		}, "broad-access": func(f *fixture) { f.probes.identity.GetServiceAccount().Spec.Access = &identity.Access{} }, "sa-birth": func(f *fixture) { f.probes.identity.GetServiceAccount().CreatedTime = timestamppb.New(at) }, "cloud-birth": func(f *fixture) { f.probes.changedBirth = true }, "project": func(f *fixture) { f.probes.namespace.ProjectId = "foreign" }, "data-name": func(f *fixture) { f.probes.data.NamespaceInfo.Name = "foreign" }, "data-id": func(f *fixture) { f.probes.data.NamespaceInfo.Id = "" }, "expected-id": func(f *fixture) { f.observer.private.config.ExpectedDataPlaneID = "different-expected" }, "data-state": func(f *fixture) { f.probes.data.NamespaceInfo.State = enums.NAMESPACE_STATE_DELETED }, "metadata-race": func(f *fixture) { f.records.change = true }, "expired": func(f *fixture) {
			f.probes.afterData = func() { f.observer.private.config.Now = func() time.Time { return at.Add(2 * time.Second) } }
		}, "raw-error": func(f *fixture) { f.probes.fail = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			change(f)
			_, e := f.observer.Observe(bg, f.ref)
			if e == nil || strings.Contains(e.Error(), sentinel) {
				t.Fatal("unsafe/invalid observation accepted")
			}
			if f.records.state.Phase != tc.CredentialStaged || f.probes.closed != 1 || f.key == nil || !f.key.private.closed {
				t.Fatal("failure changed phase or retained live material")
			}
		})
	}
}

func TestInheritedAuthorityDuplicateGuardAndOpaqueEchoes(t *testing.T) {
	for _, header := range []string{"authorization", "temporal-namespace", "temporal-cloud-api-version", "x-api-key"} {
		t.Run(header, func(t *testing.T) {
			f := setup(t)
			ctx := metadata.NewOutgoingContext(bg, metadata.Pairs(header, "unrelated-admin"))
			_, e := f.observer.Observe(ctx, f.ref)
			checkError(t, e, Denied)
			if f.secrets.reads != 0 {
				t.Fatal("inherited authority fetched candidate")
			}
		})
	}
	f := setup(t)
	f.guard.duplicate = true
	_, e := f.observer.Observe(bg, f.ref)
	checkError(t, e, Denied)
	f = setup(t)
	f.probes.identity.PrincipalApiKey.ResourceVersion = sentinel
	f.probes.identity.GetServiceAccount().ResourceVersion = sentinel
	f.probes.namespace.ResourceVersion = sentinel
	f.probes.data.NamespaceInfo.Id = sentinel
	proof, e := f.observer.Observe(bg, f.ref)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(proof)
	if bytes.Contains(raw, []byte(sentinel)) {
		t.Fatal("opaque provider echo escaped observation")
	}
	for _, value := range []any{*f.observer, f.observer, *f.key, f.key, struct{ Key material }{*f.key}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%p", "%#p", "%d", "%T", "%t", "%b", "%c", "%o", "%O", "%U", "%e", "%E", "%f", "%F", "%g", "%G", "%w", "%20v", "%-20v", "%020v"} {
			if strings.Contains(fmt.Sprintf(format, value), sentinel) {
				t.Fatal("opaque holder leaked formatting")
			}
		}
		if _, e := json.Marshal(value); e == nil {
			t.Fatal("private activity holder serialized")
		}
	}
}

func TestSafeFailureCodesPreservePermissionAndCancellationMeaning(t *testing.T) {
	for _, test := range []struct {
		err  error
		code Code
	}{
		{status.Error(codes.PermissionDenied, sentinel), AuthorizationRejected},
		{status.Error(codes.Unauthenticated, sentinel), AuthenticationRejected},
		{status.Error(codes.Unimplemented, sentinel), EvidenceUnsupported},
		{errors.New(sentinel), ReadUnknown},
		{context.Canceled, Canceled}, {context.DeadlineExceeded, Deadline},
	} {
		e := readFailure(bg, test.err, Identity)
		checkError(t, e, test.code)
	}
	f := setup(t)
	ctx, cancel := context.WithCancel(bg)
	cancel()
	_, e := f.observer.Observe(ctx, f.ref)
	checkError(t, e, Canceled)
	if f.secrets.reads != 0 || f.records.reads != 0 {
		t.Fatal("canceled observation read provider/metadata")
	}
}
