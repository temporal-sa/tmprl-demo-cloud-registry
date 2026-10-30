package awssecrets

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

const sentinel = "ONE-TIME-SECRET-SENTINEL-NEVER-IN-STATE"

type journal struct {
	state tc.CredentialState
	err   error
}

func (j *journal) Load(context.Context, string) (tc.CredentialState, bool, error) {
	return j.state, true, j.err
}
func (*journal) CompareAndSwap(context.Context, string, string, tc.CredentialState) (tc.CredentialState, error) {
	panic("store must not mutate credential journal")
}

type ledger struct {
	entry         Outcome
	found         bool
	history       []Outcome
	failPhase     OutcomePhase
	commitFailure bool
}

func (l *ledger) Load(context.Context, string) (Outcome, bool, error) { return l.entry, l.found, nil }
func (l *ledger) CompareAndSwap(_ context.Context, _ string, version string, next Outcome) (Outcome, error) {
	if l.entry.Version != version {
		return Outcome{}, errors.New(sentinel)
	}
	if l.failPhase == next.Phase && !l.commitFailure {
		return Outcome{}, errors.New(sentinel)
	}
	next.Version = "safe-ledger-" + strconv.Itoa(len(l.history)+1)
	l.entry = next
	l.found = true
	l.history = append(l.history, next)
	if l.failPhase == next.Phase {
		return Outcome{}, errors.New(sentinel)
	}
	return next, nil
}

type guard struct {
	active, foreign bool
	calls           int
}

func (g *guard) Run(ctx context.Context, _ tc.CandidateWrite, fn func(context.Context) error) error {
	g.calls++
	if g.active || g.foreign {
		return errors.New(sentinel)
	}
	return fn(ctx)
}

type fakeClient struct {
	metadata       *secretsmanager.DescribeSecretOutput
	createCount    int
	lookups        []string
	request        *secretsmanager.CreateSecretInput
	payloadCorrect bool
	responseLost   bool
	responsePatch  func(*secretsmanager.CreateSecretOutput)
	metadataPatch  func(*secretsmanager.DescribeSecretOutput)
	readError      bool
}

func (f *fakeClient) CreateSecret(_ context.Context, r *secretsmanager.CreateSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	f.createCount++
	f.payloadCorrect = string(r.SecretBinary) == sentinel && r.SecretString == nil
	copy := *r
	copy.SecretBinary = nil
	f.request = &copy // fake records metadata only
	arn := "arn:aws:secretsmanager:us-east-2:123456789012:secret:" + aws.ToString(r.Name) + "-Ab3De6"
	now := time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC)
	f.metadata = &secretsmanager.DescribeSecretOutput{ARN: aws.String(arn), Name: r.Name, KmsKeyId: r.KmsKeyId, CreatedDate: &now, Tags: r.Tags, VersionIdsToStages: map[string][]string{aws.ToString(r.ClientRequestToken): {"AWSCURRENT"}}}
	if f.metadataPatch != nil {
		f.metadataPatch(f.metadata)
	}
	response := &secretsmanager.CreateSecretOutput{ARN: aws.String(arn), Name: r.Name, VersionId: r.ClientRequestToken}
	if f.responsePatch != nil {
		f.responsePatch(response)
	}
	if f.responseLost {
		return nil, errors.New(sentinel)
	}
	return response, nil
}
func (f *fakeClient) DescribeSecret(_ context.Context, r *secretsmanager.DescribeSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	f.lookups = append(f.lookups, aws.ToString(r.SecretId))
	if f.readError {
		return nil, errors.New(sentinel)
	}
	if f.metadata == nil {
		return nil, &types.ResourceNotFoundException{Message: aws.String(sentinel)}
	}
	return f.metadata, nil
}

type fixture struct {
	s       *Store
	client  *fakeClient
	journal *journal
	ledger  *ledger
	guard   *guard
	write   tc.CandidateWrite
}

func setup(t *testing.T) *fixture {
	t.Helper()
	owner := ownership.Owner{EnvironmentID: "lab", InstallationID: "sandbox", ProjectUID: "project-uid", Lifetime: ownership.Disposable, RunID: "run-one"}
	ctx, err := ownership.NewContext(ownership.Config{Owner: owner, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.AWS, Partition: "aws", AccountID: "123456789012", Region: "us-east-2"}, {Provider: ownership.Temporal, AccountID: "temporal-lab", Namespace: "lab-namespace"}}})
	if err != nil {
		t.Fatal(err)
	}
	stage := hash("stage")
	name, err := ctx.Path(ownership.Temporal, "api-key-candidate", stage)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	intent := tc.CredentialIntent{SlotID: hash("slot"), StageID: stage, Owner: owner, NamespaceID: "lab-namespace", NamespaceIncarnationID: "lab-namespace@birth-one", OperationKey: "operation-one", AttemptID: "attempt-one", ExpiresAt: now.Add(24 * time.Hour), CandidateID: name}
	key := tc.KeyRef{ID: "key-id", ServiceAccountID: "sa-id", CreatedAt: now, VersionDigest: tc.ProviderVersionDigest("key-v1"), CreationReceiptID: "create-receipt", Verified: true, State: "ACTIVE"}
	write := tc.CandidateWrite{Intent: intent, Key: key, WriteID: hash(struct{ Stage, Key string }{stage, key.ID})}
	j := &journal{state: tc.CredentialState{Version: "journal-v1", Intent: intent, Phase: tc.CredentialStoreRequested, ServiceAccount: tc.ServiceAccountRef{ID: "sa-id", CreatedAt: now, ProvenanceID: "sa-receipt"}, Key: key, KeyReceipt: tc.Receipt{ID: key.CreationReceiptID}, CandidateWriteID: write.WriteID}}
	client := &fakeClient{}
	l := &ledger{}
	g := &guard{}
	s, err := New(Config{Ownership: ctx, StoreID: "sandbox-encrypted-store", KMS: KMSBinding{ARN: "arn:aws:kms:us-east-2:123456789012:key/11111111-2222-3333-4444-555555555555", ProvenanceID: "fixture-kms", Owner: owner}, Namespaces: []NamespaceBinding{{ID: intent.NamespaceID, IncarnationID: intent.NamespaceIncarnationID, Owner: owner}}, AllowCandidateCreate: true, CallTimeout: time.Second}, client, j, l, g)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{s, client, j, l, g, write}
}
func (f *fixture) put() (tc.SecretVersionRef, error) {
	return f.s.put(context.Background(), f.write, func(use func([]byte) error) error { b := []byte(sentinel); defer clear(b); return use(b) })
}
func code(t *testing.T, err error, want Code) {
	t.Helper()
	typed, ok := err.(*Error)
	if !ok || typed.Code != want {
		t.Fatalf("got %v want %v", err, want)
	}
}
func safe(t *testing.T, values ...any) {
	t.Helper()
	for _, v := range values {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), sentinel) || strings.Contains(fmt.Sprintf("%#v", v), sentinel) {
			t.Fatal("material escaped secure request")
		}
	}
}

func TestCandidateCreateUsesExactNameKMSAndImmutableWriteToken(t *testing.T) {
	f := setup(t)
	ref, err := f.put()
	if err != nil {
		t.Fatal(err)
	}
	if !f.client.payloadCorrect || f.client.createCount != 1 || aws.ToString(f.client.request.ClientRequestToken) != f.write.WriteID || aws.ToString(f.client.request.Name) != f.write.Intent.CandidateID || aws.ToString(f.client.request.KmsKeyId) != f.s.config.KMS.ARN {
		t.Fatal("SDK request differs from exact approved intent")
	}
	if ref.VersionID != f.write.WriteID || ref.Active || !ref.Candidate || !ref.Encrypted {
		t.Fatal("invalid safe reference")
	}
	if f.ledger.entry.Phase != MetadataVerified || f.ledger.entry.CreatedAt.IsZero() {
		t.Fatal("independent birth/ARN not pinned")
	}
	if len(f.client.lookups) != 2 || f.client.lookups[1] != f.ledger.entry.ARN {
		t.Fatal("resolve must use canonical ARN, never rediscover by name")
	}
	safe(t, ref, f.ledger.history, f.client.request)
	// Retry uses the receipt without asking for material or issuing another write.
	_, err = f.s.put(context.Background(), f.write, func(func([]byte) error) error { t.Fatal("retry consumed material"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if f.client.createCount != 1 {
		t.Fatal("duplicate create")
	}
	code(t, f.s.DeleteCandidate(context.Background(), ref), DeletionUnsupported)
}

func TestPreexistingPerfectMetadataEchoCannotAuthorizeAdoption(t *testing.T) {
	f := setup(t)
	if _, err := f.put(); err != nil {
		t.Fatal(err)
	}
	f.ledger = &ledger{}
	f.s.ledger = f.ledger // remove independent create receipt
	_, err := f.put()
	code(t, err, OwnershipUnproven)
	if f.client.createCount != 1 || f.ledger.found {
		t.Fatal("echoed object was overwritten or adopted")
	}
	resolution, err := f.s.ResolveWrite(context.Background(), f.write)
	if err != nil || resolution.Outcome != tc.WriteUnknown {
		t.Fatal("unreceipted metadata became ownership proof")
	}
}

func TestLostCreateResponseAndLateResponseNeverBecomeFinalAbsence(t *testing.T) {
	f := setup(t)
	f.client.responseLost = true
	_, err := f.put()
	code(t, err, OutcomeUnknown)
	if f.ledger.entry.Phase != CreateRequested || f.client.metadata == nil {
		t.Fatal("fixture must commit before losing response")
	}
	for _, exists := range []bool{false, true} {
		saved := f.client.metadata
		if !exists {
			f.client.metadata = nil
		}
		resolution, err := f.s.ResolveWrite(context.Background(), f.write)
		if err != nil || resolution.Outcome != tc.WriteUnknown {
			t.Fatal("late/missing outcome was guessed")
		}
		f.client.metadata = saved
	}
	_, err = f.put()
	code(t, err, OutcomeUnknown)
	if f.client.createCount != 1 {
		t.Fatal("unknown create was blindly repeated")
	}
	safe(t, err, f.ledger.history)
}

func TestCommittedLedgerReceiptResolvesAfterResponseLoss(t *testing.T) {
	for _, phase := range []OutcomePhase{CreateReceipted, MetadataVerified} {
		t.Run(string(phase), func(t *testing.T) {
			f := setup(t)
			f.ledger.failPhase = phase
			f.ledger.commitFailure = true
			_, err := f.put()
			code(t, err, JournalUnknown)
			f.ledger.failPhase = ""
			resolution, err := f.s.ResolveWrite(context.Background(), f.write)
			if err != nil || resolution.Outcome != tc.WriteCommitted || resolution.Reference.VersionID != f.write.WriteID {
				t.Fatalf("receipt did not recover: %v %v", resolution, err)
			}
			safe(t, resolution, err, f.ledger.history)
		})
	}
	f := setup(t)
	f.ledger.failPhase = CreateReceipted
	_, err := f.put()
	code(t, err, JournalUnknown)
	f.ledger.failPhase = ""
	resolution, err := f.s.ResolveWrite(context.Background(), f.write)
	if err != nil || resolution.Outcome != tc.WriteUnknown {
		t.Fatal("unpersisted ARN was adopted from metadata")
	}
}

func TestIndependentAuthorizationAndActiveRefGuard(t *testing.T) {
	for name, alter := range map[string]func(*fixture){
		"owner":           func(f *fixture) { f.write.Intent.Owner.ProjectUID = "foreign" },
		"birth":           func(f *fixture) { f.write.Intent.NamespaceIncarnationID = "other-birth" },
		"candidate":       func(f *fixture) { f.write.Intent.CandidateID = "production/runtime" },
		"write-token":     func(f *fixture) { f.write.WriteID = hash("other-write") },
		"journal-attempt": func(f *fixture) { f.journal.state.Intent.AttemptID = "other-attempt" },
		"journal-write":   func(f *fixture) { f.journal.state.CandidateWriteID = hash("other") },
		"journal-key":     func(f *fixture) { f.journal.state.Key.ID = "foreign-key" },
		"journal-phase":   func(f *fixture) { f.journal.state.Phase = tc.CredentialVerified },
		"active":          func(f *fixture) { f.guard.active = true },
		"foreign-writer":  func(f *fixture) { f.guard.foreign = true },
		"disabled":        func(f *fixture) { f.s.config.AllowCandidateCreate = false },
	} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			alter(f)
			_, err := f.put()
			if err == nil || f.client.createCount != 0 || len(f.client.lookups) != 0 {
				t.Fatal("invalid authority reached AWS client")
			}
			safe(t, err, f.ledger.history)
		})
	}
}

func TestConflictingCreateResponseCannotPinProvenance(t *testing.T) {
	for name, patch := range map[string]func(*secretsmanager.CreateSecretOutput){
		"account": func(r *secretsmanager.CreateSecretOutput) {
			r.ARN = aws.String(strings.Replace(aws.ToString(r.ARN), "123456789012", "999999999999", 1))
		},
		"region": func(r *secretsmanager.CreateSecretOutput) {
			r.ARN = aws.String(strings.Replace(aws.ToString(r.ARN), "us-east-2", "us-west-2", 1))
		},
		"name-in-arn": func(r *secretsmanager.CreateSecretOutput) {
			r.ARN = aws.String("arn:aws:secretsmanager:us-east-2:123456789012:secret:production-Ab3De6")
		},
		"name":    func(r *secretsmanager.CreateSecretOutput) { r.Name = aws.String(sentinel) },
		"version": func(r *secretsmanager.CreateSecretOutput) { r.VersionId = aws.String(sentinel) },
	} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			f.client.responsePatch = patch
			_, err := f.put()
			code(t, err, OwnershipUnproven)
			if f.ledger.entry.ARN != "" {
				t.Fatal("bad provenance persisted")
			}
			safe(t, err, f.ledger.history)
		})
	}
}

func TestPinnedMetadataMustRemainExactAndNonActive(t *testing.T) {
	for name, patch := range map[string]func(*secretsmanager.DescribeSecretOutput){
		"kms":   func(r *secretsmanager.DescribeSecretOutput) { r.KmsKeyId = aws.String("alias/foreign") },
		"birth": func(r *secretsmanager.DescribeSecretOutput) { n := r.CreatedDate.Add(time.Second); r.CreatedDate = &n },
		"arn-incarnation": func(r *secretsmanager.DescribeSecretOutput) {
			r.ARN = aws.String(strings.TrimSuffix(aws.ToString(r.ARN), "Ab3De6") + "Ab3De7")
		},
		"name":          func(r *secretsmanager.DescribeSecretOutput) { r.Name = aws.String(sentinel) },
		"owner-tag":     func(r *secretsmanager.DescribeSecretOutput) { r.Tags[0].Value = aws.String(sentinel) },
		"duplicate-tag": func(r *secretsmanager.DescribeSecretOutput) { r.Tags[1] = r.Tags[0] },
		"version": func(r *secretsmanager.DescribeSecretOutput) {
			r.VersionIdsToStages = map[string][]string{hash("wrong"): {"AWSCURRENT"}}
		},
		"extra-version": func(r *secretsmanager.DescribeSecretOutput) {
			r.VersionIdsToStages[hash("wrong")] = []string{"AWSPREVIOUS"}
		},
		"rotating": func(r *secretsmanager.DescribeSecretOutput) { r.RotationEnabled = aws.Bool(true) },
		"deleting": func(r *secretsmanager.DescribeSecretOutput) { r.DeletedDate = r.CreatedDate },
	} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			if _, err := f.put(); err != nil {
				t.Fatal(err)
			}
			patch(f.client.metadata)
			result, err := f.s.ResolveWrite(context.Background(), f.write)
			code(t, err, MetadataConflict)
			if result.Outcome != tc.WriteUnknown {
				t.Fatal("invalid metadata accepted")
			}
			safe(t, result, err, f.ledger.history)
		})
	}
	f := setup(t)
	if _, err := f.put(); err != nil {
		t.Fatal(err)
	}
	f.guard.active = true
	_, err := f.s.ResolveWrite(context.Background(), f.write)
	code(t, err, AuthorizationDenied)
}

func TestNoAmbientOrInsecureSDKConfiguration(t *testing.T) {
	credentials, err := NewCredentials("EXPLICIT-ACCESS", sentinel, "SESSION")
	if err != nil {
		t.Fatal(err)
	}
	base := ConnectionOptions{Endpoint: "https://secretsmanager.us-east-2.amazonaws.com", Region: "us-east-2", Credentials: credentials, Timeout: time.Second}
	client, err := NewSDKClient(base)
	if err != nil {
		t.Fatal(err)
	}
	opts := client.(*secretsmanager.Client).Options()
	if opts.ClientLogMode != 0 || opts.Retryer.MaxAttempts() != 1 || opts.Region != base.Region || aws.ToString(opts.BaseEndpoint) != base.Endpoint {
		t.Fatal("unsafe SDK defaults")
	}
	if _, ok := opts.Credentials.(Credentials); !ok {
		t.Fatal("ambient credential chain")
	}
	safe(t, credentials)
	for _, mutate := range []func(*ConnectionOptions){func(o *ConnectionOptions) { o.Credentials = Credentials{} }, func(o *ConnectionOptions) { o.Region = "" }, func(o *ConnectionOptions) { o.Endpoint = "" }, func(o *ConnectionOptions) { o.Endpoint = "http://localhost" }, func(o *ConnectionOptions) { o.Endpoint = "https://user:pass@example.com" }, func(o *ConnectionOptions) { o.Endpoint += "?token=bad" }, func(o *ConnectionOptions) { o.TLSConfig = &tls.Config{InsecureSkipVerify: true} }, func(o *ConnectionOptions) { o.Timeout = 0 }} {
		options := base
		mutate(&options)
		_, err := NewSDKClient(options)
		code(t, err, InvalidConfiguration)
	}
}

func TestUncheckedMutableKeyEchoIsNotStoreProvenance(t *testing.T) {
	f := setup(t)
	// Identity/birth/create receipt are independently journal-verified. Mutable
	// caller versions are unnecessary for this write and must not enter ledger.
	f.write.Key.VersionDigest = sentinel
	f.write.Key.State = sentinel
	ref, err := f.put()
	if err != nil {
		t.Fatal(err)
	}
	safe(t, ref, err, f.ledger.history, f.client.request)
	if f.ledger.entry.Write.Key.VersionDigest != "" || f.ledger.entry.Write.Key.State != "" {
		t.Fatal("unchecked mutable fields became durable provenance")
	}
}

func TestPublicStoreBoundaryRejectsUnavailableMaterial(t *testing.T) {
	f := setup(t)
	_, err := f.s.PutCandidate(context.Background(), f.write, tc.KeyMaterial{})
	code(t, err, OutcomeUnknown)
	if f.client.createCount != 0 {
		t.Fatal("unavailable material reached SDK")
	}
	safe(t, err, f.ledger.history)
}
