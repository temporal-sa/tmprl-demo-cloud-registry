package awspermission

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/aws/smithy-go"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

const bootstrapKey = "ASIA0000000000000000"
const bootstrapSecret = "synthetic-bootstrap-secret-000000000000000000000"
const bootstrapToken = "synthetic-bootstrap-session-000000000000000000000"
const readerKey = "ASIA1111111111111111"
const readerSecret = "synthetic-reader-secret-1111111111111111111111111"
const readerToken = "synthetic-reader-session-1111111111111111111111111"

type memoryLog struct {
	requested, finished   map[Step]Receipt
	failBegin, failFinish Step
}

func (m *memoryLog) begin(_ context.Context, r Receipt) error {
	if r.Step == m.failBegin {
		return Unproven
	}
	if _, ok := m.requested[r.Step]; ok {
		return AlreadyAttempted
	}
	m.requested[r.Step] = r
	return nil
}
func (m *memoryLog) finish(_ context.Context, r Receipt) error {
	if r.Step == m.failFinish {
		return Unproven
	}
	m.finished[r.Step] = r
	return nil
}

type fakeFixture struct {
	t            *testing.T
	now          time.Time
	config       Config
	bootstrap    Credentials
	log          *memoryLog
	calls        []Step
	fail         Step
	failure      error
	mutate       func(Step, any)
	bodies       [][]byte
	outputSecret *secretsmanager.GetSecretValueOutput
	outputKey    *kms.GenerateDataKeyOutput
	outputAssume *sts.AssumeRoleOutput
	sourceClosed bool
}

func fixture(t *testing.T, phase Phase) *fakeFixture {
	t.Helper()
	f := &fakeFixture{t: t, now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), log: &memoryLog{requested: map[Step]Receipt{}, finished: map[Step]Receipt{}}}
	n, e := fixtureplan.ReadProofResourceNames()
	if e != nil {
		t.Fatal(e)
	}
	f.config = Config{Phase: phase, Bindings: fixtureplan.ReadProofBindings{AllowSecretARN: "arn:aws:secretsmanager:" + fixtureplan.Region + ":" + fixtureplan.Account + ":secret:" + n.Secret + "-AbC123", AllowSecretCreatedAt: f.now.Add(-time.Minute), AllowSecretCreationReceiptSHA256: hash("create-secret"), ReaderRoleID: "AROA0123456789ABCDEFG", ReaderRoleCreatedAt: f.now.Add(-time.Minute), ReaderCreationReceiptSHA256: hash("create-reader"), TrapRoleID: "AROA1234567890ABCDEFG", TrapRoleCreatedAt: f.now.Add(-time.Minute), TrapCreationReceiptSHA256: hash("create-trap")}, SourceAdminRoleID: "AROA2345678901ABCDEFG", FixtureExpiresAt: f.now.Add(time.Hour), DenyFixtureExpiresAt: f.now.Add(2 * time.Hour), ApprovedUntil: f.now.Add(10 * time.Minute), Now: func() time.Time { return f.now }}
	if phase == Probe {
		f.config.Bindings.SeedReceiptSHA256 = hash("all-three-seeds")
	}
	f.config.ApprovalSHA256 = ConfigurationDigest(f.config)
	f.bootstrap, e = newCredentials(bootstrapKey, bootstrapSecret, bootstrapToken, f.now.Add(time.Hour), "bootstrap", f.config.Now, f.config.ApprovedUntil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.bootstrap.Close)
	return f
}
func (f *fakeFixture) factory(m Credentials, _ Config) (clients, error) {
	if m.private.kind == "reader" && !f.sourceClosed {
		f.t.Fatal("scoped clients exist before source transport closed")
	}
	return clients{secret: f, sts: f, kms: f, close: func() {
		if m.private.kind == "bootstrap" {
			f.sourceClosed = true
		}
	}}, nil
}
func (f *fakeFixture) enter(ctx context.Context) (Step, error) {
	p, ok := ctx.Value(operationKey{}).(*operation)
	if !ok || p == nil || !p.live.Load() {
		f.t.Fatal("SDK dispatched without private operation")
	}
	m := Credentials{p.material}
	if _, e := m.Retrieve(ctx); e != nil {
		f.t.Fatal("private SDK authority denied")
	}
	f.calls = append(f.calls, p.step)
	f.now = f.now.Add(time.Second)
	if p.step == f.fail {
		return p.step, f.failure
	}
	return p.step, nil
}
func (f *fakeFixture) change(s Step, v any) {
	if f.mutate != nil {
		f.mutate(s, v)
	}
}
func (f *fakeFixture) GetCallerIdentity(ctx context.Context, _ *sts.GetCallerIdentityInput, opts ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	o := sts.Options{}
	for _, fn := range opts {
		fn(&o)
	}
	if _, ok := o.Retryer.(aws.NopRetryer); !ok {
		f.t.Fatal("STS retry enabled")
	}
	s, e := f.enter(ctx)
	if e != nil {
		return nil, e
	}
	n, _ := fixtureplan.ReadProofResourceNames()
	role, arn := f.config.SourceAdminRoleID, "arn:aws:sts::"+fixtureplan.Account+":assumed-role/"+fixtureplan.ProvisionerRole[strings.LastIndex(fixtureplan.ProvisionerRole, "/")+1:]+"/bootstrap"
	if s == ReaderIdentity {
		role = f.config.Bindings.ReaderRoleID
		arn = "arn:aws:sts::" + fixtureplan.Account + ":assumed-role/" + n.ReaderRole + "/" + ReaderSessionName
	}
	r := &sts.GetCallerIdentityOutput{Account: aws.String(fixtureplan.Account), Arn: aws.String(arn), UserId: aws.String(role + ":" + arn[strings.LastIndex(arn, "/")+1:])}
	f.change(s, r)
	return r, nil
}
func (f *fakeFixture) AssumeRole(ctx context.Context, in *sts.AssumeRoleInput, opts ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	s, e := f.enter(ctx)
	if e != nil {
		return nil, e
	}
	n, _ := fixtureplan.ReadProofResourceNames()
	target := n.ReaderRole
	session := ReaderSessionName
	if s == AssumeTrap {
		target = n.TrapRole
		session = TrapSessionName
	}
	if aws.ToString(in.RoleArn) != "arn:aws:iam::"+fixtureplan.Account+":role/"+target || aws.ToString(in.RoleSessionName) != session || aws.ToInt32(in.DurationSeconds) != 900 || in.Policy != nil || len(in.PolicyArns) != 0 || len(in.Tags) != 0 {
		f.t.Fatal("unreviewed AssumeRole request")
	}
	if s == AssumeTrap && f.fail != AssumeTrap {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: readerSecret}
	}
	if s == AssumeReader && aws.ToString(in.ExternalId) != fixtureplan.ReadProofID {
		f.t.Fatal("missing exact external binding")
	}
	r := &sts.AssumeRoleOutput{AssumedRoleUser: &ststypes.AssumedRoleUser{Arn: aws.String("arn:aws:sts::" + fixtureplan.Account + ":assumed-role/" + n.ReaderRole + "/" + ReaderSessionName), AssumedRoleId: aws.String(f.config.Bindings.ReaderRoleID + ":" + ReaderSessionName)}, Credentials: &ststypes.Credentials{AccessKeyId: aws.String(readerKey), SecretAccessKey: aws.String(readerSecret), SessionToken: aws.String(readerToken), Expiration: aws.Time(f.now.Add(15 * time.Minute))}}
	f.outputAssume = r
	f.change(s, r)
	return r, nil
}
func (f *fakeFixture) DescribeSecret(ctx context.Context, in *secretsmanager.DescribeSecretInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	s, e := f.enter(ctx)
	if e != nil {
		return nil, e
	}
	deny := aws.ToString(in.SecretId) == fixtureplan.DenySecretARN
	arn, key, born, expiry := f.config.Bindings.AllowSecretARN, fixtureplan.BaseCandidateKeyARN, f.config.Bindings.AllowSecretCreatedAt, f.config.FixtureExpiresAt
	if deny {
		arn = fixtureplan.DenySecretARN
		key = fixtureplan.DenyKeyARN
		born, _ = time.Parse(time.RFC3339Nano, "2026-10-30T02:18:00.749Z")
		expiry = f.config.DenyFixtureExpiresAt
	}
	if aws.ToString(in.SecretId) != arn {
		f.t.Fatal("unreviewed metadata target")
	}
	name := strings.SplitN(arn, ":secret:", 2)[1]
	name = name[:len(name)-7]
	owner, _ := fixtureplan.Context()
	tags := map[string]string{"RegistryFixture": "registry-operator-v2", "RegistryOwner": hash(owner.Owner()), "RegistryRun": fixtureplan.RunID, "RegistryExpiresAt": expiry.UTC().Format(time.RFC3339Nano)}
	if deny {
		tags["RegistrySentinel"] = "deny"
	} else {
		tags["RegistryProof"] = fixtureplan.ReadProofID
	}
	r := &secretsmanager.DescribeSecretOutput{ARN: aws.String(arn), Name: aws.String(name), KmsKeyId: aws.String(key), CreatedDate: aws.Time(born), VersionIdsToStages: map[string][]string{}}
	for k, v := range tags {
		r.Tags = append(r.Tags, smtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	if f.config.Phase == Probe {
		for _, seed := range fixtureplan.ReadProofSeeds() {
			if deny == (seed.Target == "deny") {
				r.VersionIdsToStages[seed.VersionID] = []string{seed.Stage}
			}
		}
	}
	f.change(s, r)
	return r, nil
}
func (f *fakeFixture) PutSecretValue(ctx context.Context, in *secretsmanager.PutSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error) {
	s, e := f.enter(ctx)
	f.bodies = append(f.bodies, in.SecretBinary)
	if e != nil {
		return nil, e
	}
	i := map[Step]int{SeedAllowV1: 0, SeedAllowV2: 1, SeedDenyV1: 2}[s]
	seed := fixtureplan.ReadProofSeeds()[i]
	arn := f.config.Bindings.AllowSecretARN
	if i == 2 {
		arn = fixtureplan.DenySecretARN
	}
	if aws.ToString(in.SecretId) != arn || aws.ToString(in.ClientRequestToken) != seed.VersionID || len(in.VersionStages) != 1 || in.VersionStages[0] != seed.Stage || bytesHash(in.SecretBinary) != seed.BodySHA256 || in.SecretString != nil {
		f.t.Fatal("unreviewed seed request")
	}
	r := &secretsmanager.PutSecretValueOutput{ARN: aws.String(arn), VersionId: aws.String(seed.VersionID), VersionStages: []string{seed.Stage}}
	f.change(s, r)
	return r, nil
}
func (f *fakeFixture) GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	s, e := f.enter(ctx)
	if e != nil {
		return nil, e
	}
	i := map[Step]int{ReadV1: 0, ReadV2: 1, ReadDeny: 2}[s]
	seed := fixtureplan.ReadProofSeeds()[i]
	arn := f.config.Bindings.AllowSecretARN
	if i == 2 {
		arn = fixtureplan.DenySecretARN
	}
	if aws.ToString(in.SecretId) != arn || aws.ToString(in.VersionId) != seed.VersionID || in.VersionStage != nil {
		f.t.Fatal("unpinned GetSecretValue request")
	}
	if i != 0 && f.fail != s {
		return nil, &smithy.GenericAPIError{Code: "AccessDeniedException", Message: readerSecret}
	}
	body, _ := fixtureplan.ReadProofSeedBody(seed.Label)
	r := &secretsmanager.GetSecretValueOutput{ARN: aws.String(arn), VersionId: aws.String(seed.VersionID), SecretBinary: body}
	f.outputSecret = r
	f.change(s, r)
	return r, nil
}
func (f *fakeFixture) GenerateDataKey(ctx context.Context, in *kms.GenerateDataKeyInput, opts ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error) {
	s, e := f.enter(ctx)
	if e != nil {
		return nil, e
	}
	if aws.ToString(in.KeyId) != fixtureplan.DenyKeyARN || string(in.KeySpec) != "AES_256" || in.EncryptionContext["SecretARN"] != fixtureplan.DenySecretARN {
		f.t.Fatal("unreviewed KMS target")
	}
	if f.fail != s {
		return nil, &smithy.GenericAPIError{Code: "AccessDeniedException", Message: readerSecret}
	}
	r := &kms.GenerateDataKeyOutput{Plaintext: []byte(readerSecret), CiphertextBlob: []byte("inert-ciphertext")}
	f.outputKey = r
	return r, nil
}
func (f *fakeFixture) run() (Result, error) {
	h, e := configured(f.config, f.bootstrap, f.log, f.factory)
	if e != nil {
		return Result{}, e
	}
	return h.Run(context.Background())
}
func assertPrivate(t *testing.T, v any) {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range []string{bootstrapKey, bootstrapSecret, bootstrapToken, readerKey, readerSecret, readerToken} {
		if strings.Contains(string(b), s) {
			t.Fatal("private material escaped")
		}
	}
}

func TestSeedIsThreeFixedOneShotWritesWithSafeReceipts(t *testing.T) {
	f := fixture(t, Seed)
	h, e := configured(f.config, f.bootstrap, f.log, f.factory)
	if e != nil {
		t.Fatal(e)
	}
	r, e := h.Run(context.Background())
	if e != nil || r.Outcome != "SeededSyntheticVersions" || len(r.Steps) != 6 || len(f.calls) != 6 {
		t.Fatalf("seed failed: %v %+v", e, r)
	}
	assertPrivate(t, r)
	assertPrivate(t, f.log.requested)
	assertPrivate(t, f.log.finished)
	for _, b := range f.bodies {
		for _, v := range b {
			if v != 0 {
				t.Fatal("seed buffer retained")
			}
		}
	}
	if _, e = h.Run(context.Background()); e != AlreadyAttempted {
		t.Fatal("helper replay allowed")
	}
}
func TestProbeUsesClosedBootstrapThenPositiveAndRealOwnedNegatives(t *testing.T) {
	f := fixture(t, Probe)
	r, e := f.run()
	if e != nil || r.Outcome != "ScopedEffectiveCallsMatched" || len(r.Steps) != 11 {
		t.Fatalf("probe failed: %v %+v", e, r)
	}
	expected := []Step{SourceIdentity, BootstrapAllowMetadata, BootstrapDenyMetadata, AssumeReader, ReaderIdentity, AllowMetadata, ReadV1, ReadV2, ReadDeny, GenerateDenyKey, AssumeTrap}
	for i, s := range expected {
		if f.calls[i] != s || r.Steps[i].Persistence != "Acknowledged" {
			t.Fatal("call order or receipt acknowledgment changed")
		}
	}
	if f.outputSecret.SecretBinary != nil || f.outputAssume.Credentials.AccessKeyId != nil {
		t.Fatal("SDK response retained secret material")
	}
	assertPrivate(t, r)
	assertPrivate(t, f.log.requested)
	assertPrivate(t, f.log.finished)
}
func TestUnsafeNativeMetadataStopsBeforeAssumption(t *testing.T) {
	for _, kind := range []string{"identity", "name", "kms", "birth", "version", "tag"} {
		t.Run(kind, func(t *testing.T) {
			f := fixture(t, Probe)
			f.mutate = func(s Step, v any) {
				if kind == "identity" && s == SourceIdentity {
					v.(*sts.GetCallerIdentityOutput).UserId = aws.String("foreign:session")
				}
				if s != BootstrapAllowMetadata {
					return
				}
				r := v.(*secretsmanager.DescribeSecretOutput)
				switch kind {
				case "name":
					r.Name = aws.String("foreign")
				case "kms":
					r.KmsKeyId = aws.String(fixtureplan.DenyKeyARN)
				case "birth":
					r.CreatedDate = aws.Time(f.now)
				case "version":
					delete(r.VersionIdsToStages, fixtureplan.ReadProofSeeds()[1].VersionID)
				case "tag":
					r.Tags = nil
				}
			}
			r, e := f.run()
			if e == nil || len(f.calls) > 2 {
				t.Fatal("unsafe native observation reached role assumption")
			}
			assertPrivate(t, r)
		})
	}
}
func TestUnknownSeedOrPersistenceStopsWithoutFabricatedAck(t *testing.T) {
	for _, kind := range []string{"put", "begin", "finish"} {
		t.Run(kind, func(t *testing.T) {
			f := fixture(t, Seed)
			switch kind {
			case "put":
				f.fail = SeedAllowV1
				f.failure = errors.New(bootstrapSecret)
			case "begin":
				f.log.failBegin = SeedAllowV1
			case "finish":
				f.log.failFinish = SeedAllowV1
			}
			r, e := f.run()
			if e == nil || len(r.Steps) != 4 {
				t.Fatal("uncertainty did not stop first write")
			}
			last := r.Steps[3]
			if kind == "finish" && (last.Outcome != "Allowed" || last.Persistence != "Unknown") {
				t.Fatal("actual outcome lost or false acknowledgment")
			}
			if kind == "begin" && len(f.calls) != 3 {
				t.Fatal("dispatch before durable request")
			}
			if kind == "put" && last.Outcome != "Unproven" {
				t.Fatal("lost response incorrectly successful")
			}
			assertPrivate(t, r)
		})
	}
}
func TestNegativeNotFoundIsNotAuthorizationEvidenceAndUnexpectedSuccessStops(t *testing.T) {
	for _, s := range []Step{ReadV2, ReadDeny, GenerateDenyKey, AssumeTrap} {
		for _, unexpected := range []bool{false, true} {
			t.Run(string(s)+map[bool]string{true: "-allowed", false: "-notfound"}[unexpected], func(t *testing.T) {
				f := fixture(t, Probe)
				f.fail = s
				if !unexpected {
					f.failure = &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: readerSecret}
				}
				r, e := f.run()
				if e == nil {
					t.Fatal("negative anomaly accepted")
				}
				if f.calls[len(f.calls)-1] != s {
					t.Fatal("dispatched after anomaly")
				}
				want := ProviderFailure
				if unexpected {
					want = UnexpectedAllowed
				}
				if e != want {
					t.Fatalf("got %v want %v", e, want)
				}
				assertPrivate(t, r)
				if f.outputSecret != nil && f.outputSecret.SecretBinary != nil {
					t.Fatal("unexpected read body retained")
				}
				if f.outputKey != nil && (f.outputKey.Plaintext != nil || f.outputKey.CiphertextBlob != nil) {
					t.Fatal("unexpected data key retained")
				}
				if f.outputAssume != nil && f.outputAssume.Credentials.AccessKeyId != nil {
					t.Fatal("unexpected STS credentials retained")
				}
			})
		}
	}
}
func TestApprovalAndDistinctPrincipalGates(t *testing.T) {
	for _, kind := range []string{"digest", "expired", "overlong", "admin-is-reader", "seed-receipt", "near-fixture-expiry", "near-deny-fixture-expiry"} {
		t.Run(kind, func(t *testing.T) {
			f := fixture(t, Seed)
			switch kind {
			case "digest":
				f.config.ApprovalSHA256 = hash("wrong")
			case "expired":
				f.config.ApprovedUntil = f.now
			case "overlong":
				f.config.ApprovedUntil = f.now.Add(16 * time.Minute)
			case "admin-is-reader":
				f.config.SourceAdminRoleID = f.config.Bindings.ReaderRoleID
			case "near-fixture-expiry":
				f.config.FixtureExpiresAt = f.now.Add(time.Minute)
			case "near-deny-fixture-expiry":
				f.config.DenyFixtureExpiresAt = f.now.Add(time.Minute)
			case "seed-receipt":
				f.config.Bindings.SeedReceiptSHA256 = hash("unexpected")
			}
			if kind != "digest" {
				f.config.ApprovalSHA256 = ConfigurationDigest(f.config)
			}
			if _, e := configured(f.config, f.bootstrap, f.log, f.factory); e == nil {
				t.Fatal("unsafe configuration accepted")
			}
			if len(f.calls) != 0 {
				t.Fatal("construction dispatched")
			}
		})
	}
}

func TestExactDenyVersionMapDriftStopsBeforeAssumption(t *testing.T) {
	for _, kind := range []string{"extra-version", "wrong-stage", "extra-stage"} {
		t.Run(kind, func(t *testing.T) {
			f := fixture(t, Probe)
			f.mutate = func(s Step, v any) {
				if s != BootstrapDenyMetadata {
					return
				}
				r := v.(*secretsmanager.DescribeSecretOutput)
				seed := fixtureplan.ReadProofSeeds()[2]
				switch kind {
				case "extra-version":
					r.VersionIdsToStages[hash("unreviewed-version")] = []string{"other"}
				case "wrong-stage":
					r.VersionIdsToStages[seed.VersionID] = []string{"AWSCURRENT"}
				case "extra-stage":
					r.VersionIdsToStages[seed.VersionID] = append(r.VersionIdsToStages[seed.VersionID], "AWSCURRENT")
				}
			}
			r, e := f.run()
			if e != MetadataChanged || len(f.calls) != 3 || f.calls[2] != BootstrapDenyMetadata {
				t.Fatal("deny version drift reached role assumption")
			}
			assertPrivate(t, r)
		})
	}
}
func TestApprovalMayEndExactlyAtFixtureDeadline(t *testing.T) {
	f := fixture(t, Seed)
	f.config.FixtureExpiresAt = f.config.ApprovedUntil
	f.config.DenyFixtureExpiresAt = f.config.ApprovedUntil
	f.config.ApprovalSHA256 = ConfigurationDigest(f.config)
	if _, e := configured(f.config, f.bootstrap, f.log, f.factory); e != nil {
		t.Fatal("equal bounded deadline rejected")
	}
}

func TestUnknownEmptyTagCannotReplaceExpectedOwnershipTag(t *testing.T) {
	f := fixture(t, Probe)
	f.mutate = func(s Step, v any) {
		if s != BootstrapAllowMetadata {
			return
		}
		r := v.(*secretsmanager.DescribeSecretOutput)
		for i, tag := range r.Tags {
			if aws.ToString(tag.Key) == "RegistryOwner" {
				r.Tags[i] = smtypes.Tag{Key: aws.String("UnreviewedEmptyTag"), Value: aws.String("")}
				return
			}
		}
		t.Fatal("fixture missing expected ownership tag")
	}
	r, e := f.run()
	if e != MetadataChanged || len(f.calls) != 2 {
		t.Fatal("unknown empty tag accepted as ownership evidence")
	}
	assertPrivate(t, r)
}
