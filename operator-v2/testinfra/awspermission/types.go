// Package awspermission prepares finite synthetic fixture seeding/permission
// helpers. It has no ambient credential/profile loader, generic AWS executor,
// policy mutation, plan/apply or cleanup API. Live credential delivery and each
// exact operation require separate reviewed authorization outside this package.
package awspermission

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	Invalid           Error = "AWSReadProofInvalidContract"
	Denied            Error = "AWSReadProofAuthorityDenied"
	Unproven          Error = "AWSReadProofOutcomeUnproven"
	UnexpectedAllowed Error = "AWSReadProofUnexpectedPermit"
	WrongIdentity     Error = "AWSReadProofWrongIdentity"
	MetadataChanged   Error = "AWSReadProofMetadataChanged"
	ProviderFailure   Error = "AWSReadProofProviderFailure"
	PrivacyDenied     Error = "AWSReadProofPrivacyDenied"
	AlreadyAttempted  Error = "AWSReadProofAlreadyAttempted"
)

type Phase string

const (
	Seed  Phase = "Seed"
	Probe Phase = "Probe"
)
const ReaderSessionName = "rv2-aws-readproof"
const TrapSessionName = "rv2-denied-chain"

type Config struct {
	Phase                                  Phase
	Bindings                               fixtureplan.ReadProofBindings
	SourceAdminRoleID                      string
	FixtureExpiresAt, DenyFixtureExpiresAt time.Time
	ApprovedUntil                          time.Time
	// ApprovalSHA256 is the externally reviewed exact ConfigurationDigest,
	// not a signature or live attestation. There is no self-approval fallback.
	ApprovalSHA256 string
	Now            func() time.Time
}
type Step string

const (
	SourceIdentity         Step = "BootstrapIdentity"
	BootstrapAllowMetadata Step = "BootstrapAllowMetadata"
	BootstrapDenyMetadata  Step = "BootstrapDenyMetadata"
	AllowMetadata          Step = "AllowMetadata"
	SeedAllowV1            Step = "SeedAllowV1"
	SeedAllowV2            Step = "SeedAllowV2"
	SeedDenyV1             Step = "SeedDenyV1"
	AssumeReader           Step = "AssumeReader"
	ReaderIdentity         Step = "ReaderIdentity"
	ReadV1                 Step = "ReadAllowedV1"
	ReadV2                 Step = "DenyOtherV2"
	ReadDeny               Step = "DenyOtherSecret"
	GenerateDenyKey        Step = "DenyDirectKeyGeneration"
	AssumeTrap             Step = "DenyTransitiveAssumption"
)

type Receipt struct {
	Step                                    Step
	SpecSHA256, RequestSHA256, TargetSHA256 string
	Outcome                                 string
	Persistence                             string `json:",omitempty"`
	Code                                    string `json:",omitempty"`
	RequestID                               string `json:",omitempty"`
	At                                      time.Time
}
type Result struct {
	Contract, SpecSHA256, Outcome string
	Steps                         []Receipt
}

var shaPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var rolePattern = regexp.MustCompile(`^AROA[A-Z0-9]{17}$`)
var requestIDPattern = regexp.MustCompile(`^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$`)

func hash(v any) string {
	b, e := json.Marshal(v)
	if e != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func bytesHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func ConfigurationDigest(c Config) string {
	return hash(struct {
		Contract                    string
		Phase                       Phase
		Bindings                    fixtureplan.ReadProofBindings
		Admin                       string
		Fixture, DenyFixture, Until time.Time
	}{fixtureplan.ReadProofContract, c.Phase, c.Bindings, c.SourceAdminRoleID, c.FixtureExpiresAt.UTC(), c.DenyFixtureExpiresAt.UTC(), c.ApprovedUntil.UTC()})
}
func validateConfig(c Config) error {
	if c.Now == nil {
		return Invalid
	}
	now := c.Now()
	if c.Phase != Seed && c.Phase != Probe || !rolePattern.MatchString(c.SourceAdminRoleID) || c.SourceAdminRoleID == c.Bindings.ReaderRoleID || c.SourceAdminRoleID == c.Bindings.TrapRoleID || !c.FixtureExpiresAt.After(now) || !c.DenyFixtureExpiresAt.After(now) || !c.ApprovedUntil.After(now.Add(5*time.Second)) || c.ApprovedUntil.After(c.FixtureExpiresAt) || c.ApprovedUntil.After(c.DenyFixtureExpiresAt) || c.ApprovedUntil.After(now.Add(15*time.Minute)) || !shaPattern.MatchString(c.ApprovalSHA256) || c.ApprovalSHA256 != ConfigurationDigest(c) || fixtureplan.ValidateReadProofMetadata(c.Bindings, now) != nil {
		return Invalid
	}
	if c.Phase == Seed && c.Bindings.SeedReceiptSHA256 != "" {
		return Invalid
	}
	if c.Phase == Probe {
		if _, e := fixtureplan.RenderReadProofEnabled(c.Bindings, c.ApprovedUntil, now); e != nil {
			return Invalid
		}
	}
	return nil
}

type secretAPI interface {
	DescribeSecret(context.Context, *secretsmanager.DescribeSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error)
	PutSecretValue(context.Context, *secretsmanager.PutSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error)
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}
type stsAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
	AssumeRole(context.Context, *sts.AssumeRoleInput, ...func(*sts.Options)) (*sts.AssumeRoleOutput, error)
}
type kmsAPI interface {
	GenerateDataKey(context.Context, *kms.GenerateDataKeyInput, ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error)
}
type clients struct {
	secret secretAPI
	sts    stsAPI
	kms    kmsAPI
	close  func()
}
type factory func(Credentials, Config) (clients, error)
type operationLog interface {
	begin(context.Context, Receipt) error
	finish(context.Context, Receipt) error
}

type operationKey struct{}
type operation struct {
	material *credentialState
	step     Step
	live     atomic.Bool
	httpUsed atomic.Bool
}

// Only finite locally defined outcomes and the exact expected provider deny
// codes may enter a receipt. Arbitrary provider error text/code is never saved.
func safeCode(code string) bool {
	switch code {
	case "", "AccessDenied", "AccessDeniedException", string(Invalid), string(Denied), string(Unproven), string(UnexpectedAllowed), string(WrongIdentity), string(MetadataChanged), string(ProviderFailure), string(PrivacyDenied), string(AlreadyAttempted):
		return true
	}
	return false
}

// Explicit options only; no aws/config or default credential chain exists.
func noSecretRetry(o *secretsmanager.Options) { o.Retryer = aws.NopRetryer{} }
func noSTSRetry(o *sts.Options)               { o.Retryer = aws.NopRetryer{} }
func noKMSRetry(o *kms.Options)               { o.Retryer = aws.NopRetryer{} }
