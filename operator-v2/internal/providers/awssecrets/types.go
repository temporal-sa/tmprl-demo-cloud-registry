// Package awssecrets stages separately named encrypted candidates. It does not
// promote, overwrite runtime secrets, delete, or prove real AWS permissions.
package awssecrets

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

// Client deliberately exposes neither PutSecretValue nor deletion/activation.
type Client interface {
	CreateSecret(context.Context, *secretsmanager.CreateSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
	DescribeSecret(context.Context, *secretsmanager.DescribeSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error)
}

// NamespaceBinding and KMSBinding are fixture-attested trusted configuration,
// never project-spec inputs or metadata copied from an unverified AWS object.
type NamespaceBinding struct {
	ID, IncarnationID string
	Owner             ownership.Owner
}
type KMSBinding struct {
	ARN, ProvenanceID string
	Owner             ownership.Owner
}
type Config struct {
	Ownership            ownership.Context
	StoreID              string
	KMS                  KMSBinding
	Namespaces           []NamespaceBinding
	AllowCandidateCreate bool
	CallTimeout          time.Duration
}

// CandidateGuard must independently validate the live namespace incarnation and
// exclusive writer, serialize/drain candidate creation and active-ref changes,
// and reject an active or reserved runtime identity. It calls fn at most once.
// A stale Active=false flag or pre-read is insufficient. This interface does not
// implement provider fencing, admission, a lease, or an active-reference store.
type CandidateGuard interface {
	Run(context.Context, tc.CandidateWrite, func(context.Context) error) error
}

type OutcomePhase string

const (
	CreateRequested  OutcomePhase = "CreateRequested"
	CreateReceipted  OutcomePhase = "CreateReceipted"
	MetadataVerified OutcomePhase = "MetadataVerified"
)

// Outcome contains only safe metadata. ARN originates in a successful exact
// CreateSecret response, never a name lookup. CreatedAt subsequently pins the
// independently observed birth. Matching tags alone cannot create this receipt.
type Outcome struct {
	Version   string
	Write     tc.CandidateWrite
	Phase     OutcomePhase
	ARN       string
	CreatedAt time.Time
}

// OutcomeLedger is a separate linearizable CAS ledger, so storing a create
// receipt does not invalidate the credential manager's journal version. It must
// reject foreign write IDs, retain immutable history, and return errors for
// unknown outcomes. Unknown creates are never reset to allow another create.
type OutcomeLedger interface {
	Load(context.Context, string) (Outcome, bool, error)
	CompareAndSwap(context.Context, string, string, Outcome) (Outcome, error)
}

type Code string

const (
	InvalidConfiguration Code = "CandidateStoreInvalidConfiguration"
	InvalidWrite         Code = "CandidateStoreInvalidWrite"
	CapabilityDisabled   Code = "CandidateStoreCapabilityDisabled"
	AuthorizationDenied  Code = "CandidateStoreAuthorizationDenied"
	JournalUnknown       Code = "CandidateStoreJournalUnknown"
	OutcomeUnknown       Code = "CandidateStoreOutcomeUnknown"
	OwnershipUnproven    Code = "CandidateStoreOwnershipUnproven"
	MetadataConflict     Code = "CandidateStoreMetadataConflict"
	DeletionUnsupported  Code = "CandidateStoreDeletionUnsupported"
)

// Error never wraps provider, journal or guard errors, which may contain tokens
// or unconstrained response strings. Codes are suitable for durable diagnostics.
type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }
func fail(code Code) error     { return &Error{Code: code} }
