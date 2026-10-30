package ownership

import (
	"strings"
	"time"
)

type ErrorCode string

const (
	InvalidContext   ErrorCode = "invalid_context"
	ForeignOwner     ErrorCode = "foreign_owner"
	ForeignScope     ErrorCode = "foreign_scope"
	IdentityMismatch ErrorCode = "identity_mismatch"
	StaleRevision    ErrorCode = "stale_revision"
	StaleWriter      ErrorCode = "stale_writer"
	UnprovenCreation ErrorCode = "unproven_creation"
	FencingRequired  ErrorCode = "fencing_required"
)

type Error struct {
	Code   ErrorCode
	Detail string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Detail }

// Intent is persisted before create. OperationID is stable across retries of
// that operation; a new reconcile may use a new operation without renaming a
// stable resource. SpecFingerprint must describe the normalized desired spec.
type Intent struct {
	Owner           Owner
	Scope           Scope
	Purpose         string
	Name            string
	OperationID     string
	IdempotencyKey  string
	Revision        string
	SpecFingerprint string
}

func (c Context) Intent(provider Provider, purpose, logicalID, revision, operationID, fingerprint string, nameLimit int) (Intent, error) {
	name, err := c.Name(provider, purpose, logicalID, nameLimit)
	if err != nil {
		return Intent{}, err
	}
	return c.intentWithName(provider, purpose, name, revision, operationID, fingerprint)
}

func (c Context) PathIntent(provider Provider, purpose, logicalID, revision, operationID, fingerprint string) (Intent, error) {
	name, err := c.Path(provider, purpose, logicalID)
	if err != nil {
		return Intent{}, err
	}
	return c.intentWithName(provider, purpose, name, revision, operationID, fingerprint)
}

func (c Context) intentWithName(provider Provider, purpose, name, revision, operationID, fingerprint string) (Intent, error) {
	if !validID(revision) || !validID(operationID) || !validID(fingerprint) {
		return Intent{}, invalid("revision, operation ID and spec fingerprint are required")
	}
	scope, err := c.Scope(provider)
	if err != nil {
		return Intent{}, err
	}
	return Intent{Owner: c.owner, Scope: scope, Purpose: purpose, Name: name,
		OperationID: operationID, Revision: revision, SpecFingerprint: fingerprint,
		IdempotencyKey: digestOf(c.owner, scope, purpose, name, revision, operationID, fingerprint)}, nil
}

// ResourceRef identifies an observed provider object, not just its display name.
// AWS IDs are canonical ARNs; Kubernetes IDs are API object UIDs; Temporal IDs
// are canonical provider IDs. Scope must be verified independently by adapters.
type ResourceRef struct {
	Scope      Scope
	Purpose    string
	Name       string
	ProviderID string
}

// Record lives in the trusted ownership ledger. Owner and creation provenance
// remain stable when writer leases or desired revisions change.
type Record struct {
	Ref                    ResourceRef
	Owner                  Owner
	CreationOperationID    string
	CreationIdempotencyKey string
	Revision               string
	SpecFingerprint        string
	WriterEpoch            uint64
}

// Observation must come from an independently authenticated provider read, not
// project input or a copy of Record. Version is the provider's mutation token.
type Observation struct {
	Ref                    ResourceRef
	Owner                  Owner
	CreationIdempotencyKey string
	Revision               string
	SpecFingerprint        string
	WriterEpoch            uint64
	Version                string
}

// Writer is an assertion from an external lease coordinator. This package
// validates expiry and equality; it cannot authenticate or acquire the lease.
type Writer struct {
	ID        string
	Owner     Owner
	Epoch     uint64
	ExpiresAt time.Time
}

type CreationPreconditions struct {
	Intent      Intent
	WriterID    string
	WriterEpoch uint64
}

// AuthorizeCreate checks the lease before an adapter starts a create. Intent
// generation alone is only planning. The adapter must persist intent first,
// apply its idempotency token and owner metadata, and enforce its writer guard.
func (c Context) AuthorizeCreate(intent Intent, writer Writer, now time.Time) (CreationPreconditions, error) {
	if err := c.checkIntent(intent); err != nil {
		return CreationPreconditions{}, err
	}
	if err := c.checkWriter(writer, now); err != nil {
		return CreationPreconditions{}, err
	}
	return CreationPreconditions{Intent: intent, WriterID: writer.ID, WriterEpoch: writer.Epoch}, nil
}

// BindCreated also recovers an unknown create outcome after a crash. A match by
// name alone is never enough: both creation token and independent ownership must
// match. Callers must enforce the returned creation intent through the adapter.
func (c Context) BindCreated(intent Intent, observed Observation, writer Writer, now time.Time) (Record, error) {
	if _, err := c.AuthorizeCreate(intent, writer, now); err != nil {
		return Record{}, err
	}
	if err := c.checkRef(observed.Ref); err != nil {
		return Record{}, err
	}
	if observed.Owner != c.owner {
		return Record{}, reject(ForeignOwner, "creation observation has another owner")
	}
	if observed.Ref.Scope != intent.Scope || observed.Ref.Purpose != intent.Purpose || observed.Ref.Name != intent.Name {
		return Record{}, reject(IdentityMismatch, "creation observation does not match intent")
	}
	if observed.CreationIdempotencyKey != intent.IdempotencyKey {
		return Record{}, reject(UnprovenCreation, "creation token does not match persisted intent")
	}
	if observed.Revision != intent.Revision || observed.SpecFingerprint != intent.SpecFingerprint {
		return Record{}, reject(StaleRevision, "creation revision or fingerprint differs")
	}
	if observed.WriterEpoch != writer.Epoch {
		return Record{}, reject(StaleWriter, "creation writer epoch differs")
	}
	return Record{Ref: observed.Ref, Owner: c.owner, CreationOperationID: intent.OperationID,
		CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision,
		SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch}, nil
}

type Action string

const (
	Mutate  Action = "mutate"
	Adopt   Action = "adopt"
	Promote Action = "promote"
	Cleanup Action = "cleanup"
)

type FencingMode string

const (
	ProviderConditional FencingMode = "provider_conditional"
	// ExternalSerialization is not a provider lock. It requires an externally
	// established serialized/drained handoff, explicitly asserted by the caller.
	ExternalSerialization FencingMode = "external_serialization_assertion"
)

type SerializationAssertion struct {
	CoordinatorID         string
	LeaseID               string
	WriterEpoch           uint64
	PreviousWriterDrained bool
}

type MutationRequest struct {
	Action           Action
	Record           Record
	Observed         Observation
	ExpectedRevision string
	Writer           Writer
	Now              time.Time
	Fencing          FencingMode
	Serialization    SerializationAssertion
}

// Preconditions must actually be enforced by the capability adapter. Returning
// them is not proof the provider will reject a stale writer. In particular,
// ExternalSerialization describes an assertion, never an implemented lock.
type Preconditions struct {
	Action          Action
	Ref             ResourceRef
	Owner           Owner
	Revision        string
	SpecFingerprint string
	ProviderVersion string
	WriterID        string
	WriterEpoch     uint64
	Fencing         FencingMode
	Serialization   SerializationAssertion
}

func (c Context) Authorize(request MutationRequest) (Preconditions, error) {
	return c.authorizeWithRefCheck(request, c.checkRef)
}

func (c Context) authorizeWithRefCheck(request MutationRequest, checkRef func(ResourceRef) error) (Preconditions, error) {
	switch request.Action {
	case Mutate, Adopt, Promote, Cleanup:
	default:
		return Preconditions{}, invalid("unknown mutation action %q", request.Action)
	}
	r, o := request.Record, request.Observed
	if r.Owner != c.owner || o.Owner != c.owner {
		return Preconditions{}, reject(ForeignOwner, "record and observation must belong to this owner")
	}
	if err := checkRef(r.Ref); err != nil {
		return Preconditions{}, err
	}
	if err := checkRef(o.Ref); err != nil {
		return Preconditions{}, err
	}
	if r.Ref != o.Ref {
		return Preconditions{}, reject(IdentityMismatch, "provider identity changed; name reuse does not authorize adoption")
	}
	if !validID(r.CreationOperationID) || !validID(r.CreationIdempotencyKey) || r.CreationIdempotencyKey != o.CreationIdempotencyKey {
		return Preconditions{}, reject(UnprovenCreation, "independent creation provenance is required")
	}
	if !validID(request.ExpectedRevision) || request.ExpectedRevision != r.Revision || o.Revision != r.Revision || !validID(r.SpecFingerprint) || r.SpecFingerprint != o.SpecFingerprint {
		return Preconditions{}, reject(StaleRevision, "expected revision, ledger and provider observation differ")
	}
	if err := c.checkWriter(request.Writer, request.Now); err != nil {
		return Preconditions{}, err
	}
	if r.WriterEpoch != request.Writer.Epoch || o.WriterEpoch != request.Writer.Epoch {
		return Preconditions{}, reject(StaleWriter, "ledger, observation and active writer epochs differ")
	}
	switch request.Fencing {
	case ProviderConditional:
		if !validID(o.Version) {
			return Preconditions{}, reject(FencingRequired, "provider conditional mutation requires a provider version")
		}
	case ExternalSerialization:
		s := request.Serialization
		if !validID(s.CoordinatorID) || !validID(s.LeaseID) || s.WriterEpoch != request.Writer.Epoch || !s.PreviousWriterDrained {
			return Preconditions{}, reject(FencingRequired, "unsupported provider fencing requires an explicit external serialization and drained-writer assertion")
		}
	default:
		return Preconditions{}, reject(FencingRequired, "choose enforceable provider preconditions or explicit external serialization")
	}
	return Preconditions{Action: request.Action, Ref: r.Ref, Owner: r.Owner, Revision: r.Revision, SpecFingerprint: r.SpecFingerprint,
		ProviderVersion: o.Version, WriterID: request.Writer.ID, WriterEpoch: request.Writer.Epoch, Fencing: request.Fencing, Serialization: request.Serialization}, nil
}

func (c Context) checkIntent(intent Intent) error {
	if intent.Owner != c.owner {
		return reject(ForeignOwner, "intent has another owner")
	}
	if err := c.checkNameScope(intent.Scope, intent.Name, intent.Purpose); err != nil {
		return err
	}
	if !validID(intent.Revision) || !validID(intent.OperationID) || !validID(intent.SpecFingerprint) || intent.IdempotencyKey != digestOf(intent.Owner, intent.Scope, intent.Purpose, intent.Name, intent.Revision, intent.OperationID, intent.SpecFingerprint) {
		return reject(UnprovenCreation, "intent provenance was changed or is incomplete")
	}
	return nil
}

func (c Context) checkNameScope(scope Scope, name, purpose string) error {
	expected, err := c.Scope(scope.Provider)
	if err != nil {
		return err
	}
	if expected != scope {
		return reject(ForeignScope, "resource is outside configured provider authority")
	}
	if !slugPattern.MatchString(purpose) || !validID(name) || !(strings.HasPrefix(name, c.root+"-") || strings.HasPrefix(name, c.root+"/")) {
		return reject(IdentityMismatch, "resource name or purpose is outside the disjoint resource root")
	}
	for _, segment := range strings.Split(name, "/") {
		if !pathSegmentPattern.MatchString(segment) {
			return reject(IdentityMismatch, "resource path contains an invalid segment")
		}
	}
	return nil
}

func (c Context) checkRef(ref ResourceRef) error {
	if err := c.checkNameScope(ref.Scope, ref.Name, ref.Purpose); err != nil {
		return err
	}
	if !validID(ref.ProviderID) {
		return reject(IdentityMismatch, "canonical provider identity is required; names alone are insufficient")
	}
	if ref.Scope.Provider == AWS {
		parts := strings.SplitN(ref.ProviderID, ":", 6)
		if len(parts) != 6 || parts[0] != "arn" || parts[1] != ref.Scope.Partition || parts[2] == "" || parts[5] == "" ||
			(parts[3] != "" && parts[3] != ref.Scope.Region) || (parts[4] != "" && parts[4] != ref.Scope.AccountID) ||
			(parts[4] == "" && parts[2] != "s3") {
			return reject(ForeignScope, "canonical AWS ARN does not match configured authority")
		}
		if !arnNamesResource(parts[2], parts[5], ref.Name) {
			return reject(IdentityMismatch, "AWS ARN does not name the observed owned resource; unsupported ARN shapes require a reviewed adapter")
		}
	}
	return nil
}

// Only named ARN shapes whose identity can be checked locally are supported.
// Opaque IDs such as KMS key IDs or Route53 zone IDs need a separately reviewed
// adapter binding; a caller-supplied display name must not launder a foreign ID.
func arnNamesResource(service, resource, name string) bool {
	switch service {
	case "ecr", "ecr-public":
		return resource == "repository/"+name
	case "lambda":
		base := "function:" + name
		return resource == base || strings.HasPrefix(resource, base+":") && validID(strings.TrimPrefix(resource, base+":"))
	case "iam":
		return resource == "role/"+name || resource == "policy/"+name
	case "secretsmanager":
		prefix := "secret:" + name + "-"
		if !strings.HasPrefix(resource, prefix) {
			return false
		}
		suffix := strings.TrimPrefix(resource, prefix)
		return len(suffix) == 6 && strings.Trim(suffix, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") == ""
	case "s3":
		return resource == name
	case "codebuild":
		return resource == "project:"+name || strings.HasPrefix(resource, "build:"+name+":") && validID(strings.TrimPrefix(resource, "build:"+name+":"))
	case "logs":
		return resource == "log-group:"+name || resource == "log-group:"+name+":*"
	case "kms":
		return resource == "alias/"+name
	default:
		return false
	}
}

func (c Context) checkWriter(writer Writer, now time.Time) error {
	if writer.Owner != c.owner {
		return reject(ForeignOwner, "writer has another owner")
	}
	if !validID(writer.ID) || writer.Epoch == 0 || now.IsZero() || writer.ExpiresAt.IsZero() || !now.Before(writer.ExpiresAt) {
		return reject(StaleWriter, "explicit current time and an unexpired nonzero writer epoch are required")
	}
	return nil
}

func reject(code ErrorCode, detail string) error {
	return &Error{Code: code, Detail: detail}
}
