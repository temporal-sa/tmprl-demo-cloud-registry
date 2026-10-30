// Package credentialobservation performs one finite read-only authentication
// observation. It never promotes, mutates credential history, revokes keys or
// proves workflow-write/worker readiness. Managed lifecycle serialization is a
// separately injected guard; no distributed or external-writer fence is claimed.
package credentialobservation

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

const Contract = "temporal-candidate-namespace-auth-observation-v1"

type Code string

const (
	Invalid                Code = "CandidateObservationInvalidContract"
	Denied                 Code = "CandidateObservationAuthorityDenied"
	MetadataChanged        Code = "CandidateObservationMetadataChanged"
	SecretUnproven         Code = "CandidateObservationSecretUnproven"
	ReadUnknown            Code = "CandidateObservationReadUnknown"
	IdentityUnproven       Code = "CandidateObservationIdentityUnproven"
	NamespaceUnproven      Code = "CandidateObservationNamespaceUnproven"
	Expired                Code = "CandidateObservationExpired"
	PrivacyDenied          Code = "CandidateObservationPrivacyDenied"
	AuthenticationRejected Code = "CandidateObservationAuthenticationRejected"
	AuthorizationRejected  Code = "CandidateObservationAuthorizationRejected"
	EvidenceUnsupported    Code = "CandidateObservationEvidenceUnsupported"
	Canceled               Code = "CandidateObservationCanceled"
	Deadline               Code = "CandidateObservationDeadlineExceeded"
)

type Step string

const (
	Metadata   Step = "Metadata"
	Secret     Step = "Secret"
	Identity   Step = "Identity"
	Namespace  Step = "Namespace"
	DataPlane  Step = "DataPlane"
	Completion Step = "Completion"
)

// Error never wraps SDK/guard errors, responses, headers or values. Step and
// code are finite values set only at this package's trusted boundary.
type Error struct {
	Code Code
	Step Step
}

func (e *Error) Error() string {
	switch e.Code {
	case Invalid, Denied, MetadataChanged, SecretUnproven, ReadUnknown, IdentityUnproven, NamespaceUnproven, Expired, PrivacyDenied, AuthenticationRejected, AuthorizationRejected, EvidenceUnsupported, Canceled, Deadline:
		return string(e.Code)
	}
	return string(Invalid)
}
func failure(c Code, s Step) error { return &Error{Code: c, Step: s} }

type StateReader interface {
	Load(context.Context, string) (tc.CredentialState, bool, error)
}
type OutcomeReader interface {
	Load(context.Context, string) (awsstore.Outcome, bool, error)
}
type SecretClient interface {
	DescribeSecret(context.Context, *secretsmanager.DescribeSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error)
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// Guard serializes/drains the complete callback under independently established
// owner/incarnation/session authority. It invokes fn at most once and must not
// manufacture authority from the ref's flags or metadata copied from a name.
// The accepted staging actor does not yet implement this separate read seam.
type Guard interface {
	Run(context.Context, tc.SecretVersionRef, func(context.Context) error) error
}

// Endpoint contains explicitly supplied public trust anchors, never ambient
// certificate paths or project inputs. There is no system-root/default endpoint,
// insecure TLS, custom proxy/dialer or bootstrap-key fallback.
type Endpoint struct {
	Address, ServerName string
	RootCAPEM           []byte
}
type Config struct {
	Ownership                                  ownership.Context
	Namespace                                  tc.Binding
	ProjectID, StoreID, KMSARN, TrustBindingID string
	Cloud, Data                                Endpoint
	// Optional independent mapping. Without it the returned data-plane ID hash
	// remains observational; even with it this finite contract cannot promote.
	ExpectedDataPlaneID string
	Timeout             time.Duration
	Now                 func() time.Time
}

type Observation struct {
	Schema                                                                int
	Contract                                                              string
	Owner                                                                 ownership.Owner
	SlotID, StageID, AttemptID                                            string
	NamespaceID, NamespaceIncarnationID                                   string
	StoreID, CandidateID, WriteID, VersionID                              string
	ServiceAccountID, KeyID                                               string
	CredentialJournalVersion, OutcomeLedgerVersion                        string
	EndpointBindingDigest, DataPlaneIDHash                                string
	KeyVersionDigest, ServiceAccountVersionDigest, NamespaceVersionDigest string
	StartedAt, ObservedAt, ExpiresAt                                      time.Time
}

// Observer has one private pointer so even invalid fmt verbs on a VALUE cannot
// recursively print clients/credentials. It is activity-local, never a payload.
type Observer struct{ private *observerState }
type observerState struct {
	config   Config
	states   StateReader
	outcomes OutcomeReader
	secrets  SecretClient
	guard    Guard
	probes   probeFactory
}

func (Observer) String() string               { return "CandidateObserver{private}" }
func (Observer) GoString() string             { return "CandidateObserver{private}" }
func (o Observer) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, o.String()) }
func (Observer) MarshalJSON() ([]byte, error) { return nil, failure(PrivacyDenied, Metadata) }
func (Observer) MarshalText() ([]byte, error) { return nil, failure(PrivacyDenied, Metadata) }

// New constructs no connection and reads no secret. It owns candidate-only TLS
// connections later during Observe. Explicit AWS/guard dependencies are trusted
// activity services; no credential/config/env loading occurs here.
func New(c Config, s StateReader, o OutcomeReader, a SecretClient, g Guard) (*Observer, error) {
	return configured(c, s, o, a, g, newNetworkProbes)
}
