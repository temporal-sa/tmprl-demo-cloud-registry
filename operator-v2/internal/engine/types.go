// Package engine executes one explicit rollout action per observation. The
// interfaces define durability/fencing obligations; they do not implement a
// Temporal worker, Kubernetes controller, credential loader or provider client.
package engine

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

// Owned separates the trusted ledger from an independently read provider fact.
// Adapters must not obtain Observation by copying untrusted project input or the
// ledger. A nil Record is recoverable only using a durable matching create intent.
type Owned struct {
	Record      *ownership.Record
	Observation ownership.Observation
}

type Snapshot struct {
	State     rollout.Observed
	Candidate *Owned
	Route     *Owned
	Failure   *diagnostics.DomainFailure
}

type Observer interface {
	Snapshot(context.Context, Request) (Snapshot, error)
}

type CandidatePermit struct{ Preconditions ownership.Preconditions }
type CreationPermit struct {
	Preconditions ownership.CreationPreconditions
}
type RoutePermit struct {
	OperationID string
	Create      *ownership.CreationPreconditions
	Update      *ownership.Preconditions
	Candidate   ownership.Preconditions
}

type CandidateOperations interface {
	// Ensure enforces absence or exact independently verified creation-token
	// equality; retrying a name alone may not adopt an existing object.
	Ensure(context.Context, rollout.Desired, rollout.Action, CreationPermit) (ownership.Observation, error)
	Readiness(context.Context, rollout.Desired, rollout.Action, CandidatePermit) (ReadinessResult, error)
	Smoke(context.Context, rollout.Desired, rollout.Action, CandidatePermit) (rollout.SmokeResult, error)
	Delete(context.Context, rollout.Desired, rollout.Action, CandidatePermit) error
}

type ReadinessResult struct {
	Ready       bool
	FailureCode string
}

type RoutePublisher interface {
	// Publish reauthorizes current independent observations, then conditionally
	// writes the exact observed UID/version or performs an atomic absence create.
	// Every adapter must enforce these checks, not merely accept a permit struct.
	Publish(context.Context, rollout.Desired, rollout.Action, RoutePermit) (ownership.Observation, error)
}

type EvidenceCollector interface {
	Capture(context.Context, rollout.Desired, rollout.Action, CandidatePermit) (diagnostics.EvidenceRef, error)
}

type CandidateOutcome struct {
	Readiness     *ReadinessResult
	Smoke         *rollout.SmokeResult
	Evidence      *rollout.EvidenceState
	CleanupFailed bool
}

// MutationIntent enables crash recovery between a conditional provider write
// and the ledger update. Previous is trusted, not copied from the provider.
type MutationIntent struct {
	OperationID       string
	Purpose           string
	Previous          ownership.Record
	ExpectedVersion   string
	TargetRevision    string
	TargetFingerprint string
}

type Journal interface {
	// Writes must be durable before acknowledgment and idempotent for identical
	// identities; conflicting retries must be rejected. The in-memory simulator
	// satisfies ordering only and is explicitly not proof of disk durability.
	SaveIntent(context.Context, ownership.Intent) error
	LoadIntent(context.Context, string) (ownership.Intent, bool, error)
	SaveRecord(context.Context, ownership.Record) error
	SaveMutation(context.Context, MutationIntent) error
	LoadMutation(context.Context, string, string) (MutationIntent, bool, error)
	RecordCandidateOutcome(context.Context, rollout.Desired, rollout.Action, CandidateOutcome) error
	RecordFailureOutcome(context.Context, rollout.Desired, rollout.Action, rollout.FailureOutcome, diagnostics.DomainFailure) error
}

type Capabilities struct {
	Observer   Observer
	Candidates CandidateOperations
	Routes     RoutePublisher
	Evidence   EvidenceCollector
	Journal    Journal
	Now        func() time.Time
}

type Request struct {
	Desired     rollout.Desired
	ProjectName string
	OperationID string
	Projection  *diagnostics.OperationProjection
	// Supersede drains this exact old attempt; it never provisions or promotes.
	Supersede bool
}

type Result struct {
	Plan       rollout.Plan
	Projection diagnostics.OperationProjection
	Event      diagnostics.OperationEvent
	Executed   bool
	Terminal   bool
	// Only true after the independent durable supersession journal ACK.
	SupersessionComplete bool
}

type AttemptDisposition string

const (
	AttemptAbsent   AttemptDisposition = "Absent"
	AttemptDeleted  AttemptDisposition = "Deleted"
	AttemptRetained AttemptDisposition = "Retained"
	AttemptActive   AttemptDisposition = "ActiveRetained"
)

type SupersessionOutcome struct {
	OperationID string
	Desired     rollout.Desired
	Disposition AttemptDisposition
	Candidate   *Owned
	Route       *Owned
	Projection  diagnostics.OperationProjection
}

// SupersessionJournal is an optional executing capability, not a DTO assertion.
// Missing support denies supersession. AuthorizeAttempt gates *every* normal
// step against prior attempts and unresolved writes. PrepareSupersession first
// excludes new normal dispatch, then drains already dispatched writes and
// resolves their exact outcomes under an enforced writer/lifecycle guard.
// It returns only when a fresh observation is safe. The barrier MUST remain in
// force across observation, record repair, every drain action/retry/poll, and
// SettleSupersession; these methods cannot be momentary preflight checks.
// SettleSupersession durably archives exact old bindings/projection/disposition,
// rechecks no pending/unknown writes and permanently closes normal writes for
// the old attempt before AuthorizeAttempt may admit a new attempt. An error or
// canceled activity does not authorize releasing that barrier or admitting new
// normal writes. Adapters must enforce the guard at provider dispatch as well
// as journal admission. CAS alone cannot provide these lifecycle guarantees.
type SupersessionJournal interface {
	AuthorizeAttempt(context.Context, Request) error
	PrepareSupersession(context.Context, Request) error
	SettleSupersession(context.Context, SupersessionOutcome) error
}
