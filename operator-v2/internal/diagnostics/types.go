// Package diagnostics defines safe, bounded descriptions of an operation.
// It performs no provider calls and contains no execution or event-store logic.
package diagnostics

import "time"

const (
	SchemaVersion   = 1
	MaxJSONBytes    = 8 * 1024
	MaxFailureBytes = 2 * 1024
)

type State string

const (
	Pending    State = "Pending"
	Running    State = "Running"
	Waiting    State = "Waiting"
	Retrying   State = "Retrying"
	Succeeded  State = "Succeeded"
	Failed     State = "Failed"
	Cancelled  State = "Cancelled"
	Finalizing State = "Finalizing"
	Finalized  State = "Finalized"
)

type Category string

const (
	Validation     Category = "Validation"
	Configuration  Category = "Configuration"
	Authentication Category = "Authentication"
	Transient      Category = "Transient"
	Build          Category = "Build"
	Readiness      Category = "Readiness"
	Smoke          Category = "Smoke"
	Cleanup        Category = "Cleanup"
	Publication    Category = "Publication"
	Unknown        Category = "Unknown"
)

// Identity includes stable ownership and the particular desired-state operation.
// ReconcileKey and Generation identify intent; Sequence identifies observations.
type Identity struct {
	Environment  string `json:"environment"`
	Installation string `json:"installation"`
	ProjectUID   string `json:"projectUID"`
	ProjectName  string `json:"projectName"`
	OperationID  string `json:"operationID"`
	ReconcileKey string `json:"reconcileKey"`
	Generation   int64  `json:"generation"`
}

type Cause struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type ResourceRef struct {
	Provider string `json:"provider"`
	Kind     string `json:"kind"`
	ID       string `json:"id"`
}

type ExecutionRef struct {
	Namespace        string `json:"namespace,omitempty"`
	WorkflowID       string `json:"workflowID,omitempty"`
	RunID            string `json:"runID,omitempty"`
	ActivityID       string `json:"activityID,omitempty"`
	ActivityType     string `json:"activityType,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventID,omitempty"`
}

type ProviderRef struct {
	Service   string `json:"service,omitempty"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"requestID,omitempty"`
}

// DomainFailure describes the original cause. Recovery and publication failures
// belong in their own records and must not replace this cause.
// Producers must call SafeFailure before persisting/logging a value. MarshalJSON
// is defense in depth, not a replacement for sanitizing before durable history.
type DomainFailure struct {
	ID          string        `json:"id"`
	Code        string        `json:"code"`
	Category    Category      `json:"category"`
	Message     string        `json:"message"`
	Step        string        `json:"step,omitempty"`
	OccurredAt  time.Time     `json:"occurredAt"`
	Causes      []Cause       `json:"causes,omitempty"`
	Execution   ExecutionRef  `json:"execution"`
	Provider    ProviderRef   `json:"provider"`
	Resources   []ResourceRef `json:"resources,omitempty"`
	EvidenceIDs []string      `json:"evidenceIDs,omitempty"`
	NextAction  string        `json:"nextAction,omitempty"`
	Partial     bool          `json:"partial,omitempty"`
}

type RetryStatus string

const (
	RetryUnknown      RetryStatus = "Unknown"
	RetryScheduled    RetryStatus = "Scheduled"
	RetryExhausted    RetryStatus = "Exhausted"
	RetryNotRetryable RetryStatus = "NotRetryable"
)

// Authoritative is required to publish a concrete retry status. A retryable
// error category or configured retry policy alone is insufficient evidence.
type RetryInfo struct {
	Status          RetryStatus `json:"status"`
	Authoritative   bool        `json:"authoritative"`
	Attempt         int         `json:"attempt,omitempty"`
	MaximumAttempts int         `json:"maximumAttempts,omitempty"`
	ObservedAt      time.Time   `json:"observedAt"`
	NextAttemptAt   *time.Time  `json:"nextAttemptAt,omitempty"`
}

type RouteState string

const (
	RouteUnknown         RouteState = "Unknown"
	RouteActivePreserved RouteState = "ActivePreserved"
	RoutePromoted        RouteState = "Promoted"
	RouteUnconfirmed     RouteState = "Unconfirmed"
)

// Availability is independent of whether the candidate operation failed.
type Availability struct {
	ActiveRevision    string     `json:"activeRevision,omitempty"`
	CandidateRevision string     `json:"candidateRevision,omitempty"`
	RouteState        RouteState `json:"routeState"`
	ConfirmedAt       time.Time  `json:"confirmedAt"`
}

type OutcomeState string

const (
	OutcomeRunning   OutcomeState = "Running"
	OutcomeCompleted OutcomeState = "Completed"
	OutcomeFailed    OutcomeState = "Failed"
	OutcomeRetained  OutcomeState = "Retained"
)

type RecoveryOutcome struct {
	ID                string         `json:"id"`
	Step              string         `json:"step"`
	State             OutcomeState   `json:"state"`
	Failure           *DomainFailure `json:"failure,omitempty"`
	RetainedResources []ResourceRef  `json:"retainedResources,omitempty"`
}

type EvidenceRef struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	// Locator is an opaque authorized resolver identifier or safe link, never
	// credential-bearing URLs. Authorization remains the resolver's job.
	Locator string `json:"locator"`
	State   string `json:"state"`
	Partial bool   `json:"partial,omitempty"`
}

// OperationEvent is an observed fact, not an instruction to repeat side effects.
// SafeEvent must be called before producers persist it to durable history/logs.
type OperationEvent struct {
	SchemaVersion int               `json:"schemaVersion"`
	Identity      Identity          `json:"identity"`
	Sequence      uint64            `json:"sequence"`
	OccurredAt    time.Time         `json:"occurredAt"`
	State         State             `json:"state"`
	Step          string            `json:"step"`
	Failure       *DomainFailure    `json:"failure,omitempty"`
	Retry         *RetryInfo        `json:"retry,omitempty"`
	Availability  *Availability     `json:"availability,omitempty"`
	Recovery      []RecoveryOutcome `json:"recovery,omitempty"`
	Evidence      []EvidenceRef     `json:"evidence,omitempty"`
	Warnings      []DomainFailure   `json:"warnings,omitempty"`
	Partial       bool              `json:"partial,omitempty"`
}

type DeliveryState string

const (
	DeliveryPending   DeliveryState = "Pending"
	DeliveryUnknown   DeliveryState = "Unknown"
	DeliverySucceeded DeliveryState = "Succeeded"
	DeliveryFailed    DeliveryState = "Failed"
)

type SinkHealth struct {
	Sink           string         `json:"sink"`
	State          DeliveryState  `json:"state"`
	SourceSequence uint64         `json:"sourceSequence"`
	ObservedAt     time.Time      `json:"observedAt"`
	Failure        *DomainFailure `json:"failure,omitempty"`
}

type OperationProjection struct {
	SchemaVersion int               `json:"schemaVersion"`
	Identity      Identity          `json:"identity"`
	Sequence      uint64            `json:"sequence"`
	State         State             `json:"state"`
	Step          string            `json:"step"`
	ProgressedAt  time.Time         `json:"progressedAt"`
	Failure       *DomainFailure    `json:"failure,omitempty"`
	Retry         RetryInfo         `json:"retry"`
	Availability  Availability      `json:"availability"`
	Recovery      []RecoveryOutcome `json:"recovery,omitempty"`
	Evidence      []EvidenceRef     `json:"evidence,omitempty"`
	Warnings      []DomainFailure   `json:"warnings,omitempty"`
	Publication   []SinkHealth      `json:"publication,omitempty"`
	Partial       bool              `json:"partial,omitempty"`
}

type DesiredState struct {
	ReconcileKey string
	Generation   int64
}

type FreshnessState string

const (
	FreshnessCurrent     FreshnessState = "Current"
	FreshnessReconciling FreshnessState = "Reconciling"
	FreshnessStale       FreshnessState = "Stale"
	FreshnessUnknown     FreshnessState = "Unknown"
)

type Freshness struct {
	State                FreshnessState `json:"state"`
	ObservedReconcileKey string         `json:"observedReconcileKey"`
	DesiredReconcileKey  string         `json:"desiredReconcileKey"`
	ObservedGeneration   int64          `json:"observedGeneration"`
	DesiredGeneration    int64          `json:"desiredGeneration"`
	LastProgressAt       time.Time      `json:"lastProgressAt"`
	EvaluatedAt          time.Time      `json:"evaluatedAt"`
}
