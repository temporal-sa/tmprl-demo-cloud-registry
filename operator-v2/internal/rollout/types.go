// Package rollout plans candidate promotion without contacting a provider.
// Adapters must supply observations and enforce the plan's preconditions.
package rollout

import "time"

type Subject struct {
	Environment  string `json:"environment"`
	Installation string `json:"installation"`
	ProjectUID   string `json:"projectUid"`
}

// Desired describes one immutable rollout intent. Revision and ReconcileKey
// must identify the complete normalized runtime intent, not merely an image tag.
type Desired struct {
	Subject        Subject           `json:"subject"`
	Generation     int64             `json:"generation"`
	ReconcileKey   string            `json:"reconcileKey"`
	Revision       string            `json:"revision"`
	RequiredBuilds []string          `json:"requiredBuilds"`
	Images         map[string]string `json:"images"`
	RequireSmoke   bool              `json:"requireSmoke"`
}

type Revision struct {
	ID              string `json:"id"`
	ReconcileKey    string `json:"reconcileKey"`
	ResourceID      string `json:"resourceId"`
	ResourceVersion string `json:"resourceVersion"`
}

type Candidate struct {
	Revision
	Ready          bool          `json:"ready"`
	FailureCode    string        `json:"failureCode,omitempty"`
	Smoke          *SmokeResult  `json:"smoke,omitempty"`
	Evidence       EvidenceState `json:"evidence"`
	CleanupFailure bool          `json:"cleanupFailure,omitempty"`
}

type SmokeResult struct {
	Passed      bool   `json:"passed"`
	FailureCode string `json:"failureCode,omitempty"`
}

type EvidenceState string

const (
	EvidencePending  EvidenceState = "Pending"
	EvidenceCaptured EvidenceState = "Captured"
	EvidenceFailed   EvidenceState = "Failed"
)

// ObservationToken is adapter-owned. A plan is not permission to write: the
// executor must revalidate it and apply provider-side preconditions or serialize
// and drain operations where the provider cannot enforce those preconditions.
type Observed struct {
	Subject          Subject    `json:"subject"`
	Generation       int64      `json:"generation"`
	ObservationToken string     `json:"observationToken"`
	Active           *Revision  `json:"active,omitempty"`
	Route            *Route     `json:"route,omitempty"`
	Candidate        *Candidate `json:"candidate,omitempty"`
	ExternalBlockers []string   `json:"externalBlockers,omitempty"`
	// SettledFailure survives candidate deletion. Without this durable intent
	// outcome, a later reconcile could recreate the failed candidate forever.
	SettledFailure *FailureOutcome `json:"settledFailure,omitempty"`
}

type Route struct {
	Subject         Subject   `json:"subject"`
	ResourceID      string    `json:"resourceId"`
	ResourceVersion string    `json:"resourceVersion"`
	Revision        string    `json:"revision"`
	ConfirmedAt     time.Time `json:"confirmedAt"`
}

type FailureOutcome struct {
	Revision          string `json:"revision"`
	ReconcileKey      string `json:"reconcileKey"`
	Code              string `json:"code"`
	CandidateRetained bool   `json:"candidateRetained"`
}

type Phase string

const (
	WaitingForImages    Phase = "WaitingForImages"
	WaitingForExternal  Phase = "WaitingForExternalResources"
	Deploying           Phase = "DeployingCandidate"
	WaitingForReadiness Phase = "WaitingForReadiness"
	CheckingSmoke       Phase = "CheckingSmoke"
	Promoting           Phase = "PromotingCandidate"
	Succeeded           Phase = "Succeeded"
	Failed              Phase = "Failed"
	Retained            Phase = "CandidateRetained"
)

type ActionKind string

const (
	EnsureCandidate      ActionKind = "EnsureCandidate"
	ObserveReadiness     ActionKind = "ObserveReadiness"
	CheckSmoke           ActionKind = "CheckSmoke"
	PromoteCandidate     ActionKind = "PromoteCandidate"
	CaptureEvidence      ActionKind = "CaptureEvidence"
	RecordFailureOutcome ActionKind = "RecordFailureOutcome"
	DeleteCandidate      ActionKind = "DeleteCandidate"
)

type Preconditions struct {
	ObservationToken         string `json:"observationToken"`
	ActiveRevision           string `json:"activeRevision,omitempty"`
	RouteRevision            string `json:"routeRevision,omitempty"`
	RouteResourceID          string `json:"routeResourceId,omitempty"`
	RouteResourceVersion     string `json:"routeResourceVersion,omitempty"`
	RouteMustBeAbsent        bool   `json:"routeMustBeAbsent"`
	CandidateResourceID      string `json:"candidateResourceId,omitempty"`
	CandidateResourceVersion string `json:"candidateResourceVersion,omitempty"`
}

type Action struct {
	OperationID    string        `json:"operationID,omitempty"`
	Kind           ActionKind    `json:"kind"`
	Revision       string        `json:"revision"`
	IdempotencyKey string        `json:"idempotencyKey"`
	Preconditions  Preconditions `json:"preconditions"`
	// Persist this outcome durably before acknowledging RecordFailureOutcome.
	// A later DeleteCandidate action relies on this crash-recovery latch.
	FailureOutcome *FailureOutcome `json:"failureOutcome,omitempty"`
}

type Plan struct {
	Subject              Subject  `json:"subject"`
	ReconcileKey         string   `json:"reconcileKey"`
	Revision             string   `json:"revision"`
	Phase                Phase    `json:"phase"`
	Code                 string   `json:"code,omitempty"`
	MissingBuilds        []string `json:"missingBuilds,omitempty"`
	Blockers             []string `json:"blockers,omitempty"`
	ActiveRevision       string   `json:"activeRevision,omitempty"`
	ActiveRouteConfirmed bool     `json:"activeRouteConfirmed"`
	Next                 *Action  `json:"next,omitempty"`
}
