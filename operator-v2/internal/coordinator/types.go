// Package coordinator contains the durable Temporal actor for rollout intents.
// It depends on capability contracts, never concrete provider implementations.
package coordinator

import (
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

const (
	SchemaVersion       = 1
	WorkflowName        = "registry.rollout.v2"
	StepActivityName    = "registry.rollout.step.v2"
	CleanupActivityName = "registry.rollout.cleanup.v2"
	ReconcileSignal     = "reconcile"
	FinalizeSignal      = "finalize"
	ShutdownSignal      = "shutdown"
	StatusQuery         = "status"
)

type Input struct {
	SchemaVersion int              `json:"schemaVersion"`
	Subject       rollout.Subject  `json:"subject"`
	ProjectName   string           `json:"projectName"`
	Initial       *rollout.Desired `json:"initial,omitempty"`
	Carry         *Carry           `json:"carry,omitempty"`
	PollInterval  time.Duration    `json:"pollInterval"`
	MaxCompleted  uint32           `json:"maxCompleted"`
}

type IntentIdentity struct {
	ReconcileKey string `json:"reconcileKey"`
	Generation   int64  `json:"generation"`
}

type Carry struct {
	Current              *rollout.Desired                 `json:"current,omitempty"`
	Pending              *rollout.Desired                 `json:"pending,omitempty"`
	LastDesired          *rollout.Desired                 `json:"lastDesired,omitempty"`
	Projection           *diagnostics.OperationProjection `json:"projection,omitempty"`
	PreviousFailure      *diagnostics.OperationProjection `json:"previousFailure,omitempty"`
	Settled              IntentIdentity                   `json:"settled"`
	Completed            uint32                           `json:"completed"`
	NextAttempt          uint64                           `json:"nextAttempt"`
	CurrentAttempt       uint64                           `json:"currentAttempt"`
	LastAttempt          uint64                           `json:"lastAttempt"`
	Finalizing           bool                             `json:"finalizing"`
	FinalizeBlocked      bool                             `json:"finalizeBlocked"`
	LastRejected         *diagnostics.DomainFailure       `json:"lastRejected,omitempty"`
	CleanupFailure       *diagnostics.DomainFailure       `json:"cleanupFailure,omitempty"`
	Finalized            bool                             `json:"finalized"`
	LastDrainedOperation string                           `json:"lastDrainedOperation,omitempty"`
	ExecutionBlocked     bool                             `json:"executionBlocked"`
	Superseding          bool                             `json:"superseding"`
	SupersessionBlocked  bool                             `json:"supersessionBlocked"`
}

type Status struct {
	SchemaVersion       int                              `json:"schemaVersion"`
	Projection          *diagnostics.OperationProjection `json:"projection,omitempty"`
	PreviousFailure     *diagnostics.OperationProjection `json:"previousFailure,omitempty"`
	LastRejected        *diagnostics.DomainFailure       `json:"lastRejected,omitempty"`
	CleanupFailure      *diagnostics.DomainFailure       `json:"cleanupFailure,omitempty"`
	Finalized           bool                             `json:"finalized"`
	Finalizing          bool                             `json:"finalizing"`
	FinalizeBlocked     bool                             `json:"finalizeBlocked"`
	Stopped             bool                             `json:"stopped"`
	ExecutionBlocked    bool                             `json:"executionBlocked"`
	Superseding         bool                             `json:"superseding"`
	SupersessionBlocked bool                             `json:"supersessionBlocked"`
	BlockedNextAction   string                           `json:"blockedNextAction,omitempty"`
}

type CleanupReason string

const (
	CancelRollout   CleanupReason = "CancelRollout"
	FinalizeProject CleanupReason = "FinalizeProject"
)

type CleanupRequest struct {
	Desired     rollout.Desired                  `json:"desired"`
	ProjectName string                           `json:"projectName"`
	OperationID string                           `json:"operationID"`
	Reason      CleanupReason                    `json:"reason"`
	Projection  *diagnostics.OperationProjection `json:"projection,omitempty"`
}

type CleanupResult struct {
	// Complete is acknowledged only after confirmed cleanup or an explicit
	// recorded retain decision. Requesting deletion alone is not completion.
	Complete          bool                             `json:"complete"`
	Retained          bool                             `json:"retained"`
	RetainedResources []diagnostics.ResourceRef        `json:"retainedResources,omitempty"`
	Projection        *diagnostics.OperationProjection `json:"projection,omitempty"`
}
