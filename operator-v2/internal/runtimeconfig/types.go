// Package runtimeconfig composes finite local typed creates, original private
// inputs, durable receipts, readiness observations and Namespace retirement.
// It does not execute a whole render bundle, publish traffic or provide a
// distributed writer fence. All managed writers must use one Worker.
package runtimeconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	appsv1 "k8s.io/api/apps/v1"
)

type Mode string

const LocalCleanHandoff Mode = "local_single_process_clean_handoff"

type Config struct {
	Mode    Mode
	Cell    *kubejournal.Cell
	Project ownership.Context
	Client  *kube.Client
	Writer  func() ownership.Writer
	// Diagnostics opts this local actor into operation-bound durable failure
	// projections. Historical rows without accepted diagnostic identity cannot
	// be silently upgraded. Production composition must supply this capability.
	Diagnostics *DiagnosticConfig
	// DeploymentDesired reconstructs historical accepted intent from an
	// independent immutable source. It must not derive desired from the live
	// Deployment, copied annotations, a name, or current mutable authoring.
	// Missing/changed renderer inputs fail clean-open before admission. The
	// returned complete body must hash to the original journal fingerprint.
	DeploymentDesired func(context.Context, DeploymentRestore) (*appsv1.Deployment, error)
	// DeploymentInputs persists complete private accepted bodies before Pending
	// and resolves their exact journal reference across clean process handoff.
	// Production composition must use retained inputs; DeploymentDesired is the
	// explicitly local, zero-reference prototype seam only.
	DeploymentInputs DeploymentInputStore
	// ServiceInputs is mandatory for admitted Service work; there is no
	// mutable-authoring or observed-body fallback for its original intent.
	ServiceInputs ServiceInputStore
	// ServiceAccountInputs opts admitted ServiceAccount work into independently
	// retained original bodies. There is no provider-only fallback for this kind.
	ServiceAccountInputs ServiceAccountInputStore
}

type ServiceAccountInputStore interface {
	RetainServiceAccount(context.Context, runtimeinputs.ServiceAccountCapture) (journal.InputReference, error)
	ResolveServiceAccount(context.Context, journal.InputReference, runtimeinputs.Target) (runtimeinputs.ServiceAccountPayload, error)
}

// ServiceAccountReceipt keeps historical creation evidence separate from the
// current read. On a later failed GET, Record/CreationResourceVersion may remain
// known while ResourceVersion is empty and ObservedMinimalPolicy is false.
type ServiceAccountReceipt struct {
	Receipt
	CreationResourceVersion string
	ObservedMinimalPolicy   bool
}

type DiagnosticConfig struct{ ProjectName string }

type ServiceInputStore interface {
	RetainService(context.Context, runtimeinputs.ServiceCapture) (journal.InputReference, error)
	ResolveService(context.Context, journal.InputReference, runtimeinputs.Target) (runtimeinputs.ServicePayload, error)
}
type ServiceReceipt struct {
	Receipt
	SpecContract, AllocatedSpecDigest string
	ObservedAllocated                 bool
	BackendReadiness                  string
}

type DeploymentInputStore interface {
	Retain(context.Context, runtimeinputs.Capture) (journal.InputReference, error)
	Resolve(context.Context, journal.InputReference, runtimeinputs.Target) (runtimeinputs.Payload, error)
}

type DeploymentRestore struct {
	Namespace                                             ownership.NamespacedContext
	AttemptID, Revision, DesiredDigest, Name, Fingerprint string
}

type DeploymentReceipt struct {
	Receipt
	Readiness kube.DeploymentReadiness
}
type Code string

const (
	Invalid         Code = "invalid_local_runtime_contract"
	SubjectMismatch Code = "runtime_subject_owner_mismatch"
	Blocked         Code = "runtime_unresolved_write_blocks_dispatch"
	Changed         Code = "runtime_pinned_state_changed"
	Uncertain       Code = "runtime_write_outcome_uncertain"
	Closed          Code = "runtime_worker_closed"
	Consumed        Code = "runtime_namespace_attempt_consumed"
	Rejected        Code = "runtime_create_rejected"
	Unavailable     Code = "runtime_observation_unavailable"
)

type Error struct {
	Code Code
	// Fault contains finite safe facts only; the original provider/input error
	// is never wrapped, stored or serialized by this runtime boundary.
	Fault *runtimefault.Fault `json:"fault,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return "unverified local runtime failure"
	}
	if e.Fault != nil && e.Fault.Valid() {
		return string(e.Code) + ": " + e.Fault.Error()
	}
	return string(e.Code)
}
func failure(code Code) error { return &Error{Code: code} }

type Receipt struct {
	Subject         rollout.Subject
	AttemptID       string
	Record          ownership.NamespacedRecord
	ResourceVersion string
	JournalVersion  journal.Version
}

type checkpointValue struct {
	Schema       int                  `json:"schema"`
	ProjectOwner ownership.Owner      `json:"projectOwner"`
	Identity     kubejournal.Identity `json:"identity"`
	Handoff      []byte               `json:"handoff"`
}

// Checkpoint is trusted local handoff data, not a lease or a provider credential.
// Only a successfully quiesced old worker may issue it. A process supervisor
// must observe that worker's exit before starting the next process. Do not mint
// or accept handoff bytes from project authoring or a read of a terminal cell.
// Admission consumes the exact ready version/session by conditional CAS before
// this runtime can dispatch. Copied or stale bytes cannot admit two successors.
// The supervisor still must observe old process exit; this is not RPC fencing.
type Checkpoint struct{ value checkpointValue }

const maxCheckpointBytes = 16384

func (c Checkpoint) JSON() ([]byte, error) {
	owner := c.value.ProjectOwner
	if c.value.Schema != 2 || owner.Lifetime != ownership.Disposable || owner.RunID == "" || c.value.Identity.Subject != (rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}) || c.value.Identity.CellUID == "" {
		return nil, failure(Invalid)
	}
	if _, err := admission.Parse(c.value.Handoff); err != nil {
		return nil, failure(Invalid)
	}
	data, err := json.Marshal(c.value)
	if err != nil || len(data) > maxCheckpointBytes {
		return nil, failure(Invalid)
	}
	return data, nil
}
func ParseCheckpoint(data []byte) (Checkpoint, error) {
	if len(data) > maxCheckpointBytes {
		return Checkpoint{}, failure(Invalid)
	}
	var c Checkpoint
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&c.value) != nil || decoder.Decode(new(any)) != io.EOF {
		return Checkpoint{}, failure(Invalid)
	}
	if _, err := c.JSON(); err != nil {
		return Checkpoint{}, err
	}
	return c, nil
}
