package runtimeconfig

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type Worker struct {
	mu               sync.Mutex
	config           Config
	gate             *admission.Gate
	snapshot         journal.Snapshot
	closed, poisoned bool
	retirements      map[string]*Retirement
}
type Session struct {
	worker  *Worker
	attempt string
	binding *kube.NamespaceBinding
}

func (s *Session) Context() ownership.NamespacedContext { return s.binding.Context() }

func OpenInitial(ctx context.Context, config Config) (*Worker, error) { return open(ctx, config, nil) }
func OpenClean(ctx context.Context, config Config, checkpoint Checkpoint) (*Worker, error) {
	return open(ctx, config, &checkpoint)
}
func open(ctx context.Context, config Config, checkpoint *Checkpoint) (*Worker, error) {
	if config.Diagnostics != nil {
		frozen := *config.Diagnostics
		config.Diagnostics = &frozen
		if !validDiagnosticConfig(frozen) {
			return nil, failure(Invalid)
		}
	}
	if config.Mode != LocalCleanHandoff || config.Cell == nil || config.Client == nil || config.Writer == nil || config.Project.Owner().Lifetime != ownership.Disposable {
		return nil, failure(Invalid)
	}
	identity := config.Cell.Identity()
	owner := config.Project.Owner()
	if identity.Subject != (rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}) || identity.ControlOwner.EnvironmentID != owner.EnvironmentID || identity.ControlOwner.InstallationID != owner.InstallationID {
		return nil, failure(SubjectMismatch)
	}
	scope, err := config.Project.Scope(ownership.Kubernetes)
	if err != nil || scope.Namespace != "" || scope.ClusterID != identity.ControlParent.ClusterID || identity.ControlParent.UID == "" {
		return nil, failure(Invalid)
	}
	repository, err := journal.New(config.Cell, identity.Subject, identity.CellUID)
	if err != nil {
		return nil, failure(Invalid)
	}
	var handoff admission.Handoff
	if checkpoint != nil {
		if _, err := checkpoint.JSON(); err != nil {
			return nil, err
		}
		c := checkpoint.value
		if c.ProjectOwner != owner || !reflect.DeepEqual(c.Identity, identity) {
			return nil, failure(Changed)
		}
		handoff, err = admission.Parse(c.Handoff)
		if err != nil {
			return nil, failure(Invalid)
		}
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		return nil, classify(err)
	}
	w := &Worker{config: config, snapshot: snapshot}
	if err := w.admit(); err != nil {
		return nil, err
	}
	if err := w.preflight(ctx); err != nil {
		return nil, err
	}
	admissionConfig := admission.Config{Mode: admission.LocalCleanHandoff, Repository: repository}
	var claim *admission.Claim
	if checkpoint == nil {
		claim, err = admission.BeginInitial(ctx, admissionConfig)
	} else {
		claim, err = admission.BeginResume(ctx, admissionConfig, handoff)
	}
	if err != nil {
		return nil, classify(err)
	}
	// Claim/Gate never escape this Worker. Preflight performed only reads; the
	// exact conditional claim must acknowledge before any project mutation.
	w.gate, err = claim.Confirm(ctx)
	if err != nil {
		return nil, classify(err)
	}
	w.snapshot, err = w.gate.Snapshot(ctx)
	if err != nil {
		return nil, classify(err)
	}
	if err := w.admit(); err != nil {
		return nil, err
	}
	return w, nil
}
func (w *Worker) preflight(ctx context.Context) error {
	if err := w.preflightDiagnosticIdentity(); err != nil {
		return err
	}
	// A typed bridge may persist additional reviewed kinds later. This runtime
	// cannot ignore their history and then claim complete replay/teardown safety.
	for _, write := range w.snapshot.Envelope.Writes {
		if write.Preconditions.Kind != "Namespace" && write.Preconditions.Kind != "ConfigMap" && write.Preconditions.Kind != "Deployment" && write.Preconditions.Kind != "Service" && write.Preconditions.Kind != "ServiceAccount" {
			return failure(Invalid)
		}
		if write.Preconditions.Kind == "ServiceAccount" {
			index := w.attemptIndex(write.OperationID)
			if index >= 0 && w.snapshot.Envelope.Attempts[index].State == journal.Observed && w.config.ServiceAccountInputs == nil {
				return w.missingInputStore(ctx, write)
			}
			if index < 0 || !write.Input.MatchesKind("ServiceAccount") {
				return failure(Invalid)
			}
		}
		if write.Preconditions.Kind == "Deployment" {
			index := w.attemptIndex(write.OperationID)
			if index >= 0 && w.snapshot.Envelope.Attempts[index].State == journal.Observed && write.Input != (journal.InputReference{}) && w.config.DeploymentInputs == nil {
				return w.missingInputStore(ctx, write)
			}
			if index < 0 || w.snapshot.Envelope.Attempts[index].State == journal.Observed && (write.Input == (journal.InputReference{}) && w.config.DeploymentDesired == nil || write.Input != (journal.InputReference{}) && w.config.DeploymentInputs == nil) {
				return failure(Invalid)
			}
		}
		if write.Preconditions.Kind == "Service" {
			index := w.attemptIndex(write.OperationID)
			if index >= 0 && w.snapshot.Envelope.Attempts[index].State == journal.Observed && write.Input.MatchesKind("Service") && w.config.ServiceInputs == nil {
				return w.missingInputStore(ctx, write)
			}
			if index < 0 || w.snapshot.Envelope.Attempts[index].State == journal.Observed && (w.config.ServiceInputs == nil || !write.Input.MatchesKind("Service")) {
				return failure(Invalid)
			}
		}
	}
	// Observe pinned records before admitting even a clean process. No name-only
	// adoption, missing-object recreation or unresolved-write recovery occurs.
	for _, attempt := range w.snapshot.Envelope.Attempts {
		if attempt.State == journal.Draining {
			parent, err := w.namespaceRecord(attempt)
			if err != nil {
				return err
			}
			if _, err := w.config.Client.ResumeDrainingNamespace(ctx, w.config.Project, parent, attempt.OperationID); err != nil && !apierrors.IsNotFound(err) {
				return classify(err)
			}
		}
		if attempt.State == journal.Observed {
			binding, err := w.rehydrate(ctx, attempt)
			if err != nil {
				return err
			}
			for _, write := range w.snapshot.Envelope.Writes {
				if write.OperationID == attempt.OperationID && write.Preconditions.Kind == "ServiceAccount" && write.State == journal.Committed {
					if err := w.restoreServiceAccount(ctx, binding, attempt, write); err != nil {
						return err
					}
				}
				if write.OperationID == attempt.OperationID && write.Preconditions.Kind == "Service" && write.State == journal.Committed {
					if err := w.restoreService(ctx, binding, attempt, write); err != nil {
						return err
					}
				}
				if write.OperationID == attempt.OperationID && write.Preconditions.Kind == "Deployment" && write.State == journal.Committed {
					if err := w.restoreDeployment(ctx, binding, attempt, write); err != nil {
						return err
					}
				}
				if write.OperationID == attempt.OperationID && write.Preconditions.Kind == "ConfigMap" && write.State == journal.Committed {
					if write.Preconditions.Action != "Create" {
						return failure(Invalid)
					}
					record, err := w.configRecord(binding, write)
					if err != nil {
						return err
					}
					observed, err := binding.ObserveConfigMap(ctx, record)
					if err != nil {
						return classify(err)
					}
					if !metadataMatches(observed, write) || !bodyMatchesStored(observed, write.Preconditions.Fingerprint) {
						return failure(Changed)
					}
				}
			}
		}
	}
	return nil
}
func (w *Worker) admit() error {
	if w.closed {
		return failure(Closed)
	}
	if w.poisoned {
		return failure(Blocked)
	}
	owner := w.config.Project.Owner()
	identity := w.config.Cell.Identity()
	if identity.Subject != (rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}) {
		return failure(SubjectMismatch)
	}
	for _, write := range w.snapshot.Envelope.Writes {
		if write.State == journal.Pending || write.State == journal.Unknown {
			return failure(Blocked)
		}
	}
	_, err := w.dispatchWriter()
	return err
}
func (w *Worker) dispatchWriter() (ownership.Writer, error) {
	owner := w.config.Project.Owner()
	identity := w.config.Cell.Identity()
	writer := w.config.Writer()
	if identity.Subject != (rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}) || identity.ControlOwner.EnvironmentID != owner.EnvironmentID || identity.ControlOwner.InstallationID != owner.InstallationID || writer.Owner != owner || writer.Epoch != w.snapshot.Envelope.WriterEpoch || writer.ID == "" || !time.Now().Before(writer.ExpiresAt) {
		return ownership.Writer{}, failure(SubjectMismatch)
	}
	return writer, nil
}
func (w *Worker) current(ctx context.Context) error {
	if err := w.admit(); err != nil {
		return err
	}
	current, err := w.gate.Snapshot(ctx)
	if err != nil {
		return classify(err)
	}
	if current.Version != w.snapshot.Version || current.Envelope.Digest != w.snapshot.Envelope.Digest {
		w.poisoned = true
		return failure(Changed)
	}
	return nil
}
func clone(e journal.Envelope) journal.Envelope {
	encoded, _ := json.Marshal(e)
	var result journal.Envelope
	_ = json.Unmarshal(encoded, &result)
	return result
}
func (w *Worker) commit(ctx context.Context, next journal.Envelope) error {
	snapshot, err := w.gate.Commit(ctx, next)
	if err != nil {
		w.poisoned = true
		return classify(err)
	}
	w.snapshot = snapshot
	return nil
}
func classify(err error) error {
	var provider *kube.Error
	// A typed nil must not reach the provider's Unwrap receiver while probing
	// status/sentinel classes. It supplies no observed provider outcome.
	if errors.As(err, &provider) && provider == nil {
		return failure(Unavailable)
	}
	if errors.Is(err, admission.ErrUncertain) {
		return failure(Uncertain)
	}
	if errors.Is(err, admission.ErrChanged) {
		return failure(Changed)
	}
	if errors.Is(err, admission.ErrInvalid) {
		return failure(Invalid)
	}
	if errors.Is(err, admission.ErrBlocked) {
		return failure(Blocked)
	}
	if errors.Is(err, admission.ErrClosed) {
		return failure(Closed)
	}
	if apierrors.IsNotFound(err) {
		return failure(Changed)
	}
	if errors.Is(err, journal.ErrUnknown) {
		return failure(Uncertain)
	}
	if errors.Is(err, journal.ErrMissing) || errors.Is(err, journal.ErrConflict) {
		return failure(Changed)
	}
	if errors.Is(err, journal.ErrInvalid) {
		return failure(Invalid)
	}
	if errors.As(err, &provider) {
		switch provider.Code {
		case kube.UnknownWriteOutcome:
			return failure(Uncertain)
		case kube.OwnershipRejected, kube.AttemptConsumed, kube.ConditionalConflict, kube.ParentTerminating, kube.DeploymentSpecMismatch, kube.ServiceSpecMismatch, kube.ServiceAccountSpecMismatch:
			return failure(Changed)
		}
	}
	return failure(Unavailable)
}
func (w *Worker) retainUnknown(ctx context.Context, index int) error {
	return w.retainMutationFault(ctx, index, &kube.Error{Code: kube.UnknownWriteOutcome}, false)
}
func (w *Worker) finishWrite(ctx context.Context, index int, state journal.WriteState, uid, version string, parent bool) error {
	next := clone(w.snapshot.Envelope)
	next.Writes[index].State = state
	next.Writes[index].ResultUID, next.Writes[index].ResultVersion = uid, version
	if parent && state == journal.Committed {
		for i := range next.Attempts {
			if next.Attempts[i].OperationID == next.Writes[index].OperationID {
				next.Attempts[i].State = journal.Observed
				next.Attempts[i].NamespaceUID = uid
			}
		}
	}
	return w.commit(ctx, next)
}
func (w *Worker) retainFailure(ctx context.Context, index int, err error) error {
	return w.retainMutationFault(ctx, index, err, false)
}

// Quiesce closes the worker and drains its serialized callback before asking
// admission to persist HandoffReady. It never reopens dispatch. The supervisor
// must additionally observe this worker process exit before starting another.
func (w *Worker) Quiesce(ctx context.Context) (Checkpoint, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return Checkpoint{}, failure(Closed)
	}
	w.closed = true
	if err := w.providerDrained(); err != nil {
		return Checkpoint{}, err
	}
	handoff, err := w.gate.Ready(ctx)
	if err != nil {
		return Checkpoint{}, classify(err)
	}
	return w.checkpoint(handoff)
}

func (w *Worker) providerDrained() error {
	for _, attempt := range w.snapshot.Envelope.Attempts {
		if len(w.config.Client.Lifecycle(attempt.NamespaceName).Unresolved) != 0 {
			return failure(Blocked)
		}
	}
	return nil
}
func (w *Worker) checkpoint(handoff admission.Handoff) (Checkpoint, error) {
	data, err := handoff.JSON()
	if err != nil {
		return Checkpoint{}, failure(Invalid)
	}
	return Checkpoint{checkpointValue{Schema: 2, ProjectOwner: w.config.Project.Owner(), Identity: w.config.Cell.Identity(), Handoff: data}}, nil
}

// RecoverQuiesce acknowledges only the closed Gate's exact retained journal
// proposal/ready proposal. It never resumes this worker or resolves uncertain
// provider writes by name, absence or copied metadata.
func (w *Worker) RecoverQuiesce(ctx context.Context) (Checkpoint, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		return Checkpoint{}, failure(Invalid)
	}
	recovered, err := w.gate.RecoverCommit(ctx)
	if err == nil {
		w.snapshot = recovered
	} else if !errors.Is(err, admission.ErrInvalid) {
		return Checkpoint{}, classify(err)
	}
	if err := w.providerDrained(); err != nil {
		return Checkpoint{}, err
	}
	handoff, err := w.gate.RecoverReady(ctx)
	if err != nil {
		return Checkpoint{}, classify(err)
	}
	return w.checkpoint(handoff)
}
