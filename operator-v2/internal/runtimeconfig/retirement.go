package runtimeconfig

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type RetirementRequest struct {
	AttemptID string
	Reason    journal.DrainReason
	// Projection is the current accepted operation. Historical failures stay in
	// the immutable archive; cleanup progress is added to this operation only.
	Projection diagnostics.OperationProjection
}
type RetirementProgress struct {
	Consumed          bool
	ForeignFinalizers []string
}

// Retirement is a private live continuation, never a restart checkpoint. A
// known accepted DELETE remains Pending until absence and is remembered only
// by this actor. A crash/unknown response cannot manufacture that acknowledgement.
type Retirement struct {
	worker             *Worker
	attemptID          string
	binding            *kube.NamespaceBinding
	deleteIndex        int
	deleteAcknowledged bool
}

func (w *Worker) normalWorkAllowed() error {
	// Finalization belongs to this pinned subject, not just one attempt. Its
	// persisted reason survives namespace consumption and clean handoff.
	for _, attempt := range w.snapshot.Envelope.Attempts {
		if attempt.DrainReason == journal.DrainProjectFinalization {
			return failure(Blocked)
		}
	}
	if err := w.normalObservationAllowed(); err != nil {
		return err
	}
	if w.config.Diagnostics != nil && operationFailed(w.snapshot.Envelope.Projection) && len(w.snapshot.Envelope.Attempts) > 0 && w.snapshot.Envelope.Attempts[len(w.snapshot.Envelope.Attempts)-1].State != journal.Consumed {
		return failure(Blocked)
	}
	return nil
}
func (w *Worker) normalObservationAllowed() error {
	for _, attempt := range w.snapshot.Envelope.Attempts {
		if attempt.State == journal.Draining {
			return failure(Blocked)
		}
	}
	return nil
}
func (w *Worker) attemptIndex(id string) int {
	for index, attempt := range w.snapshot.Envelope.Attempts {
		if attempt.OperationID == id {
			return index
		}
	}
	return -1
}
func retirementRecoveryID(id string) string { return writeID("retirement", id, "namespace") }

func (w *Worker) recoveryProjection(projection diagnostics.OperationProjection, attempt journal.Attempt, state diagnostics.OutcomeState) (diagnostics.OperationProjection, error) {
	subject := w.config.Cell.Identity().Subject
	if projection.Identity.Environment != subject.Environment || projection.Identity.Installation != subject.Installation || projection.Identity.ProjectUID != subject.ProjectUID {
		return diagnostics.OperationProjection{}, failure(SubjectMismatch)
	}
	if old := w.snapshot.Envelope.Projection; old != nil && old.Identity != projection.Identity {
		return diagnostics.OperationProjection{}, failure(Changed)
	}
	if _, err := projection.JSON(); err != nil {
		return diagnostics.OperationProjection{}, failure(Invalid)
	}
	if projection.Sequence == ^uint64(0) {
		return diagnostics.OperationProjection{}, failure(Invalid)
	}
	at := time.Now().UTC()
	if at.Before(projection.ProgressedAt) {
		return diagnostics.OperationProjection{}, failure(Invalid)
	}
	var retained []diagnostics.ResourceRef
	if state != diagnostics.OutcomeCompleted && attempt.NamespaceUID != "" {
		retained = []diagnostics.ResourceRef{{Provider: "kubernetes", Kind: "Namespace", ID: attempt.NamespaceUID}}
	}
	// Cleanup of a historical attempt must not replace the current operation's
	// state/step or original cause. Recovery carries its independent progress.
	updated, err := projection.Apply(diagnostics.OperationEvent{Identity: projection.Identity, Sequence: projection.Sequence + 1, OccurredAt: at, State: projection.State, Step: projection.Step, Retry: &projection.Retry, Availability: &projection.Availability, Recovery: []diagnostics.RecoveryOutcome{{ID: retirementRecoveryID(attempt.OperationID), Step: "namespace-cleanup", State: state, RetainedResources: retained}}})
	if err != nil {
		return diagnostics.OperationProjection{}, failure(Invalid)
	}
	return updated, nil
}

// BeginRetirement acknowledges the sticky reason and recovery/original cause
// before any teardown RPC. All methods share the Worker's admission and mutex.
// The goal is canonical absence of the finite runtime's known namespaces;
// payload shutdown and resources in other providers are separate capabilities.
func (w *Worker) BeginRetirement(ctx context.Context, request RetirementRequest) (*Retirement, error) {
	if w == nil || request.Reason == journal.DrainProjectFinalization && request.Projection.State != diagnostics.Finalizing {
		return nil, failure(Invalid)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.current(ctx); err != nil {
		return nil, err
	}
	if existing := w.retirements[request.AttemptID]; existing != nil {
		index := w.attemptIndex(request.AttemptID)
		if index < 0 || w.snapshot.Envelope.Attempts[index].DrainReason != request.Reason {
			return nil, failure(Changed)
		}
		return existing, nil
	}
	index := w.attemptIndex(request.AttemptID)
	if index < 0 {
		return nil, failure(Invalid)
	}
	attempt := w.snapshot.Envelope.Attempts[index]
	if attempt.State == journal.Consumed {
		return nil, failure(Consumed)
	}
	if request.Reason != journal.DrainCandidateFailed && request.Reason != journal.DrainSuperseded && request.Reason != journal.DrainProjectFinalization {
		return nil, failure(Invalid)
	}
	if attempt.DrainReason != "" && attempt.DrainReason != request.Reason {
		return nil, failure(Changed)
	}
	progress := diagnostics.OutcomeRunning
	if attempt.State == journal.Reserved {
		progress = diagnostics.OutcomeCompleted
	}
	projection, err := w.recoveryProjection(request.Projection, attempt, progress)
	if err != nil {
		return nil, err
	}
	next := clone(w.snapshot.Envelope)
	next.Projection = &projection
	next.Attempts[index].DrainReason = request.Reason
	if attempt.State == journal.Reserved {
		// A known rejected/non-dispatched allocation with no UID is consumed
		// without inventing a provider request or an observed object identity.
		next.Attempts[index].State = journal.Consumed
	} else {
		next.Attempts[index].State = journal.Draining
	}
	if err := w.commit(ctx, next); err != nil {
		return nil, err
	}
	r := &Retirement{worker: w, attemptID: request.AttemptID, deleteIndex: -1}
	if w.retirements == nil {
		w.retirements = map[string]*Retirement{}
	}
	w.retirements[request.AttemptID] = r
	return r, nil
}

func (r *Retirement) current(ctx context.Context) (int, error) {
	w := r.worker
	if w.closed {
		return -1, failure(Closed)
	}
	if w.poisoned {
		return -1, failure(Blocked)
	}
	if _, err := w.dispatchWriter(); err != nil {
		return -1, err
	}
	current, err := w.gate.Snapshot(ctx)
	if err != nil {
		return -1, classify(err)
	}
	if current.Version != w.snapshot.Version || current.Envelope.Digest != w.snapshot.Envelope.Digest {
		w.poisoned = true
		return -1, failure(Changed)
	}
	for index, write := range w.snapshot.Envelope.Writes {
		if write.State == journal.Unknown || write.State == journal.Pending && !(index == r.deleteIndex && r.deleteAcknowledged) {
			return -1, failure(Blocked)
		}
	}
	index := w.attemptIndex(r.attemptID)
	if index < 0 || w.snapshot.Envelope.Attempts[index].State != journal.Draining && w.snapshot.Envelope.Attempts[index].State != journal.Consumed {
		return -1, failure(Changed)
	}
	return index, nil
}

func (r *Retirement) consume(ctx context.Context, index int) (RetirementProgress, error) {
	w := r.worker
	next := clone(w.snapshot.Envelope)
	attempt := next.Attempts[index]
	next.Attempts[index].State = journal.Consumed
	next.Attempts[index].CanonicalAbsent = attempt.NamespaceUID != ""
	if r.deleteIndex >= 0 {
		if !r.deleteAcknowledged {
			return RetirementProgress{}, failure(Blocked)
		}
		next.Writes[r.deleteIndex].State = journal.Committed
		next.Writes[r.deleteIndex].ConfirmedAbsent = true
	}
	if next.Projection == nil {
		return RetirementProgress{}, failure(Invalid)
	}
	projection, err := w.recoveryProjection(*next.Projection, attempt, diagnostics.OutcomeCompleted)
	if err != nil {
		return RetirementProgress{}, err
	}
	next.Projection = &projection
	if err := w.commit(ctx, next); err != nil {
		return RetirementProgress{}, err
	}
	return RetirementProgress{Consumed: true}, nil
}

func (r *Retirement) pending(ctx context.Context, action string, description kube.NamespaceCleanupDescription) (int, error) {
	w := r.worker
	index := w.attemptIndex(r.attemptID)
	if index < 0 {
		return -1, failure(Invalid)
	}
	attempt := w.snapshot.Envelope.Attempts[index]
	if description.Namespace != attempt.NamespaceName || description.UID != attempt.NamespaceUID {
		return -1, failure(Changed)
	}
	preconditions, err := cleanupPreconditions(attempt, action, description)
	if err != nil {
		return -1, err
	}
	next := clone(w.snapshot.Envelope)
	next.Writes = append(next.Writes, journal.Write{ID: description.OperationID, OperationID: r.attemptID, WriterEpoch: next.WriterEpoch, State: journal.Pending, Preconditions: preconditions})
	if err := w.commit(ctx, next); err != nil {
		return -1, err
	}
	return len(next.Writes) - 1, nil
}

func cleanupPreconditions(attempt journal.Attempt, action string, description kube.NamespaceCleanupDescription) (journal.Preconditions, error) {
	// Tickets identify the hash algorithm explicitly; the reviewed journal
	// schema uses canonical raw SHA-256 hex. Normalize only this exact boundary.
	fingerprint, tagged := strings.CutPrefix(description.BodyDigest, "sha256:")
	digest, err := hex.DecodeString(fingerprint)
	if !tagged || err != nil || len(digest) != 32 || hex.EncodeToString(digest) != fingerprint || action != "Delete" && action != "Finalize" {
		return journal.Preconditions{}, failure(Invalid)
	}
	return journal.Preconditions{Kind: "Namespace", Action: action, Name: description.Namespace, UID: description.UID, Version: description.ResourceVersion, Fingerprint: fingerprint, TargetRevision: attempt.Revision, ProviderOperationID: description.OperationID}, nil
}

// Advance performs at most one conditional cleanup RPC. A retained namespace
// is a truthful progress result, not a failure of the original candidate.
func (r *Retirement) Advance(ctx context.Context) (RetirementProgress, error) {
	if r == nil || r.worker == nil {
		return RetirementProgress{}, failure(Invalid)
	}
	w := r.worker
	w.mu.Lock()
	defer w.mu.Unlock()
	index, err := r.current(ctx)
	if err != nil {
		return RetirementProgress{}, err
	}
	attempt := w.snapshot.Envelope.Attempts[index]
	if attempt.State == journal.Consumed {
		return RetirementProgress{Consumed: true}, nil
	}
	if r.binding == nil {
		parent, err := w.namespaceRecord(attempt)
		if err != nil {
			return RetirementProgress{}, err
		}
		r.binding, err = w.config.Client.ResumeDrainingNamespace(ctx, w.config.Project, parent, attempt.OperationID)
		if apierrors.IsNotFound(err) {
			return r.consume(ctx, index)
		}
		if err != nil {
			return RetirementProgress{}, classify(err)
		}
	}
	observed, err := r.binding.ObserveDrainingNamespace(ctx)
	if err != nil {
		return RetirementProgress{}, classify(err)
	}
	if observed.ConfirmedAbsent {
		return r.consume(ctx, index)
	}
	writer, err := w.dispatchWriter()
	if err != nil {
		return RetirementProgress{}, err
	}
	op := writeID("cleanup", r.attemptID, fmt.Sprint(len(w.snapshot.Envelope.Writes)+1))
	if !observed.Terminating {
		if r.deleteIndex >= 0 {
			return RetirementProgress{}, failure(Changed)
		}
		ticket, err := r.binding.PrepareNamespaceDelete(ctx, writer, op)
		if apierrors.IsNotFound(err) {
			return r.consume(ctx, index)
		}
		if err != nil {
			return RetirementProgress{}, classify(err)
		}
		writeIndex, err := r.pending(ctx, "Delete", ticket.Description())
		if err != nil {
			return RetirementProgress{}, err
		}
		result, err := ticket.Execute(ctx)
		if err != nil {
			return RetirementProgress{}, w.retainFailure(ctx, writeIndex, err)
		}
		if !result.Acknowledged {
			return RetirementProgress{}, w.retainUnknown(ctx, writeIndex)
		}
		r.deleteIndex, r.deleteAcknowledged = writeIndex, true
		return RetirementProgress{ForeignFinalizers: append([]string(nil), observed.ForeignFinalizers...)}, nil
	}
	if observed.OwnFinalizerPresent {
		ticket, err := r.binding.PrepareOwnFinalizerRemoval(ctx, writer, op)
		if apierrors.IsNotFound(err) {
			return r.consume(ctx, index)
		}
		if err != nil {
			return RetirementProgress{}, classify(err)
		}
		writeIndex, err := r.pending(ctx, "Finalize", ticket.Description())
		if err != nil {
			return RetirementProgress{}, err
		}
		result, err := ticket.Execute(ctx)
		if err != nil {
			return RetirementProgress{}, w.retainFailure(ctx, writeIndex, err)
		}
		if !result.Acknowledged {
			return RetirementProgress{}, w.retainUnknown(ctx, writeIndex)
		}
		if result.ConfirmedAbsent {
			next := clone(w.snapshot.Envelope)
			next.Writes[writeIndex].State, next.Writes[writeIndex].ConfirmedAbsent = journal.Committed, true
			if r.deleteIndex >= 0 {
				next.Writes[r.deleteIndex].State, next.Writes[r.deleteIndex].ConfirmedAbsent = journal.Committed, true
			}
			next.Attempts[index].State, next.Attempts[index].CanonicalAbsent = journal.Consumed, true
			projection, err := w.recoveryProjection(*next.Projection, attempt, diagnostics.OutcomeCompleted)
			if err != nil {
				return RetirementProgress{}, err
			}
			next.Projection = &projection
			if err := w.commit(ctx, next); err != nil {
				return RetirementProgress{}, err
			}
			return RetirementProgress{Consumed: true}, nil
		}
		if err := w.finishWrite(ctx, writeIndex, journal.Committed, result.UID, result.ResourceVersion, false); err != nil {
			return RetirementProgress{}, err
		}
	}
	return RetirementProgress{ForeignFinalizers: append([]string(nil), observed.ForeignFinalizers...)}, nil
}
