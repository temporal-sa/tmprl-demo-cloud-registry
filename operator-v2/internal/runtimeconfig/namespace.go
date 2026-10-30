package runtimeconfig

import (
	"context"
	"reflect"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

func namespacePlan(project ownership.Context, attempt journal.Attempt) (renderer.NamespacePlan, error) {
	plan, err := renderer.RenderNamespace(project, renderer.NamespaceRequest{Revision: attempt.Revision, OperationID: attempt.OperationID, Fingerprint: attempt.DesiredDigest})
	if err != nil || plan.Object.Name != attempt.NamespaceName {
		return renderer.NamespacePlan{}, failure(Changed)
	}
	return plan, nil
}
func admitAttempt(attempt journal.Attempt, revision, digest, name string) error {
	if attempt.Revision != revision || attempt.DesiredDigest != digest || attempt.NamespaceName != name {
		return failure(Changed)
	}
	if attempt.State == journal.Consumed {
		return failure(Consumed)
	}
	if attempt.State == journal.Draining {
		return failure(Blocked)
	}
	if attempt.State == journal.Reserved {
		return failure(Rejected)
	}
	if attempt.State != journal.Observed || attempt.NamespaceUID == "" {
		return failure(Invalid)
	}
	return nil
}
func (w *Worker) rehydrate(ctx context.Context, attempt journal.Attempt) (*kube.NamespaceBinding, error) {
	if attempt.State != journal.Observed || attempt.NamespaceUID == "" {
		return nil, failure(Consumed)
	}
	parent, err := w.namespaceRecord(attempt)
	if err != nil {
		return nil, err
	}
	binding, err := w.config.Client.BindNamespace(ctx, w.config.Project, parent, attempt.OperationID)
	if err != nil {
		return nil, classify(err)
	}
	return binding, nil
}

func (w *Worker) namespaceRecord(attempt journal.Attempt) (ownership.Record, error) {
	plan, err := namespacePlan(w.config.Project, attempt)
	if err != nil {
		return ownership.Record{}, err
	}
	var birth *journal.Write
	for _, write := range w.snapshot.Envelope.Writes {
		if write.OperationID == attempt.OperationID && write.Preconditions.Kind == "Namespace" && write.Preconditions.Action == "Create" && write.State == journal.Committed {
			copy := write
			birth = &copy
		}
	}
	if birth == nil || birth.ResultUID != attempt.NamespaceUID || birth.Preconditions.Name != plan.Object.Name || birth.Preconditions.Fingerprint != plan.Intent.SpecFingerprint || birth.Preconditions.TargetRevision != plan.Intent.Revision || birth.Preconditions.ProviderOperationID != plan.Intent.OperationID || birth.WriterEpoch != w.snapshot.Envelope.WriterEpoch {
		return ownership.Record{}, failure(Changed)
	}
	parent := ownership.Record{Ref: ownership.ResourceRef{Scope: plan.Intent.Scope, Purpose: plan.Intent.Purpose, Name: plan.Intent.Name, ProviderID: birth.ResultUID}, Owner: plan.Intent.Owner, CreationOperationID: plan.Intent.OperationID, CreationIdempotencyKey: plan.Intent.IdempotencyKey, Revision: plan.Intent.Revision, SpecFingerprint: plan.Intent.SpecFingerprint, WriterEpoch: birth.WriterEpoch}
	return parent, nil
}

// EnsureNamespace accepts one immutable desired intent and accepted attempt ID.
// It renders bootstrap internally, commits exact pending intent before POST and
// commits observed UID/RV before returning a context usable by the pure renderer.
// A recorded create is never dispatched again, including known rejection.
func (w *Worker) EnsureNamespace(ctx context.Context, desired rollout.Desired, acceptedAttemptID string) (*Session, error) {
	return w.namespace(ctx, desired, acceptedAttemptID, true)
}

// ObserveNamespace opens only an already acknowledged canonical attempt. It
// never reserves a name, records a new attempt or invokes namespace creation.
// A failed operation remains immutable while its resources can be inspected.
func (w *Worker) ObserveNamespace(ctx context.Context, desired rollout.Desired, acceptedAttemptID string) (*Session, error) {
	return w.namespace(ctx, desired, acceptedAttemptID, false)
}
func (w *Worker) namespace(ctx context.Context, desired rollout.Desired, acceptedAttemptID string, create bool) (*Session, error) {
	if w == nil || w.gate == nil || w.config.Cell == nil || w.config.Client == nil {
		return nil, failure(Invalid)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.current(ctx); err != nil {
		return nil, err
	}
	var allowed error
	if create {
		allowed = w.normalWorkAllowed()
	} else {
		allowed = w.normalObservationAllowed()
	}
	if allowed != nil {
		return nil, allowed
	}
	if desired.Subject != w.config.Cell.Identity().Subject {
		return nil, failure(SubjectMismatch)
	}
	if _, err := rollout.Compute(desired, rollout.Observed{Subject: desired.Subject, Generation: desired.Generation, ObservationToken: "local-runtime-admission"}); err != nil {
		return nil, failure(Invalid)
	}
	digest := journal.DesiredDigest(desired)
	plan, err := renderer.RenderNamespace(w.config.Project, renderer.NamespaceRequest{Revision: desired.Revision, OperationID: acceptedAttemptID, Fingerprint: digest})
	if err != nil {
		return nil, failure(Invalid)
	}
	control := w.config.Cell.Identity().ControlParent
	if control.Name == plan.Object.Name {
		return nil, failure(Invalid)
	}
	for _, attempt := range w.snapshot.Envelope.Attempts {
		if attempt.OperationID != acceptedAttemptID {
			continue
		}
		if err := admitAttempt(attempt, desired.Revision, digest, plan.Object.Name); err != nil {
			return nil, err
		}
		if w.config.Diagnostics != nil && (w.snapshot.Envelope.Projection == nil || w.snapshot.Envelope.Projection.Identity != acceptedDiagnosticIdentity(*w.config.Diagnostics, desired, acceptedAttemptID)) {
			return nil, failure(Changed)
		}
		binding, err := w.rehydrate(ctx, attempt)
		if err != nil {
			return nil, err
		}
		return &Session{worker: w, attempt: acceptedAttemptID, binding: binding}, nil
	}
	if !create {
		return nil, failure(Changed)
	}
	next := clone(w.snapshot.Envelope)
	number := uint64(1)
	if len(next.Attempts) > 0 {
		number = next.Attempts[len(next.Attempts)-1].Number + 1
		if number == 0 {
			return nil, failure(Invalid)
		}
	}
	next.Attempts = append(next.Attempts, journal.Attempt{Number: number, OperationID: acceptedAttemptID, Revision: desired.Revision, DesiredDigest: digest, NamespaceName: plan.Object.Name, State: journal.Reserved})
	// The previous primary belongs to its completed accepted attempt. The real
	// repository archives it exactly when the new attempt clears or replaces it.
	next.Projection, err = w.initialProjection(desired, acceptedAttemptID)
	if err != nil {
		return nil, err
	}
	pending := journal.Write{ID: writeID("namespace", acceptedAttemptID, plan.Object.Name), OperationID: acceptedAttemptID, WriterEpoch: w.snapshot.Envelope.WriterEpoch, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: plan.Object.Name, Fingerprint: plan.Intent.SpecFingerprint, TargetRevision: plan.Intent.Revision, ProviderOperationID: plan.Intent.OperationID}}
	next.Writes = append(next.Writes, pending)
	index := len(next.Writes) - 1
	if err := w.commit(ctx, next); err != nil {
		return nil, err
	}
	writer, err := w.dispatchWriter()
	if err != nil {
		return nil, w.retainUndispatched(ctx, index, err)
	}
	binding, err := w.config.Client.BootstrapNamespace(ctx, kube.NamespaceBootstrap{Context: w.config.Project, Intent: plan.Intent, Object: plan.Object, Writer: writer})
	if err != nil {
		return nil, w.retainFailure(ctx, index, err)
	}
	observed, err := binding.Refresh(ctx)
	if err != nil {
		return nil, w.retainObservationFailure(ctx, index, err)
	}
	if observed.Ref.ProviderID != binding.Parent().UID || !reflect.DeepEqual(observed.Owner, w.config.Project.Owner()) {
		return nil, w.retainUnknown(ctx, index)
	}
	if err := w.finishWrite(ctx, index, journal.Committed, binding.Parent().UID, observed.Version, true); err != nil {
		return nil, err
	}
	return &Session{worker: w, attempt: acceptedAttemptID, binding: binding}, nil
}
