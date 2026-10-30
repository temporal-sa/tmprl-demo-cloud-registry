package runtimeconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

// Unknown future codes do not inherit a noncommit promise merely by having the
// same Go error type as today's reviewed finite capability errors.
func rejectedMutation(err error, undispatched bool) bool {
	if undispatched {
		return true
	}
	var p *kube.Error
	if !errors.As(err, &p) || p == nil {
		return false
	}
	switch p.Code {
	case kube.InvalidConfiguration, kube.UnsupportedMode, kube.ClusterMismatch, kube.UnsupportedKind, kube.ParentUnavailable, kube.ChildUnavailable, kube.ParentTerminating, kube.ParentUnprotected, kube.NamespaceRetained, kube.OwnershipRejected, kube.ConditionalConflict, kube.LifecycleDraining, kube.AttemptConsumed, kube.DeploymentUnsupportedSpec, kube.DeploymentSpecMismatch, kube.DeploymentIntentUnverified, kube.ServiceUnsupportedSpec, kube.ServiceSpecMismatch, kube.ServiceIntentUnverified, kube.ServiceAccountUnsupportedSpec, kube.ServiceAccountSpecMismatch, kube.ServiceAccountIntentUnverified:
		return true
	}
	return false
}
func (w *Worker) faultContext(write journal.Write, step runtimefault.Step, dispatch runtimefault.Dispatch) runtimefault.Context {
	p := write.Preconditions
	purpose := map[string]string{"Namespace": "runtime-namespace", "ConfigMap": "configmap", "Deployment": "deployment", "Service": "service", "ServiceAccount": "service-account"}[p.Kind]
	uid := p.ParentUID
	if p.Kind == "Namespace" {
		uid = p.UID
	}
	c := runtimefault.Context{Step: step, Kind: runtimefault.Kind(p.Kind), Purpose: purpose, OperationID: write.OperationID, WriteID: write.ID, Name: p.Name, NamespaceUID: uid, Fingerprint: p.Fingerprint, OccurredAt: time.Now().UTC(), Dispatch: dispatch}
	if p.Kind == "Namespace" {
		c.NamespaceName = p.Name
	} else if index := w.attemptIndex(write.OperationID); index >= 0 {
		c.NamespaceName = w.snapshot.Envelope.Attempts[index].NamespaceName
	}
	if write.Input != (journal.InputReference{}) {
		r := write.Input
		c.Input = &r
	}
	// Cleanup can target a historical attempt. Its diagnostic belongs to the
	// current operation; exact historical write/namespace identity remains in
	// WriteID/Target and the independently retained journal row.
	if (step == runtimefault.Delete || step == runtimefault.Finalize) && w.snapshot.Envelope.Projection != nil {
		c.OperationID = w.snapshot.Envelope.Projection.Identity.OperationID
	}
	return c
}
func faultError(code Code, f runtimefault.Fault) error {
	if !f.Valid() {
		return failure(code)
	}
	return &Error{Code: code, Fault: &f}
}
func delivery(f runtimefault.Fault, sink runtimefault.Sink, state runtimefault.Delivery) runtimefault.Fault {
	updated, err := f.WithDelivery(sink, state, time.Now().UTC())
	if err != nil {
		return f
	}
	return updated
}

func (w *Worker) projectFault(next *journal.Envelope, f runtimefault.Fault) error {
	if w.config.Diagnostics == nil {
		return nil
	}
	p := next.Projection
	if p == nil || p.Sequence == ^uint64(0) {
		return failure(Invalid)
	}
	outcome, err := runtimefault.NewOutcome(p.Identity, writeID("diagnostic", p.Identity.OperationID, f.View().Failure.ID), p.Sequence+1, time.Now().UTC(), runtimefault.Admitted, f)
	if err != nil {
		return failure(Invalid)
	}
	event, err := outcome.Event()
	if err != nil {
		return failure(Invalid)
	}
	if event.Failure == nil {
		// Cleanup reports independent recovery, preserving original state/step
		// and availability instead of disguising the domain failure as cleanup.
		event.State, event.Step = p.State, p.Step
		event.Retry, event.Availability = &p.Retry, &p.Availability
	} else if p.Failure != nil {
		// A subsequent inspection failure is independently visible without
		// replacing the original cause. Keep latest bounded warnings by stable
		// fault identity; an explicit Partial flag records any older omission.
		current := *event.Failure
		warnings := make([]diagnostics.DomainFailure, 0, 4)
		for _, old := range p.Warnings {
			if old.ID != current.ID {
				warnings = append(warnings, old)
			}
		}
		if len(warnings) >= 4 {
			warnings = warnings[len(warnings)-3:]
			p.Partial = true
		}
		p.Warnings = warnings
		event.Failure = nil
		event.Warnings = []diagnostics.DomainFailure{current}
		event.State, event.Step = p.State, p.Step
		event.Retry, event.Availability = &p.Retry, &p.Availability
	}
	updated, err := p.Apply(event)
	if err != nil {
		return failure(Invalid)
	}
	// General bounded projection JSON can drop secondary summaries. An ACK
	// for this particular diagnostic requires it to survive actual persistence.
	if !materializesFault(updated, f.View().Failure) {
		return failure(Invalid)
	}
	next.Projection = &updated
	return nil
}

func materializesFault(p diagnostics.OperationProjection, required diagnostics.DomainFailure) bool {
	raw, err := p.JSON()
	if err != nil {
		return false
	}
	var stored diagnostics.OperationProjection
	if json.Unmarshal(raw, &stored) != nil {
		return false
	}
	want, err := json.Marshal(required)
	if err != nil {
		return false
	}
	matches := func(value *diagnostics.DomainFailure) bool {
		if value == nil {
			return false
		}
		got, err := json.Marshal(value)
		return err == nil && bytes.Equal(want, got)
	}
	if matches(stored.Failure) {
		return true
	}
	for _, value := range stored.Recovery {
		if matches(value.Failure) {
			return true
		}
	}
	for i := range stored.Warnings {
		if matches(&stored.Warnings[i]) {
			return true
		}
	}
	return false
}
func (w *Worker) acknowledgeFault(ctx context.Context, next journal.Envelope, f runtimefault.Fault) (runtimefault.Fault, error) {
	if err := w.projectFault(&next, f); err != nil {
		w.poisoned = true
		f = delivery(f, runtimefault.Diagnostic, runtimefault.DeliveryRejected)
		// Serialization/identity validation rejected this proposal before CAS.
		// Do not describe a known local rejection as an attempted unknown write.
		return delivery(f, runtimefault.Journal, runtimefault.DeliveryRejected), err
	}
	if err := w.commit(ctx, next); err != nil {
		f = delivery(f, runtimefault.Journal, runtimefault.DeliveryUnknown)
		if w.config.Diagnostics != nil {
			f = delivery(f, runtimefault.Diagnostic, runtimefault.DeliveryUnknown)
		}
		return f, err
	}
	f = delivery(f, runtimefault.Journal, runtimefault.Acknowledged)
	if w.config.Diagnostics != nil {
		f = delivery(f, runtimefault.Diagnostic, runtimefault.Acknowledged)
	}
	return f, nil
}
func (w *Worker) retainUndispatched(ctx context.Context, index int, err error) error {
	return w.retainMutationFault(ctx, index, err, true)
}
func (w *Worker) retainMutationFault(ctx context.Context, index int, original error, undispatched bool) error {
	state, code, dispatch := journal.Unknown, Uncertain, runtimefault.DispatchUnknown
	if rejectedMutation(original, undispatched) {
		state, code, dispatch = journal.Rejected, Rejected, runtimefault.Rejected
	}
	if undispatched {
		dispatch = runtimefault.NotDispatched
		code, _ = localAuthorityFailure(original)
	}
	write := w.snapshot.Envelope.Writes[index]
	step := runtimefault.Create
	if write.Preconditions.Action == "Delete" {
		step = runtimefault.Delete
	}
	if write.Preconditions.Action == "Finalize" {
		step = runtimefault.Finalize
	}
	var f runtimefault.Fault
	var err error
	if undispatched {
		_, reason := localAuthorityFailure(original)
		f, err = runtimefault.MapAuthority(w.faultContext(write, runtimefault.DispatchAuthorize, dispatch), reason)
	} else {
		f, err = runtimefault.MapProvider(w.faultContext(write, step, dispatch), original)
	}
	if err != nil {
		w.poisoned = true
		return failure(Uncertain)
	}
	next := clone(w.snapshot.Envelope)
	next.Writes[index].State = state
	f, err = w.acknowledgeFault(ctx, next, f)
	if err != nil {
		return faultError(Uncertain, f)
	}
	if state == journal.Unknown {
		w.poisoned = true
	}
	return faultError(code, f)
}

func localAuthorityFailure(err error) (Code, runtimefault.AuthorityReason) {
	var cause *Error
	if errors.As(err, &cause) && cause != nil {
		switch cause.Code {
		case SubjectMismatch:
			return SubjectMismatch, runtimefault.AuthorityMismatch
		case Changed:
			return Changed, runtimefault.StateChanged
		case Blocked:
			return Blocked, runtimefault.UnresolvedWrites
		case Closed:
			return Closed, runtimefault.ActorClosed
		case Invalid:
			return Invalid, runtimefault.AuthorityUnclassified
		}
	}
	return Rejected, runtimefault.AuthorityUnclassified
}

func (w *Worker) inputFailure(ctx context.Context, code Code, kind string, target runtimeinputs.Target, ref journal.InputReference, step runtimefault.Step, original error) error {
	if original == nil {
		original = runtimeinputs.ErrUnproven
	}
	write := journal.Write{ID: writeID(map[string]string{"Deployment": "deployment", "Service": "service", "ServiceAccount": "service-account"}[kind], target.AttemptID, target.Name), OperationID: target.AttemptID, Input: ref, Preconditions: journal.Preconditions{Kind: kind, Name: target.Name, ParentUID: target.Parent.UID, Fingerprint: target.Fingerprint}}
	f, err := runtimefault.MapInput(w.faultContext(write, step, runtimefault.NotDispatched), original)
	if err != nil {
		return failure(code)
	}
	return w.recordReadFault(ctx, code, f)
}
func (w *Worker) missingInputStore(ctx context.Context, write journal.Write) error {
	f, err := runtimefault.MapInput(w.faultContext(write, runtimefault.InputResolve, runtimefault.NotDispatched), runtimeinputs.ErrNotConfigured)
	if err != nil {
		return failure(Invalid)
	}
	return w.recordReadFault(ctx, Invalid, f)
}

// The POST receipt was known in this callback, but its durable result is not
// yet acknowledged. Preserve that dispatch fact separately from the failed
// observation and the retained Unknown row; a GET cannot recover provenance.
func (w *Worker) retainObservationFailure(ctx context.Context, index int, original error) error {
	if original == nil {
		original = &kube.Error{Code: kube.OwnershipRejected}
	}
	f, err := runtimefault.MapProvider(w.faultContext(w.snapshot.Envelope.Writes[index], runtimefault.Observe, runtimefault.Applied), original)
	if err != nil {
		w.poisoned = true
		return failure(Uncertain)
	}
	next := clone(w.snapshot.Envelope)
	next.Writes[index].State = journal.Unknown
	f, err = w.acknowledgeFault(ctx, next, f)
	w.poisoned = true
	return faultError(Uncertain, f)
}

func (w *Worker) committedObservationFailure(ctx context.Context, index int, original error) error {
	return w.observationFailure(ctx, w.snapshot.Envelope.Writes[index], original)
}
func (w *Worker) observationFailure(ctx context.Context, write journal.Write, original error) error {
	if write.State != journal.Committed {
		return failure(Invalid)
	}
	if original == nil {
		original = &kube.Error{Code: kube.OwnershipRejected}
	}
	f, err := runtimefault.MapProvider(w.faultContext(write, runtimefault.Observe, runtimefault.Applied), original)
	if err != nil {
		return classify(original)
	}
	code := Unavailable
	var classified *Error
	if errors.As(classify(original), &classified) && classified != nil {
		code = classified.Code
	}
	return w.recordReadFault(ctx, code, f)
}

// There is no earlier domain failure to replace: this specific failure IS the
// unconfirmed persistence of a known native creation receipt. It is returned
// as safe data; the already-poisoned Gate cannot attest its own failed ACK.
func (w *Worker) receiptAcknowledgementFailure(index int) error {
	f, err := runtimefault.MapReceiptAcknowledgement(w.faultContext(w.snapshot.Envelope.Writes[index], runtimefault.ReceiptAcknowledge, runtimefault.Applied))
	if err != nil {
		return failure(Uncertain)
	}
	return faultError(Uncertain, f)
}

// recordReadFault has no provider callback. Before clean admission there is no
// Gate and therefore no attempt to change the pinned HandoffReady journal.
// An activity/controller may materialize the returned safe fault independently.
func (w *Worker) recordReadFault(ctx context.Context, code Code, f runtimefault.Fault) error {
	if w.gate == nil || w.config.Diagnostics == nil {
		return faultError(code, f)
	}
	updated, err := w.acknowledgeFault(ctx, clone(w.snapshot.Envelope), f)
	if err != nil {
		return faultError(Uncertain, updated)
	}
	return faultError(code, updated)
}

// Original cause remains independent of cleanup/publication. Consumers use the
// finite safe Fault rather than serializing or printing the private raw error.
func operationFailed(p *diagnostics.OperationProjection) bool { return p != nil && p.Failure != nil }
