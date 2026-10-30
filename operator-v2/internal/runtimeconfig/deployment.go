package runtimeconfig

import (
	"context"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/deploymentjournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	appsv1 "k8s.io/api/apps/v1"
)

func (w *Worker) deploymentRecord(binding *kube.NamespaceBinding, write journal.Write) (ownership.NamespacedRecord, ownership.NamespacedIntent, error) {
	p := write.Preconditions
	if p.Kind != "Deployment" || p.Action != "Create" || p.ParentUID != binding.Parent().UID || write.State != journal.Committed || write.WriterEpoch != w.snapshot.Envelope.WriterEpoch {
		return ownership.NamespacedRecord{}, ownership.NamespacedIntent{}, failure(Changed)
	}
	intent, err := binding.Context().Intent("deployment", p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
	if err != nil {
		return ownership.NamespacedRecord{}, ownership.NamespacedIntent{}, failure(Invalid)
	}
	record := ownership.NamespacedRecord{Parent: binding.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Intent.Scope, Purpose: "deployment", Name: p.Name, ProviderID: write.ResultUID}, Owner: intent.Intent.Owner, CreationOperationID: intent.Intent.OperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: p.TargetRevision, SpecFingerprint: p.Fingerprint, WriterEpoch: write.WriterEpoch}}
	return record, intent, nil
}

func (w *Worker) restoreDeployment(ctx context.Context, binding *kube.NamespaceBinding, attempt journal.Attempt, write journal.Write) error {
	record, intent, err := w.deploymentRecord(binding, write)
	if err != nil {
		return err
	}
	object, err := w.deploymentDesired(ctx, binding, attempt, write)
	if err != nil {
		return err
	}
	_, err = binding.ObserveDeployment(ctx, kube.DeploymentRead{Desired: object, Intent: intent, Record: record})
	if err != nil {
		return w.observationFailure(ctx, write, err)
	}
	return nil
}
func (w *Worker) deploymentDesired(ctx context.Context, binding *kube.NamespaceBinding, attempt journal.Attempt, write journal.Write) (*appsv1.Deployment, error) {
	p := write.Preconditions
	var object *appsv1.Deployment
	var err error
	if write.Input != (journal.InputReference{}) {
		if w.config.DeploymentInputs == nil {
			return nil, w.missingInputStore(ctx, write)
		}
		var payload runtimeinputs.Payload
		payload, err = w.config.DeploymentInputs.Resolve(ctx, write.Input, w.childInputTarget(binding, attempt, p.Name, p.Fingerprint))
		object = payload.ObjectForProvider()
	} else {
		if w.config.DeploymentDesired == nil {
			return nil, failure(Invalid)
		}
		object, err = w.config.DeploymentDesired(ctx, DeploymentRestore{Namespace: binding.Context(), AttemptID: attempt.OperationID, Revision: attempt.Revision, DesiredDigest: attempt.DesiredDigest, Name: p.Name, Fingerprint: p.Fingerprint})
	}
	if err != nil || object == nil {
		if write.Input != (journal.InputReference{}) {
			return nil, w.inputFailure(ctx, Unavailable, "Deployment", w.childInputTarget(binding, attempt, p.Name, p.Fingerprint), write.Input, runtimefault.InputResolve, err)
		}
		return nil, failure(Unavailable)
	}
	// Freeze the resolver result, then independently enforce the persisted hash.
	// The live resource is never provided to this resolver as desired input.
	object = object.DeepCopy()
	fingerprint, err := kube.DeploymentFingerprint(object)
	if err != nil || fingerprint != p.Fingerprint {
		if write.Input != (journal.InputReference{}) {
			return nil, w.inputFailure(ctx, Changed, "Deployment", w.childInputTarget(binding, attempt, p.Name, p.Fingerprint), write.Input, runtimefault.InputResolve, runtimeinputs.ErrUnproven)
		}
		return nil, failure(Changed)
	}
	return object, nil
}

func (w *Worker) childInputTarget(binding *kube.NamespaceBinding, attempt journal.Attempt, name, fingerprint string) runtimeinputs.Target {
	return runtimeinputs.Target{CellUID: w.config.Cell.Identity().CellUID, Parent: binding.Parent(), AttemptID: attempt.OperationID, Revision: attempt.Revision, DesiredDigest: attempt.DesiredDigest, Name: name, Fingerprint: fingerprint}
}

// ApplyDeployment accepts a complete rendered baseline Deployment. Pending is
// acknowledged before the privately constructed journal verifier can authorize
// its POST. A committed replay only observes the exact canonical UID/spec.
// A known failed readiness observation returns a nonzero canonical receipt
// together with a safe fault. Its JournalVersion predates fault publication;
// it is a historical read receipt, not the latest diagnostic CAS authority.
func (s *Session) ApplyDeployment(ctx context.Context, desired renderer.DesiredObject) (DeploymentReceipt, error) {
	return s.deployment(ctx, desired, true)
}

// ObserveDeployment never creates a missing write or resource and never clears
// a retained unknown create. A known failed readiness observation returns its
// canonical receipt together with a safe fault; receipt delivery and resource
// observations remain distinct from diagnostic acknowledgement. JournalVersion
// captures the read's known snapshot before any diagnostic CAS.
func (s *Session) ObserveDeployment(ctx context.Context, desired renderer.DesiredObject) (DeploymentReceipt, error) {
	return s.deployment(ctx, desired, false)
}

func (s *Session) deployment(ctx context.Context, desired renderer.DesiredObject, create bool) (DeploymentReceipt, error) {
	var empty DeploymentReceipt
	if s == nil || s.worker == nil || s.binding == nil {
		return empty, failure(Invalid)
	}
	object, ok := desired.Object.(*appsv1.Deployment)
	if !ok || object == nil {
		return empty, failure(Invalid)
	}
	object = object.DeepCopy()
	w := s.worker
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.current(ctx); err != nil {
		return empty, err
	}
	var allowed error
	if create {
		allowed = w.normalWorkAllowed()
	} else {
		allowed = w.normalObservationAllowed()
	}
	if allowed != nil {
		return empty, allowed
	}
	index := w.attemptIndex(s.attempt)
	if index < 0 {
		return empty, failure(Invalid)
	}
	attempt := w.snapshot.Envelope.Attempts[index]
	i := desired.Intent
	if index != len(w.snapshot.Envelope.Attempts)-1 || attempt.State != journal.Observed || attempt.NamespaceUID != s.binding.Parent().UID || i.Parent != s.binding.Parent() || i.Intent.Owner != w.config.Project.Owner() || i.Intent.Scope != s.binding.Context().Scope() || i.Intent.Purpose != "deployment" || i.Intent.Name != object.Name || i.Intent.OperationID != attempt.OperationID || i.Intent.Revision != attempt.Revision {
		return empty, failure(Changed)
	}
	fingerprint, err := kube.DeploymentFingerprint(object)
	if err != nil || fingerprint != i.Intent.SpecFingerprint {
		return empty, failure(Invalid)
	}
	pending := journal.Write{ID: writeID("deployment", attempt.OperationID, object.Name), OperationID: attempt.OperationID, WriterEpoch: w.snapshot.Envelope.WriterEpoch, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create", Name: object.Name, ParentUID: attempt.NamespaceUID, Fingerprint: fingerprint, TargetRevision: attempt.Revision, ProviderOperationID: i.Intent.OperationID}}
	for _, write := range w.snapshot.Envelope.Writes {
		if write.ID != pending.ID {
			continue
		}
		if write.Preconditions != pending.Preconditions || write.OperationID != pending.OperationID || write.State != journal.Committed {
			return empty, failure(Changed)
		}
		// Referenced private input remains independently required on replay,
		// even when this call also supplies a matching desired object.
		if write.Input != (journal.InputReference{}) {
			object, err = w.deploymentDesired(ctx, s.binding, attempt, write)
			if err != nil {
				return empty, err
			}
		}
		record, intent, err := w.deploymentRecord(s.binding, write)
		if err != nil {
			return empty, err
		}
		observed, err := s.binding.ObserveDeployment(ctx, kube.DeploymentRead{Desired: object, Intent: intent, Record: record})
		if err != nil {
			return empty, w.observationFailure(ctx, write, err)
		}
		receipt := w.deploymentReceipt(attempt.OperationID, record, observed)
		receipt.Readiness, err = w.deploymentReadiness(ctx, write, receipt.Readiness)
		return receipt, err
	}
	if !create {
		return empty, failure(Changed)
	}
	writer, err := w.dispatchWriter()
	if err != nil {
		return empty, err
	}
	if w.config.DeploymentInputs != nil {
		pending.Input, err = w.config.DeploymentInputs.Retain(ctx, runtimeinputs.Capture{Target: w.childInputTarget(s.binding, attempt, object.Name, fingerprint), Desired: object})
		if err != nil || !pending.Input.Valid() {
			if err == nil {
				err = runtimeinputs.ErrInvalid
			}
			return empty, w.inputFailure(ctx, Unavailable, "Deployment", w.childInputTarget(s.binding, attempt, object.Name, fingerprint), journal.InputReference{}, runtimefault.InputRetain, err)
		}
		// Body persistence may block. Revalidate the current admitted snapshot
		// before its reference can enter Pending and authorize a provider write.
		if err = w.current(ctx); err != nil {
			return empty, err
		}
		writer, err = w.dispatchWriter()
		if err != nil {
			return empty, err
		}
	} else if w.config.Diagnostics != nil {
		return empty, w.inputFailure(ctx, Invalid, "Deployment", w.childInputTarget(s.binding, attempt, object.Name, fingerprint), journal.InputReference{}, runtimefault.InputRetain, runtimeinputs.ErrNotConfigured)
	}
	guard, err := deploymentjournal.New(ctx, deploymentjournal.Config{Gate: w.gate, Owner: w.config.Project.Owner(), Parent: s.binding.Parent(), WriterID: writer.ID, Input: pending.Input})
	if err != nil {
		return empty, failure(Blocked)
	}
	next := clone(w.snapshot.Envelope)
	next.Writes = append(next.Writes, pending)
	writeIndex := len(next.Writes) - 1
	if err := w.commit(ctx, next); err != nil {
		return empty, err
	}
	writer, err = w.dispatchWriter()
	if err != nil {
		return empty, w.retainUndispatched(ctx, writeIndex, err)
	}
	creation, err := s.binding.CreateDeploymentWithReceipt(ctx, kube.DeploymentCreate{Desired: object, Intent: i, Writer: writer, WriteID: pending.ID, Verifier: guard})
	if err != nil {
		return empty, w.retainFailure(ctx, writeIndex, err)
	}
	record := creation.Record
	// Preserve the validated native response before a separate readiness read.
	// A later GET cannot manufacture this creation receipt or its historical RV.
	if err := w.finishWrite(ctx, writeIndex, journal.Committed, record.Record.Ref.ProviderID, creation.CreationResourceVersion, false); err != nil {
		return empty, w.receiptAcknowledgementFailure(writeIndex)
	}
	observed, err := s.binding.ObserveDeployment(ctx, kube.DeploymentRead{Desired: object, Intent: i, Record: record})
	if err != nil {
		return empty, w.committedObservationFailure(ctx, writeIndex, err)
	}
	receipt := w.deploymentReceipt(attempt.OperationID, record, observed)
	receipt.Readiness, err = w.deploymentReadiness(ctx, w.snapshot.Envelope.Writes[writeIndex], receipt.Readiness)
	return receipt, err
}

func (w *Worker) deploymentReceipt(attempt string, record ownership.NamespacedRecord, observed kube.DeploymentSnapshot) DeploymentReceipt {
	view := observed.View()
	return DeploymentReceipt{Receipt: Receipt{Subject: w.config.Cell.Identity().Subject, AttemptID: attempt, Record: record, ResourceVersion: view.ResourceVersion, JournalVersion: w.snapshot.Version}, Readiness: view.Readiness}
}
