package runtimeconfig

import (
	"context"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/servicejournal"
	corev1 "k8s.io/api/core/v1"
)

func (w *Worker) serviceRecord(binding *kube.NamespaceBinding, write journal.Write) (kube.ServiceRecord, ownership.NamespacedIntent, error) {
	p := write.Preconditions
	if p.Kind != "Service" || p.Action != "Create" || p.ParentUID != binding.Parent().UID || write.State != journal.Committed || write.WriterEpoch != w.snapshot.Envelope.WriterEpoch || write.ResultUID == "" || write.ResultSpecDigest == "" || write.ResultSpecContract != kube.ServiceSpecContract || !write.Input.MatchesKind("Service") {
		return kube.ServiceRecord{}, ownership.NamespacedIntent{}, failure(Changed)
	}
	intent, err := binding.Context().Intent("service", p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
	if err != nil {
		return kube.ServiceRecord{}, ownership.NamespacedIntent{}, failure(Invalid)
	}
	record := ownership.NamespacedRecord{Parent: binding.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Intent.Scope, Purpose: "service", Name: p.Name, ProviderID: write.ResultUID}, Owner: intent.Intent.Owner, CreationOperationID: intent.Intent.OperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: p.TargetRevision, SpecFingerprint: p.Fingerprint, WriterEpoch: write.WriterEpoch}}
	return kube.ServiceRecord{Resource: record, SpecContract: write.ResultSpecContract, AllocatedSpecDigest: write.ResultSpecDigest}, intent, nil
}

func (w *Worker) serviceDesired(ctx context.Context, binding *kube.NamespaceBinding, attempt journal.Attempt, write journal.Write) (*corev1.Service, error) {
	if !write.Input.MatchesKind("Service") {
		return nil, failure(Invalid)
	}
	if w.config.ServiceInputs == nil {
		return nil, w.missingInputStore(ctx, write)
	}
	p := write.Preconditions
	payload, err := w.config.ServiceInputs.ResolveService(ctx, write.Input, w.childInputTarget(binding, attempt, p.Name, p.Fingerprint))
	object := payload.ObjectForProvider()
	if err != nil || object == nil {
		return nil, w.inputFailure(ctx, Unavailable, "Service", w.childInputTarget(binding, attempt, p.Name, p.Fingerprint), write.Input, runtimefault.InputResolve, err)
	}
	fp, err := kube.ServiceFingerprint(object)
	if err != nil || fp != p.Fingerprint {
		return nil, w.inputFailure(ctx, Changed, "Service", w.childInputTarget(binding, attempt, p.Name, p.Fingerprint), write.Input, runtimefault.InputResolve, runtimeinputs.ErrUnproven)
	}
	return object, nil
}
func (w *Worker) restoreService(ctx context.Context, binding *kube.NamespaceBinding, attempt journal.Attempt, write journal.Write) error {
	record, intent, err := w.serviceRecord(binding, write)
	if err != nil {
		return err
	}
	object, err := w.serviceDesired(ctx, binding, attempt, write)
	if err != nil {
		return err
	}
	_, err = binding.ObserveService(ctx, kube.ServiceRead{Desired: object, Intent: intent, Record: record})
	if err != nil {
		return w.observationFailure(ctx, write, err)
	}
	return nil
}

// ApplyService preserves original input before Pending and persists allocation
// provenance only from a known successful, fully validated create response.
func (s *Session) ApplyService(ctx context.Context, desired renderer.DesiredObject) (ServiceReceipt, error) {
	return s.service(ctx, desired, true)
}

// ObserveService never creates, learns a lost allocation receipt or resolves an
// uncertain create from a GET. Allocation is independent of backend readiness.
func (s *Session) ObserveService(ctx context.Context, desired renderer.DesiredObject) (ServiceReceipt, error) {
	return s.service(ctx, desired, false)
}
func (s *Session) service(ctx context.Context, desired renderer.DesiredObject, create bool) (ServiceReceipt, error) {
	var empty ServiceReceipt
	if s == nil || s.worker == nil || s.binding == nil {
		return empty, failure(Invalid)
	}
	object, ok := desired.Object.(*corev1.Service)
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
	if index != len(w.snapshot.Envelope.Attempts)-1 || attempt.State != journal.Observed || attempt.NamespaceUID != s.binding.Parent().UID || i.Parent != s.binding.Parent() || i.Intent.Owner != w.config.Project.Owner() || i.Intent.Scope != s.binding.Context().Scope() || i.Intent.Purpose != "service" || i.Intent.Name != object.Name || i.Intent.OperationID != attempt.OperationID || i.Intent.Revision != attempt.Revision {
		return empty, failure(Changed)
	}
	fingerprint, err := kube.ServiceFingerprint(object)
	if err != nil || fingerprint != i.Intent.SpecFingerprint {
		return empty, failure(Invalid)
	}
	pending := journal.Write{ID: writeID("service", attempt.OperationID, object.Name), OperationID: attempt.OperationID, WriterEpoch: w.snapshot.Envelope.WriterEpoch, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Service", Action: "Create", Name: object.Name, ParentUID: attempt.NamespaceUID, Fingerprint: fingerprint, TargetRevision: attempt.Revision, ProviderOperationID: i.Intent.OperationID}}
	for _, write := range w.snapshot.Envelope.Writes {
		if write.ID != pending.ID {
			continue
		}
		if write.Preconditions != pending.Preconditions || write.OperationID != pending.OperationID || write.State != journal.Committed {
			return empty, failure(Changed)
		}
		record, intent, err := w.serviceRecord(s.binding, write)
		if err != nil {
			return empty, err
		}
		original, err := w.serviceDesired(ctx, s.binding, attempt, write)
		if err != nil {
			return empty, err
		}
		observed, err := s.binding.ObserveService(ctx, kube.ServiceRead{Desired: original, Intent: intent, Record: record})
		if err != nil {
			return empty, w.observationFailure(ctx, write, err)
		}
		return w.serviceReceipt(attempt.OperationID, record, observed), nil
	}
	if !create {
		return empty, failure(Changed)
	}
	if w.config.ServiceInputs == nil {
		return empty, w.inputFailure(ctx, Invalid, "Service", w.childInputTarget(s.binding, attempt, object.Name, fingerprint), journal.InputReference{}, runtimefault.InputRetain, runtimeinputs.ErrNotConfigured)
	}
	if _, err := w.dispatchWriter(); err != nil {
		return empty, err
	}
	pending.Input, err = w.config.ServiceInputs.RetainService(ctx, runtimeinputs.ServiceCapture{Target: w.childInputTarget(s.binding, attempt, object.Name, fingerprint), Desired: object})
	if err != nil || !pending.Input.MatchesKind("Service") {
		if err == nil {
			err = runtimeinputs.ErrInvalid
		}
		return empty, w.inputFailure(ctx, Unavailable, "Service", w.childInputTarget(s.binding, attempt, object.Name, fingerprint), journal.InputReference{}, runtimefault.InputRetain, err)
	}
	if err := w.current(ctx); err != nil {
		return empty, err
	}
	writer, err := w.dispatchWriter()
	if err != nil {
		return empty, err
	}
	guard, err := servicejournal.New(ctx, servicejournal.Config{Gate: w.gate, Owner: w.config.Project.Owner(), Parent: s.binding.Parent(), WriterID: writer.ID, Input: pending.Input})
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
	record, err := s.binding.CreateService(ctx, kube.ServiceCreate{Desired: object, Intent: i, Writer: writer, WriteID: pending.ID, Verifier: guard})
	if err != nil {
		return empty, w.retainFailure(ctx, writeIndex, err)
	}
	next = clone(w.snapshot.Envelope)
	next.Writes[writeIndex].State = journal.Committed
	next.Writes[writeIndex].ResultUID = record.Resource.Record.Ref.ProviderID
	next.Writes[writeIndex].ResultVersion = record.CreationResourceVersion
	next.Writes[writeIndex].ResultSpecDigest = record.AllocatedSpecDigest
	next.Writes[writeIndex].ResultSpecContract = record.SpecContract
	if err := w.commit(ctx, next); err != nil {
		return empty, w.receiptAcknowledgementFailure(writeIndex)
	}
	observed, err := s.binding.ObserveService(ctx, kube.ServiceRead{Desired: object, Intent: i, Record: record})
	if err != nil {
		return empty, w.committedObservationFailure(ctx, writeIndex, err)
	}
	return w.serviceReceipt(attempt.OperationID, record, observed), nil
}
func (w *Worker) serviceReceipt(attempt string, record kube.ServiceRecord, observed kube.ServiceSnapshot) ServiceReceipt {
	v := observed.View()
	return ServiceReceipt{Receipt: Receipt{Subject: w.config.Cell.Identity().Subject, AttemptID: attempt, Record: record.Resource, ResourceVersion: v.ResourceVersion, JournalVersion: w.snapshot.Version}, SpecContract: record.SpecContract, AllocatedSpecDigest: record.AllocatedSpecDigest, ObservedAllocated: v.ObservedAllocated, BackendReadiness: v.BackendReadiness}
}
