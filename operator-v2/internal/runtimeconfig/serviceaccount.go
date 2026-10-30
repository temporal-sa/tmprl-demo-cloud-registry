package runtimeconfig

import (
	"context"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/serviceaccountjournal"
	corev1 "k8s.io/api/core/v1"
)

func (w *Worker) serviceAccountRecord(binding *kube.NamespaceBinding, write journal.Write) (ownership.NamespacedRecord, ownership.NamespacedIntent, error) {
	p := write.Preconditions
	if p.Kind != "ServiceAccount" || p.Action != "Create" || p.ParentUID != binding.Parent().UID || write.State != journal.Committed || write.WriterEpoch != w.snapshot.Envelope.WriterEpoch || write.ResultUID == "" || write.ResultVersion == "" || !write.Input.MatchesKind("ServiceAccount") {
		return ownership.NamespacedRecord{}, ownership.NamespacedIntent{}, failure(Changed)
	}
	i, err := binding.Context().Intent("service-account", p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
	if err != nil {
		return ownership.NamespacedRecord{}, ownership.NamespacedIntent{}, failure(Invalid)
	}
	r := ownership.NamespacedRecord{Parent: binding.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: i.Intent.Scope, Purpose: "service-account", Name: p.Name, ProviderID: write.ResultUID}, Owner: i.Intent.Owner, CreationOperationID: i.Intent.OperationID, CreationIdempotencyKey: i.Intent.IdempotencyKey, Revision: p.TargetRevision, SpecFingerprint: p.Fingerprint, WriterEpoch: write.WriterEpoch}}
	return r, i, nil
}

func (w *Worker) serviceAccountDesired(ctx context.Context, binding *kube.NamespaceBinding, attempt journal.Attempt, write journal.Write) (*corev1.ServiceAccount, error) {
	if !write.Input.MatchesKind("ServiceAccount") {
		return nil, failure(Invalid)
	}
	if w.config.ServiceAccountInputs == nil {
		return nil, w.missingInputStore(ctx, write)
	}
	p := write.Preconditions
	target := w.childInputTarget(binding, attempt, p.Name, p.Fingerprint)
	payload, err := w.config.ServiceAccountInputs.ResolveServiceAccount(ctx, write.Input, target)
	o := payload.ObjectForProvider()
	if err != nil || o == nil {
		return nil, w.inputFailure(ctx, Unavailable, "ServiceAccount", target, write.Input, runtimefault.InputResolve, err)
	}
	fp, err := kube.ServiceAccountFingerprint(o)
	if err != nil || fp != p.Fingerprint {
		return nil, w.inputFailure(ctx, Changed, "ServiceAccount", target, write.Input, runtimefault.InputResolve, runtimeinputs.ErrUnproven)
	}
	return o, nil
}

func (w *Worker) restoreServiceAccount(ctx context.Context, binding *kube.NamespaceBinding, attempt journal.Attempt, write journal.Write) error {
	r, i, err := w.serviceAccountRecord(binding, write)
	if err != nil {
		return err
	}
	o, err := w.serviceAccountDesired(ctx, binding, attempt, write)
	if err != nil {
		return err
	}
	if _, err = binding.ObserveServiceAccount(ctx, kube.ServiceAccountRead{Desired: o, Intent: i, Record: r}); err != nil {
		return w.observationFailure(ctx, write, err)
	}
	return nil
}

// ApplyServiceAccount accepts only the finite minimal identity-free provider
// baseline. It retains original input before Pending, then records the exact
// native creation receipt before an independent policy read. No RBAC is applied.
func (s *Session) ApplyServiceAccount(ctx context.Context, desired renderer.DesiredObject) (ServiceAccountReceipt, error) {
	return s.serviceAccount(ctx, desired, true)
}

// ObserveServiceAccount never creates, learns a missing UID or recreates lost
// original inputs. A known historical receipt does not assert current policy.
func (s *Session) ObserveServiceAccount(ctx context.Context, desired renderer.DesiredObject) (ServiceAccountReceipt, error) {
	return s.serviceAccount(ctx, desired, false)
}

func (s *Session) serviceAccount(ctx context.Context, desired renderer.DesiredObject, create bool) (ServiceAccountReceipt, error) {
	var empty ServiceAccountReceipt
	if s == nil || s.worker == nil || s.binding == nil {
		return empty, failure(Invalid)
	}
	o, ok := desired.Object.(*corev1.ServiceAccount)
	if !ok || o == nil {
		return empty, failure(Invalid)
	}
	o = o.DeepCopy()
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
	a := w.snapshot.Envelope.Attempts[index]
	i := desired.Intent
	if index != len(w.snapshot.Envelope.Attempts)-1 || a.State != journal.Observed || a.NamespaceUID != s.binding.Parent().UID || i.Parent != s.binding.Parent() || i.Intent.Owner != w.config.Project.Owner() || i.Intent.Scope != s.binding.Context().Scope() || i.Intent.Purpose != "service-account" || i.Intent.Name != o.Name || i.Intent.OperationID != a.OperationID || i.Intent.Revision != a.Revision {
		return empty, failure(Changed)
	}
	fp, err := kube.ServiceAccountFingerprint(o)
	if err != nil || fp != i.Intent.SpecFingerprint {
		return empty, failure(Invalid)
	}
	pending := journal.Write{ID: writeID("service-account", a.OperationID, o.Name), OperationID: a.OperationID, WriterEpoch: w.snapshot.Envelope.WriterEpoch, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "ServiceAccount", Action: "Create", Name: o.Name, ParentUID: a.NamespaceUID, Fingerprint: fp, TargetRevision: a.Revision, ProviderOperationID: i.Intent.OperationID}}
	for _, write := range w.snapshot.Envelope.Writes {
		if write.ID != pending.ID {
			continue
		}
		if write.Preconditions != pending.Preconditions || write.OperationID != pending.OperationID || write.State != journal.Committed {
			return empty, failure(Changed)
		}
		r, intent, e := w.serviceAccountRecord(s.binding, write)
		if e != nil {
			return empty, e
		}
		original, e := w.serviceAccountDesired(ctx, s.binding, a, write)
		if e != nil {
			return empty, e
		}
		receipt := w.serviceAccountReceipt(a.OperationID, r, write.ResultVersion, kube.ServiceAccountSnapshot{})
		observed, e := s.binding.ObserveServiceAccount(ctx, kube.ServiceAccountRead{Desired: original, Intent: intent, Record: r})
		if e != nil {
			return receipt, w.observationFailure(ctx, write, e)
		}
		return w.serviceAccountReceipt(a.OperationID, r, write.ResultVersion, observed), nil
	}
	if !create {
		return empty, failure(Changed)
	}
	target := w.childInputTarget(s.binding, a, o.Name, fp)
	if w.config.ServiceAccountInputs == nil {
		return empty, w.inputFailure(ctx, Invalid, "ServiceAccount", target, journal.InputReference{}, runtimefault.InputRetain, runtimeinputs.ErrNotConfigured)
	}
	if _, err = w.dispatchWriter(); err != nil {
		return empty, err
	}
	pending.Input, err = w.config.ServiceAccountInputs.RetainServiceAccount(ctx, runtimeinputs.ServiceAccountCapture{Target: target, Desired: o})
	if err != nil || !pending.Input.MatchesKind("ServiceAccount") {
		if err == nil {
			err = runtimeinputs.ErrInvalid
		}
		return empty, w.inputFailure(ctx, Unavailable, "ServiceAccount", target, journal.InputReference{}, runtimefault.InputRetain, err)
	}
	if err = w.current(ctx); err != nil {
		return empty, err
	}
	writer, err := w.dispatchWriter()
	if err != nil {
		return empty, err
	}
	guard, err := serviceaccountjournal.New(ctx, serviceaccountjournal.Config{Gate: w.gate, Owner: w.config.Project.Owner(), Parent: s.binding.Parent(), WriterID: writer.ID, Input: pending.Input})
	if err != nil {
		return empty, failure(Blocked)
	}
	next := clone(w.snapshot.Envelope)
	next.Writes = append(next.Writes, pending)
	writeIndex := len(next.Writes) - 1
	if err = w.commit(ctx, next); err != nil {
		return empty, err
	}
	writer, err = w.dispatchWriter()
	if err != nil {
		return empty, w.retainUndispatched(ctx, writeIndex, err)
	}
	creation, err := s.binding.CreateServiceAccount(ctx, kube.ServiceAccountCreate{Desired: o, Intent: i, Writer: writer, WriteID: pending.ID, Verifier: guard})
	if err != nil {
		return empty, w.retainFailure(ctx, writeIndex, err)
	}
	if err = w.finishWrite(ctx, writeIndex, journal.Committed, creation.Record.Record.Ref.ProviderID, creation.CreationResourceVersion, false); err != nil {
		return empty, w.receiptAcknowledgementFailure(writeIndex)
	}
	receipt := w.serviceAccountReceipt(a.OperationID, creation.Record, creation.CreationResourceVersion, kube.ServiceAccountSnapshot{})
	observed, err := s.binding.ObserveServiceAccount(ctx, kube.ServiceAccountRead{Desired: o, Intent: i, Record: creation.Record})
	if err != nil {
		return receipt, w.committedObservationFailure(ctx, writeIndex, err)
	}
	return w.serviceAccountReceipt(a.OperationID, creation.Record, creation.CreationResourceVersion, observed), nil
}

func (w *Worker) serviceAccountReceipt(attempt string, record ownership.NamespacedRecord, creationVersion string, observed kube.ServiceAccountSnapshot) ServiceAccountReceipt {
	v := observed.View()
	return ServiceAccountReceipt{Receipt: Receipt{Subject: w.config.Cell.Identity().Subject, AttemptID: attempt, Record: record, ResourceVersion: v.ResourceVersion, JournalVersion: w.snapshot.Version}, CreationResourceVersion: creationVersion, ObservedMinimalPolicy: v.ObservedMinimalPolicy}
}
