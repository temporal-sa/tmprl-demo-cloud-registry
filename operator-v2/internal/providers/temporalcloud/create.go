package temporalcloud

import (
	"context"
	"maps"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/protobuf/proto"
)

func (a *Adapter) creationRequest(intent ownership.Intent, desired DesiredNamespace) (*cloudservice.CreateNamespaceRequest, Receipt, error) {
	if !a.capabilities.CreateNamespaces {
		return nil, Receipt{}, fail(CapabilityDisabled)
	}
	id := intent.Name + "." + a.scope.AccountID
	configured, ok := a.creates[id]
	if !ok || configured != intent {
		return nil, Receipt{}, fail(OwnershipConflict)
	}
	inputFingerprint, err := NamespaceFingerprint(desired)
	if err != nil {
		return nil, Receipt{}, err
	}
	if intent.SpecFingerprint != inputFingerprint {
		return nil, Receipt{}, fail(InvalidRequest)
	}
	if !a.regions[desired.RegionID] || desired.RetentionDays > a.maxRetention {
		return nil, Receipt{}, fail(InvalidRequest)
	}
	if len(desired.Tags) > 0 && !a.capabilities.GlobalTagWrites {
		return nil, Receipt{}, fail(CapabilityDisabled)
	}
	receipt := a.receipt(id, "create", intent.OperationID, inputFingerprint)
	request := &cloudservice.CreateNamespaceRequest{Spec: createSpec(intent.Name, desired), AsyncOperationId: receipt.ID, ProjectId: a.projectID, Tags: maps.Clone(desired.Tags)}
	return request, receipt, nil
}

// StartCreate uses only pre-persisted exact creation intent. Creation is disabled
// by zero-value/test capabilities; there is no create-or-adopt-by-name path.
func (a *Adapter) StartCreate(ctx context.Context, intent ownership.Intent, desired DesiredNamespace, writer ownership.Writer) (Operation, error) {
	request, receipt, err := a.creationRequest(intent, desired)
	if err != nil {
		return Operation{}, err
	}
	if _, err := a.owner.AuthorizeCreate(intent, writer, a.now()); err != nil {
		return Operation{}, fail(OwnershipConflict)
	}
	var result Operation
	err = a.guarded(ctx, receipt.NamespaceID, writer, func(guardContext context.Context) error {
		if _, err := a.owner.AuthorizeCreate(intent, writer, a.now()); err != nil {
			return fail(OwnershipConflict)
		}
		call, cancel := context.WithTimeout(guardContext, a.timeout)
		defer cancel()
		response, err := a.transport.CreateNamespace(call, request)
		if err != nil {
			return providerError(err, receipt, true)
		}
		if response == nil || response.Namespace != receipt.NamespaceID {
			return invalidWriteResponse(receipt)
		}
		result, err = operationResult(receipt, response.AsyncOperation)
		return err
	})
	return result, err
}

// RecoverCreate first verifies the exact provider operation and echoed input.
// A namespace discovered only by name is NEVER enough to obtain a binding.
// Unknown/omitted operation input fails closed pending explicit reconciliation.
// The returned binding must be persisted conditionally in the trusted ledger
// before constructing an adapter with mutation authority for this namespace.
func (a *Adapter) RecoverCreate(ctx context.Context, intent ownership.Intent, desired DesiredNamespace, writer ownership.Writer) (CreatedNamespace, error) {
	expected, receipt, err := a.creationRequest(intent, desired)
	if err != nil {
		return CreatedNamespace{}, err
	}
	if _, err := a.owner.AuthorizeCreate(intent, writer, a.now()); err != nil {
		return CreatedNamespace{}, fail(OwnershipConflict)
	}
	current, err := a.pollRaw(ctx, receipt)
	if err != nil {
		if typed, ok := err.(*Error); ok && typed.Code == NamespaceMissing {
			return CreatedNamespace{}, &Error{Code: UnprovenCreateOutcome, Receipt: receipt, Retryable: true}
		}
		return CreatedNamespace{}, err
	}
	result, err := operationResult(receipt, current)
	if err != nil {
		return CreatedNamespace{Operation: result}, err
	}
	if !result.Done {
		return CreatedNamespace{Operation: result}, nil
	}
	actual := &cloudservice.CreateNamespaceRequest{}
	if current.OperationInput == nil || current.OperationInput.UnmarshalTo(actual) != nil || !proto.Equal(actual, expected) || current.StartedTime == nil || current.FinishedTime == nil || current.StartedTime.CheckValid() != nil || current.FinishedTime.CheckValid() != nil {
		return CreatedNamespace{}, &Error{Code: UnprovenCreateOutcome, Receipt: receipt}
	}
	ns, err := a.read(ctx, receipt.NamespaceID)
	if err != nil {
		return CreatedNamespace{}, err
	}
	if ns == nil {
		return CreatedNamespace{}, &Error{Code: ProviderUnavailable, Retryable: true, Receipt: receipt}
	}
	born := ns.CreatedTime.AsTime()
	if ns.Spec.Name != intent.Name || ns.State == resource.ResourceState_RESOURCE_STATE_DELETED || born.Before(current.StartedTime.AsTime()) || born.After(current.FinishedTime.AsTime()) {
		return CreatedNamespace{}, &Error{Code: UnprovenCreateOutcome, Receipt: receipt}
	}
	ref := ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: IncarnationID(ns.Namespace, born)}
	observation := ownership.Observation{Ref: ref, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch, Version: ns.ResourceVersion}
	record, err := a.owner.BindCreated(intent, observation, writer, a.now())
	if err != nil {
		return CreatedNamespace{}, fail(OwnershipConflict)
	}
	binding := Binding{NamespaceID: ns.Namespace, CreatedAt: born, ProvenanceID: receipt.ID, Record: record}
	return CreatedNamespace{Snapshot: snapshot(ns), Binding: binding, Operation: result}, nil
}
