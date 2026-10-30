package runtimefault

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

func diagnosticID(value string) string {
	s := sha256.Sum256([]byte(value))
	return "runtime-fault-" + hex.EncodeToString(s[:16])
}

// MapInput recognizes only finite sentinel contracts; raw paths/provider bodies
// or error strings never enter the returned handle. These steps occur before
// the NEW target provider action, so that action is authoritatively undispatched.
// This says nothing about earlier actions in the same namespace/attempt.
func MapInput(c Context, err error) (Fault, error) {
	if !validContext(c) || (c.Step != InputRetain && c.Step != InputResolve) || err == nil || c.Dispatch != "" && c.Dispatch != DispatchUnknown && c.Dispatch != NotDispatched {
		return Fault{}, ErrContext
	}
	c.Dispatch = NotDispatched
	code, category, message, action := "AcceptedInputUnavailable", diagnostics.Unknown, "The accepted runtime input could not be obtained.", "Retain the namespace and verify the accepted private input source before another provider action."
	persistence := NotAttempted
	if c.Step == InputRetain {
		persistence = DeliveryUnknown
	}
	switch {
	case errors.Is(err, runtimeinputs.ErrUncertain):
		code = "AcceptedInputRetentionUncertain"
		message = "Saving the accepted runtime input has an unknown outcome."
		action = "Retain the partial input artifact and resolve its exact persistence receipt before retrying."
		persistence = DeliveryUnknown
		if c.Step == InputResolve {
			code = "AcceptedInputVerificationUncertain"
			message = "Verification of the accepted runtime input has an unknown persistence outcome."
			action = "Retain the exact accepted input reference and verify its original durability receipt before another provider action."
		}
	case errors.Is(err, runtimeinputs.ErrNotConfigured):
		code = "AcceptedInputStoreNotConfigured"
		category = diagnostics.Configuration
		message = "The required private accepted-input store is not configured."
		action = "Configure the pinned accepted-input store for this actor and exact recorded input reference before provider dispatch."
		if c.Step == InputRetain {
			action = "Configure the pinned accepted-input store for this actor and accepted target before retaining input or dispatching."
		}
		persistence = NotAttempted
	case errors.Is(err, runtimeinputs.ErrCapacity):
		if c.Step != InputRetain {
			return Fault{}, ErrContext
		}
		code = "AcceptedInputCapacityReached"
		category = diagnostics.Configuration
		message = "The private accepted-input store has reached its configured capacity."
		action = "Review owned retained artifacts and configured capacity; preserve accepted inputs still referenced by the journal."
		persistence = DeliveryRejected
	case errors.Is(err, runtimeinputs.ErrUnproven):
		code = "AcceptedInputUnproven"
		category = diagnostics.Configuration
		message = "The saved accepted runtime input is missing, changed or cannot be verified."
		action = "Restore the original accepted input matching the recorded owner, namespace UID, contract and digest; retain the namespace."
	case errors.Is(err, runtimeinputs.ErrInvalid):
		if c.Step == InputRetain {
			persistence = DeliveryRejected
		}
		code = "AcceptedInputInvalid"
		category = diagnostics.Configuration
		message = "The accepted runtime input does not satisfy the pinned input contract."
		action = "Verify the accepted input kind, target and contract before attempting another provider action."
	default:
		if c.Step == InputRetain {
			persistence = DeliveryUnknown
		}
	}
	f := safeFailure(c, code, category, message, action)
	f.private.InputPersistence = DeliveryReport{State: persistence}
	if persistence != NotAttempted {
		f.private.InputPersistence.ObservedAt = c.OccurredAt.UTC()
	}
	return f, nil
}

type providerDescription struct {
	category        diagnostics.Category
	message, action string
}

var providerDescriptions = map[kube.Code]providerDescription{
	kube.InvalidConfiguration:           {diagnostics.Configuration, "The explicit Kubernetes capability configuration is invalid.", "Verify the pinned local connection, accepted target and trusted bindings."},
	kube.UnsupportedKind:                {diagnostics.Configuration, "The requested Kubernetes kind is outside the reviewed typed capability.", "Use a reviewed typed provider capability for this exact accepted target."},
	kube.UnsupportedMode:                {diagnostics.Configuration, "The requested Kubernetes execution mode is unsupported.", "Use the reviewed serialized local capability; establish independent enforcement before widening execution."},
	kube.ClusterMismatch:                {diagnostics.Configuration, "The independently observed Kubernetes cluster incarnation does not match.", "Stop writes and verify the pinned cluster identity and explicit endpoint."},
	kube.OwnershipRejected:              {diagnostics.Configuration, "The target ownership or creation provenance could not be verified.", "Re-observe the canonical namespace/resource UID and exact accepted owner; do not adopt by name."},
	kube.ParentUnavailable:              {diagnostics.Unknown, "The recorded runtime namespace could not be independently observed.", "Verify the canonical namespace UID and retained operation before another provider action."},
	kube.ChildUnavailable:               {diagnostics.Unknown, "The canonical runtime resource could not be independently observed.", "Inspect the exact recorded UID and desired input; absence does not authorize name reuse."},
	kube.ParentTerminating:              {diagnostics.Cleanup, "The runtime namespace is already terminating.", "Retain the operation and follow its authorized drain/cleanup protocol."},
	kube.ParentUnprotected:              {diagnostics.Configuration, "The namespace lifecycle protection could not be verified.", "Restore the reviewed lifecycle authority; a pre-read alone cannot authorize child creation."},
	kube.ConditionalConflict:            {diagnostics.Transient, "The observed resource preconditions no longer match.", "Re-observe the canonical UID/version and accepted intent before any conditional retry."},
	kube.UnknownWriteOutcome:            {diagnostics.Unknown, "The Kubernetes mutation has an unknown provider outcome.", "Retain the namespace and resolve the exact in-flight operation from independent evidence; do not repeat the mutation blindly."},
	kube.LifecycleDraining:              {diagnostics.Cleanup, "The controlled namespace lifecycle is draining.", "Wait for the authorized drain outcome; normal child writes remain blocked."},
	kube.AttemptConsumed:                {diagnostics.Configuration, "This namespace attempt has already been consumed.", "Use a new accepted attempt and namespace name; never recreate the observed incarnation."},
	kube.NamespaceRetained:              {diagnostics.Cleanup, "Namespace cleanup is incomplete and resources remain retained.", "Inspect independently verified remaining contents/finalizers and continue only the authorized cleanup protocol."},
	kube.DeploymentUnsupportedSpec:      {diagnostics.Configuration, "The accepted Deployment exceeds the reviewed spec/defaulting contract.", "Use the pinned Deployment baseline or review and prove the additional capability."},
	kube.DeploymentSpecMismatch:         {diagnostics.Configuration, "The canonical Deployment body differs from its accepted desired input.", "Retain the namespace and compare the full accepted body; copied fingerprint annotations are insufficient."},
	kube.DeploymentIntentUnverified:     {diagnostics.Configuration, "The exact pending Deployment create intent was not acknowledged and verified.", "Verify active admission and the exact durable Pending write before dispatch."},
	kube.ServiceUnsupportedSpec:         {diagnostics.Configuration, "The accepted Service exceeds the reviewed single-stack ClusterIP contract.", "Use the pinned ClusterIP baseline or review and prove the additional capability."},
	kube.ServiceSpecMismatch:            {diagnostics.Configuration, "The canonical Service spec or allocation receipt differs from accepted intent.", "Retain the namespace and verify the original body plus immutable creation-spec receipt; never learn it from GET."},
	kube.ServiceIntentUnverified:        {diagnostics.Configuration, "The exact pending Service create intent was not acknowledged and verified.", "Verify active admission and the exact durable Pending Service write before dispatch."},
	kube.ServiceAccountUnsupportedSpec:  {diagnostics.Configuration, "The accepted ServiceAccount exceeds the reviewed automount-disabled policy.", "Use the pinned minimal ServiceAccount baseline without secret references, identity annotations or finalizers."},
	kube.ServiceAccountSpecMismatch:     {diagnostics.Configuration, "The canonical ServiceAccount policy differs from its accepted desired input.", "Retain the namespace and verify the original body, canonical UID and empty secret/pull references; copied fingerprints are insufficient."},
	kube.ServiceAccountIntentUnverified: {diagnostics.Configuration, "The exact pending ServiceAccount create intent was not acknowledged and verified.", "Verify active admission and the exact durable Pending ServiceAccount write before dispatch."},
}

func providerCodeAllowed(c Context, code kube.Code) bool {
	if c.Step != Create && c.Step != Observe && c.Step != Delete && c.Step != Finalize {
		return false
	}
	switch code {
	case kube.DeploymentUnsupportedSpec, kube.DeploymentSpecMismatch:
		return c.Kind == Deployment && (c.Step == Create || c.Step == Observe)
	case kube.DeploymentIntentUnverified:
		return c.Kind == Deployment && c.Step == Create
	case kube.ServiceUnsupportedSpec, kube.ServiceSpecMismatch:
		return c.Kind == Service && (c.Step == Create || c.Step == Observe)
	case kube.ServiceIntentUnverified:
		return c.Kind == Service && c.Step == Create
	case kube.ServiceAccountUnsupportedSpec, kube.ServiceAccountSpecMismatch:
		return c.Kind == ServiceAccount && (c.Step == Create || c.Step == Observe)
	case kube.ServiceAccountIntentUnverified:
		return c.Kind == ServiceAccount && c.Step == Create
	case kube.UnknownWriteOutcome:
		return c.Step == Create || cleanupStep(c.Step)
	case kube.ChildUnavailable:
		return c.Step == Observe && c.Kind != Namespace
	case kube.NamespaceRetained:
		return c.Kind == Namespace && cleanupStep(c.Step)
	case kube.ParentUnprotected:
		return c.Step == Create && c.Kind != Namespace
	}
	return true
}
func MapProvider(c Context, err error) (Fault, error) {
	if !validContext(c) || c.Step != Create && c.Step != Observe && !cleanupStep(c.Step) || err == nil {
		return Fault{}, ErrContext
	}
	code := kube.Code("RuntimeProviderFailureUnclassified")
	var typed *kube.Error
	if errors.As(err, &typed) {
		if typed == nil {
			return providerFault(c, code, "")
		}
		if _, known := providerDescriptions[typed.Code]; known {
			code = typed.Code
		}
	}
	return providerFault(c, code, conditionForError(err))
}

// providerFault reconstructs only a finite capability/condition combination. It
// is also used by the strict parser; a parsed condition remains historical
// diagnostic data, never independent response/dispatch/admission proof.
func providerFault(c Context, code kube.Code, condition Condition) (Fault, error) {
	if !validContext(c) || c.Step != Create && c.Step != Observe && !cleanupStep(c.Step) {
		return Fault{}, ErrContext
	}
	if desc, known := providerDescriptions[code]; known {
		if !providerCodeAllowed(c, code) {
			return Fault{}, ErrContext
		}
		if code == kube.UnknownWriteOutcome {
			if c.Dispatch != "" && c.Dispatch != DispatchUnknown {
				return Fault{}, ErrContext
			}
			c.Dispatch = DispatchUnknown
		}
		return addCondition(safeFailure(c, string(code), desc.category, desc.message, desc.action), condition)
	}
	if string(code) != "RuntimeProviderFailureUnclassified" {
		return Fault{}, ErrContext
	}
	// Unknown raw codes/text are never copied. Independently supplied dispatch
	// facts survive classification uncertainty; no condition infers noncommit.
	if c.Dispatch == "" {
		c.Dispatch = DispatchUnknown
	}
	return addCondition(safeFailure(c, string(code), diagnostics.Unknown, "The runtime provider returned an unclassified failure.", "Retain the operation and inspect separately authorized provider evidence before another mutation."), condition)
}
