package runtimefault

import "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"

type AuthorityReason string

const (
	AuthorityMismatch     AuthorityReason = "Mismatch"
	OwnerMismatch         AuthorityReason = "OwnerMismatch"
	EpochMismatch         AuthorityReason = "EpochMismatch"
	LeaseExpired          AuthorityReason = "LeaseExpired"
	ActorClosed           AuthorityReason = "Closed"
	StateChanged          AuthorityReason = "Changed"
	UnresolvedWrites      AuthorityReason = "UnresolvedWrites"
	AuthorityUnclassified AuthorityReason = "Unclassified"
)

type authorityDescription struct{ code, message, action string }

var authorityDescriptions = map[AuthorityReason]authorityDescription{
	AuthorityMismatch:     {"RuntimeAuthorityMismatch", "The local owner, epoch or lease authority does not match the accepted actor state.", "Verify the exact accepted owner, current writer epoch and lease before dispatch."},
	OwnerMismatch:         {"RuntimeOwnerMismatch", "The local actor owner does not match the accepted project owner.", "Verify the exact accepted environment, installation, project UID and runtime owner before dispatch."},
	EpochMismatch:         {"RuntimeEpochMismatch", "The local writer epoch does not match the acknowledged actor epoch.", "Re-establish the exact admitted actor epoch and pinned journal state before dispatch."},
	LeaseExpired:          {"RuntimeLeaseExpired", "The local dispatch lease has expired.", "Obtain valid authority for the exact admitted actor before dispatch; preserve the current accepted attempt."},
	ActorClosed:           {"RuntimeActorClosed", "The local actor is closed and cannot dispatch.", "Complete the authorized handoff and use the admitted active actor."},
	StateChanged:          {"RuntimeStateChanged", "The actor journal or accepted state changed before dispatch.", "Re-read the exact pinned state and complete the authorized handoff; retain the accepted attempt."},
	UnresolvedWrites:      {"RuntimeUnresolvedWrites", "Pending or unknown operation writes block local dispatch.", "Resolve the exact retained pending or unknown outcomes before allowing another provider action."},
	AuthorityUnclassified: {"RuntimeAuthorityUnclassified", "Local dispatch authority could not be verified.", "Verify the accepted owner, current epoch, lease and actor state before dispatch."},
}

// MapAuthority describes an independently observed LOCAL authorization denial.
// The closed reason is not a raw RuntimeCode/error, and no provider invocation
// is implied. The caller must already know this exact action was undispatched.
// This diagnostic grants no replacement authority and schedules no retry.
func MapAuthority(c Context, reason AuthorityReason) (Fault, error) {
	if !validContext(c) || c.Step != DispatchAuthorize || c.Dispatch != NotDispatched {
		return Fault{}, ErrContext
	}
	description, ok := authorityDescriptions[reason]
	if !ok {
		return Fault{}, ErrContext
	}
	return safeFailure(c, description.code, diagnostics.Configuration, description.message, description.action), nil
}

// MapReceiptAcknowledgement is ONLY for a verified native Deployment/Service/
// ServiceAccount
// create with no original provider fault, followed by an unconfirmed receipt
// journal acknowledgement. Applied is supplied independently of the commit
// error; it cannot be inferred from that error. A poisoned Gate cannot persist
// this report about itself: return it as safe activity/history data for separate
// authorized handling. Existing original faults use WithDelivery instead.
func MapReceiptAcknowledgement(c Context) (Fault, error) {
	if !validContext(c) || c.Step != ReceiptAcknowledge || c.Dispatch != Applied || !stableID.MatchString(c.WriteID) {
		return Fault{}, ErrContext
	}
	fault := safeFailure(c, "CanonicalCreateReceiptUnconfirmed", diagnostics.Publication, "The provider create was applied, but persistence of its canonical receipt is unconfirmed.", "Retain the exact journal proposal and confirm its acknowledgement before another provider action.")
	return fault.WithDelivery(Journal, DeliveryUnknown, c.OccurredAt)
}
