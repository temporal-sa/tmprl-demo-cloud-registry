// Package kubernetes implements explicit, local-fixture Kubernetes capabilities.
// Production and multi-process lifecycle modes remain disabled until a durable
// coordinator can enforce the required namespace allocation and writer model.
package kubernetes

import (
	"encoding/json"
	"fmt"
	"io"
)

type Code string

const (
	InvalidConfiguration Code = "invalid_kubernetes_configuration"
	UnsupportedMode      Code = "unsupported_kubernetes_mode"
	ClusterMismatch      Code = "kubernetes_cluster_identity_mismatch"
	UnsupportedKind      Code = "unsupported_kubernetes_kind"
	ParentUnavailable    Code = "kubernetes_parent_unavailable"
	ChildUnavailable     Code = "kubernetes_child_unavailable"
	ParentTerminating    Code = "kubernetes_parent_terminating"
	ParentUnprotected    Code = "kubernetes_parent_unprotected"
	NamespaceRetained    Code = "kubernetes_namespace_retained"
	OwnershipRejected    Code = "kubernetes_ownership_rejected"
	ConditionalConflict  Code = "kubernetes_conditional_conflict"
	UnknownWriteOutcome  Code = "kubernetes_unknown_write_outcome"
	LifecycleDraining    Code = "kubernetes_lifecycle_draining"
	AttemptConsumed      Code = "kubernetes_namespace_attempt_consumed"
)

// Error deliberately excludes provider response bodies and credentials from its
// printable/JSON form. Callers may inspect a cause for classification, not emit
// it as an unredacted user-facing diagnostic.
type Error struct {
	Code     Code
	Recovery string
	cause    *privateCause
}
type privateCause struct{ err error }

func (e *Error) Error() string             { return string(e.Code) + ": " + e.Recovery }
func (e *Error) GoString() string          { return e.Error() }
func (e Error) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, e.Error()) }
func (e *Error) Unwrap() error {
	if e.cause == nil {
		return nil
	}
	return e.cause.err
}
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Code     Code   `json:"code"`
		Recovery string `json:"recovery"`
	}{e.Code, e.Recovery})
}
func failure(code Code, cause error) error {
	recovery := map[Code]string{
		InvalidConfiguration: "Supply explicit local TLS credentials and pinned cluster identity; ambient credentials are never used.",
		UnsupportedMode:      "This adapter supports one controlled local fixture process only; production and multi-process execution are disabled.",
		ClusterMismatch:      "Stop writes and verify the owned cluster incarnation and endpoint.",
		UnsupportedKind:      "Use a reviewed typed capability; unknown or cluster-scoped child kinds are disabled.",
		ParentUnavailable:    "Observe the recorded parent namespace and its ownership before retrying; do not recreate an observed namespace name.",
		ChildUnavailable:     "Reconcile the canonical child UID with the trusted journal; absence is not permission to adopt or recreate a name.",
		ParentTerminating:    "Namespace teardown has begun; retain the operation and do not create new children.",
		ParentUnprotected:    "Use the separate managed namespace bootstrap; child creates require the exact owned lifecycle finalizer and accepted attempt.",
		NamespaceRetained:    "Inspect remaining namespace contents and finalizers; remove only finalizers whose ownership is independently proven.",
		OwnershipRejected:    "Inspect parent/child canonical UID, ownership and creation provenance; do not adopt an object by name.",
		ConditionalConflict:  "Re-observe the owned resource and accepted intent; do not retry a stale UID/version write blindly.",
		UnknownWriteOutcome:  "Retain the namespace and resolve the in-flight operation from independent evidence before teardown or reuse.",
		LifecycleDraining:    "Wait for the controlled namespace lifecycle to drain; new writers are refused.",
		AttemptConsumed:      "Allocate a new accepted namespace attempt and a new name; this namespace name must never be recreated.",
	}[code]
	return &Error{Code: code, Recovery: recovery, cause: &privateCause{err: cause}}
}
