package runtimefault

import (
	"context"
	"errors"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition is an optional finite explanation beneath the ORIGINAL capability
// code. It does not replace that cause or describe mutation commitment/retry.
// Empty means no recognized condition; its omission preserves legacy wire.
type Condition string

const (
	ConditionForbidden        Condition = "Forbidden"
	ConditionUnauthenticated  Condition = "Unauthenticated"
	ConditionNotFound         Condition = "NotFound"
	ConditionDeadlineExceeded Condition = "DeadlineExceeded"
	ConditionCanceled         Condition = "Canceled"
)

type conditionDescription struct{ sentence, action string }

var conditionDescriptions = map[Condition]conditionDescription{
	ConditionForbidden:        {"The Kubernetes API denied access.", "Verify the configured principal has the required permissions for the pinned cluster and accepted resources."},
	ConditionUnauthenticated:  {"The Kubernetes API rejected authentication.", "Verify the explicitly configured authentication and admitted principal through the approved bootstrap."},
	ConditionNotFound:         {"The Kubernetes API reported the requested resource was not found.", "Re-observe the exact recorded resource identity and retain the accepted attempt; absence does not authorize name reuse."},
	ConditionDeadlineExceeded: {"The API request exceeded its deadline.", "Verify request deadlines and the exact retained outcome before any retry; a deadline does not establish mutation noncommit."},
	ConditionCanceled:         {"The API request was canceled.", "Inspect the exact retained operation and actor lifecycle before any retry; cancellation does not establish mutation noncommit."},
}

func conditionForError(err error) Condition {
	// The pinned client returns concrete StatusError. Inspect ONLY Code/Reason:
	// Message, Details, target names, request IDs and raw Error text are ignored.
	// A typed status is the response fact; incidental context sentinels do not
	// override it. Unknown/mismatched statuses remain generic.
	var status *apierrors.StatusError
	if errors.As(err, &status) && status != nil {
		code, reason := status.ErrStatus.Code, status.ErrStatus.Reason
		switch {
		case code == 403 && reason == metav1.StatusReasonForbidden:
			return ConditionForbidden
		case code == 401 && reason == metav1.StatusReasonUnauthorized:
			return ConditionUnauthenticated
		case code == 404 && reason == metav1.StatusReasonNotFound:
			return ConditionNotFound
		default:
			return ""
		}
	}
	// Do not guess a deadline from server text, arbitrary net.Error or other
	// status reasons. These exact sentinels are the only timeout/cancel evidence.
	if errors.Is(err, context.DeadlineExceeded) {
		return ConditionDeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return ConditionCanceled
	}
	return ""
}
func addCondition(f Fault, condition Condition) (Fault, error) {
	if !f.Valid() {
		return Fault{}, ErrContext
	}
	if condition == "" {
		return f, nil
	}
	description, known := conditionDescriptions[condition]
	if !known {
		return Fault{}, ErrContext
	}
	v := f.View()
	v.Condition = condition
	v.Failure.Message += " " + description.sentence
	v.Failure.NextAction += " " + description.action
	v.Failure = diagnostics.SafeFailure(v.Failure)
	return Fault{private: &v}, nil
}
