package engine

import (
	"errors"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"time"
)

func failureFor(code, step string, at time.Time) diagnostics.DomainFailure {
	f := diagnostics.DomainFailure{ID: "failure-" + code, Code: code, Category: diagnostics.Unknown, Message: "The operation failed; the cause has not been classified.", Step: step, OccurredAt: at, NextAction: "Inspect the operation's authorized evidence before requesting another reconcile."}
	switch code {
	case "RolloutSuperseded":
		f.Category = diagnostics.Configuration
		f.Message = "A newer accepted reconcile intent superseded this candidate."
		f.NextAction = "The next intent may start only after the previous attempt is durably drained or retained."
	case "ImagePullUnauthorized":
		f.Category = diagnostics.Readiness
		f.Message = "Kubernetes reports an unauthorized candidate image pull."
		f.NextAction = "Verify the candidate image and pull-role access, then request a new reconcile."
	case "CandidateNotReady":
		f.Category = diagnostics.Readiness
		f.Message = "The candidate did not become ready."
		f.NextAction = "Inspect candidate pod events and readiness evidence before requesting a new reconcile."
	case "SmokeCheckFailed":
		f.Category = diagnostics.Smoke
		f.Message = "The candidate smoke check failed."
		f.NextAction = "Inspect the smoke-check response and candidate logs, then correct the candidate."
	case "EvidenceCaptureFailed":
		f.Category = diagnostics.Cleanup
		f.Message = "Candidate evidence capture failed."
		f.NextAction = "Capture authorized evidence before deleting retained candidate resources."
	case "CandidateCleanupFailed":
		f.Category = diagnostics.Cleanup
		f.Message = "The failed candidate could not be deleted."
		f.NextAction = "Inspect cleanup permissions and retained resources; keep the active route intact."
	case "ObservationOrAuthorizationFailed":
		f.Category = diagnostics.Configuration
		f.NextAction = "Inspect the operation's ownership, provider identity and writer fencing before retrying."
	}
	return diagnostics.SafeFailure(f)
}

func executionFailureFor(cause error, step string, at time.Time) diagnostics.DomainFailure {
	var safe interface {
		Diagnostic() diagnostics.DomainFailure
	}
	if errors.As(cause, &safe) {
		f := diagnostics.SafeFailure(safe.Diagnostic())
		if f.Step == "" {
			f.Step = step
		}
		if f.OccurredAt.IsZero() {
			f.OccurredAt = at
		}
		return f
	}
	f := failureFor("CapabilityExecutionFailed", step, at)
	f.Message = "The capability failed without a safe provider diagnosis."
	var auth *ownership.Error
	if errors.As(cause, &auth) {
		f.Code = "OwnershipRejected"
		f.Category = diagnostics.Configuration
		f.Message = "Ownership authorization rejected the operation (" + string(auth.Code) + ")."
		f.NextAction = "Re-read the owned resource, writer lease and provider version before retrying."
	}
	return f
}

func recoveryFailureFor(code, step string, cause error, at time.Time) diagnostics.DomainFailure {
	f := failureFor(code, step, at)
	safe := executionFailureFor(cause, step, at)
	f.Message = safe.Message
	f.Causes = []diagnostics.Cause{{Type: safe.Code, Message: safe.Message}}
	return diagnostics.SafeFailure(f)
}

// SafeBoundaryError discards the raw unwrap chain before an adapter passes an
// error to Temporal history, logs or another durable sink. Step preserves its
// original error only for local control flow such as errors.Is/As.
func SafeBoundaryError(cause error, step string, at time.Time) error {
	if cause == nil {
		return nil
	}
	return diagnostics.NewError(executionFailureFor(cause, step, at))
}
