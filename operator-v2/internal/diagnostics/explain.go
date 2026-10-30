package diagnostics

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Explain renders the same safe wire projection consumed by an API/UI. It never
// invents a cause or remediation when the provider has not supplied evidence.
func Explain(projection OperationProjection, sensitiveValues ...string) string {
	data, err := projection.JSON(sensitiveValues...)
	if err != nil {
		return "Diagnostic view unavailable: invalid or oversized operation envelope."
	}
	var p OperationProjection
	if err = json.Unmarshal(data, &p); err != nil {
		return "Diagnostic view unavailable: unreadable operation envelope."
	}
	var text strings.Builder
	fmt.Fprintf(&text, "%s: %s", p.Identity.ProjectName, p.State)
	if p.Step != "" {
		fmt.Fprintf(&text, " at %s", p.Step)
	}
	text.WriteString(".\n")
	if p.Failure != nil {
		label := "Cause"
		if p.State == Succeeded || p.State == Finalized {
			label = "Previous failure"
		}
		fmt.Fprintf(&text, "%s [%s]: %s\n", label, p.Failure.Code, p.Failure.Message)
		if p.Failure.Execution.ActivityID != "" {
			fmt.Fprintf(&text, "Activity: %s; event %d.\n", p.Failure.Execution.ActivityID, p.Failure.Execution.ScheduledEventID)
		}
	}
	switch p.Retry.Status {
	case RetryScheduled:
		text.WriteString("Automatic retry is scheduled")
		if p.Retry.Attempt > 0 {
			fmt.Fprintf(&text, "; last observed attempt %d", p.Retry.Attempt)
		}
		if p.Retry.NextAttemptAt != nil {
			fmt.Fprintf(&text, "; next attempt %s", p.Retry.NextAttemptAt.UTC().Format(time.RFC3339))
		}
		text.WriteString(".\n")
	case RetryExhausted:
		text.WriteString("Automatic retries are exhausted")
		if p.Retry.Attempt > 0 {
			fmt.Fprintf(&text, " after %d observed attempts", p.Retry.Attempt)
		}
		text.WriteString(".\n")
	case RetryNotRetryable:
		text.WriteString("No automatic retry: the observed failure is not retryable.\n")
	default:
		text.WriteString("Automatic retry status is unknown.\n")
	}
	switch p.Availability.RouteState {
	case RouteActivePreserved:
		fmt.Fprintf(&text, "Previous active revision %s remains routed (confirmed %s).\n", p.Availability.ActiveRevision, p.Availability.ConfirmedAt.UTC().Format(time.RFC3339))
	case RoutePromoted:
		fmt.Fprintf(&text, "Revision %s is routed (confirmed %s).\n", p.Availability.ActiveRevision, p.Availability.ConfirmedAt.UTC().Format(time.RFC3339))
	default:
		text.WriteString("Serving route availability is unconfirmed.\n")
	}
	for _, outcome := range p.Recovery {
		fmt.Fprintf(&text, "Recovery %s: %s", outcome.Step, outcome.State)
		if outcome.Failure != nil {
			fmt.Fprintf(&text, " [%s]: %s", outcome.Failure.Code, outcome.Failure.Message)
		}
		if len(outcome.RetainedResources) > 0 {
			fmt.Fprintf(&text, "; %d retained resource reference(s)", len(outcome.RetainedResources))
		}
		text.WriteString(".\n")
		if outcome.Failure != nil && outcome.Failure.NextAction != "" {
			fmt.Fprintf(&text, "Recovery action: %s\n", outcome.Failure.NextAction)
		}
	}
	for _, warning := range p.Warnings {
		fmt.Fprintf(&text, "Warning [%s]: %s\n", warning.Code, warning.Message)
	}
	for _, report := range p.Publication {
		if report.State == DeliveryUnknown {
			fmt.Fprintf(&text, "Status delivery to %s has an unknown acknowledgement independently of the operation.\n", report.Sink)
		}
		if report.State == DeliveryFailed {
			fmt.Fprintf(&text, "Status delivery to %s failed independently of the operation", report.Sink)
			if report.Failure != nil {
				fmt.Fprintf(&text, ": %s", report.Failure.Message)
			}
			text.WriteString(".\n")
		}
		if report.SourceSequence < p.Sequence {
			fmt.Fprintf(&text, "Status delivery to %s covers older observation %d (current %d).\n", report.Sink, report.SourceSequence, p.Sequence)
		}
	}
	if p.Failure != nil && p.Failure.NextAction != "" {
		fmt.Fprintf(&text, "Next: %s\n", p.Failure.NextAction)
	}
	for _, evidence := range p.Evidence {
		fmt.Fprintf(&text, "Evidence %s: %s", evidence.ID, evidence.State)
		if evidence.Locator != "" {
			fmt.Fprintf(&text, " (%s)", evidence.Locator)
		}
		text.WriteString(".\n")
	}
	if p.Partial {
		text.WriteString("This summary is partial; inspect the operation's authorized evidence for omitted detail.\n")
	}
	fmt.Fprintf(&text, "Operation %s; environment %s; observed generation %d; reconcile %s; observation %d.", p.Identity.OperationID, p.Identity.Environment, p.Identity.Generation, p.Identity.ReconcileKey, p.Sequence)
	return truncate(text.String(), MaxJSONBytes)
}
