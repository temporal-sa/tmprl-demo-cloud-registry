package diagnosticcli

import (
	"bytes"
	"fmt"
	"io"
	"time"

	c "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
)

const PrivilegeNotice = "PRIVILEGED OPERATOR-LOCAL tool: the supplied HS256 verification key also permits issuing sessions. Never supply or distribute a production signing key."

// WriteView emits only a completed authorized view. Errors never append a raw
// cause or partially written protected report. JSON is the existing model wire.
func WriteView(out io.Writer, view c.View, asJSON bool) error {
	var data []byte
	var err error
	if asJSON {
		data, err = view.JSON()
	} else {
		var b bytes.Buffer
		fmt.Fprintln(&b, PrivilegeNotice)
		fmt.Fprintf(&b, "%s: %s\nFreshness: %s\n", view.Target.ProjectUID, view.Status, view.Freshness.State)
		if p := view.Operation; p != nil {
			if f := p.Failure; f != nil {
				fmt.Fprintf(&b, "Original cause [%s]: %s\nStep: %s\nNext action: %s\n", f.Code, f.Message, f.Step, f.NextAction)
			}
			for _, f := range p.Warnings {
				fmt.Fprintf(&b, "Additional warning [%s]: %s\n", f.Code, f.Message)
			}
			if h := view.CreateHistory; h != nil {
				fmt.Fprintln(&b, "Historical create records (not current existence, readiness or serving):")
				for _, g := range h.Groups {
					fmt.Fprintf(&b, "%s: %s (%d)\n", g.Kind, g.Outcome, g.Count)
				}
			}
			fmt.Fprintf(&b, "Route confirmation: %s; active revision: %s; candidate revision: %s\nAutomatic retry: %s\n", p.Availability.RouteState, p.Availability.ActiveRevision, p.Availability.CandidateRevision, p.Retry.Status)
			fmt.Fprintln(&b, "Route independently observed:", timeText(p.Availability.ConfirmedAt))
			if p.Retry.Authoritative {
				fmt.Fprintln(&b, "Retry independently observed:", timeText(p.Retry.ObservedAt))
			}
			for _, r := range p.Recovery {
				fmt.Fprintf(&b, "Recovery %s: %s\n", r.Step, r.State)
				if r.Failure != nil {
					fmt.Fprintf(&b, "Recovery cause [%s]: %s\n", r.Failure.Code, r.Failure.Message)
				}
			}
			for _, e := range p.Evidence {
				fmt.Fprintf(&b, "Evidence capture %s: %s (partial=%t)\n", e.Kind, e.State, e.Partial)
			}
			for _, s := range p.Publication {
				fmt.Fprintf(&b, "Status publication %s: %s (independent of original cause)\n", s.Sink, s.State)
				if s.Failure != nil {
					fmt.Fprintf(&b, "Publication cause [%s]: %s\n", s.Failure.Code, s.Failure.Message)
				}
			}
			fmt.Fprintln(&b, "Last operation progress:", timeText(p.ProgressedAt))
		}
		if !view.Source.PersistenceTimeKnown {
			fmt.Fprintln(&b, "Persistence time was not recorded; a recent read does not prove recent progress.")
		}
		if view.Source.PersistenceTimeKnown && view.Source.RecordedAt != nil {
			fmt.Fprintln(&b, "Status persisted at:", timeText(*view.Source.RecordedAt))
		}
		if view.Source.DesiredObservedAt != nil {
			fmt.Fprintln(&b, "Desired intent independently observed:", timeText(*view.Source.DesiredObservedAt))
		}
		fmt.Fprintln(&b, "Source read at:", timeText(view.Source.ReadAt))
		if view.Partial {
			fmt.Fprintln(&b, "Partial diagnostic record.")
		}
		data = b.Bytes()
	}
	if err != nil || len(data) > c.MaxResponseBytes {
		return fail("LocalDiagnosticOutputInvalid")
	}
	if n, err := out.Write(data); err != nil || n != len(data) {
		return fail("LocalDiagnosticOutputUnavailable")
	}
	return nil
}

func timeText(t time.Time) string {
	if t.IsZero() {
		return "Unknown"
	}
	return t.UTC().Format(time.RFC3339)
}
