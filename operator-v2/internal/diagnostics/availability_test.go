package diagnostics

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAvailabilitySanitizationKeepsExplicitUnconfirmedWithoutEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   Availability
		want RouteState
	}{
		{"unconfirmed without evidence", Availability{RouteState: RouteUnconfirmed}, RouteUnconfirmed},
		{"unconfirmed candidate only", Availability{RouteState: RouteUnconfirmed, CandidateRevision: "candidate"}, RouteUnconfirmed},
		{"unconfirmed historical evidence", Availability{RouteState: RouteUnconfirmed, ActiveRevision: "previous", ConfirmedAt: observed}, RouteUnconfirmed},
		{"unknown stays unknown", Availability{RouteState: RouteUnknown}, RouteUnknown},
		{"unknown despite historical evidence", Availability{RouteState: RouteUnknown, ActiveRevision: "previous", ConfirmedAt: observed}, RouteUnknown},
		{"unsupported state", Availability{RouteState: "Healthy", ActiveRevision: "previous", ConfirmedAt: observed}, RouteUnknown},
		{"preserved missing time", Availability{RouteState: RouteActivePreserved, ActiveRevision: "previous"}, RouteUnknown},
		{"promoted missing time", Availability{RouteState: RoutePromoted, ActiveRevision: "candidate"}, RouteUnknown},
		{"preserved missing revision", Availability{RouteState: RouteActivePreserved, ConfirmedAt: observed}, RouteUnknown},
		{"promoted missing revision", Availability{RouteState: RoutePromoted, ConfirmedAt: observed}, RouteUnknown},
		{"preserved with evidence", Availability{RouteState: RouteActivePreserved, ActiveRevision: "previous", ConfirmedAt: observed}, RouteActivePreserved},
		{"promoted with evidence", Availability{RouteState: RoutePromoted, ActiveRevision: "candidate", ConfirmedAt: observed}, RoutePromoted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := failedEvent()
			e.Availability = &tc.in
			p := apply(t, projection(t), e)
			want := tc.in
			want.RouteState = tc.want
			if p.Availability != want {
				t.Fatalf("producer changed availability facts: got %+v, want %+v", p.Availability, want)
			}
			data, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			var decoded OperationProjection
			if err := json.Unmarshal(data, &decoded); err != nil || decoded.Availability != want {
				t.Fatalf("wire changed availability facts: got %+v, want %+v, error %v", decoded.Availability, want, err)
			}
			if tc.want == RouteUnknown || tc.want == RouteUnconfirmed {
				explanation := Explain(p)
				if !strings.Contains(explanation, "Serving route availability is unconfirmed.") || strings.Contains(explanation, "is routed") || strings.Contains(explanation, "remains routed") {
					t.Fatal("absence of route confirmation became a positive serving claim")
				}
			}
			if tc.in.ConfirmedAt.IsZero() && !decoded.Availability.ConfirmedAt.Equal(time.Time{}) {
				t.Fatal("serialization invented a route confirmation timestamp")
			}
		})
	}
}
