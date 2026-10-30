package catalogdiagnostics

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

// Synthetic scoped grants/sources exercise the real read model and HTTP JSON
// serialization. They do not prove a live endpoint or durable storage adapter.
func TestModelAndHTTPWireKeepHonestRouteConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		availability diagnostics.Availability
		origin       Origin
		want         diagnostics.RouteState
	}{
		{"never confirmed", diagnostics.Availability{RouteState: diagnostics.RouteUnknown}, Materialized, diagnostics.RouteUnconfirmed},
		{"explicit unconfirmed", diagnostics.Availability{RouteState: diagnostics.RouteUnconfirmed, CandidateRevision: "candidate"}, Materialized, diagnostics.RouteUnconfirmed},
		{"invalid positive evidence", diagnostics.Availability{RouteState: diagnostics.RoutePromoted, ActiveRevision: "candidate"}, Materialized, diagnostics.RouteUnconfirmed},
		{"stale confirmation", diagnostics.Availability{RouteState: diagnostics.RouteActivePreserved, ActiveRevision: "previous", ConfirmedAt: testNow.Add(-time.Hour)}, Materialized, diagnostics.RouteUnconfirmed},
		{"future confirmation", diagnostics.Availability{RouteState: diagnostics.RoutePromoted, ActiveRevision: "candidate", ConfirmedAt: testNow.Add(time.Hour)}, Materialized, diagnostics.RouteUnconfirmed},
		{"retired source", diagnostics.Availability{RouteState: diagnostics.RouteActivePreserved, ActiveRevision: "previous", ConfirmedAt: testNow}, Archived, diagnostics.RouteUnconfirmed},
		{"unknown with timestamp", diagnostics.Availability{RouteState: diagnostics.RouteUnknown, ConfirmedAt: testNow}, Materialized, diagnostics.RouteUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := snapshot(t)
			s.Operation.Availability, s.Origin = tc.availability, tc.origin
			// The journal source has no independently persisted commit timestamp.
			s.RecordedAt = time.Time{}
			m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil }))
			inMemory, err := m.Read(context.Background(), grant(t), target)
			if err != nil || inMemory.Operation == nil || inMemory.Operation.Availability.RouteState != tc.want {
				t.Fatalf("unexpected model route: error %v", err)
			}
			assert := func(name string, data []byte) {
				t.Helper()
				var decoded View
				if err := json.Unmarshal(data, &decoded); err != nil || decoded.Operation == nil {
					t.Fatalf("%s did not decode: %v", name, err)
				}
				if decoded.Operation.Availability != inMemory.Operation.Availability || decoded.Operation.Availability.ConfirmedAt != tc.availability.ConfirmedAt {
					t.Fatalf("%s changed route/timestamp between model and wire", name)
				}
				if decoded.Source.RecordedAt != nil || decoded.Source.PersistenceTimeKnown || decoded.Freshness.State != diagnostics.FreshnessUnknown || !decoded.Partial {
					t.Fatalf("%s invented persistence freshness", name)
				}
				if decoded.Operation.Failure == nil || decoded.Operation.Failure.Code != "SmokeFailed" || decoded.Operation.Retry.Status != diagnostics.RetryUnknown || decoded.Operation.Retry.Authoritative {
					t.Fatalf("%s changed independent original failure/retry facts", name)
				}
				if !strings.Contains(decoded.Explanation, "Serving route availability is unconfirmed.") || strings.Contains(decoded.Explanation, "remains routed") || strings.Contains(decoded.Explanation, "is routed") {
					t.Fatalf("%s advertised a confirmed serving route", name)
				}
			}
			data, err := inMemory.JSON()
			if err != nil {
				t.Fatal(err)
			}
			assert("View.JSON", data)
			response := request(handler(t, m, allowed(t)), "GET")
			if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store, private" {
				t.Fatalf("HTTP response %d lost transport contract", response.Code)
			}
			assert("HTTP", response.Body.Bytes())
			if s.Operation.Availability != tc.availability {
				t.Fatal("read/serialization mutated source availability")
			}
		})
	}
}
