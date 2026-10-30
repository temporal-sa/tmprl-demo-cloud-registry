package catalogdiagnostics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

var testNow = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
var target = Target{Environment: "local", Installation: "install", ProjectUID: "project-123"}

type sourceFunc func(context.Context, ReadGrant) (Snapshot, error)

func (f sourceFunc) Read(ctx context.Context, g ReadGrant) (Snapshot, error) { return f(ctx, g) }

type authFunc func(context.Context, *http.Request, Target) (ReadGrant, error)

func (f authFunc) AuthorizeRead(ctx context.Context, r *http.Request, t Target) (ReadGrant, error) {
	return f(ctx, r, t)
}
func grant(t *testing.T) ReadGrant {
	t.Helper()
	g, err := IssueReadGrant("verified-session-subject", "policy-v1", target, testNow.Add(-time.Minute), testNow.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func snapshot(t *testing.T) Snapshot {
	t.Helper()
	id := diagnostics.Identity{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID, ProjectName: "demo", OperationID: "op-1", ReconcileKey: "revision-1", Generation: 2}
	p, err := diagnostics.NewProjection(id)
	if err != nil {
		t.Fatal(err)
	}
	cause := diagnostics.DomainFailure{ID: "smoke", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Candidate smoke check failed", OccurredAt: testNow.Add(-time.Minute)}
	cleanup := diagnostics.DomainFailure{ID: "cleanup", Code: "DeleteFailed", Category: diagnostics.Cleanup, Message: "Candidate retained for inspection", OccurredAt: testNow.Add(-time.Minute)}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 3, OccurredAt: testNow.Add(-time.Minute), State: diagnostics.Failed, Step: "smoke", Failure: &cause, Availability: &diagnostics.Availability{ActiveRevision: "active-old", CandidateRevision: "candidate-new", RouteState: diagnostics.RouteActivePreserved, ConfirmedAt: testNow.Add(-time.Minute)}, Recovery: []diagnostics.RecoveryOutcome{{ID: "cleanup", Step: "cleanup", State: diagnostics.OutcomeRetained, Failure: &cleanup}}, Evidence: []diagnostics.EvidenceRef{{ID: "evidence-1", Kind: "PodLogs", Locator: "s3://private-bucket/object?token=secret", State: "Available"}}})
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.WithPublication(diagnostics.SinkHealth{Sink: "catalog", State: diagnostics.DeliveryFailed, SourceSequence: p.Sequence, ObservedAt: testNow, Failure: &diagnostics.DomainFailure{ID: "publish", Code: "PublishFailed", Category: diagnostics.Publication, Message: "Catalog write failed", OccurredAt: testNow}})
	if err != nil {
		t.Fatal(err)
	}
	return Snapshot{Target: target, Origin: Materialized, Operation: &p, RecordedAt: testNow, Desired: diagnostics.DesiredState{ReconcileKey: id.ReconcileKey, Generation: id.Generation}, DesiredKnown: true, DesiredObservedAt: testNow}
}
func model(t *testing.T, s StatusSource) *Model {
	t.Helper()
	m, err := NewModel(s, Config{Now: func() time.Time { return testNow }, MaxProgressAge: 5 * time.Minute, MaxDesiredAge: 2 * time.Minute, MaxAvailabilityAge: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func handler(t *testing.T, m *Model, a ReadAuthorizer) *Handler {
	t.Helper()
	h, err := NewHandler(target.Environment, target.Installation, a, m)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func allowed(t *testing.T) ReadAuthorizer {
	return authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) { return grant(t), nil })
}
func request(h http.Handler, method string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/api/v2/projects/"+target.ProjectUID+"/diagnostics", nil)
	r.Header.Set("X-Forwarded-Email", "administrator@example.com")
	r.Header.Set("X-Environment", "production")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAuthorizationBeforeSourceAndNoHeaderAuthentication(t *testing.T) {
	calls := 0
	m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { calls++; return snapshot(t), nil }))
	if _, err := NewHandler("local", "install", nil, m); err == nil {
		t.Fatal("nil authorizer accepted")
	}
	cases := []struct {
		name   string
		auth   ReadAuthorizer
		status int
	}{
		{"anonymous", authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) {
			return ReadGrant{}, ErrUnauthenticated
		}), 401},
		{"denied", authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) { return ReadGrant{}, ErrForbidden }), 404},
		{"unverified zero grant", authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) { return ReadGrant{}, nil }), 404},
		{"raw auth error", authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) {
			return ReadGrant{}, errors.New("password=SECRET-SENTINEL")
		}), 503},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := request(handler(t, m, tc.auth), "GET")
			if w.Code != tc.status || strings.Contains(w.Body.String(), "SECRET") {
				t.Fatalf("unsafe response %d %s", w.Code, w.Body.String())
			}
			if calls != 0 {
				t.Fatal("source accessed before authorization")
			}
			if w.Header().Get("Cache-Control") != "no-store, private" {
				t.Fatal("error cache policy missing")
			}
		})
	}
	for _, mutation := range []func(*ReadGrant){func(g *ReadGrant) { g.target.ProjectUID = "foreign" }, func(g *ReadGrant) { g.expiresAt = testNow }, func(g *ReadGrant) { g.issuedAt = testNow.Add(time.Second) }} {
		g := grant(t)
		mutation(&g)
		_, err := m.Read(context.Background(), g, target)
		if !errors.Is(err, ErrForbidden) || calls != 0 {
			t.Fatal("invalid grant accessed source")
		}
	}
	if _, err := IssueReadGrant("subject", "policy", target, testNow, testNow.Add(MaxGrantLifetime+time.Second)); !errors.Is(err, ErrForbidden) {
		t.Fatal("overlong grant accepted")
	}
}

func TestAuthorizedAbsenceUnavailableInvalidAndMissingStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot Snapshot
		err      error
		status   int
	}{
		{name: "absent", err: ErrNotFound, status: 404},
		{name: "unavailable", err: errors.New("api_key=SECRET-SENTINEL"), status: 503},
		{name: "wrong scope", snapshot: Snapshot{Target: Target{Environment: "production", Installation: "install", ProjectUID: target.ProjectUID}, Origin: Materialized}, status: 502},
		{name: "missing status", snapshot: Snapshot{Target: target, Origin: Materialized}, status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return tc.snapshot, tc.err }))
			w := request(handler(t, m, allowed(t)), "GET")
			if w.Code != tc.status || strings.Contains(w.Body.String(), "SECRET") {
				t.Fatalf("response %d %s", w.Code, w.Body.String())
			}
			if tc.name == "missing status" && !strings.Contains(w.Body.String(), "NotObserved") {
				t.Fatal("missing status became absence")
			}
		})
	}
	s := snapshot(t)
	s.Operation.Identity.ProjectUID = "foreign"
	_, err := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil })).Read(context.Background(), grant(t), target)
	if !errors.Is(err, ErrInvalidSource) {
		t.Fatal("cross-project projection accepted")
	}
}

func TestFailureRecoveryPublicationAndArchivedReadWithoutWorker(t *testing.T) {
	for _, origin := range []Origin{Materialized, Archived} {
		t.Run(string(origin), func(t *testing.T) {
			s := snapshot(t)
			s.Origin = origin
			m := model(t, sourceFunc(func(_ context.Context, g ReadGrant) (Snapshot, error) {
				if g.Target() != target {
					t.Fatal("unscoped source read")
				}
				return s, nil
			}))
			v, err := m.Read(context.Background(), grant(t), target)
			if err != nil {
				t.Fatal(err)
			}
			if v.Operation.Failure.Code != "SmokeFailed" || v.Operation.Recovery[0].Failure.Code != "DeleteFailed" || v.Operation.Publication[0].Failure.Code != "PublishFailed" {
				t.Fatal("failure layers collapsed")
			}
			if v.Operation.Evidence[0].Locator != "" || v.EvidenceResolutionAvailable {
				t.Fatal("evidence capability leaked")
			}
			if s.Operation.Evidence[0].Locator == "" {
				t.Fatal("read mutated source")
			}
			if v.Freshness.State != diagnostics.FreshnessCurrent {
				t.Fatal("terminal failure freshness lost")
			}
			if origin == Archived && (v.Operation.Availability.RouteState != diagnostics.RouteUnconfirmed || !strings.Contains(v.Explanation, "Archived")) {
				t.Fatal("archive asserts live availability")
			}
		})
	}
}

func TestFreshnessUsesDesiredAndProgressNotProjectionTime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Snapshot)
		want   diagnostics.FreshnessState
		route  diagnostics.RouteState
	}{
		{"old running progress", func(s *Snapshot) {
			s.Operation.State = diagnostics.Running
			s.Operation.ProgressedAt = testNow.Add(-time.Hour)
		}, diagnostics.FreshnessStale, diagnostics.RouteActivePreserved},
		{"new desired", func(s *Snapshot) { s.Desired.Generation++; s.Desired.ReconcileKey = "revision-2" }, diagnostics.FreshnessReconciling, diagnostics.RouteActivePreserved},
		{"old desired observation", func(s *Snapshot) { s.DesiredObservedAt = testNow.Add(-time.Hour) }, diagnostics.FreshnessStale, diagnostics.RouteActivePreserved},
		{"unknown desired", func(s *Snapshot) { s.DesiredKnown = false; s.Desired.ReconcileKey = strings.Repeat("untrusted", 1000) }, diagnostics.FreshnessUnknown, diagnostics.RouteActivePreserved},
		{"stale route", func(s *Snapshot) { s.Operation.Availability.ConfirmedAt = testNow.Add(-time.Hour) }, diagnostics.FreshnessCurrent, diagnostics.RouteUnconfirmed},
		{"future desired", func(s *Snapshot) { s.DesiredObservedAt = testNow.Add(time.Hour) }, diagnostics.FreshnessUnknown, diagnostics.RouteActivePreserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := snapshot(t)
			tc.change(&s)
			v, err := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil })).Read(context.Background(), grant(t), target)
			if err != nil {
				t.Fatal(err)
			}
			if v.Freshness.State != tc.want || v.Operation.Availability.RouteState != tc.route {
				t.Fatalf("freshness %s route %s", v.Freshness.State, v.Operation.Availability.RouteState)
			}
		})
	}
}

func TestRedactionBoundsAndTransportPolicy(t *testing.T) {
	s := snapshot(t)
	s.Operation.Failure.Message = "password=SECRET-SENTINEL " + strings.Repeat("<", 4000)
	s.Operation.Warnings = make([]diagnostics.DomainFailure, 100)
	m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil }))
	h := handler(t, m, allowed(t))
	w := request(h, "GET")
	if w.Code != 200 || strings.Contains(w.Body.String(), "SECRET-SENTINEL") || strings.Contains(w.Body.String(), "private-bucket") || len(w.Body.Bytes()) > MaxResponseBytes || !strings.Contains(w.Body.String(), "\"partial\":true") {
		t.Fatalf("unsafe/bounds response %d bytes=%d %s", w.Code, w.Body.Len(), w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store, private" || w.Header().Get("Vary") != "Authorization, Cookie" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("cache/security policy missing")
	}
	if w = request(h, "HEAD"); w.Code != 200 || w.Body.Len() != 0 {
		t.Fatal("HEAD has body")
	}
	if w = request(h, "POST"); w.Code != 405 || w.Header().Get("Allow") != "GET, HEAD" {
		t.Fatal("mutation method accepted")
	}
	r := httptest.NewRequest("GET", "/api/v2/projects/"+target.ProjectUID+"/diagnostics?environment=production", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("request can select foreign scope")
	}
}

func TestExpiryDuringReadAndInvalidSourceFailClosed(t *testing.T) {
	now := testNow
	s := snapshot(t)
	m, err := NewModel(sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) {
		now = now.Add(2 * time.Minute)
		return Snapshot{}, ErrNotFound
	}), Config{Now: func() time.Time { return now }, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Read(context.Background(), grant(t), target); !errors.Is(err, ErrForbidden) {
		t.Fatal("expired grant reveals source result")
	}
	for _, change := range []func(*Snapshot){func(s *Snapshot) { s.Operation.SchemaVersion = 999 }, func(s *Snapshot) { s.Operation.State = "UnknownState" }, func(s *Snapshot) { s.RecordedAt = testNow.Add(time.Hour) }, func(s *Snapshot) { s.Desired.ReconcileKey = strings.Repeat("x", 129) }} {
		s = snapshot(t)
		change(&s)
		_, err = model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return s, nil })).Read(context.Background(), grant(t), target)
		if !errors.Is(err, ErrInvalidSource) {
			t.Fatal("invalid source accepted")
		}
	}
}
