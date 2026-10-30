//go:build integration

package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

type diagnosticPolicy struct {
	scope catalogdiagnostics.Target
	now   time.Time
	allow bool
}

func (p *diagnosticPolicy) CheckDiagnosticRead(_ context.Context, i catalogdiagnostics.SessionIdentity, scope catalogdiagnostics.Target) (catalogdiagnostics.ReadDecision, error) {
	return catalogdiagnostics.ReadDecision{Allowed: p.allow && scope == p.scope && i.Subject == "google:owned-fixture-user", PolicyID: "owned-policy-v1", ValidUntil: p.now.Add(time.Minute)}, nil
}

type countedDiagnosticSource struct {
	source catalogdiagnostics.StatusSource
	reads  int
}

func (s *countedDiagnosticSource) Read(c context.Context, g catalogdiagnostics.ReadGrant) (catalogdiagnostics.Snapshot, error) {
	s.reads++
	return s.source.Read(c, g)
}

func TestDurableDiagnosticsVerifiedSessionReadsRetiredOwnedJournal(t *testing.T) {
	_, config := controlCell(t, "diagnostic-retired")
	repo := openRepository(t, config)
	initial, err := repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	scope := catalogdiagnostics.Target{Environment: config.Subject.Environment, Installation: config.Subject.Installation, ProjectUID: config.Subject.ProjectUID}
	id := diagnostics.Identity{Environment: scope.Environment, Installation: scope.Installation, ProjectUID: scope.ProjectUID, ProjectName: "owned-example", OperationID: "retired-operation", ReconcileKey: "failed-source-revision", Generation: 2}
	p, err := diagnostics.NewProjection(id)
	if err != nil {
		t.Fatal(err)
	}
	failure := diagnostics.DomainFailure{ID: "candidate-smoke", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Candidate smoke probe failed", OccurredAt: now}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 1, OccurredAt: now, State: diagnostics.Failed, Step: "smoke", Failure: &failure, Availability: &diagnostics.Availability{ActiveRevision: "old-healthy", CandidateRevision: "failed-candidate", RouteState: diagnostics.RouteActivePreserved, ConfirmedAt: now}})
	if err != nil {
		t.Fatal(err)
	}
	next := initial.Envelope
	next.Attempts = []journal.Attempt{{Number: 1, OperationID: id.OperationID, NamespaceName: "never-started-retired-candidate", Revision: id.ReconcileKey, DesiredDigest: strings.Repeat("b", 64), State: journal.Reserved}}
	next.Projection = &p
	commitJournal(t, repo, initial, next)
	// A different actual ConfigMap stores the latest accepted intent. The read
	// model cannot manufacture current desired state from the failed projection.
	control, ok := config.Namespace.(*provider.NamespaceBinding)
	if !ok {
		t.Fatal("expected concrete owned namespace adapter")
	}
	accepted := catalogdiagnostics.AcceptedDesired{SchemaVersion: 1, Target: scope, ReconcileKey: "independently-newer-intent", Generation: 9, DecisionID: "accepted-new-intent"}
	desiredObject, err := catalogdiagnostics.DesiredBootstrapObject(control.Context().NamespaceName(), accepted)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := provider.ConfigMapFingerprint(desiredObject)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := control.Context().Intent("configmap", desiredObject.Name, catalogdiagnostics.DesiredRevision, "bootstrap-desired-intent", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	pinnedDesired, err := control.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desiredObject, Intent: intent, Writer: config.Writer()})
	if err != nil {
		t.Fatal(err)
	}
	desired, err := catalogdiagnostics.NewPinnedDesired(catalogdiagnostics.PinnedDesiredConfig{Mode: catalogdiagnostics.LocalPinnedDesired, Target: scope, Namespace: control, Pinned: pinnedDesired, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := desired.ObserveDesired(context.Background(), scope)
	if err != nil || !observed.Known || observed.Desired.ReconcileKey != accepted.ReconcileKey || observed.Desired.Generation != 9 {
		t.Fatal("independent desired API observation failed")
	}
	// Fresh repository reads actual pinned API storage. There is no workflow
	// worker, query client or live operation closure in the diagnostic stack.
	fresh := openRepository(t, config)
	source, err := catalogdiagnostics.NewJournalSource(scope.Environment, scope.Installation, []catalogdiagnostics.JournalBinding{{Target: scope, Repository: fresh, Origin: catalogdiagnostics.Archived}}, desired, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	counted := &countedDiagnosticSource{source: source}
	model, err := catalogdiagnostics.NewModel(counted, catalogdiagnostics.Config{Now: func() time.Time { return now }, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	keyBytes := []byte("owned-local-diagnostic-key-synthetic-12345")
	key, err := catalogdiagnostics.NewSessionKey(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Close()
	policy := &diagnosticPolicy{scope: scope, now: now, allow: true}
	authorizer, err := catalogdiagnostics.NewSessionAuthorizer(catalogdiagnostics.SessionConfig{Environment: scope.Environment, Installation: scope.Installation, Issuer: "https://owned-catalog.example.test", Audience: "owned-catalog", AllowedDomain: "example.test", MaxSessionLifetime: time.Hour, Now: func() time.Time { return now }, Key: key, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := catalogdiagnostics.NewHandler(scope.Environment, scope.Installation, authorizer, model)
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := json.Marshal(map[string]any{"typ": "session", "iss": "https://owned-catalog.example.test", "aud": "owned-catalog", "sub": "google:owned-fixture-user", "email": "fixture@example.test", "hd": "example.test", "iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	encode := base64.RawURLEncoding.EncodeToString
	input := encode([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha256.New, keyBytes)
	_, _ = mac.Write([]byte(input))
	token := input + "." + encode(mac.Sum(nil))
	read := func(cookie bool, project string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/v2/projects/"+project+"/diagnostics", nil)
		r.Header.Set("X-Temporal-Auth-Email", "fixture@example.test")
		if cookie {
			r.AddCookie(&http.Cookie{Name: "temporal_demo_auth", Value: token})
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := read(false, scope.ProjectUID); w.Code != 401 || counted.reads != 0 {
		t.Fatal("unverified headers reached durable source")
	}
	if w := read(true, "foreign-project"); w.Code != 404 || counted.reads != 0 {
		t.Fatal("foreign project reached durable source")
	}
	w := read(true, scope.ProjectUID)
	if w.Code != 200 || counted.reads != 1 {
		t.Fatalf("durable diagnostic read failed: %d", w.Code)
	}
	var view catalogdiagnostics.View
	if json.Unmarshal(w.Body.Bytes(), &view) != nil || view.Operation == nil || view.Operation.Failure == nil || view.Operation.Failure.Code != "SmokeFailed" || view.Source.Origin != catalogdiagnostics.Archived || view.Source.RecordedAt != nil || view.Source.PersistenceTimeKnown || view.Source.DesiredObservedAt == nil || view.Freshness.State != diagnostics.FreshnessUnknown || !view.Partial || view.Operation.Availability.RouteState != diagnostics.RouteUnconfirmed {
		t.Fatal("retired failure or honest freshness lost")
	}
	if w.Header().Get("Cache-Control") != "no-store, private" || strings.Contains(w.Body.String(), token) {
		t.Fatal("unsafe transport caching or session exposure")
	}
	policy.allow = false
	if w := read(true, scope.ProjectUID); w.Code != 404 || counted.reads != 1 {
		t.Fatal("revoked policy reached durable source")
	}
	policy.allow = true
	path := "/api/v1/namespaces/" + desiredObject.Namespace + "/configmaps/" + desiredObject.Name
	actual := request(t, "GET", path, nil, 200)
	accepted.Generation = 10
	modified, _ := accepted.JSON()
	field(actual, "data")[catalogdiagnostics.DesiredDataKey] = string(modified)
	request(t, "PUT", path, actual, 200)
	// Same UID and copied creation markers do not justify mutated desired bytes
	// with an old stamped fingerprint. Keep the historical cause readable.
	if _, err := desired.ObserveDesired(context.Background(), scope); err == nil {
		t.Fatal("same-UID desired tampering accepted")
	}
	w = read(true, scope.ProjectUID)
	view = catalogdiagnostics.View{}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &view) != nil || view.Operation == nil || view.Operation.Failure == nil || view.Operation.Failure.Code != "SmokeFailed" || view.Source.DesiredObservedAt != nil || !view.Partial {
		t.Fatal("unavailable desired observation hid durable original failure")
	}
}
