//go:build integration

package integration

import (
	"bytes"
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
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

func diagnosticControlBinding(t *testing.T, input diagnosticProcessInput) *provider.NamespaceBinding {
	t.Helper()
	adapter, _ := typedClient(t)
	base, e := ownership.NewContext(input.Base.Pin.Control)
	if e != nil {
		t.Fatal(e)
	}
	bound, e := adapter.BindNamespace(context.Background(), base, input.Base.Pin.Parent, input.Base.Pin.Parent.CreationOperationID)
	if e != nil {
		t.Fatal(e)
	}
	return bound
}
func diagnosticTarget(input diagnosticProcessInput) catalogdiagnostics.Target {
	s := input.Base.Pin.Subject
	return catalogdiagnostics.Target{Environment: s.Environment, Installation: s.Installation, ProjectUID: s.ProjectUID}
}
func bootstrapDiagnosticDesired(t *testing.T, input diagnosticProcessInput) ownership.NamespacedRecord {
	t.Helper()
	bound := diagnosticControlBinding(t, input)
	d := desiredRuntimeDeployment(input.Base.Pin)
	object, e := catalogdiagnostics.DesiredBootstrapObject(bound.Context().NamespaceName(), catalogdiagnostics.AcceptedDesired{SchemaVersion: 1, Target: diagnosticTarget(input), ReconcileKey: d.ReconcileKey, Generation: d.Generation, DecisionID: "accepted-runtime-diagnostic-fixture"})
	if e != nil {
		t.Fatal(e)
	}
	fp, e := provider.ConfigMapFingerprint(object)
	if e != nil {
		t.Fatal(e)
	}
	intent, e := bound.Context().Intent("configmap", object.Name, catalogdiagnostics.DesiredRevision, "bootstrap-diagnostic-desired", fp)
	if e != nil {
		t.Fatal(e)
	}
	writer := ownership.Writer{ID: "control-writer", Owner: bound.Context().Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	record, e := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: object, Intent: intent, Writer: writer})
	if e != nil {
		t.Fatal(e)
	}
	return record
}

// Actual pinned API storage, fresh process/repository, and verified synthetic
// fixture session feed the real read-only HTTP wire. This is not live catalog
// session integration, browser execution or an invented persistence timestamp.
func diagnosticHTTPWire(t *testing.T, input diagnosticProcessInput) []byte {
	t.Helper()
	scope := diagnosticTarget(input)
	now := time.Now().UTC()
	_, repository := retirementConfig(t, input.Base.Pin, "", nil)
	snapshot, e := repository.Load(context.Background())
	if e != nil || snapshot.Envelope.Projection == nil || snapshot.Envelope.Projection.Availability.RouteState != diagnostics.RouteUnknown {
		t.Fatal("stored cause invented availability", e)
	}
	desired, e := catalogdiagnostics.NewPinnedDesired(catalogdiagnostics.PinnedDesiredConfig{Mode: catalogdiagnostics.LocalPinnedDesired, Target: scope, Namespace: diagnosticControlBinding(t, input), Pinned: input.Desired, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	source, e := catalogdiagnostics.NewJournalSource(scope.Environment, scope.Installation, []catalogdiagnostics.JournalBinding{{Target: scope, Repository: repository, Origin: catalogdiagnostics.Materialized}}, desired, func() time.Time { return now })
	if e != nil {
		t.Fatal(e)
	}
	counted := &countedDiagnosticSource{source: source}
	model, e := catalogdiagnostics.NewModel(counted, catalogdiagnostics.Config{Now: func() time.Time { return now }, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	keyBytes := []byte("owned-runtime-diagnostic-fixture-key-12345")
	key, e := catalogdiagnostics.NewSessionKey(keyBytes)
	if e != nil {
		t.Fatal(e)
	}
	defer key.Close()
	policy := &diagnosticPolicy{scope: scope, now: now, allow: true}
	auth, e := catalogdiagnostics.NewSessionAuthorizer(catalogdiagnostics.SessionConfig{Environment: scope.Environment, Installation: scope.Installation, Issuer: "https://owned-catalog.example.test", Audience: "owned-catalog", AllowedDomain: "example.test", MaxSessionLifetime: time.Hour, Now: func() time.Time { return now }, Key: key, Policy: policy})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := catalogdiagnostics.NewHandler(scope.Environment, scope.Installation, auth, model)
	if e != nil {
		t.Fatal(e)
	}
	claims, _ := json.Marshal(map[string]any{"typ": "session", "iss": "https://owned-catalog.example.test", "aud": "owned-catalog", "sub": "google:owned-fixture-user", "email": "fixture@example.test", "hd": "example.test", "iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	encode := base64.RawURLEncoding.EncodeToString
	signed := encode([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha256.New, keyBytes)
	_, _ = mac.Write([]byte(signed))
	token := signed + "." + encode(mac.Sum(nil))
	read := func(cookie bool, project string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "/api/v2/projects/"+project+"/diagnostics", nil)
		request.Header.Set("X-Temporal-Auth-Email", "fixture@example.test")
		if cookie {
			request.AddCookie(&http.Cookie{Name: "temporal_demo_auth", Value: token})
		}
		result := httptest.NewRecorder()
		handler.ServeHTTP(result, request)
		return result
	}
	if result := read(false, scope.ProjectUID); result.Code != 401 || counted.reads != 0 {
		t.Fatal("header-only identity reached persisted diagnostic")
	}
	if result := read(true, "foreign-project"); result.Code != 404 || counted.reads != 0 {
		t.Fatal("foreign project reached persisted diagnostic")
	}
	result := read(true, scope.ProjectUID)
	var view catalogdiagnostics.View
	decodeErr := json.Unmarshal(result.Body.Bytes(), &view)
	flags := map[string]bool{"http200": result.Code == 200, "decoded": decodeErr == nil, "target": view.Target == scope, "statusFailed": view.Status == "Failed", "operation": view.Operation != nil}
	if view.Operation != nil {
		p := view.Operation
		flags["identity"], flags["failure"], flags["step"], flags["retryUnknown"], flags["retryNonAuthoritative"], flags["routeUnconfirmed"] = p.Identity == diagnosticIdentity(input), p.Failure != nil, p.Step == string(runtimefault.InputRetain), p.Retry.Status == diagnostics.RetryUnknown, !p.Retry.Authoritative, p.Availability.RouteState == diagnostics.RouteUnconfirmed
		if p.Failure != nil {
			flags["failureCode"], flags["action"] = p.Failure.Code == input.ExpectedFailure, p.Failure.NextAction != ""
		}
	}
	required := []string{"http200", "decoded", "target", "statusFailed", "operation", "identity", "failure", "step", "retryUnknown", "retryNonAuthoritative", "routeUnconfirmed", "failureCode", "action"}
	valid := true
	for _, name := range required {
		value, exists := flags[name]
		valid = valid && exists && value
	}
	if !valid {
		safe, _ := json.Marshal(flags)
		t.Fatalf("diagnostic HTTP safe contract flags: %s", safe)
	}
	if view.Source.Origin != catalogdiagnostics.Materialized || view.Source.RecordedAt != nil || view.Source.PersistenceTimeKnown || view.Source.DesiredObservedAt == nil || view.Freshness.State != diagnostics.FreshnessUnknown || !view.Partial || result.Header().Get("Cache-Control") != "no-store, private" {
		t.Fatal("read wire invented persistence freshness or caching")
	}
	if bytes.Contains(result.Body.Bytes(), []byte(runtimeServicePrivateMarker)) || strings.Contains(result.Body.String(), input.Base.Directory) || strings.Contains(result.Body.String(), token) {
		t.Fatal("private bootstrap/input/session escaped diagnostic wire")
	}
	return append([]byte(nil), result.Body.Bytes()...)
}
