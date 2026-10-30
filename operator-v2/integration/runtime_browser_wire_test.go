//go:build integration && browser_integration

package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

type browserStatusSource struct {
	source catalogdiagnostics.StatusSource
	reads  atomic.Int64
}

func (s *browserStatusSource) Read(ctx context.Context, grant catalogdiagnostics.ReadGrant) (catalogdiagnostics.Snapshot, error) {
	s.reads.Add(1)
	return s.source.Read(ctx, grant)
}

type browserPageFixture struct {
	target           catalogdiagnostics.Target
	handler          http.Handler
	source           *browserStatusSource
	key              *catalogdiagnostics.SessionKey
	cookie           string
	privacyViolation atomic.Bool
	bound            *provider.NamespaceBinding
	policyRecord     catalogdiagnostics.ProjectReadPolicy
	policyCell       ownership.NamespacedRecord
	desiredCell      ownership.NamespacedRecord
	writer           ownership.Writer
}

func newBrowserPageFixture(t *testing.T, input nativeProcessInput, old nativeProcessOutput) *browserPageFixture {
	t.Helper()
	_, repository := retirementConfig(t, input.Base.Pin, "", nil)
	return newBrowserPageFixtureWithRepository(t, input, old, repository)
}

// Test-only composition seam. The original wrapper still supplies its direct
// pinned Repository; the separate outage case builds an independently pinned
// real Repository over its exact owned read-only connector. No fake source or
// public/production repository override is introduced.
func newBrowserPageFixtureWithRepository(t *testing.T, input nativeProcessInput, old nativeProcessOutput, repository *journal.Repository) *browserPageFixture {
	t.Helper()
	if repository == nil {
		t.Fatal("BrowserPinnedRepositoryMissing")
	}
	diagnostic := diagnosticProcessInput{Base: input.Base}
	bound := diagnosticControlBinding(t, diagnostic)
	target := diagnosticTarget(diagnostic)
	desiredCell := bootstrapDiagnosticDesired(t, diagnostic)
	snapshot, err := repository.Load(context.Background())
	if err != nil || snapshot.Envelope.Projection == nil || snapshot.Envelope.Projection.Failure == nil || !canonicalFailureWireEqual(*snapshot.Envelope.Projection.Failure, old.Primary) {
		t.Fatal("BrowserFreshRepositoryLostOriginalCause")
	}
	desired, err := catalogdiagnostics.NewPinnedDesired(catalogdiagnostics.PinnedDesiredConfig{Mode: catalogdiagnostics.LocalPinnedDesired, Target: target, Namespace: bound, Pinned: desiredCell, Now: time.Now})
	if err != nil {
		t.Fatal("BrowserPinnedDesiredInvalid")
	}
	journalSource, err := catalogdiagnostics.NewJournalSource(target.Environment, target.Installation, []catalogdiagnostics.JournalBinding{{Target: target, Repository: repository, Origin: catalogdiagnostics.Materialized}}, desired, time.Now)
	if err != nil {
		t.Fatal("BrowserActualJournalSourceInvalid")
	}
	source := &browserStatusSource{source: journalSource}
	model, err := catalogdiagnostics.NewModel(source, catalogdiagnostics.Config{Now: time.Now, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if err != nil {
		t.Fatal("BrowserActualModelInvalid")
	}
	now := time.Now().UTC().Truncate(time.Second)
	policyRecord := catalogdiagnostics.ProjectReadPolicy{SchemaVersion: 1, Target: target, Issuer: "https://owned-catalog.example.test", IssuedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Subjects: []catalogdiagnostics.PolicySubject{{Subject: "google:owned-fixture-user", Allowed: true, AuthenticatedAtOrAfter: now.Add(-time.Minute)}}}
	policyObject, err := catalogdiagnostics.PolicyBootstrapObject(bound.Context().NamespaceName(), policyRecord)
	if err != nil {
		t.Fatal("BrowserPolicyBootstrapInvalid")
	}
	fingerprint, err := provider.ConfigMapFingerprint(policyObject)
	if err != nil {
		t.Fatal("BrowserPolicyFingerprintInvalid")
	}
	intent, err := bound.Context().Intent("configmap", policyObject.Name, catalogdiagnostics.PolicyRevision, "browser-policy-bootstrap", fingerprint)
	if err != nil {
		t.Fatal("BrowserPolicyIntentInvalid")
	}
	writer := ownership.Writer{ID: "control-writer", Owner: bound.Context().Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	pinned, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: policyObject, Intent: intent, Writer: writer})
	if err != nil {
		t.Fatal("BrowserActualPolicyCreateFailed")
	}
	policy, err := catalogdiagnostics.NewPinnedReadPolicy(catalogdiagnostics.PinnedPolicyConfig{Mode: catalogdiagnostics.LocalPinnedPolicy, Environment: target.Environment, Installation: target.Installation, Cells: []catalogdiagnostics.PolicyCell{{Target: target, Namespace: bound, Pinned: pinned}}, Now: time.Now})
	if err != nil {
		t.Fatal("BrowserActualPolicyReaderInvalid")
	}
	keyBytes := make([]byte, 32)
	if _, err = rand.Read(keyBytes); err != nil {
		t.Fatal("BrowserSyntheticKeyUnavailable")
	}
	key, err := catalogdiagnostics.NewSessionKey(keyBytes)
	if err != nil {
		t.Fatal("BrowserSyntheticKeyInvalid")
	}
	t.Cleanup(key.Close)
	authorizer, err := catalogdiagnostics.NewSessionAuthorizer(catalogdiagnostics.SessionConfig{Environment: target.Environment, Installation: target.Installation, Issuer: policyRecord.Issuer, Audience: "owned-catalog", AllowedDomain: "example.test", MaxSessionLifetime: time.Hour, Now: time.Now, Key: key, Policy: policy})
	if err != nil {
		t.Fatal("BrowserActualSessionVerifierInvalid")
	}
	handler, err := catalogdiagnostics.NewHandler(target.Environment, target.Installation, authorizer, model)
	if err != nil {
		t.Fatal("BrowserActualHandlerInvalid")
	}
	asset := func(name string) []byte {
		data, err := os.ReadFile("/browser-assets/" + name)
		if err != nil || len(data) == 0 || len(data) > 65536 {
			t.Fatal("BrowserReviewedAssetUnavailable")
		}
		return data
	}
	page, err := catalogdiagnostics.NewLocalPage(catalogdiagnostics.LocalPageConfig{Enabled: true, Target: target, Handler: handler, Assets: catalogdiagnostics.LocalPageAssets{HTML: asset("diagnostic-page.html"), Module: asset("diagnostic-page.mjs"), Stylesheet: asset("diagnostic-page.css"), Panel: asset("diagnostics.mjs")}})
	if err != nil {
		t.Fatal("BrowserActualLocalPageInvalid")
	}
	claims, _ := json.Marshal(map[string]any{"typ": "session", "iss": policyRecord.Issuer, "aud": "owned-catalog", "sub": "google:owned-fixture-user", "email": "fixture@example.test", "hd": "example.test", "iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	encode := base64.RawURLEncoding.EncodeToString
	signed := encode([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha256.New, keyBytes)
	_, _ = mac.Write([]byte(signed))
	token := signed + "." + encode(mac.Sum(nil))
	for i := range keyBytes {
		keyBytes[i] = 0
	}
	fixture := &browserPageFixture{target: target, source: source, key: key, cookie: token, bound: bound, policyRecord: policyRecord, policyCell: pinned, desiredCell: desiredCell, writer: writer}
	fixture.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed := httptest.NewRecorder()
		page.ServeHTTP(observed, r)
		// Independent fixture witness only: the real handler produces every
		// response. Private bytes MUST NOT be forwarded, logged or compared in
		// a failure diff. No fake diagnostic endpoint or mutation route exists.
		for _, forbidden := range []string{token, input.Base.Directory, runtimeServicePrivateMarker, runtimeDeploymentPrivateMarker} {
			if forbidden != "" && bytes.Contains(observed.Body.Bytes(), []byte(forbidden)) {
				fixture.privacyViolation.Store(true)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		for name, values := range observed.Header() {
			w.Header()[name] = append([]string(nil), values...)
		}
		w.WriteHeader(observed.Code)
		_, _ = w.Write(observed.Body.Bytes())
	})
	return fixture
}

func (f *browserPageFixture) revoke(t *testing.T) {
	t.Helper()
	observed, err := f.bound.ObserveConfigMap(context.Background(), f.policyCell)
	if err != nil {
		t.Fatal("BrowserPolicyRevokeReadFailed")
	}
	f.policyRecord.Subjects[0].Allowed = false
	desired, err := catalogdiagnostics.PolicyBootstrapObject(f.bound.Context().NamespaceName(), f.policyRecord)
	if err != nil {
		t.Fatal("BrowserPolicyRevokeBodyInvalid")
	}
	fingerprint, err := provider.ConfigMapFingerprint(desired)
	if err != nil {
		t.Fatal("BrowserPolicyRevokeFingerprintInvalid")
	}
	intent, err := f.bound.Context().Intent("configmap", desired.Name, catalogdiagnostics.PolicyRevision, "browser-policy-revoke", fingerprint)
	if err != nil {
		t.Fatal("BrowserPolicyRevokeIntentInvalid")
	}
	current := f.policyCell
	current.Record.Revision = observed.Observation.Observation.Revision
	current.Record.SpecFingerprint = observed.Observation.Observation.SpecFingerprint
	current.Record.WriterEpoch = observed.Observation.Observation.WriterEpoch
	if _, err = f.bound.UpdateConfigMap(context.Background(), provider.ConfigMapUpdate{ConfigMapMutation: provider.ConfigMapMutation{OperationID: intent.Intent.OperationID, Record: current, ExpectedVersion: observed.Object.ResourceVersion, Writer: f.writer}, Desired: desired, Intent: intent}); err != nil {
		t.Fatal("BrowserActualPolicyRevokeCASFailed")
	}
}
