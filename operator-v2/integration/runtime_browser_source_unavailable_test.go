//go:build integration && browser_integration

package integration

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
)

// The connector forwards real owned API reads only. It never manufactures a
// diagnostic view, source snapshot or Kubernetes status. Its deliberate close
// is a real transport outage confined to this new reader's source connection.
type browserSourceConnector struct {
	server      *httptest.Server
	denied      atomic.Int64
	active      atomic.Int64
	closed      atomic.Bool
	closeFailed atomic.Bool
	once        sync.Once
}

func browserSourceReadAllowed(r *http.Request, allowed map[string]bool) bool {
	// Pinned client-go v0.37 copies the explicit HTTP timeout into Request and
	// serializes its exact timeout=5s query. Empty remains the original allowed
	// form; no other query representation or implicit body is admitted.
	return r != nil && r.URL != nil && r.Method == http.MethodGet && allowed[r.URL.Path] && r.URL.RawPath == "" && (r.URL.RawQuery == "" || r.URL.RawQuery == "timeout=5s") && !r.URL.ForceQuery && r.ContentLength == 0 && r.TransferEncoding == nil
}

// Pure regression executed by this existing fixture before listener startup;
// it performs no HTTP/API call and introduces no extra top-level native case.
func browserSourceRequestPolicyRegression(t *testing.T, allowed map[string]bool) {
	t.Helper()
	for path := range allowed {
		for _, query := range []string{"", "timeout=5s"} {
			r := httptest.NewRequest(http.MethodGet, "https://127.0.0.1"+path, nil)
			r.URL.RawQuery = query
			if !browserSourceReadAllowed(r, allowed) {
				t.Fatal("BrowserSourceSDKRequestPolicyRejected")
			}
		}
	}
	path := "/api/v1/namespaces/kube-system"
	for _, change := range []func(*http.Request){
		func(r *http.Request) { r.Method = http.MethodPost },
		func(r *http.Request) { r.Method = http.MethodHead },
		func(r *http.Request) { r.URL.Path += "/configmaps/foreign" },
		func(r *http.Request) { r.URL.RawPath = "/api/v1/namespaces/%6bube-system" },
		func(r *http.Request) { r.URL.ForceQuery = true },
		func(r *http.Request) { r.URL.RawQuery = "timeout=6s" },
		func(r *http.Request) { r.URL.RawQuery = "timeout=%35s" },
		func(r *http.Request) { r.URL.RawQuery = "timeout=5s&timeout=5s" },
		func(r *http.Request) { r.URL.RawQuery = "timeout=5s&watch=true" },
		func(r *http.Request) { r.ContentLength = -1 },
		func(r *http.Request) { r.ContentLength = 1 },
		func(r *http.Request) { r.TransferEncoding = []string{"chunked"} },
	} {
		r := httptest.NewRequest(http.MethodGet, "https://127.0.0.1"+path+"?timeout=5s", nil)
		change(r)
		if browserSourceReadAllowed(r, allowed) {
			t.Fatal("BrowserSourceRequestPolicyBroadened")
		}
	}
	if browserSourceReadAllowed(nil, allowed) {
		t.Fatal("BrowserSourceRequestPolicyMissingRequest")
	}
}

func newBrowserSourceConnector(t *testing.T, input nativeProcessInput) (*browserSourceConnector, string, []byte) {
	t.Helper()
	target, err := url.Parse(local.Server)
	if err != nil || target.Scheme != "https" || target.Host != local.NodeIP+":6443" || target.Path != "" {
		t.Fatal("BrowserSourceUpstreamBindingInvalid")
	}
	parent := input.Base.Pin.Parent.Ref.Name
	cell := input.Base.Pin.Cell.Record.Ref.Name
	allowed := map[string]bool{
		"/api/v1/namespaces/kube-system":                       true,
		"/api/v1/namespaces/" + parent:                         true,
		"/api/v1/namespaces/" + parent + "/configmaps/" + cell: true,
	}
	browserSourceRequestPolicyRegression(t, allowed)
	p := &browserSourceConnector{}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport // Explicit owned synthetic mTLS client.
	forward.ErrorLog = log.New(io.Discard, "", 0)
	forward.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// No thrown URL/provider/body/credential data enters logs or browser.
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.active.Add(1)
		defer p.active.Add(-1)
		if p.closed.Load() || !browserSourceReadAllowed(r, allowed) {
			p.denied.Add(1)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		forward.ServeHTTP(w, r)
	})
	ca, err := base64.StdEncoding.DecodeString(local.CA)
	pool := x509.NewCertPool()
	if err != nil || !pool.AppendCertsFromPEM(ca) {
		t.Fatal("BrowserSourceClientTrustInvalid")
	}
	p.server = httptest.NewUnstartedServer(handler)
	p.server.Config.ErrorLog = log.New(io.Discard, "", 0)
	p.server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	p.server.StartTLS()
	t.Cleanup(p.close)
	return p, p.server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.server.Certificate().Raw})
}

func (p *browserSourceConnector) close() {
	p.once.Do(func() {
		p.closed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if p.server.Config.Shutdown(ctx) != nil {
			p.closeFailed.Store(true)
			_ = p.server.Config.Close()
		}
		// Close also drains established/idle keepalive connections. No listener
		// restart/recovery or alternative source/URL is permitted in this case.
		p.server.Close()
	})
}

// Private independent native evidence, never browser/history/protocol payload.
type browserSourceEvidence struct {
	journalVersion                journal.Version
	journalHash                   [32]byte
	policyVersion, desiredVersion string
	policyHash, desiredHash       [32]byte
}

func browserDirectSourceEvidence(t *testing.T, input nativeProcessInput, page *browserPageFixture) browserSourceEvidence {
	t.Helper()
	_, repository := retirementConfig(t, input.Base.Pin, "", nil)
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal("BrowserDirectJournalReadUnavailable")
	}
	data, err := snapshot.Envelope.JSON()
	if err != nil {
		t.Fatal("BrowserDirectJournalInvalid")
	}
	policy, err := page.bound.ObserveConfigMap(context.Background(), page.policyCell)
	if err != nil || policy.Object == nil || string(policy.Object.UID) != page.policyCell.Record.Ref.ProviderID {
		t.Fatal("BrowserDirectPolicyReadUnavailable")
	}
	desired, err := page.bound.ObserveConfigMap(context.Background(), page.desiredCell)
	if err != nil || desired.Object == nil || string(desired.Object.UID) != page.desiredCell.Record.Ref.ProviderID {
		t.Fatal("BrowserDirectDesiredReadUnavailable")
	}
	policyData, e1 := json.Marshal(policy.Object.Data)
	desiredData, e2 := json.Marshal(desired.Object.Data)
	if e1 != nil || e2 != nil || len(policy.Object.BinaryData) != 0 || len(desired.Object.BinaryData) != 0 {
		t.Fatal("BrowserDirectPolicyDesiredInvalid")
	}
	return browserSourceEvidence{journalVersion: snapshot.Version, journalHash: sha256.Sum256(data), policyVersion: policy.Object.ResourceVersion, desiredVersion: desired.Object.ResourceVersion, policyHash: sha256.Sum256(policyData), desiredHash: sha256.Sum256(desiredData)}
}
