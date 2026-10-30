//go:build integration && browser_integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

const browserSourceProtocol = "OWNED-BROWSER-SOURCE "

// Witness the actual handler output without changing its headers, status or
// bytes. A browser may abort the failed fetch before its response body remains
// available to Playwright, so the exact safe error code is checked here rather
// than by rereading that aborted fetch. This is not a replacement response.
type browserSourceResponseWitness struct {
	http.ResponseWriter
	status   int
	body     []byte
	overflow bool
}

func (w *browserSourceResponseWitness) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *browserSourceResponseWitness) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if len(data) > catalogdiagnostics.MaxResponseBytes-len(w.body) {
		w.overflow = true
	} else if !w.overflow {
		w.body = append(w.body, data...)
	}
	return w.ResponseWriter.Write(data)
}

func (w *browserSourceResponseWitness) exactSourceUnavailable() bool {
	return !w.overflow && w.status == http.StatusServiceUnavailable && bytes.Equal(w.body, []byte(`{"error":"DiagnosticSourceUnavailable"}`))
}

func browserSourceEmit(t *testing.T, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil || len(data) > 8192 {
		t.Fatal("BrowserSourceProtocolOutputInvalid")
	}
	// Ready includes a synthetic cookie over PRIVATE owned parent stdio only.
	// The host must never echo protocol bytes, exceptions or captured child log.
	if _, err = io.WriteString(os.Stdout, browserSourceProtocol+string(data)+"\n"); err != nil {
		t.Fatal("BrowserSourceProtocolOutputUnavailable")
	}
}

func TestLocalDiagnosticSourceUnavailableAfterWriterExit(t *testing.T) {
	TestOwnedAPIAndNetworkBoundary(t)
	if os.Getenv("OPERATOR_LOCAL_BROWSER_FIXTURE") != "1" {
		t.Fatal("UseExplicitOwnedBrowserSourceMode")
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	start := browserReadCommand(t, decoder, "start")
	frontend := net.ParseIP(start.FrontendIP)
	if start.Run != local.RunID || frontend == nil || frontend.To4() == nil || frontend.IsLoopback() || frontend.IsUnspecified() || frontend.Equal(net.ParseIP(local.NodeIP)) {
		t.Fatal("BrowserSourceFrontendBindingInvalid")
	}
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal("BrowserSourceFrontendInterfaceUnavailable")
	}
	owned := false
	for _, address := range interfaces {
		if network, ok := address.(*net.IPNet); ok && network.IP.Equal(frontend) {
			owned = true
		}
	}
	if !owned {
		t.Fatal("BrowserSourceFrontendNotOwned")
	}

	base, _, _ := runtimeInputProcessFixture(t, "browser-source", "service")
	input := nativeProcessInput{Base: base, Kind: runtimefault.Service}
	endpoint, ca, capture := nativeCreationProxy(t, input)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	old := runNativeProcess(t, input, "initial") // Synchronous actual Wait/reap.
	created, posts, checks := capture.snapshot()
	if posts != 1 || checks != 2 || len(old.Checkpoint) == 0 || old.Write.ResultUID != created.UID || old.Write.ResultVersion != created.Version {
		t.Fatal("BrowserSourceExitedWriterReceiptUnproven")
	}

	connector, sourceEndpoint, sourceCA := newBrowserSourceConnector(t, input)
	_, repository := retirementConfig(t, input.Base.Pin, sourceEndpoint, sourceCA)
	page := newBrowserPageFixtureWithRepository(t, input, old, repository)
	before := browserDirectSourceEvidence(t, input, page)
	if connector.denied.Load() != 0 || page.source.reads.Load() != 0 {
		t.Fatal("BrowserSourceReadBootstrapInvalid")
	}
	var activeDiagnostic atomic.Int64
	var unavailableResponses atomic.Int64
	var responseWitnessInvalid atomic.Bool
	diagnosticPath := "/api/v2/projects/" + page.target.ProjectUID + "/diagnostics"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == diagnosticPath {
			activeDiagnostic.Add(1)
			defer activeDiagnostic.Add(-1)
			witness := &browserSourceResponseWitness{ResponseWriter: w}
			page.handler.ServeHTTP(witness, r)
			if witness.overflow {
				responseWitnessInvalid.Store(true)
			}
			if witness.status == http.StatusServiceUnavailable {
				if r.Method == http.MethodGet && witness.exactSourceUnavailable() {
					unavailableResponses.Add(1)
				} else {
					responseWitnessInvalid.Store(true)
				}
			}
			return
		}
		page.handler.ServeHTTP(w, r) // Every response comes from actual handler.
	})
	listener, err := net.Listen("tcp4", net.JoinHostPort(start.FrontendIP, "8080"))
	if err != nil || listener.Addr().String() != net.JoinHostPort(start.FrontendIP, "8080") {
		t.Fatal("BrowserSourceExactFrontendListenFailed")
	}
	server := &http.Server{Handler: handler, ErrorLog: log.New(io.Discard, "", 0), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if server.Shutdown(ctx) != nil {
			_ = server.Close()
			t.Error("BrowserSourceServerShutdownFailed")
		}
	}()
	origin := "http://" + net.JoinHostPort(start.FrontendIP, "8080")
	browserSourceEmit(t, map[string]any{"phase": "ready", "run": local.RunID, "origin": origin, "target": page.target, "cookie": page.cookie, "writerExited": true})
	browserReadCommand(t, decoder, "disconnect-source")
	if page.source.reads.Load() != 1 || activeDiagnostic.Load() != 0 || connector.active.Load() != 0 || connector.denied.Load() != 0 || page.privacyViolation.Load() || unavailableResponses.Load() != 0 || responseWitnessInvalid.Load() {
		t.Fatal("BrowserSourceFirstAuthorizedReadUnproven")
	}
	connector.close()
	if !connector.closed.Load() || connector.closeFailed.Load() || connector.active.Load() != 0 || browserDirectSourceEvidence(t, input, page) != before {
		t.Fatal("BrowserSourceClosureOrEvidenceUnproven")
	}
	browserSourceEmit(t, map[string]any{"phase": "source-disconnected", "sourceReads": int64(1), "proxyClosed": true, "evidenceRetained": true})
	browserReadCommand(t, decoder, "finish")
	if page.source.reads.Load() != 2 || activeDiagnostic.Load() != 0 || connector.active.Load() != 0 || connector.denied.Load() != 0 || page.privacyViolation.Load() || unavailableResponses.Load() != 1 || responseWitnessInvalid.Load() || browserDirectSourceEvidence(t, input, page) != before {
		t.Fatal("BrowserSourceOutageReadOrRetainedEvidenceInvalid")
	}
	_, posts, _ = capture.snapshot()
	if posts != 1 {
		t.Fatal("BrowserSourceReadRepostedManagedResource")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if server.Shutdown(ctx) != nil {
		t.Fatal("BrowserSourceServerShutdownFailed")
	}
	if err = <-done; err != http.ErrServerClosed {
		t.Fatal("BrowserSourceServerUnexpectedExit")
	}
	browserSourceEmit(t, map[string]any{"phase": "finished", "sourceReads": int64(2), "managedPosts": posts, "writerExited": true, "serverClosed": true, "proxyClosed": true, "evidenceRetained": true})
}
