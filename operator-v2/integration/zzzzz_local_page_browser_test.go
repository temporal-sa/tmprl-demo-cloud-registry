//go:build integration && browser_integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

const browserProtocol = "OWNED-BROWSER "

type browserCommand struct {
	Phase      string `json:"phase"`
	Run        string `json:"run,omitempty"`
	FrontendIP string `json:"frontendIP,omitempty"`
}

func browserReadCommand(t *testing.T, decoder *json.Decoder, phase string) browserCommand {
	t.Helper()
	var command browserCommand
	if decoder.Decode(&command) != nil || command.Phase != phase {
		t.Fatal("BrowserProtocolInvalid")
	}
	if phase != "start" && (command.Run != "" || command.FrontendIP != "") {
		t.Fatal("BrowserProtocolUnexpectedFields")
	}
	return command
}

func browserEmit(t *testing.T, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil || len(data) > 8192 {
		t.Fatal("BrowserProtocolOutputInvalid")
	}
	// The ready record includes a SYNTHETIC fixture cookie. The host harness
	// consumes this private protocol line and MUST NOT echo it on any failure.
	if _, err = io.WriteString(os.Stdout, browserProtocol+string(data)+"\n"); err != nil {
		t.Fatal("BrowserProtocolOutputUnavailable")
	}
}

func TestLocalDiagnosticBrowserAfterWriterExit(t *testing.T) {
	TestOwnedAPIAndNetworkBoundary(t)
	if os.Getenv("OPERATOR_LOCAL_BROWSER_FIXTURE") != "1" {
		t.Fatal("UseExplicitOwnedBrowserMode")
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	start := browserReadCommand(t, decoder, "start")
	frontend := net.ParseIP(start.FrontendIP)
	if start.Run != local.RunID || frontend == nil || frontend.To4() == nil || frontend.IsLoopback() || frontend.IsUnspecified() || frontend.Equal(net.ParseIP(local.NodeIP)) {
		t.Fatal("BrowserFrontendBindingInvalid")
	}
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal("BrowserFrontendInterfaceUnavailable")
	}
	ownedFrontend := false
	for _, address := range interfaces {
		if network, ok := address.(*net.IPNet); ok && network.IP.Equal(frontend) {
			ownedFrontend = true
		}
	}
	if !ownedFrontend {
		t.Fatal("BrowserFrontendNotOwned")
	}

	base, _, _ := runtimeInputProcessFixture(t, "browser", "service")
	input := nativeProcessInput{Base: base, Kind: runtimefault.Service}
	endpoint, ca, capture := nativeCreationProxy(t, input)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	old := runNativeProcess(t, input, "initial")
	// CombinedOutput returned only after the old process was waited/reaped.
	// No listener, reader or browser bootstrap existed while that worker lived.
	created, posts, checks := capture.snapshot()
	if posts != 1 || checks != 2 || len(old.Checkpoint) == 0 || old.Write.ResultUID != created.UID || old.Write.ResultVersion != created.Version {
		t.Fatal("BrowserFailedWriterReceiptUnproven")
	}
	page := newBrowserPageFixture(t, input, old)
	listener, err := net.Listen("tcp4", net.JoinHostPort(start.FrontendIP, "8080"))
	if err != nil || listener.Addr().String() != net.JoinHostPort(start.FrontendIP, "8080") {
		t.Fatal("BrowserExactFrontendListenFailed")
	}
	server := &http.Server{Handler: page.handler, ErrorLog: log.New(io.Discard, "", 0), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			t.Error("BrowserServerShutdownFailed")
		}
	}()
	origin := "http://" + net.JoinHostPort(start.FrontendIP, "8080")
	browserEmit(t, map[string]any{"phase": "ready", "run": local.RunID, "origin": origin, "target": page.target, "cookie": page.cookie, "writerExited": true})
	browserReadCommand(t, decoder, "revoke")
	before := page.source.reads.Load()
	// Four explicit authorized manual loads; anonymous/foreign/logout reads
	// must never reach this source and there is no automatic polling.
	if before != 4 || page.privacyViolation.Load() {
		t.Fatal("BrowserBeforeRevocationEvidenceInvalid")
	}
	page.revoke(t)
	browserEmit(t, map[string]any{"phase": "revoked", "sourceReads": before})
	browserReadCommand(t, decoder, "finish")
	if page.source.reads.Load() != before || page.privacyViolation.Load() {
		t.Fatal("BrowserRevokedReadReachedProtectedSource")
	}
	_, posts, _ = capture.snapshot()
	if posts != 1 {
		t.Fatal("BrowserReadRepostedManagedResource")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if server.Shutdown(ctx) != nil {
		t.Fatal("BrowserServerShutdownFailed")
	}
	if err = <-done; err != http.ErrServerClosed {
		t.Fatal("BrowserServerUnexpectedExit")
	}
	// No raw cookie/pin/failure/provider data survives this public receipt.
	browserEmit(t, map[string]any{"phase": "finished", "sourceReads": before, "managedPosts": posts, "writerExited": true, "serverClosed": true})
}
