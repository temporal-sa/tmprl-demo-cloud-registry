//go:build integration

package integration

// Shared owned private-store/process scaffolding only. Resource verification
// remains explicit in each typed fixture. A successful old child is always
// waited/reaped before the parent can spawn a clean successor.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

type privateRuntimeProcessInput struct {
	Pin              runtimePin
	Endpoint         string
	ProxyCA          []byte
	Checkpoint       []byte
	Directory        string
	StoreConfig      runtimeinputs.Config
	StorePin         runtimeinputs.Pin
	WantOpenError    runtimeconfig.Code
	WithoutInputs    bool
	ChangedAuthoring bool
}

func openRuntimeInputs(t *testing.T, input privateRuntimeProcessInput) (*runtimeinputs.Store, *os.Root) {
	t.Helper()
	root, err := os.OpenRoot(input.Directory)
	if err != nil {
		t.Fatal("owned private input root unavailable")
	}
	store, err := runtimeinputs.Open(context.Background(), root, input.StorePin, input.StoreConfig)
	if err != nil {
		root.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	return store, root
}

func runtimeInputProcessFixture(t *testing.T, family, suffix string) (privateRuntimeProcessInput, *runtimeinputs.Store, *os.Root) {
	t.Helper()
	return runtimeInputProcessFixtureWithCapacity(t, family, suffix, 8)
}
func runtimeInputProcessFixtureWithCapacity(t *testing.T, family, suffix string, capacity int) (privateRuntimeProcessInput, *runtimeinputs.Store, *os.Root) {
	t.Helper()
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	pin := runtimeControlPin(t, family+"-runtime-"+suffix)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	owner := ownership.Owner{EnvironmentID: pin.Subject.Environment, InstallationID: pin.Subject.Installation, ProjectUID: pin.Subject.ProjectUID, Lifetime: ownership.Disposable, RunID: local.RunID}
	config := runtimeinputs.Config{BootstrapUID: hex.EncodeToString(nonce[:]), Owner: owner, CellUID: pin.Cell.Record.Ref.ProviderID, MaximumEntries: capacity}
	store, binding, err := runtimeinputs.Bootstrap(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	return privateRuntimeProcessInput{Pin: pin, Directory: directory, StoreConfig: config, StorePin: binding}, store, root
}

func runPrivateRuntimeProcess(t *testing.T, testName, phaseVariable, prefix, phase string, input, result any, forbidden ...string) {
	t.Helper()
	data, err := json.Marshal(input)
	if err != nil || len(data) > 32768 {
		t.Fatal("owned process input exceeds its contract")
	}
	command := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v", "-test.timeout=100s")
	command.Env = []string{"PATH=/unavailable", "HOME=/tmp/owned-private-runtime", "TMPDIR=/tmp", "OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, phaseVariable + "=" + phase}
	command.Stdin = bytes.NewReader(data)
	output, runErr := command.CombinedOutput()
	// Check before reporting even a failed child's output.
	for _, marker := range forbidden {
		if marker != "" && bytes.Contains(output, []byte(marker)) {
			t.Fatal("private payload escaped owned process DTO")
		}
	}
	if len(output) > 1<<20 {
		t.Fatal("owned process output exceeds its contract")
	}
	if runErr != nil {
		t.Fatalf("owned process %s: %v\n%s", phase, runErr, output)
	}
	// CombinedOutput synchronously waits/reaps. A checkpoint is not an RPC fence.
	count := 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, prefix) {
			count++
			if json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), result) != nil {
				t.Fatal("invalid safe process receipt")
			}
		}
	}
	if count != 1 {
		t.Fatal("owned process must exit with exactly one bounded result")
	}
}

func startRuntimeDispatchProxy(t *testing.T, handler http.Handler) (string, []byte) {
	t.Helper()
	ca, err := base64.StdEncoding.DecodeString(local.CA)
	pool := x509.NewCertPool()
	if err != nil || !pool.AppendCertsFromPEM(ca) {
		t.Fatal("owned fixture CA unproven")
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
}
