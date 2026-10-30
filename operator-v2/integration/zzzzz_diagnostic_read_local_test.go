//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	c "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnosticcli"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

type diagnosticCLIBuffer struct {
	bytes.Buffer
	limit int
}

func (b *diagnosticCLIBuffer) Write(data []byte) (int, error) {
	if len(data) > b.limit-b.Len() {
		return 0, io.ErrShortBuffer
	}
	return b.Buffer.Write(data)
}

func executeDiagnosticCLI(t *testing.T, bootstrap diagnosticcli.Bootstrap, asJSON, denied bool) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	args := []string{"read-local"}
	if asJSON {
		args = append(args, "--json")
	}
	command := exec.CommandContext(ctx, "/registry-diagnostics", args...)
	command.Env = []string{"PATH=/unavailable", "HOME=/tmp/owned-diagnostic-cli"}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal("DiagnosticCLIPipeUnavailable")
	}
	command.ExtraFiles = []*os.File{read}
	stdout, stderr := &diagnosticCLIBuffer{limit: c.MaxResponseBytes}, &diagnosticCLIBuffer{limit: 1024}
	command.Stdout, command.Stderr = stdout, stderr
	if err = command.Start(); err != nil {
		read.Close()
		write.Close()
		t.Fatal("DiagnosticCLIStartFailed")
	}
	// START before writing potentially >8KiB; close the parent's read handle,
	// bound writes, then EOF and actual Wait/reap before any next process.
	read.Close()
	deadlineErr := write.SetWriteDeadline(time.Now().Add(5 * time.Second))
	var writeErr error
	if deadlineErr == nil {
		writeErr = bootstrap.WritePrivate(write)
	}
	closeErr := write.Close()
	waitErr := command.Wait()
	if deadlineErr != nil || writeErr != nil || closeErr != nil || command.ProcessState == nil || !command.ProcessState.Exited() {
		t.Fatal("DiagnosticCLIPipeOrWaitUnconfirmed")
	}
	if denied {
		if waitErr == nil || stdout.Len() != 0 || strings.TrimSpace(stderr.String()) != diagnosticcli.PrivilegeNotice+"\nLocalDiagnosticAccessDenied" {
			t.Fatal("DiagnosticCLIDenialLeakedOrSucceeded")
		}
	} else if waitErr != nil || strings.TrimSpace(stderr.String()) != diagnosticcli.PrivilegeNotice {
		t.Fatal("DiagnosticCLIReadFailed")
	}
	return append([]byte(nil), stdout.Bytes()...)
}

func TestPrivilegedLocalDiagnosticCommandReadsAndRevokes(t *testing.T) {
	TestOwnedAPIAndNetworkBoundary(t)
	if os.Getenv("OPERATOR_LOCAL_DIAGNOSTIC_CLI") != "1" {
		t.Fatal("UseExplicitOwnedDiagnosticCLIMode")
	}
	proofBytes, err := os.ReadFile("/diagnostic-cli-proof.json")
	var proof map[string]string
	if err != nil || len(proofBytes) > 4096 || json.Unmarshal(proofBytes, &proof) != nil || len(proof) != 3 || len(proof["compilerInputSHA256"]) != 64 {
		t.Fatal("DiagnosticCLIArtifactProofUnavailable")
	}
	binary, err := os.ReadFile("/registry-diagnostics")
	if err != nil || len(binary) > 128*1024*1024 {
		t.Fatal("DiagnosticCLIExecutableUnavailable")
	}
	digest := sha256.Sum256(binary)
	if hex.EncodeToString(digest[:]) != proof["commandELFSHA256"] {
		t.Fatal("DiagnosticCLIExecutableChanged")
	}
	base, _, _ := runtimeInputProcessFixture(t, "diagnostic-cli", "service")
	input := nativeProcessInput{Base: base, Kind: runtimefault.Service}
	endpoint, ca, capture := nativeCreationProxy(t, input)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	old := runNativeProcess(t, input, "initial") // Native writer actually exited/reaped.
	created, posts, checks := capture.snapshot()
	if posts != 1 || checks != 2 || old.Write.ResultUID != created.UID || old.Write.ResultVersion != created.Version {
		t.Fatal("DiagnosticCLICanonicalWriterUnproven")
	}
	diagnostic := diagnosticProcessInput{Base: input.Base}
	bound := diagnosticControlBinding(t, diagnostic)
	target := diagnosticTarget(diagnostic)
	desiredRecord := bootstrapDiagnosticDesired(t, diagnostic)
	now := time.Now().UTC().Truncate(time.Second)
	policy := c.ProjectReadPolicy{SchemaVersion: 1, Target: target, Issuer: "https://owned-catalog.example.test", IssuedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Subjects: []c.PolicySubject{{Subject: "google:owned-local-operator", Allowed: true, AuthenticatedAtOrAfter: now.Add(-time.Minute)}}}
	object, err := c.PolicyBootstrapObject(bound.Context().NamespaceName(), policy)
	if err != nil {
		t.Fatal("DiagnosticCLIPolicyInvalid")
	}
	fingerprint, err := provider.ConfigMapFingerprint(object)
	if err != nil {
		t.Fatal("DiagnosticCLIPolicyInvalid")
	}
	intent, err := bound.Context().Intent("configmap", object.Name, c.PolicyRevision, "diagnostic-cli-policy", fingerprint)
	if err != nil {
		t.Fatal("DiagnosticCLIPolicyInvalid")
	}
	writer := ownership.Writer{ID: "control-writer", Owner: bound.Context().Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	policyRecord, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: object, Intent: intent, Writer: writer})
	if err != nil {
		t.Fatal("DiagnosticCLIPolicyBootstrapFailed")
	}
	_, repository := retirementConfig(t, input.Base.Pin, "", nil)
	before, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal("DiagnosticCLIJournalEvidenceUnavailable")
	}
	desiredBefore, err := bound.ObserveConfigMap(context.Background(), desiredRecord)
	if err != nil {
		t.Fatal("DiagnosticCLIDesiredEvidenceUnavailable")
	}
	journalPath := "/api/v1/namespaces/" + bound.Context().NamespaceName() + "/configmaps/" + input.Base.Pin.Cell.Record.Ref.Name
	allowed := map[string]bool{"/api/v1/namespaces/kube-system": true, "/api/v1/namespaces/" + bound.Context().NamespaceName(): true, journalPath: true, "/api/v1/namespaces/" + bound.Context().NamespaceName() + "/configmaps/" + desiredRecord.Record.Ref.Name: true, "/api/v1/namespaces/" + bound.Context().NamespaceName() + "/configmaps/" + policyRecord.Record.Ref.Name: true}
	var journalReads, deniedCalls atomic.Int64
	upstream, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(upstream)
	forward.Transport = client.Transport
	forward.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(http.StatusServiceUnavailable) }
	readEndpoint, readCA := startRuntimeDispatchProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !allowed[r.URL.Path] || r.URL.RawPath != "" || r.URL.ForceQuery || r.URL.RawQuery != "" && r.URL.RawQuery != "timeout=5s" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			deniedCalls.Add(1)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == journalPath {
			journalReads.Add(1)
		}
		forward.ServeHTTP(w, r)
	}))
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal("DiagnosticCLISyntheticKeyUnavailable")
	}
	defer clear(key)
	claims, _ := json.Marshal(map[string]any{"typ": "session", "iss": policy.Issuer, "aud": "owned-catalog", "sub": "google:owned-local-operator", "email": "fixture@example.test", "hd": "example.test", "iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	encode := base64.RawURLEncoding.EncodeToString
	signed := encode([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(signed))
	cookie := signed + "." + encode(mac.Sum(nil))
	certificate, _ := base64.StdEncoding.DecodeString(local.Certificate)
	privateKey, _ := base64.StdEncoding.DecodeString(local.Key)
	secrets, err := diagnosticcli.NewSecretInputs(bytes.Repeat(readCA, 16), certificate, privateKey, key, cookie)
	clear(privateKey)
	if err != nil {
		t.Fatal("DiagnosticCLIPrivateBootstrapInvalid")
	}
	defer secrets.Close()
	pin := input.Base.Pin
	bootstrap, err := diagnosticcli.NewBootstrap(diagnosticcli.Metadata{Mode: diagnosticcli.LocalMode, Target: target, Owner: pin.Control.Owner, ResourceRoot: pin.Control.ResourceRoot, Cluster: provider.ClusterIdentity{ID: local.RunID, KubeSystemUID: local.NamespaceUID}, Endpoint: readEndpoint, ControlNamespace: pin.Parent, ControlAttempt: pin.Parent.CreationOperationID, Journal: pin.Cell, Desired: desiredRecord, Policy: policyRecord, Issuer: policy.Issuer, Audience: "owned-catalog", AllowedDomain: "example.test", SessionSeconds: 3600}, secrets)
	if err != nil {
		t.Fatal("DiagnosticCLIPrivateBootstrapInvalid")
	}
	defer bootstrap.Close()
	human := executeDiagnosticCLI(t, bootstrap, false, false)
	for _, text := range []string{diagnosticcli.PrivilegeNotice, "The Kubernetes API denied access.", "The Kubernetes API reported the requested resource was not found.", "Namespace: CanonicalReceiptRecorded (1)", "Service: CanonicalReceiptRecorded (1)", "not current existence, readiness or serving", "Automatic retry: Unknown"} {
		if !bytes.Contains(human, []byte(text)) {
			t.Fatal("DiagnosticCLIHandoffHumanFactsMissing")
		}
	}
	wire := executeDiagnosticCLI(t, bootstrap, true, false)
	var view c.View
	if json.Unmarshal(wire, &view) != nil || view.Target != target || view.Operation == nil || view.Operation.Failure == nil || !canonicalFailureWireEqual(*view.Operation.Failure, old.Primary) || view.Operation.Availability.RouteState != diagnostics.RouteUnconfirmed || view.Operation.Retry.Status != diagnostics.RetryUnknown || view.CreateHistory == nil || !reflect.DeepEqual(view.CreateHistory.Groups, []c.CreateHistoryGroup{{Kind: "Namespace", Outcome: "CanonicalReceiptRecorded", Count: 1}, {Kind: "Service", Outcome: "CanonicalReceiptRecorded", Count: 1}}) || journalReads.Load() != 2 || deniedCalls.Load() != 0 {
		t.Fatal("DiagnosticCLIJSONCanonicalFactsUnproven")
	}
	for _, output := range [][]byte{human, wire} {
		if bytes.Contains(output, []byte(cookie)) || bytes.Contains(output, key) || bytes.Contains(output, []byte(input.Base.Directory)) {
			t.Fatal("DiagnosticCLIPrivateInputLeaked")
		}
	}
	observed, err := bound.ObserveConfigMap(context.Background(), policyRecord)
	if err != nil {
		t.Fatal("DiagnosticCLIRevokeReadFailed")
	}
	policy.Subjects[0].Allowed = false
	object, err = c.PolicyBootstrapObject(bound.Context().NamespaceName(), policy)
	if err != nil {
		t.Fatal("DiagnosticCLIRevokeInvalid")
	}
	fingerprint, _ = provider.ConfigMapFingerprint(object)
	intent, err = bound.Context().Intent("configmap", object.Name, c.PolicyRevision, "diagnostic-cli-revoke", fingerprint)
	if err != nil {
		t.Fatal("DiagnosticCLIRevokeInvalid")
	}
	current := policyRecord
	current.Record.SpecFingerprint = observed.Observation.Observation.SpecFingerprint
	current.Record.WriterEpoch = observed.Observation.Observation.WriterEpoch
	current.Record.Revision = observed.Observation.Observation.Revision
	if _, err = bound.UpdateConfigMap(context.Background(), provider.ConfigMapUpdate{ConfigMapMutation: provider.ConfigMapMutation{OperationID: intent.Intent.OperationID, Record: current, ExpectedVersion: observed.Object.ResourceVersion, Writer: writer}, Desired: object, Intent: intent}); err != nil {
		t.Fatal("DiagnosticCLIActualRevokeCASFailed")
	}
	executeDiagnosticCLI(t, bootstrap, true, true)
	after, err := repository.Load(context.Background())
	if err != nil || after.Version != before.Version || after.Envelope.Digest != before.Envelope.Digest || journalReads.Load() != 2 || deniedCalls.Load() != 0 {
		t.Fatal("DiagnosticCLIDenialTouchedOrChangedJournal")
	}
	desiredAfter, err := bound.ObserveConfigMap(context.Background(), desiredRecord)
	if err != nil || desiredAfter.Object.UID != desiredBefore.Object.UID || desiredAfter.Object.ResourceVersion != desiredBefore.Object.ResourceVersion || !reflect.DeepEqual(desiredAfter.Object.Data, desiredBefore.Object.Data) {
		t.Fatal("DiagnosticCLIDesiredEvidenceChanged")
	}
	_, posts, _ = capture.snapshot()
	if posts != 1 {
		t.Fatal("DiagnosticCLIReadRepeatedProviderWrite")
	}
}
