//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	corev1 "k8s.io/api/core/v1"
)

const runtimeProject = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: runtime-example
spec:
  source:
    repo: https://github.com/temporal-sa/fixture
  builds:
    app:
      context: .
      dockerfile: Dockerfile
  components:
    app:
      build: app
      ports:
        - containerPort: 8080
`

type runtimePin struct {
	Control ownership.Config
	Parent  ownership.Record
	Cell    ownership.NamespacedRecord
	Subject rollout.Subject
}
type runtimeChildInput struct {
	Pin                runtimePin
	Endpoint           string
	ProxyCA            []byte
	Checkpoint         []byte
	EnsureOnly         bool
	WantCode           runtimeconfig.Code
	WrongOwner         bool
	OpenMustFail       bool
	AllowChanged       bool
	HoldBeforeDispatch bool
	RecoverClose       bool
}
type runtimeChildOutput struct {
	Checkpoint []byte
	Receipt    runtimeconfig.Receipt
	ErrorCode  runtimeconfig.Code
}

func runtimeControlPin(t *testing.T, suffix string) runtimePin {
	t.Helper()
	if !boundaryVerified {
		TestOwnedAPIAndNetworkBoundary(t)
	}
	config := ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: local.RunID, ProjectUID: "control-runtime-" + suffix, Lifetime: ownership.Disposable, RunID: local.RunID}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}}
	base, err := ownership.NewContext(config)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := base.Intent(ownership.Kubernetes, "namespace", "control", "control-v1", "bootstrap-"+suffix, "control-namespace-spec", 63)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "control-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	annotations := provider.CreationAnnotations(intent, writer)
	annotations[provider.AttemptAnnotation] = intent.OperationID
	created := request(t, "POST", "/api/v1/namespaces", object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": intent.Name, "annotations": annotations, "finalizers": []string{provider.NamespaceLifecycleFinalizer}}}, 201)
	parent := ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: text(metadata(created), "uid")}, Owner: intent.Owner, CreationOperationID: intent.OperationID, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1}
	adapter, _ := typedClient(t)
	bound, err := adapter.BindNamespace(context.Background(), base, parent, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	subject := rollout.Subject{Environment: "lab", Installation: local.RunID, ProjectUID: "project-runtime-" + suffix}
	initial, err := journal.Initial(subject, 1)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := kubejournal.BootstrapObject(intent.Name, initial)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := provider.ConfigMapFingerprint(desired)
	childIntent, err := bound.Context().Intent("configmap", desired.Name, kubejournal.Revision, "bootstrap-cell-"+suffix, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desired, Intent: childIntent, Writer: writer})
	if err != nil {
		t.Fatal(err)
	}
	return runtimePin{Control: config, Parent: parent, Cell: cell, Subject: subject}
}
func runtimeCell(t *testing.T, pin runtimePin, adapter *provider.Client) *kubejournal.Cell {
	t.Helper()
	base, err := ownership.NewContext(pin.Control)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := adapter.BindNamespace(context.Background(), base, pin.Parent, pin.Parent.CreationOperationID)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := kubejournal.New(kubejournal.Config{Namespace: bound, Pinned: pin.Cell, Subject: pin.Subject, Writer: func() ownership.Writer {
		return ownership.Writer{ID: "control-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	}})
	if err != nil {
		t.Fatal(err)
	}
	return cell
}
func desiredRuntime(pin runtimePin) rollout.Desired {
	return rollout.Desired{Subject: pin.Subject, Generation: 1, Revision: "git-abcdef1", ReconcileKey: "complete-fixture-config-v1", RequiredBuilds: []string{"app"}, Images: map[string]string{"app": local.Image}}
}
func runtimeAttempt(pin runtimePin) string { return "accepted-" + pin.Subject.ProjectUID }
func runtimeChild(t *testing.T) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input runtimeChildInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	adapter, options := typedClient(t)
	if input.Endpoint != "" {
		options.CallTimeout = 10 * time.Second
		certificate, _ := base64.StdEncoding.DecodeString(local.Certificate)
		key, _ := base64.StdEncoding.DecodeString(local.Key)
		var err error
		options.Credentials, err = provider.NewCredentials(input.ProxyCA, certificate, key)
		if err != nil {
			t.Fatal(err)
		}
		options.Endpoint = input.Endpoint
		adapter, err = provider.New(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
	}
	cell := runtimeCell(t, input.Pin, adapter)
	owner := ownership.Owner{EnvironmentID: input.Pin.Subject.Environment, InstallationID: input.Pin.Subject.Installation, ProjectUID: input.Pin.Subject.ProjectUID, Lifetime: ownership.Disposable, RunID: local.RunID}
	if input.WrongOwner {
		owner.ProjectUID = "another-project"
	}
	base, err := ownership.NewContext(ownership.Config{Owner: owner, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}})
	if err != nil {
		t.Fatal(err)
	}
	config := runtimeconfig.Config{Mode: runtimeconfig.LocalCleanHandoff, Cell: cell, Project: base, Client: adapter, Writer: func() ownership.Writer {
		return ownership.Writer{ID: "runtime-writer", Owner: owner, Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	}}
	var worker *runtimeconfig.Worker
	if os.Getenv("OPERATOR_RUNTIME_CHILD") == "initial" {
		worker, err = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		var checkpoint runtimeconfig.Checkpoint
		checkpoint, err = runtimeconfig.ParseCheckpoint(input.Checkpoint)
		if err == nil {
			worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
		}
	}
	var output runtimeChildOutput
	if input.OpenMustFail && err == nil {
		t.Fatal("OpenClean admitted altered/missing/unresolved pinned state before replay")
	}
	if err == nil && input.HoldBeforeDispatch {
		var release struct{ Exit bool }
		if readErr := decoder.Decode(&release); readErr != nil || !release.Exit {
			t.Fatalf("missing explicit supervisor release: %v", readErr)
		}
	}
	if err == nil {
		var session *runtimeconfig.Session
		session, err = worker.EnsureNamespace(context.Background(), desiredRuntime(input.Pin), runtimeAttempt(input.Pin))
		if err == nil && !input.EnsureOnly {
			document, decodeErr := project.Decode([]byte(runtimeProject))
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			desired := desiredRuntime(input.Pin)
			bundle, renderErr := renderer.RenderCandidate(document, renderer.Bindings{Namespace: session.Context(), Revision: desired.Revision, OperationID: runtimeAttempt(input.Pin), Images: desired.Images})
			if renderErr != nil {
				t.Fatal(renderErr)
			}
			var found bool
			for _, object := range bundle.Objects {
				if object.Object.GetName() == "platform-project-metadata" {
					found = true
					output.Receipt, err = session.ApplyConfigMap(context.Background(), object)
					break
				}
			}
			if !found {
				t.Fatal("real renderer metadata ConfigMap missing")
			}
			if err == nil {
				repository, repositoryErr := journal.New(cell, input.Pin.Subject, input.Pin.Cell.Record.Ref.ProviderID)
				if repositoryErr != nil {
					t.Fatal(repositoryErr)
				}
				stored, readErr := repository.Load(context.Background())
				if readErr != nil || stored.Version != output.Receipt.JournalVersion {
					t.Fatalf("receipt preceded durable journal acknowledgement: %v", readErr)
				}
				parentCommitted, childCommitted := false, false
				for _, write := range stored.Envelope.Writes {
					if write.State != journal.Committed {
						continue
					}
					if write.Preconditions.Kind == "Namespace" && write.ResultUID == output.Receipt.Record.Parent.UID {
						parentCommitted = true
					}
					if write.Preconditions.Kind == "ConfigMap" && write.ResultUID == output.Receipt.Record.Record.Ref.ProviderID && write.ResultVersion == output.Receipt.ResourceVersion {
						childCommitted = true
					}
				}
				if !parentCommitted || !childCommitted {
					t.Fatal("receipt preceded durable canonical parent/child UID result")
				}
			}
		}
		if err == nil {
			checkpoint, closeErr := worker.Quiesce(context.Background())
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			output.Checkpoint, err = checkpoint.JSON()
			// A closed worker cannot dispatch even with the same previously accepted input.
			_, closedErr := worker.EnsureNamespace(context.Background(), desiredRuntime(input.Pin), runtimeAttempt(input.Pin))
			var typed *runtimeconfig.Error
			if !errors.As(closedErr, &typed) || typed.Code != runtimeconfig.Closed {
				t.Fatalf("quiesced worker reopened: %v", closedErr)
			}
		} else {
			var failure *runtimeconfig.Error
			checkpoint, closeErr := worker.Quiesce(context.Background())
			if closeErr == nil && errors.As(err, &failure) && (failure.Code == runtimeconfig.Uncertain || failure.Code == runtimeconfig.Blocked) {
				if data, _ := checkpoint.JSON(); len(data) > 0 {
					t.Fatal("uncertain worker issued clean checkpoint")
				}
			}
			if input.RecoverClose {
				if closeErr == nil {
					t.Fatal("fault fixture did not close through uncertainty")
				}
				fmt.Println("RUNTIME-CLOSED")
				var release struct{ Exit bool }
				if readErr := decoder.Decode(&release); readErr != nil || !release.Exit {
					t.Fatalf("missing recovery supervisor release: %v", readErr)
				}
				checkpoint, recoveryErr := worker.RecoverQuiesce(context.Background())
				if recoveryErr != nil {
					t.Fatal(recoveryErr)
				}
				output.Checkpoint, recoveryErr = checkpoint.JSON()
				if recoveryErr != nil {
					t.Fatal(recoveryErr)
				}
				_, closedErr := worker.EnsureNamespace(context.Background(), desiredRuntime(input.Pin), runtimeAttempt(input.Pin))
				var typed *runtimeconfig.Error
				if !errors.As(closedErr, &typed) || typed.Code != runtimeconfig.Closed {
					t.Fatal("closed recovery reopened runtime dispatch")
				}
			}
		}
	}
	if input.WantCode != "" {
		var failure *runtimeconfig.Error
		if !errors.As(err, &failure) || failure.Code != input.WantCode {
			t.Fatalf("wanted %s, got %v", input.WantCode, err)
		}
		output.ErrorCode = failure.Code
	} else if err != nil {
		var failure *runtimeconfig.Error
		if input.AllowChanged && errors.As(err, &failure) && failure.Code == runtimeconfig.Changed {
			output.ErrorCode = failure.Code
		} else {
			t.Fatal(err)
		}
	}
	data, _ := json.Marshal(output)
	fmt.Println("RUNTIME-CHILD " + string(data))
}
func runRuntimeChild(t *testing.T, input runtimeChildInput, initial bool) runtimeChildOutput {
	t.Helper()
	encoded, _ := json.Marshal(input)
	command := exec.Command(os.Args[0], "-test.run=^TestRuntimeConfigCleanProcessHandoff$", "-test.v", "-test.timeout=90s")
	role := "resume"
	if initial {
		role = "initial"
	}
	command.Env = []string{"OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, "OPERATOR_RUNTIME_CHILD=" + role}
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.CombinedOutput()
	if err != nil || command.ProcessState == nil || !command.ProcessState.Exited() || !command.ProcessState.Success() {
		t.Fatalf("owned worker process did not exit cleanly: %v\n%s", err, output)
	}
	// CombinedOutput has waited/reaped this exact child. The caller cannot start
	// the next worker before this return; no old process remains to write.
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "RUNTIME-CHILD ") {
			var result runtimeChildOutput
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RUNTIME-CHILD ")), &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
	}
	t.Fatalf("missing clean worker result: %s", output)
	return runtimeChildOutput{}
}

type runtimeProxy struct {
	endpoint               string
	ca                     []byte
	namespaces, configmaps atomic.Int64
	loseConfigMap          atomic.Bool
	mu                     sync.Mutex
	failures               []string
	claimSequence          atomic.Uint64
	claims                 atomic.Int64
	loseClaim              atomic.Bool
	loseResult             atomic.Bool
	blockClaimRead         atomic.Bool
	claimArrivals          chan struct{}
	releaseClaims          chan struct{}
}

func runtimeJournalProxy(t *testing.T, pin runtimePin) *runtimeProxy {
	t.Helper()
	proof := &runtimeProxy{}
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	cellPath := "/api/v1/namespaces/" + pin.Parent.Ref.Name + "/configmaps/" + pin.Cell.Record.Ref.Name
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == cellPath && proof.blockClaimRead.Load() {
			http.Error(w, "synthetic lost admission acknowledgement", 502)
			return
		}
		if r.Method == "PUT" && r.URL.Path == cellPath && (proof.claimSequence.Load() != 0 || proof.loseResult.Load()) {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, "fixture body read failed", 500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var object corev1.ConfigMap
			if json.Unmarshal(body, &object) == nil {
				envelope, decodeErr := journal.Decode([]byte(object.Data[kubejournal.DataKey]))
				resultTarget := false
				if decodeErr == nil && proof.loseResult.Load() {
					for _, write := range envelope.Writes {
						if write.Preconditions.Kind == "ConfigMap" && write.Preconditions.Action == "Create" && write.Preconditions.Name == "platform-project-metadata" && write.State == journal.Committed {
							resultTarget = true
						}
					}
				}
				if decodeErr == nil && (envelope.Sequence == proof.claimSequence.Load() || resultTarget) && envelope.Admission != nil && envelope.Admission.State == journal.AdmissionActive {
					proof.claims.Add(1)
					proof.mu.Lock()
					arrivals, release := proof.claimArrivals, proof.releaseClaims
					proof.mu.Unlock()
					if arrivals != nil {
						arrivals <- struct{}{}
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					}
					if proof.loseClaim.Load() || resultTarget {
						outbound := r.Clone(r.Context())
						outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
						outbound.Host = target.Host
						outbound.RequestURI = ""
						response, upstreamErr := client.Transport.RoundTrip(outbound)
						if upstreamErr != nil {
							http.Error(w, "upstream admission failed", 502)
							return
						}
						_, _ = io.Copy(io.Discard, response.Body)
						response.Body.Close()
						if response.StatusCode != 200 {
							http.Error(w, "upstream admission rejected", response.StatusCode)
							return
						}
						proof.blockClaimRead.Store(true)
						connection, _, hijackErr := w.(http.Hijacker).Hijack()
						if hijackErr == nil {
							connection.Close()
						}
						return
					}
				}
			}
		}
		if r.Method != "POST" {
			forward.ServeHTTP(w, r)
			return
		}
		kind := "ConfigMap"
		if r.URL.Path == "/api/v1/namespaces" {
			kind = "Namespace"
			proof.namespaces.Add(1)
		} else {
			proof.configmaps.Add(1)
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err == nil {
			err = provePendingRuntimePost(pin, cellPath, kind, body)
		}
		if err != nil {
			proof.mu.Lock()
			proof.failures = append(proof.failures, err.Error())
			proof.mu.Unlock()
			http.Error(w, "durable dispatch precondition missing", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if kind == "ConfigMap" && proof.loseConfigMap.Load() {
			outbound := r.Clone(r.Context())
			outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
			outbound.Host = target.Host
			outbound.RequestURI = ""
			response, err := client.Transport.RoundTrip(outbound)
			if err != nil {
				http.Error(w, "upstream failed", 502)
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode != 201 {
				http.Error(w, "upstream did not create", response.StatusCode)
				return
			}
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				connection.Close()
			}
			return
		}
		forward.ServeHTTP(w, r)
	}))
	ca, _ := base64.StdEncoding.DecodeString(local.CA)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	t.Cleanup(server.Close)
	proof.endpoint = server.URL
	proof.ca = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	return proof
}
func provePendingRuntimePost(pin runtimePin, path, kind string, body []byte) error {
	response, err := client.Get(local.Server + path)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var cell corev1.ConfigMap
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&cell) != nil || string(cell.UID) != pin.Cell.Record.Ref.ProviderID {
		return fmt.Errorf("pinned journal not independently readable")
	}
	envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
	if err != nil {
		return err
	}
	if envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive {
		return fmt.Errorf("project dispatch occurred before active admission")
	}
	var object struct {
		Metadata struct {
			Name, Namespace string
			Annotations     map[string]string
		}
	}
	if json.Unmarshal(body, &object) != nil {
		return fmt.Errorf("invalid posted object")
	}
	var actualOwner ownership.Owner
	if json.Unmarshal([]byte(object.Metadata.Annotations[provider.OwnerAnnotation]), &actualOwner) != nil || actualOwner.EnvironmentID != pin.Subject.Environment || actualOwner.InstallationID != pin.Subject.Installation || actualOwner.ProjectUID != pin.Subject.ProjectUID {
		return fmt.Errorf("dispatch owner did not match actual project subject")
	}
	for _, write := range envelope.Writes {
		p := write.Preconditions
		if write.State == journal.Pending && p.Kind == kind && p.Action == "Create" && p.Name == object.Metadata.Name && p.Fingerprint == object.Metadata.Annotations[provider.FingerprintAnnotation] && p.TargetRevision == object.Metadata.Annotations[provider.RevisionAnnotation] && p.ProviderOperationID == object.Metadata.Annotations[provider.OperationAnnotation] && (kind == "Namespace" || p.ParentUID == object.Metadata.Annotations[ownership.ParentNamespaceUIDAnnotation]) {
			return nil
		}
	}
	return fmt.Errorf("exact pending %s intent was not durable before POST", kind)
}
func requireRuntimeProxy(t *testing.T, proof *runtimeProxy, namespaces, configmaps int64) {
	t.Helper()
	proof.mu.Lock()
	defer proof.mu.Unlock()
	if len(proof.failures) > 0 || proof.namespaces.Load() != namespaces || proof.configmaps.Load() != configmaps {
		t.Fatalf("unexpected dispatch: namespace=%d configmap=%d evidence=%v", proof.namespaces.Load(), proof.configmaps.Load(), proof.failures)
	}
}

func TestRuntimeConfigCleanProcessHandoff(t *testing.T) {
	if os.Getenv("OPERATOR_RUNTIME_CHILD") != "" {
		runtimeChild(t)
		return
	}
	pin := runtimeControlPin(t, "restart")
	proof := runtimeJournalProxy(t, pin)
	input := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca}
	first := runRuntimeChild(t, input, true)
	if len(first.Checkpoint) == 0 || first.Receipt.Record.Record.Ref.ProviderID == "" || first.Receipt.Record.Parent.UID == pin.Parent.Ref.ProviderID {
		t.Fatal("missing canonical result or independent control namespace")
	}
	requireRuntimeProxy(t, proof, 1, 1)
	input.Checkpoint = first.Checkpoint
	second := runRuntimeChild(t, input, false)
	if second.Receipt.Record != first.Receipt.Record || second.Receipt.ResourceVersion != first.Receipt.ResourceVersion {
		t.Fatal("clean restart changed canonical child/parent/provenance")
	}
	requireRuntimeProxy(t, proof, 1, 1)
	// Missing canonical child is never a reason to POST another one on restart.
	path := "/api/v1/namespaces/" + first.Receipt.Record.Parent.Name + "/configmaps/" + first.Receipt.Record.Record.Ref.Name
	before := request(t, "GET", path, nil, 200)
	originalBody := field(before, "data")["project.json"]
	// Preserve the copied fingerprint/operation annotations while tampering with
	// the same canonical UID. OpenClean must hash actual contents and reject
	// before returning a Worker, even before ApplyConfigMap replay is reached.
	field(before, "data")["project.json"] = "tampered-body-with-copied-provenance"
	request(t, "PUT", path, before, 200)
	input.Checkpoint = second.Checkpoint
	input.WantCode = runtimeconfig.Changed
	input.OpenMustFail = true
	runRuntimeChild(t, input, false)
	requireRuntimeProxy(t, proof, 1, 1)
	request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": first.Receipt.Record.Record.Ref.ProviderID}}, 200)
	input.Checkpoint = second.Checkpoint
	input.WantCode = runtimeconfig.Changed
	runRuntimeChild(t, input, false)
	requireRuntimeProxy(t, proof, 1, 1)
	for _, key := range []string{"uid", "resourceVersion", "creationTimestamp", "managedFields"} {
		delete(metadata(before), key)
	}
	field(before, "data")["project.json"] = originalBody
	replacement := request(t, "POST", "/api/v1/namespaces/"+first.Receipt.Record.Parent.Name+"/configmaps", before, 201)
	if text(metadata(replacement), "uid") == first.Receipt.Record.Record.Ref.ProviderID {
		t.Fatal("replacement fixture did not change canonical UID")
	}
	runRuntimeChild(t, input, false)
	requireRuntimeProxy(t, proof, 1, 1)
}
func TestRuntimeConfigDurableUnknownBlocksFreshClientAndCleanCheckpoint(t *testing.T) {
	pin := runtimeControlPin(t, "unknown")
	proof := runtimeJournalProxy(t, pin)
	input := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, EnsureOnly: true}
	first := runRuntimeChild(t, input, true)
	input.Checkpoint = first.Checkpoint
	input.EnsureOnly = false
	input.WantCode = runtimeconfig.Uncertain
	proof.loseConfigMap.Store(true)
	uncertain := runRuntimeChild(t, input, false)
	if len(uncertain.Checkpoint) != 0 {
		t.Fatal("unknown write issued clean handoff")
	}
	// Delayed/lost response has already committed the real child. Reading it with
	// a new client cannot erase the independently durable Unknown write.
	input.WantCode = runtimeconfig.Blocked
	input.OpenMustFail = true
	runRuntimeChild(t, input, false)
	requireRuntimeProxy(t, proof, 1, 1)
	input.Checkpoint = nil
	input.WantCode = runtimeconfig.Invalid
	runRuntimeChild(t, input, false)
	requireRuntimeProxy(t, proof, 1, 1)
}
func TestRuntimeConfigRejectsProjectOwnerDifferentFromJournalSubject(t *testing.T) {
	pin := runtimeControlPin(t, "foreign")
	proof := runtimeJournalProxy(t, pin)
	input := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, WrongOwner: true, WantCode: runtimeconfig.SubjectMismatch}
	runRuntimeChild(t, input, true)
	requireRuntimeProxy(t, proof, 0, 0)
}
