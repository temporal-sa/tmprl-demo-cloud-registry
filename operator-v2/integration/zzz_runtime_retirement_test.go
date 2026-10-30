//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	corev1 "k8s.io/api/core/v1"
)

type retirementChildInput struct {
	Pin        runtimePin
	Endpoint   string
	ProxyCA    []byte
	Checkpoint []byte
}

func retirementConfig(t *testing.T, pin runtimePin, endpoint string, ca []byte) (runtimeconfig.Config, *journal.Repository) {
	t.Helper()
	adapter, options := typedClient(t)
	if endpoint != "" {
		certificate, _ := base64.StdEncoding.DecodeString(local.Certificate)
		key, _ := base64.StdEncoding.DecodeString(local.Key)
		var err error
		options.Credentials, err = provider.NewCredentials(ca, certificate, key)
		if err != nil {
			t.Fatal(err)
		}
		options.Endpoint = endpoint
		options.CallTimeout = 5 * time.Second
		adapter, err = provider.New(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
	}
	cell := runtimeCell(t, pin, adapter)
	owner := ownership.Owner{EnvironmentID: pin.Subject.Environment, InstallationID: pin.Subject.Installation, ProjectUID: pin.Subject.ProjectUID, Lifetime: ownership.Disposable, RunID: local.RunID}
	base, err := ownership.NewContext(ownership.Config{Owner: owner, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}})
	if err != nil {
		t.Fatal(err)
	}
	config := runtimeconfig.Config{Mode: runtimeconfig.LocalCleanHandoff, Cell: cell, Project: base, Client: adapter, Writer: func() ownership.Writer {
		return ownership.Writer{ID: "runtime-writer", Owner: owner, Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	}}
	repository, err := journal.New(cell, pin.Subject, pin.Cell.Record.Ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	return config, repository
}

func failedRetirementProjection(t *testing.T, pin runtimePin) diagnostics.OperationProjection {
	t.Helper()
	id := diagnostics.Identity{Environment: pin.Subject.Environment, Installation: pin.Subject.Installation, ProjectUID: pin.Subject.ProjectUID, ProjectName: "runtime-example", OperationID: runtimeAttempt(pin), ReconcileKey: desiredRuntime(pin).ReconcileKey, Generation: 1}
	p, err := diagnostics.NewProjection(id)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	p, err = p.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Step: "smoke", Failure: &diagnostics.DomainFailure{ID: "original-retirement-smoke", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Candidate smoke failed", OccurredAt: at}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func retirementChild(t *testing.T, phase string) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input retirementChildInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	config, repository := retirementConfig(t, input.Pin, input.Endpoint, input.ProxyCA)
	var worker *runtimeconfig.Worker
	var err error
	if phase == "drain" {
		worker, err = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		checkpoint, decodeErr := runtimeconfig.ParseCheckpoint(input.Checkpoint)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
	}
	if err != nil {
		t.Fatal(err)
	}
	switch phase {
	case "drain":
		session, err := worker.EnsureNamespace(context.Background(), desiredRuntime(input.Pin), runtimeAttempt(input.Pin))
		if err != nil {
			t.Fatal(err)
		}
		document, err := project.Decode([]byte(runtimeProject))
		if err != nil {
			t.Fatal(err)
		}
		desired := desiredRuntime(input.Pin)
		bundle, err := renderer.RenderCandidate(document, renderer.Bindings{Namespace: session.Context(), Revision: desired.Revision, OperationID: runtimeAttempt(input.Pin), Images: desired.Images})
		if err != nil {
			t.Fatal(err)
		}
		applied := false
		for _, object := range bundle.Objects {
			if object.Object.GetName() == "platform-project-metadata" {
				if _, err := session.ApplyConfigMap(context.Background(), object); err != nil {
					t.Fatal(err)
				}
				applied = true
			}
		}
		if !applied {
			t.Fatal("real renderer metadata not applied")
		}
		if _, err := worker.BeginRetirement(context.Background(), runtimeconfig.RetirementRequest{AttemptID: runtimeAttempt(input.Pin), Reason: journal.DrainCandidateFailed, Projection: failedRetirementProjection(t, input.Pin)}); err != nil {
			t.Fatal(err)
		}
	case "retire":
		_, err := worker.EnsureNamespace(context.Background(), desiredRuntime(input.Pin), runtimeAttempt(input.Pin))
		var denied *runtimeconfig.Error
		if !errors.As(err, &denied) || denied.Code != runtimeconfig.Blocked {
			t.Fatalf("cleanup-only resume allowed normal work: %v", err)
		}
		snapshot, err := repository.Load(context.Background())
		if err != nil || snapshot.Envelope.Projection == nil {
			t.Fatal("recorded cleanup projection missing", err)
		}
		retirement, err := worker.BeginRetirement(context.Background(), runtimeconfig.RetirementRequest{AttemptID: runtimeAttempt(input.Pin), Reason: journal.DrainCandidateFailed, Projection: *snapshot.Envelope.Projection})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			progress, err := retirement.Advance(context.Background())
			if err != nil {
				var rejected *runtimeconfig.Error
				if !errors.As(err, &rejected) || rejected.Code != runtimeconfig.Rejected {
					saved, loadErr := repository.Load(context.Background())
					t.Fatalf("Advance failed: %v journalRead=%v attempts=%+v writes=%+v", err, loadErr, saved.Envelope.Attempts, saved.Envelope.Writes)
				}
				// A known rejected CAS may be explicitly retried by the actor;
				// Advance prepares and journals a distinct exact ticket each time.
			}
			if progress.Consumed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("owned namespace did not reach canonical absence")
			}
			time.Sleep(100 * time.Millisecond)
		}
	case "new":
		_, err := worker.EnsureNamespace(context.Background(), desiredRuntime(input.Pin), runtimeAttempt(input.Pin))
		var denied *runtimeconfig.Error
		if !errors.As(err, &denied) || denied.Code != runtimeconfig.Consumed {
			t.Fatalf("consumed name became recreatable: %v", err)
		}
		desired := desiredRuntime(input.Pin)
		desired.Generation, desired.Revision, desired.ReconcileKey = 2, "git-newrevision", "new-config"
		if _, err := worker.EnsureNamespace(context.Background(), desired, "new-"+runtimeAttempt(input.Pin)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unknown controlled retirement phase")
	}
	checkpoint, err := worker.Quiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := checkpoint.JSON()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("RETIREMENT-CHILD " + base64.StdEncoding.EncodeToString(encoded))
}

func runRetirementChild(t *testing.T, input retirementChildInput, phase string) []byte {
	t.Helper()
	data, _ := json.Marshal(input)
	command := exec.Command(os.Args[0], "-test.run=^TestRuntimeRetirementPersistsFailureAcrossCleanProcessHandoff$", "-test.v", "-test.timeout=90s")
	command.Env = []string{"PATH=/unavailable", "HOME=/tmp/owned-runtime-retirement", "TMPDIR=/tmp", "OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, "OPERATOR_RETIREMENT_CHILD=" + phase}
	command.Stdin = bytes.NewReader(data)
	output, err := command.CombinedOutput()
	// CombinedOutput waits/reaps this exact child before this helper returns.
	// The parent never starts its successor while the old worker can dispatch.
	if err != nil {
		t.Fatalf("retirement child %s: %v\n%s", phase, err, output)
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "RETIREMENT-CHILD ") {
			checkpoint, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, "RETIREMENT-CHILD "))
			if err != nil {
				t.Fatal(err)
			}
			return checkpoint
		}
	}
	t.Fatal("child exited without exact acknowledged checkpoint")
	return nil
}

func provePendingCleanup(pin runtimePin, cellPath, method string, body []byte) error {
	response, err := client.Get(local.Server + cellPath)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var cell corev1.ConfigMap
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&cell) != nil || string(cell.UID) != pin.Cell.Record.Ref.ProviderID {
		return fmt.Errorf("pinned journal missing before cleanup")
	}
	envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
	if err != nil {
		return err
	}
	if envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive || envelope.Projection == nil || envelope.Projection.Failure == nil || envelope.Projection.Failure.ID != "original-retirement-smoke" || len(envelope.Projection.Recovery) == 0 {
		return fmt.Errorf("cleanup preceded active admission/original cause/recovery")
	}
	var raw struct {
		Metadata      struct{ Name, UID, ResourceVersion string }
		Preconditions struct{ UID, ResourceVersion string }
	}
	if json.Unmarshal(body, &raw) != nil {
		return fmt.Errorf("invalid cleanup body")
	}
	uid, version, action := raw.Metadata.UID, raw.Metadata.ResourceVersion, "Finalize"
	if method == "DELETE" {
		uid, version, action = raw.Preconditions.UID, raw.Preconditions.ResourceVersion, "Delete"
	}
	digest := sha256.Sum256(body)
	fingerprint := hex.EncodeToString(digest[:])
	for _, attempt := range envelope.Attempts {
		if attempt.OperationID != runtimeAttempt(pin) || attempt.State != journal.Draining || attempt.DrainReason != journal.DrainCandidateFailed || attempt.NamespaceUID != uid {
			continue
		}
		for _, write := range envelope.Writes {
			p := write.Preconditions
			if write.State == journal.Pending && write.OperationID == attempt.OperationID && p.Kind == "Namespace" && p.Action == action && p.Name == attempt.NamespaceName && p.UID == uid && p.Version == version && p.Fingerprint == fingerprint && p.TargetRevision == attempt.Revision && p.ProviderOperationID == write.ID {
				return nil
			}
		}
	}
	return fmt.Errorf("cleanup preceded exact byte/UID/version Pending ACK")
}

func retirementProxy(t *testing.T, pin runtimePin) (string, []byte, *atomic.Int64) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	type cleanupResponseKey struct{}
	forward.ModifyResponse = func(response *http.Response) error {
		wanted, _ := response.Request.Context().Value(cleanupResponseKey{}).(*corev1.Namespace)
		if wanted == nil {
			return nil
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			return err
		}
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(body))
		var actual corev1.Namespace
		decodeErr := json.Unmarshal(body, &actual)
		t.Logf("owned finalizer response status=%d decoded=%t kind=%s uidMatch=%t rvChanged=%t labelsMatch=%t annotationsMatch=%t ownersMatch=%t metadataFinalizersMatch=%t specMatch=%t", response.StatusCode, decodeErr == nil, actual.Kind, actual.UID == wanted.UID, actual.ResourceVersion != wanted.ResourceVersion, reflect.DeepEqual(actual.Labels, wanted.Labels), reflect.DeepEqual(actual.Annotations, wanted.Annotations), reflect.DeepEqual(actual.OwnerReferences, wanted.OwnerReferences), reflect.DeepEqual(actual.Finalizers, wanted.Finalizers), reflect.DeepEqual(actual.Spec, wanted.Spec))
		return nil
	}
	cellPath := "/api/v1/namespaces/" + pin.Parent.Ref.Name + "/configmaps/" + pin.Cell.Record.Ref.Name
	count := &atomic.Int64{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" || r.Method == "PUT" && r.URL.Path != cellPath {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err == nil {
				err = provePendingCleanup(pin, cellPath, r.Method, body)
			}
			if err != nil {
				t.Errorf("cleanup dispatch guard: %v", err)
				http.Error(w, "cleanup not durably authorized", 500)
				return
			}
			count.Add(1)
			r.Body = io.NopCloser(bytes.NewReader(body))
			if r.Method == "PUT" {
				var wanted corev1.Namespace
				if json.Unmarshal(body, &wanted) != nil {
					http.Error(w, "fixture decode failed", 500)
					return
				}
				r = r.WithContext(context.WithValue(r.Context(), cleanupResponseKey{}, &wanted))
			}
		}
		if r.Method == "POST" {
			kind := "ConfigMap"
			if r.URL.Path == "/api/v1/namespaces" {
				kind = "Namespace"
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err == nil {
				err = provePendingRuntimePost(pin, cellPath, kind, body)
			}
			if err != nil {
				t.Errorf("create dispatch guard: %v", err)
				http.Error(w, "create not durably authorized", 500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		forward.ServeHTTP(w, r)
	}))
	ca, _ := base64.StdEncoding.DecodeString(local.CA)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), count
}

func TestRuntimeRetirementPersistsFailureAcrossCleanProcessHandoff(t *testing.T) {
	if phase := os.Getenv("OPERATOR_RETIREMENT_CHILD"); phase != "" {
		retirementChild(t, phase)
		return
	}
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	pin := runtimeControlPin(t, "retirement")
	endpoint, ca, dispatches := retirementProxy(t, pin)
	input := retirementChildInput{Pin: pin, Endpoint: endpoint, ProxyCA: ca}
	input.Checkpoint = runRetirementChild(t, input, "drain")
	if dispatches.Load() != 0 {
		t.Fatal("drain checkpoint preceded unexplained cleanup RPC")
	}
	input.Checkpoint = runRetirementChild(t, input, "retire")
	if dispatches.Load() < 2 {
		t.Fatalf("expected conditional DELETE and Finalize, got %d", dispatches.Load())
	}
	_, repository := retirementConfig(t, pin, "", nil)
	retired, err := repository.Load(context.Background())
	if err != nil || retired.Envelope.Projection == nil {
		t.Fatal("completed cleanup projection missing", err)
	}
	retiredProjection, err := retired.Envelope.Projection.JSON()
	if err != nil {
		t.Fatal(err)
	}
	input.Checkpoint = runRetirementChild(t, input, "new")
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Envelope.Attempts) != 2 || snapshot.Envelope.Attempts[0].State != journal.Consumed || !snapshot.Envelope.Attempts[0].CanonicalAbsent || snapshot.Envelope.Attempts[0].NamespaceName == snapshot.Envelope.Attempts[1].NamespaceName || len(snapshot.Envelope.Archive) != 1 || snapshot.Envelope.Archive[0].Failure == nil || snapshot.Envelope.Archive[0].Failure.ID != "original-retirement-smoke" {
		t.Fatal("retirement lost cause, consumed identity or new-attempt separation")
	}
	archived, err := snapshot.Envelope.Archive[0].JSON()
	if err != nil || !bytes.Equal(archived, retiredProjection) || snapshot.Envelope.Projection != nil || snapshot.Envelope.Archive[0].Identity.OperationID != runtimeAttempt(pin) || snapshot.Envelope.Attempts[1].OperationID != "new-"+runtimeAttempt(pin) {
		t.Fatal("new attempt rewrote archived cause/recovery or retained old primary", err)
	}
	cleanupRows := int64(0)
	for _, write := range snapshot.Envelope.Writes {
		if write.Preconditions.Kind == "Namespace" && (write.Preconditions.Action == "Delete" || write.Preconditions.Action == "Finalize") {
			cleanupRows++
		}
		if write.State == journal.Pending || write.State == journal.Unknown {
			t.Fatal("clean process successor bypassed unresolved cleanup")
		}
	}
	if cleanupRows != dispatches.Load() {
		t.Fatal("cleanup RPC escaped exact durable journal ticket")
	}
}

func TestRuntimeUnknownCleanupRetainsCauseAndRejectsFreshClientAfterAbsence(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	pin := runtimeControlPin(t, "retirement-unknown")
	config, repository := retirementConfig(t, pin, "", nil)
	desired := desiredRuntime(pin)
	plan, err := renderer.RenderNamespace(config.Project, renderer.NamespaceRequest{Revision: desired.Revision, OperationID: runtimeAttempt(pin), Fingerprint: journal.DesiredDigest(desired)})
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/namespaces/" + plan.Object.Name
	options, attempts := acceptedButLostProxy(t, "DELETE", path)
	config.Client, err = provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := runtimeconfig.OpenInitial(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.EnsureNamespace(context.Background(), desired, runtimeAttempt(pin)); err != nil {
		t.Fatal(err)
	}
	retirement, err := worker.BeginRetirement(context.Background(), runtimeconfig.RetirementRequest{AttemptID: runtimeAttempt(pin), Reason: journal.DrainCandidateFailed, Projection: failedRetirementProjection(t, pin)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = retirement.Advance(context.Background())
	var uncertain *runtimeconfig.Error
	if !errors.As(err, &uncertain) || uncertain.Code != runtimeconfig.Uncertain || attempts.Load() != 1 {
		t.Fatalf("ambiguous cleanup not retained exactly once: %v count=%d", err, attempts.Load())
	}
	checkpoint, err := worker.Quiesce(context.Background())
	if err == nil {
		t.Fatal("unknown cleanup issued clean checkpoint", checkpoint)
	}
	// An explicit fixture administrator releases the already accepted DELETE's
	// hold only to prove later absence cannot settle the earlier ambiguous RPC.
	current := request(t, "GET", path, nil, 200)
	finalizers, _ := metadata(current)["finalizers"].([]any)
	if len(finalizers) != 1 || finalizers[0] != provider.NamespaceLifecycleFinalizer {
		t.Fatal("unexpected foreign hold; refuse fixture release")
	}
	metadata(current)["finalizers"] = []string{}
	request(t, "PUT", path, current, 200)
	wait(t, func() bool {
		response, err := client.Get(local.Server + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusNotFound
	})
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	unknown := 0
	for _, write := range snapshot.Envelope.Writes {
		if write.Preconditions.Action == "Delete" && write.State == journal.Unknown {
			unknown++
		}
	}
	if unknown != 1 || snapshot.Envelope.Attempts[0].State != journal.Draining || snapshot.Envelope.Attempts[0].CanonicalAbsent || snapshot.Envelope.Projection == nil || snapshot.Envelope.Projection.Failure == nil || snapshot.Envelope.Projection.Failure.ID != "original-retirement-smoke" {
		t.Fatal("absence overwrote unknown cleanup or original cause")
	}
	fresh, _ := retirementConfig(t, pin, "", nil)
	_, err = runtimeconfig.OpenInitial(context.Background(), fresh)
	var blocked *runtimeconfig.Error
	if !errors.As(err, &blocked) || blocked.Code != runtimeconfig.Blocked {
		t.Fatalf("fresh client bypassed durable unknown after absence: %v", err)
	}
}
