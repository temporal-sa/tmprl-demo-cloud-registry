//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type deploymentProcessOutput struct {
	Checkpoint []byte
	Receipt    runtimeconfig.DeploymentReceipt
	Input      journal.InputReference
	Error      runtimeconfig.Code
}

func desiredRuntimeDeployment(pin runtimePin) rollout.Desired {
	desired := desiredRuntime(pin)
	desired.Images = map[string]string{"app": local.ImageDigest}
	return desired
}

func deploymentFromFixture(t *testing.T, session *runtimeconfig.Session, pin runtimePin, changed bool) renderer.DesiredObject {
	t.Helper()
	source := runtimeDeploymentDocument
	if changed {
		source = strings.ReplaceAll(source, "-revision=runtime-deployment", "-revision=current-authoring")
	}
	document, err := project.Decode([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	desired := desiredRuntimeDeployment(pin)
	bundle, err := renderer.RenderCandidate(document, renderer.Bindings{Namespace: session.Context(), Revision: desired.Revision, OperationID: runtimeAttempt(pin), Images: desired.Images})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range bundle.Objects {
		if _, ok := object.Object.(*appsv1.Deployment); ok {
			return object
		}
	}
	t.Fatal("real renderer did not produce local Deployment")
	return renderer.DesiredObject{}
}

func runtimeDeploymentProcess(t *testing.T, phase string) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input privateRuntimeProcessInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	config, repository := retirementConfig(t, input.Pin, input.Endpoint, input.ProxyCA)
	store, _ := openRuntimeInputs(t, input)
	if !input.WithoutInputs {
		config.DeploymentInputs = store
	}
	config.DeploymentDesired = func(context.Context, runtimeconfig.DeploymentRestore) (*appsv1.Deployment, error) {
		t.Error("referenced body fell back to mutable legacy renderer")
		return nil, fmt.Errorf("legacy disabled in this proof")
	}
	var worker *runtimeconfig.Worker
	var err error
	if phase == "initial" || phase == "unknown" || phase == "blocked" {
		worker, err = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		checkpoint, decodeErr := runtimeconfig.ParseCheckpoint(input.Checkpoint)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
	}
	var output deploymentProcessOutput
	if input.WantOpenError != "" {
		var denied *runtimeconfig.Error
		if !errors.As(err, &denied) || denied.Code != input.WantOpenError {
			t.Fatalf("clean open wanted %s, got %v", input.WantOpenError, err)
		}
		output.Error = denied.Code
	} else {
		if err != nil {
			t.Fatal(err)
		}
		desired := desiredRuntimeDeployment(input.Pin)
		session, err := worker.EnsureNamespace(context.Background(), desired, runtimeAttempt(input.Pin))
		if err != nil {
			t.Fatal(err)
		}
		var object renderer.DesiredObject
		if phase == "initial" || phase == "unknown" {
			// This proof composes only typed Deployment runtime. Its public local
			// ServiceAccount prerequisite is explicitly created by the owned
			// fixture administrator, not silently claimed as runtime capability.
			request(t, "POST", "/api/v1/namespaces/"+session.Context().NamespaceName()+"/serviceaccounts", map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "app", "labels": map[string]any{"operator.local/run-id": local.RunID}}}, 201)
			object = deploymentFromFixture(t, session, input.Pin, false)
			if _, err := session.ObserveDeployment(context.Background(), object); err == nil {
				t.Fatal("observe created a missing write")
			}
			output.Receipt, err = session.ApplyDeployment(context.Background(), object)
		} else {
			snapshot, loadErr := repository.Load(context.Background())
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			for _, write := range snapshot.Envelope.Writes {
				if write.Preconditions.Kind != "Deployment" {
					continue
				}
				p := write.Preconditions
				payload, resolveErr := store.Resolve(context.Background(), write.Input, runtimeinputs.Target{CellUID: input.Pin.Cell.Record.Ref.ProviderID, Parent: session.Context().Parent(), AttemptID: runtimeAttempt(input.Pin), Revision: desired.Revision, DesiredDigest: journal.DesiredDigest(desired), Name: p.Name, Fingerprint: p.Fingerprint})
				if resolveErr != nil {
					t.Fatal(resolveErr)
				}
				body := payload.ObjectForProvider()
				intent, intentErr := session.Context().Intent("deployment", body.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
				if intentErr != nil {
					t.Fatal(intentErr)
				}
				object = renderer.DesiredObject{Object: body, Intent: intent}
			}
			if object.Object == nil {
				t.Fatal("accepted private Deployment record missing")
			}
			if input.ChangedAuthoring {
				current := deploymentFromFixture(t, session, input.Pin, true)
				_, changedErr := session.ApplyDeployment(context.Background(), current)
				var changed *runtimeconfig.Error
				if !errors.As(changedErr, &changed) || changed.Code != runtimeconfig.Changed {
					t.Fatalf("current authoring replaced old body: %v", changedErr)
				}
			}
			output.Receipt, err = session.ApplyDeployment(context.Background(), object)
		}
		if phase == "unknown" {
			var unknown *runtimeconfig.Error
			if !errors.As(err, &unknown) || unknown.Code != runtimeconfig.Uncertain {
				t.Fatalf("expected retained unknown create, got %v", err)
			}
			if _, closeErr := worker.Quiesce(context.Background()); closeErr == nil {
				t.Fatal("unknown Deployment emitted a handoff")
			}
			output.Error = unknown.Code
		} else {
			if err != nil {
				t.Fatal(err)
			}
			wait(t, func() bool {
				var err error
				output.Receipt, err = session.ObserveDeployment(context.Background(), object)
				if err != nil {
					t.Fatal(err)
				}
				if output.Receipt.Readiness.State == "Failed" {
					t.Fatal("owned local Deployment failed readiness")
				}
				return output.Receipt.Readiness.State == "Ready"
			})
			snapshot, err := repository.Load(context.Background())
			if err != nil || snapshot.Version != output.Receipt.JournalVersion {
				t.Fatal("receipt not bound to acknowledged journal", err)
			}
			for _, write := range snapshot.Envelope.Writes {
				if write.Preconditions.Kind == "Deployment" {
					if write.State != journal.Committed || write.ResultUID != output.Receipt.Record.Record.Ref.ProviderID || !write.Input.Valid() {
						t.Fatal("receipt preceded canonical UID/private reference commit")
					}
					output.Input = write.Input
				}
			}
			if !output.Input.Valid() {
				t.Fatal("private body reference absent")
			}
			checkpoint, err := worker.Quiesce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			output.Checkpoint, err = checkpoint.JSON()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	data, _ := json.Marshal(output)
	fmt.Println("DEPLOYMENT-PROCESS " + string(data))
}

func runDeploymentProcess(t *testing.T, input privateRuntimeProcessInput, phase string) deploymentProcessOutput {
	t.Helper()
	var output deploymentProcessOutput
	runPrivateRuntimeProcess(t, "TestRuntimeDeploymentPrivateInputsAcrossCleanProcessHandoff", "OPERATOR_DEPLOYMENT_PROCESS", "DEPLOYMENT-PROCESS ", phase, input, &output, runtimeDeploymentPrivateMarker)
	return output
}

func deploymentProcessFixture(t *testing.T, suffix string) (privateRuntimeProcessInput, *runtimeinputs.Store, *os.Root) {
	return runtimeInputProcessFixture(t, "dep", suffix)
}

func deploymentDispatchProxy(t *testing.T, input privateRuntimeProcessInput, store *runtimeinputs.Store, lose bool) (string, []byte, *atomic.Int64) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	cellPath := "/api/v1/namespaces/" + input.Pin.Parent.Ref.Name + "/configmaps/" + input.Pin.Cell.Record.Ref.Name
	count := &atomic.Int64{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			forward.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "owned fixture read failed", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if !strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/") {
			kind := "ConfigMap"
			if r.URL.Path == "/api/v1/namespaces" {
				kind = "Namespace"
			}
			if err := provePendingRuntimePost(input.Pin, cellPath, kind, body); err != nil {
				t.Error(err)
				http.Error(w, "missing pending create", 500)
				return
			}
			forward.ServeHTTP(w, r)
			return
		}
		var object appsv1.Deployment
		if json.Unmarshal(body, &object) != nil {
			http.Error(w, "invalid owned Deployment", 500)
			return
		}
		response, err := client.Get(local.Server + cellPath)
		if err != nil {
			t.Error(err)
			http.Error(w, "journal unavailable", 500)
			return
		}
		var cell corev1.ConfigMap
		decodeErr := json.NewDecoder(response.Body).Decode(&cell)
		response.Body.Close()
		if response.StatusCode != 200 || decodeErr != nil || string(cell.UID) != input.Pin.Cell.Record.Ref.ProviderID {
			t.Error("pinned journal unavailable")
			http.Error(w, "journal unproven", 500)
			return
		}
		envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
		if err != nil || envelope.Subject != input.Pin.Subject || envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive {
			t.Error("Deployment preceded active exact subject")
			http.Error(w, "admission missing", 500)
			return
		}
		verified := false
		for _, attempt := range envelope.Attempts {
			if attempt.State != journal.Observed || attempt.OperationID != runtimeAttempt(input.Pin) || attempt.NamespaceUID != object.Annotations[ownership.ParentNamespaceUIDAnnotation] {
				continue
			}
			for _, write := range envelope.Writes {
				p := write.Preconditions
				if write.State != journal.Pending || write.OperationID != attempt.OperationID || p.Kind != "Deployment" || p.Action != "Create" || p.Name != object.Name || p.ParentUID != attempt.NamespaceUID || p.Fingerprint != object.Annotations[provider.FingerprintAnnotation] || p.TargetRevision != attempt.Revision || p.ProviderOperationID != object.Annotations[provider.OperationAnnotation] || !write.Input.Valid() {
					continue
				}
				parent := request(t, "GET", "/api/v1/namespaces/"+attempt.NamespaceName, nil, 200)
				var owner ownership.Owner
				if json.Unmarshal([]byte(text(field(metadata(parent), "annotations"), provider.OwnerAnnotation)), &owner) != nil || owner != input.StoreConfig.Owner || text(metadata(parent), "uid") != attempt.NamespaceUID {
					t.Error("dispatch parent owner/UID unproven")
					continue
				}
				parentPin := ownership.NamespaceParent{ClusterID: local.RunID, Name: attempt.NamespaceName, UID: attempt.NamespaceUID, Owner: owner, CreationOperationID: text(field(metadata(parent), "annotations"), provider.CreationOperationAnnotation), CreationIdempotencyKey: text(field(metadata(parent), "annotations"), provider.CreationKeyAnnotation)}
				payload, resolveErr := store.Resolve(context.Background(), write.Input, runtimeinputs.Target{CellUID: input.Pin.Cell.Record.Ref.ProviderID, Parent: parentPin, AttemptID: attempt.OperationID, Revision: attempt.Revision, DesiredDigest: attempt.DesiredDigest, Name: p.Name, Fingerprint: p.Fingerprint})
				if resolveErr != nil || payload.ObjectForProvider() == nil {
					t.Error("private accepted body not durable before POST", resolveErr)
					continue
				}
				verified = true
			}
		}
		if !verified {
			http.Error(w, "exact referenced pending body unproven", 500)
			return
		}
		count.Add(1)
		if lose {
			outbound := r.Clone(r.Context())
			outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
			outbound.Host = target.Host
			outbound.RequestURI = ""
			response, err := client.Transport.RoundTrip(outbound)
			if err != nil {
				http.Error(w, "owned upstream failed", 502)
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode != 201 {
				http.Error(w, "owned upstream create rejected", response.StatusCode)
				return
			}
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				connection.Close()
			}
			return
		}
		forward.ServeHTTP(w, r)
	})
	endpoint, ca := startRuntimeDispatchProxy(t, handler)
	return endpoint, ca, count
}

func TestRuntimeDeploymentPrivateInputsAcrossCleanProcessHandoff(t *testing.T) {
	if phase := os.Getenv("OPERATOR_DEPLOYMENT_PROCESS"); phase != "" {
		runtimeDeploymentProcess(t, phase)
		return
	}
	input, store, root := deploymentProcessFixture(t, "replay")
	endpoint, proxyCA, posts := deploymentDispatchProxy(t, input, store, false)
	input.Endpoint, input.ProxyCA = endpoint, proxyCA
	first := runDeploymentProcess(t, input, "initial")
	if len(first.Checkpoint) == 0 || !first.Input.Valid() || posts.Load() != 1 {
		t.Fatal("initial private Deployment receipt missing")
	}
	input.Checkpoint = first.Checkpoint
	input.WithoutInputs, input.WantOpenError = true, runtimeconfig.Invalid
	runDeploymentProcess(t, input, "deny")
	input.WithoutInputs = false
	name := first.Input.UID + ".input.json"
	original, err := root.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.WriteFile(name, []byte(`{"unproven":"fixture-only"}`), 0600); err != nil {
		t.Fatal(err)
	}
	input.WantOpenError = runtimeconfig.Unavailable
	runDeploymentProcess(t, input, "deny")
	// Explicit test administrator restores the exact original bytes solely to
	// prove rejected preflight did not consume the one-use clean checkpoint.
	if err := root.WriteFile(name, original, 0600); err != nil {
		t.Fatal(err)
	}
	input.WantOpenError = ""
	input.ChangedAuthoring = true
	second := runDeploymentProcess(t, input, "replay")
	if second.Receipt.Record != first.Receipt.Record || second.Input != first.Input || posts.Load() != 1 {
		t.Fatal("historical replay changed canonical resource/body or posted twice")
	}
	if err := root.Remove(name); err != nil {
		t.Fatal(err)
	}
	input.Checkpoint, input.WantOpenError = second.Checkpoint, runtimeconfig.Unavailable
	runDeploymentProcess(t, input, "deny")
	if _, err := root.Lstat(name); !os.IsNotExist(err) || posts.Load() != 1 {
		t.Fatal("missing body was rerendered/repaired or dispatched")
	}
}

func TestRuntimeDeploymentUnknownCreateCannotAdoptFromPrivateBodyOrFreshProcess(t *testing.T) {
	input, store, _ := deploymentProcessFixture(t, "unknown")
	endpoint, proxyCA, posts := deploymentDispatchProxy(t, input, store, true)
	input.Endpoint, input.ProxyCA = endpoint, proxyCA
	unknown := runDeploymentProcess(t, input, "unknown")
	if unknown.Error != runtimeconfig.Uncertain || len(unknown.Checkpoint) != 0 || posts.Load() != 1 {
		t.Fatal("unknown create was retried or issued checkpoint")
	}
	input.WantOpenError = runtimeconfig.Blocked
	runDeploymentProcess(t, input, "blocked")
	_, repository := retirementConfig(t, input.Pin, "", nil)
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	unknownRows := 0
	for _, write := range snapshot.Envelope.Writes {
		if write.Preconditions.Kind == "Deployment" && write.State == journal.Unknown {
			unknownRows++
			if !write.Input.Valid() {
				t.Fatal("unknown lost private intent reference")
			}
		}
	}
	if unknownRows != 1 || posts.Load() != 1 {
		t.Fatal("fresh process adopted native accepted unknown resource")
	}
}

func TestRuntimeDeploymentCleanOpenRejectsMissingOrReplacedCanonicalUID(t *testing.T) {
	for _, mode := range []string{"missing", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			input, store, _ := deploymentProcessFixture(t, "uid-"+mode)
			endpoint, proxyCA, posts := deploymentDispatchProxy(t, input, store, false)
			input.Endpoint, input.ProxyCA = endpoint, proxyCA
			first := runDeploymentProcess(t, input, "initial")
			parent := first.Receipt.Record.Parent
			path := "/apis/apps/v1/namespaces/" + parent.Name + "/deployments/app"
			before := request(t, "GET", path, nil, 200)
			oldUID := first.Receipt.Record.Record.Ref.ProviderID
			request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": oldUID}}, 200)
			wait(t, func() bool {
				response, err := client.Get(local.Server + path)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				return response.StatusCode == http.StatusNotFound
			})
			if mode == "replacement" {
				// Deliberately bypass the managed actor as an owned fixture
				// administrator to prove perfect metadata cannot adopt a new UID.
				for _, name := range []string{"uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "deletionTimestamp", "deletionGracePeriodSeconds"} {
					delete(metadata(before), name)
				}
				delete(before, "status")
				replacement := request(t, "POST", "/apis/apps/v1/namespaces/"+parent.Name+"/deployments", before, 201)
				if text(metadata(replacement), "uid") == oldUID {
					t.Fatal("fixture did not replace canonical Deployment")
				}
			}
			input.Checkpoint, input.WantOpenError = first.Checkpoint, runtimeconfig.Changed
			runDeploymentProcess(t, input, "deny")
			if posts.Load() != 1 {
				t.Fatal("missing/replaced canonical child caused managed POST")
			}
			_, repository := retirementConfig(t, input.Pin, "", nil)
			snapshot, err := repository.Load(context.Background())
			if err != nil || snapshot.Envelope.Admission == nil || snapshot.Envelope.Admission.State != journal.AdmissionReady {
				t.Fatal("failed provider preflight consumed clean admission", err)
			}
		})
	}
}
