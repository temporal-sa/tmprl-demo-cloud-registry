//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"os"
	"strings"
	"testing"
)

const runtimeServicePrivateMarker = "PRIVATE_SERVICE_INPUT_FIXTURE_ONLY"

type serviceProcessOutput struct {
	Checkpoint []byte
	Receipt    runtimeconfig.ServiceReceipt
	Input      journal.InputReference
	Error      runtimeconfig.Code
}

func serviceFromRuntimeFixture(t *testing.T, session *runtimeconfig.Session, pin runtimePin, changed bool) renderer.DesiredObject {
	t.Helper()
	source := runtimeDeploymentDocument
	if changed {
		source = strings.ReplaceAll(source, "containerPort: 8080", "containerPort: 8081")
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
		if service, ok := object.Object.(*corev1.Service); ok {
			// Explicit accepted fixture transform, not an authored bundle proof:
			// retain a synthetic private annotation to detect payload disclosure.
			service = service.DeepCopy()
			if service.Annotations == nil {
				service.Annotations = map[string]string{}
			}
			service.Annotations["operator.local/private-fixture"] = runtimeServicePrivateMarker
			fingerprint, err := provider.ServiceFingerprint(service)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := session.Context().Intent("service", service.Name, desired.Revision, runtimeAttempt(pin), fingerprint)
			if err != nil {
				t.Fatal(err)
			}
			return renderer.DesiredObject{Object: service, Intent: intent}
		}
	}
	t.Fatal("accepted fixture did not render a Service")
	return renderer.DesiredObject{}
}

func runtimeServiceProcess(t *testing.T, phase string) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input privateRuntimeProcessInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil {
		t.Fatal("invalid owned Service process input")
	}
	config, repository := retirementConfig(t, input.Pin, input.Endpoint, input.ProxyCA)
	store, _ := openRuntimeInputs(t, input)
	if !input.WithoutInputs {
		config.ServiceInputs = store
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
	var output serviceProcessOutput
	if input.WantOpenError != "" {
		var denied *runtimeconfig.Error
		if !errors.As(err, &denied) || denied.Code != input.WantOpenError {
			t.Fatalf("Service open wanted %s, got %v", input.WantOpenError, err)
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
			object = serviceFromRuntimeFixture(t, session, input.Pin, false)
			if _, err := session.ObserveService(context.Background(), object); err == nil {
				t.Fatal("Service observe manufactured a missing journal receipt")
			}
		} else {
			snapshot, loadErr := repository.Load(context.Background())
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			for _, write := range snapshot.Envelope.Writes {
				if write.Preconditions.Kind != "Service" {
					continue
				}
				p := write.Preconditions
				payload, resolveErr := store.ResolveService(context.Background(), write.Input, runtimeinputs.Target{CellUID: input.Pin.Cell.Record.Ref.ProviderID, Parent: session.Context().Parent(), AttemptID: runtimeAttempt(input.Pin), Revision: desired.Revision, DesiredDigest: journal.DesiredDigest(desired), Name: p.Name, Fingerprint: p.Fingerprint})
				if resolveErr != nil {
					t.Fatal(resolveErr)
				}
				body := payload.ObjectForProvider()
				intent, intentErr := session.Context().Intent("service", body.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
				if intentErr != nil {
					t.Fatal(intentErr)
				}
				object = renderer.DesiredObject{Object: body, Intent: intent}
			}
			if object.Object == nil {
				t.Fatal("accepted Service input missing")
			}
			if input.ChangedAuthoring {
				current := serviceFromRuntimeFixture(t, session, input.Pin, true)
				_, changedErr := session.ApplyService(context.Background(), current)
				var changed *runtimeconfig.Error
				if !errors.As(changedErr, &changed) || changed.Code != runtimeconfig.Changed {
					t.Fatalf("current authoring replaced original Service: %v", changedErr)
				}
			}
		}
		output.Receipt, err = session.ApplyService(context.Background(), object)
		if phase == "unknown" {
			var unknown *runtimeconfig.Error
			if !errors.As(err, &unknown) || unknown.Code != runtimeconfig.Uncertain {
				t.Fatalf("Service create wanted retained unknown, got %v", err)
			}
			if _, retryErr := session.ApplyService(context.Background(), object); retryErr == nil {
				t.Fatal("unknown Service dispatched again")
			}
			if _, closeErr := worker.Quiesce(context.Background()); closeErr == nil {
				t.Fatal("unknown Service emitted clean handoff")
			}
			output.Error = unknown.Code
		} else {
			if err != nil {
				t.Fatal(err)
			}
			output.Receipt, err = session.ObserveService(context.Background(), object)
			if err != nil || !output.Receipt.ObservedAllocated || output.Receipt.BackendReadiness != "NotEvaluated" {
				t.Fatal("allocation confused with backend readiness", err)
			}
			snapshot, loadErr := repository.Load(context.Background())
			if loadErr != nil || snapshot.Version != output.Receipt.JournalVersion {
				t.Fatal("Service receipt not bound to acknowledged journal", loadErr)
			}
			for _, write := range snapshot.Envelope.Writes {
				if write.Preconditions.Kind != "Service" {
					continue
				}
				if write.State != journal.Committed || write.ResultUID != output.Receipt.Record.Record.Ref.ProviderID || write.ResultSpecDigest != output.Receipt.AllocatedSpecDigest || write.ResultSpecContract != output.Receipt.SpecContract || write.ResultSpecContract != provider.ServiceSpecContract || !write.Input.MatchesKind("Service") {
					t.Fatal("Service advanced before complete immutable receipt")
				}
				output.Input = write.Input
			}
			if !output.Input.MatchesKind("Service") {
				t.Fatal("Service private original reference absent")
			}
			checkpoint, closeErr := worker.Quiesce(context.Background())
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			output.Checkpoint, err = checkpoint.JSON()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	data, _ := json.Marshal(output)
	fmt.Println("SERVICE-PROCESS " + string(data))
}

func runServiceProcess(t *testing.T, input privateRuntimeProcessInput, phase string) serviceProcessOutput {
	t.Helper()
	var output serviceProcessOutput
	runPrivateRuntimeProcess(t, "TestRuntimeServicePrivateInputsAcrossCleanProcessHandoff", "OPERATOR_SERVICE_PROCESS", "SERVICE-PROCESS ", phase, input, &output, "\"selector\"", "\"clusterIP\"", runtimeServicePrivateMarker)
	return output
}
func serviceProcessFixture(t *testing.T, suffix string) (privateRuntimeProcessInput, *runtimeinputs.Store, *os.Root) {
	t.Helper()
	return runtimeInputProcessFixture(t, "svc", suffix)
}

func TestRuntimeServicePrivateInputsAcrossCleanProcessHandoff(t *testing.T) {
	if phase := os.Getenv("OPERATOR_SERVICE_PROCESS"); phase != "" {
		runtimeServiceProcess(t, phase)
		return
	}
	input, store, root := serviceProcessFixture(t, "replay")
	endpoint, ca, posts, _ := serviceDispatchProxy(t, input, store, false)
	input.Endpoint, input.ProxyCA = endpoint, ca
	first := runServiceProcess(t, input, "initial")
	if len(first.Checkpoint) == 0 || !first.Input.MatchesKind("Service") || posts.Load() != 1 {
		t.Fatal("known Service create lost receipt/input")
	}
	input.Checkpoint = first.Checkpoint
	input.WithoutInputs, input.WantOpenError = true, runtimeconfig.Invalid
	runServiceProcess(t, input, "deny")
	input.WithoutInputs = false
	name := first.Input.UID + ".input.json"
	original, err := root.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if root.WriteFile(name, []byte("fixture-only-unproven-input"), 0600) != nil {
		t.Fatal("fixture tamper failed")
	}
	input.WantOpenError = runtimeconfig.Unavailable
	runServiceProcess(t, input, "deny")
	// Only fixture administration restores exact bytes; runtime never repairs.
	if root.WriteFile(name, original, 0600) != nil {
		t.Fatal("fixture restore failed")
	}
	input.WantOpenError = ""
	input.ChangedAuthoring = true
	second := runServiceProcess(t, input, "replay")
	if second.Receipt.Record != first.Receipt.Record || second.Input != first.Input || second.Receipt.SpecContract != first.Receipt.SpecContract || second.Receipt.AllocatedSpecDigest != first.Receipt.AllocatedSpecDigest || posts.Load() != 1 {
		t.Fatal("clean process changed original input/canonical allocation or reposted")
	}
	input.Checkpoint = first.Checkpoint
	input.WantOpenError = runtimeconfig.Changed
	runServiceProcess(t, input, "deny")
	if root.Remove(name) != nil {
		t.Fatal("fixture input removal failed")
	}
	input.Checkpoint, input.WantOpenError = second.Checkpoint, runtimeconfig.Unavailable
	runServiceProcess(t, input, "deny")
	if _, err := root.Lstat(name); !os.IsNotExist(err) || posts.Load() != 1 {
		t.Fatal("missing Service body recreated or dispatched")
	}
}

func TestRuntimeServiceUnknownCreateCannotLearnAllocationFromGETOrFreshProcess(t *testing.T) {
	input, store, _ := serviceProcessFixture(t, "unknown")
	endpoint, ca, posts, _ := serviceDispatchProxy(t, input, store, true)
	input.Endpoint, input.ProxyCA = endpoint, ca
	result := runServiceProcess(t, input, "unknown")
	if result.Error != runtimeconfig.Uncertain || len(result.Checkpoint) != 0 || posts.Load() != 1 {
		t.Fatal("unknown Service retried or handed off")
	}
	input.WantOpenError = runtimeconfig.Blocked
	runServiceProcess(t, input, "blocked")
	_, repository := retirementConfig(t, input.Pin, "", nil)
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, write := range snapshot.Envelope.Writes {
		if write.Preconditions.Kind != "Service" {
			continue
		}
		count++
		if write.State != journal.Unknown || !write.Input.MatchesKind("Service") || write.ResultUID != "" || write.ResultVersion != "" || write.ResultSpecDigest != "" || write.ResultSpecContract != "" {
			t.Fatal("unknown Service gained observed allocation receipt")
		}
		// Independent fixture GET establishes accepted native object, not adoption.
		actual := request(t, "GET", "/api/v1/namespaces/"+snapshot.Envelope.Attempts[0].NamespaceName+"/services/"+write.Preconditions.Name, nil, 200)
		if text(metadata(actual), "uid") == "" || text(field(actual, "spec"), "clusterIP") == "" {
			t.Fatal("native unknown acceptance not established")
		}
	}
	if count != 1 || posts.Load() != 1 {
		t.Fatal("unknown canonical object adopted or reposted")
	}
}

func corruptServiceAllocationReceipt(t *testing.T, input privateRuntimeProcessInput) {
	t.Helper()
	path := "/api/v1/namespaces/" + input.Pin.Parent.Ref.Name + "/configmaps/" + input.Pin.Cell.Record.Ref.Name
	current := request(t, "GET", path, nil, 200)
	encoded, _ := json.Marshal(current)
	var cell corev1.ConfigMap
	if json.Unmarshal(encoded, &cell) != nil {
		t.Fatal("invalid fixture cell")
	}
	envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for index := range envelope.Writes {
		if envelope.Writes[index].Preconditions.Kind == "Service" {
			envelope.Writes[index].ResultSpecDigest = ""
			envelope.Writes[index].ResultSpecContract = ""
			found = true
		}
	}
	if !found {
		t.Fatal("fixture lacks committed Service row")
	}
	// Explicit fixture-admin corruption: valid outer checksum isolates missing receipt.
	envelope.Digest = ""
	raw, _ := json.Marshal(envelope)
	sum := sha256.Sum256(raw)
	envelope.Digest = hex.EncodeToString(sum[:])
	raw, _ = json.Marshal(envelope)
	if _, err := journal.Decode(raw); !errors.Is(err, journal.ErrInvalid) {
		t.Fatal("lost receipt remained valid journal")
	}
	cell.Data[kubejournal.DataKey] = string(raw)
	desired := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: cell.Name, Namespace: cell.Namespace, Labels: cell.Labels}, Data: cell.Data}
	fingerprint, err := provider.ConfigMapFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	cell.Annotations[provider.FingerprintAnnotation] = fingerprint
	request(t, "PUT", path, &cell, 200)
}

func TestRuntimeServiceCleanOpenRejectsReceiptLossSpecDriftAndCanonicalUIDChange(t *testing.T) {
	for _, mode := range []string{"receipt-loss", "spec-drift", "missing", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			input, store, _ := serviceProcessFixture(t, "deny-"+mode)
			endpoint, ca, posts, gets := serviceDispatchProxy(t, input, store, false)
			input.Endpoint, input.ProxyCA = endpoint, ca
			first := runServiceProcess(t, input, "initial")
			input.Checkpoint, input.WantOpenError = first.Checkpoint, runtimeconfig.Changed
			path := "/api/v1/namespaces/" + first.Receipt.Record.Parent.Name + "/services/" + first.Receipt.Record.Record.Ref.Name
			before := request(t, "GET", path, nil, 200)
			switch mode {
			case "receipt-loss":
				corruptServiceAllocationReceipt(t, input)
				input.WantOpenError = runtimeconfig.Invalid
			case "spec-drift":
				ports := field(before, "spec")["ports"].([]any)
				ports[0].(map[string]any)["targetPort"] = 8081
				after := request(t, "PUT", path, before, 200)
				if text(metadata(after), "uid") != first.Receipt.Record.Record.Ref.ProviderID {
					t.Fatal("spec drift fixture replaced UID")
				}
			default:
				request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": first.Receipt.Record.Record.Ref.ProviderID, "resourceVersion": text(metadata(before), "resourceVersion")}}, 200)
				wait(t, func() bool {
					response, err := client.Get(local.Server + path)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					return response.StatusCode == http.StatusNotFound
				})
				if mode == "replacement" {
					for _, key := range []string{"uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "deletionTimestamp", "deletionGracePeriodSeconds"} {
						delete(metadata(before), key)
					}
					delete(before, "status")
					// Fixture admin copies ownership metadata/allocation; new UID still rejects.
					replacement := request(t, "POST", "/api/v1/namespaces/"+first.Receipt.Record.Parent.Name+"/services", before, 201)
					if text(metadata(replacement), "uid") == first.Receipt.Record.Record.Ref.ProviderID {
						t.Fatal("fixture failed to replace Service")
					}
				}
			}
			previousGets := gets.Load()
			runServiceProcess(t, input, "deny")
			if posts.Load() != 1 {
				t.Fatal("lost receipt/body/UID caused managed Service POST")
			}
			if mode == "receipt-loss" && gets.Load() != previousGets {
				t.Fatal("malformed receipt journal consulted Service to learn allocation")
			}
			if mode != "receipt-loss" {
				_, repository := retirementConfig(t, input.Pin, "", nil)
				snapshot, err := repository.Load(context.Background())
				if err != nil || snapshot.Envelope.Admission == nil || snapshot.Envelope.Admission.State != journal.AdmissionReady {
					t.Fatal("failed Service preflight consumed clean admission", err)
				}
			}
		})
	}
}
