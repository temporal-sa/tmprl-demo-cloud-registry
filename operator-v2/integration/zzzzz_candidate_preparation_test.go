//go:build integration && preparation_integration

package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	corev1 "k8s.io/api/core/v1"
)

func preparationSnapshotBytes(t *testing.T, input preparationProcessInput) []byte {
	t.Helper()
	value, version := preparationVersionedEnvelope(t, input)
	data, err := json.Marshal(struct {
		Envelope journal.Envelope
		Version  journal.Version
	}{value, version})
	if err != nil {
		t.Fatal("PreparationSnapshotInvalid")
	}
	return data
}

func preparationCaptureCounters(capture *preparationCapture) (map[string]int, map[string]int, int, int) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	posts, checks := map[string]int{}, map[string]int{}
	for k, v := range capture.posts {
		posts[k] = v
	}
	for k, v := range capture.checks {
		checks[k] = v
	}
	return posts, checks, capture.mutations, capture.reads
}

func preparationRequireCanonical(t *testing.T, first, next preparationProcessOutput, count int) {
	t.Helper()
	if first.ViewHash == "" || first.ViewHash != next.ViewHash || len(first.Writes) != count || !reflect.DeepEqual(first.Writes, next.Writes) || first.SA.Record != next.SA.Record || first.SA.CreationResourceVersion != next.SA.CreationResourceVersion || !next.SA.ObservedMinimalPolicy {
		t.Fatal("PreparationOriginalNativeReceiptsOrInputsChangedAcrossProcessExit")
	}
	if count == 4 && (first.Deployment.Record != next.Deployment.Record || first.Service.Record != next.Service.Record || first.Service.SpecContract != next.Service.SpecContract || first.Service.AllocatedSpecDigest != next.Service.AllocatedSpecDigest || !next.Service.ObservedAllocated) {
		t.Fatal("PreparationCanonicalChildUIDOrAllocationChangedAcrossProcessExit")
	}
}

func preparationAssertCounters(t *testing.T, capture *preparationCapture, kinds []string) {
	t.Helper()
	posts, checks, mutations, _ := preparationCaptureCounters(capture)
	if len(posts) != len(kinds) || mutations != len(kinds) {
		t.Fatal("PreparationUnexpectedManagedMutation")
	}
	for _, kind := range kinds {
		if posts[kind] != 1 || checks[kind] < 1 {
			t.Fatal("PreparationNativePostOrReceiptBeforeReadCountInvalid")
		}
	}
}

// Explicit fixture-admin corruption, never a runtime update/adoption API. The
// exact canonical SA UID and current RV remain conditional; copied metadata
// cannot authorize OpenClean after this same-UID policy drift.
func preparationDriftSA(t *testing.T, output preparationProcessOutput) {
	t.Helper()
	record := output.SA.Record.Record.Ref
	path := "/api/v1/namespaces/" + record.Scope.Namespace + "/serviceaccounts/" + record.Name
	response, err := client.Get(local.Server + path)
	if err != nil {
		t.Fatal("PreparationAdminSAReadFailed")
	}
	var object corev1.ServiceAccount
	err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&object)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || string(object.UID) != record.ProviderID || object.ResourceVersion == "" || object.AutomountServiceAccountToken == nil || *object.AutomountServiceAccountToken {
		t.Fatal("PreparationAdminSACanonicalIdentityInvalid")
	}
	beforeUID, beforeRV := object.UID, object.ResourceVersion
	enabled := true
	object.AutomountServiceAccountToken = &enabled
	body, _ := json.Marshal(object)
	request, err := http.NewRequest(http.MethodPut, local.Server+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal("PreparationAdminSARequestInvalid")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal("PreparationAdminSAUpdateUnknown")
	}
	defer response.Body.Close()
	var actual corev1.ServiceAccount
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&actual) != nil || actual.UID != beforeUID || actual.ResourceVersion == beforeRV || actual.AutomountServiceAccountToken == nil || !*actual.AutomountServiceAccountToken || !reflect.DeepEqual(actual.Annotations, object.Annotations) {
		t.Fatal("PreparationAdminSADriftUnproven")
	}
}

func TestNativePreparationPlanAcrossCleanWaitHandoff(t *testing.T) {
	if os.Getenv("OPERATOR_LOCAL_PREPARATION_FIXTURE") != "1" {
		t.Fatal("PreparationExplicitOwnedRunnerRequired")
	}
	if phase := os.Getenv("OPERATOR_PREPARATION_PHASE"); phase != "" {
		preparationProcess(t, phase)
		return
	}
	preparationNamespaceWitnessContract(t)
	TestOwnedAPIAndNetworkBoundary(t)
	input, currentAuthoring, store := newPreparationProcessFixture(t, "plan")
	endpoint, ca, capture := preparationCreationProxy(t, input, store, false)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	gate := blockPreparationScheduling(t)
	first := runPreparationChild(t, "TestNativePreparationPlanAcrossCleanWaitHandoff", "wait", input)
	if first.State != "Waiting" || len(first.Checkpoint) == 0 || len(first.Writes) != 4 {
		t.Fatal("PreparationCurrentGenerationWaitingOrCleanExitUnproven")
	}
	// CombinedOutput has actually Waited/reaped the old process. Only now may
	// the parent remove scheduling control or launch a successor.
	gate.remove(t)
	if t.Failed() {
		return
	}
	input.Base.Checkpoint = first.Checkpoint
	before := preparationSnapshotBytes(t, input)
	_, _, mutations, reads := preparationCaptureCounters(capture)
	missing := input
	missing.Accepted = nil
	if denied := runPreparationChild(t, "TestNativePreparationPlanAcrossCleanWaitHandoff", "missing-bootstrap", missing); denied.Denied != "PreparationBootstrapMissing" {
		t.Fatal("PreparationMissingAcceptedBytesAdmitted")
	}
	changed := input
	var replacement preparationBootstrap
	_ = json.Unmarshal(input.Accepted, &replacement)
	replacement.Raw = []byte(strings.ReplaceAll(string(replacement.Raw), preparationPrivateMarker, "private-current-authoring"))
	changed.Accepted, _ = json.Marshal(replacement)
	if denied := runPreparationChild(t, "TestNativePreparationPlanAcrossCleanWaitHandoff", "changed-bootstrap", changed); denied.Denied != "PreparationBootstrapChanged" {
		t.Fatal("PreparationChangedAcceptedBytesAdmitted")
	}
	_, _, afterMutations, afterReads := preparationCaptureCounters(capture)
	if !bytes.Equal(before, preparationSnapshotBytes(t, input)) || mutations != afterMutations || reads != afterReads {
		t.Fatal("PreparationBootstrapDenialTouchedAdmissionOrProvider")
	}
	// Current authoring is mutable caller state; use only the supervisor's
	// earlier accepted byte copy, never re-encode this changed state.
	currentAuthoring.Raw[0] = '!'
	currentAuthoring.Images["app"] = "unaccepted-current-image"
	next := runPreparationChild(t, "TestNativePreparationPlanAcrossCleanWaitHandoff", "ready", input)
	if next.State != "Ready" || len(next.Checkpoint) == 0 {
		t.Fatal("PreparationCurrentControllerReadyOrCleanExitUnproven")
	}
	preparationRequireCanonical(t, first, next, 4)
	input.Base.Checkpoint = next.Checkpoint
	preparationDriftSA(t, next)
	before = preparationSnapshotBytes(t, input)
	_, _, mutations, _ = preparationCaptureCounters(capture)
	denied := runPreparationChild(t, "TestNativePreparationPlanAcrossCleanWaitHandoff", "sa-drift", input)
	_, _, afterMutations, _ = preparationCaptureCounters(capture)
	if denied.Denied != string(runtimeconfig.Changed) || mutations != afterMutations || !bytes.Equal(before, preparationSnapshotBytes(t, input)) {
		t.Fatal("PreparationSADriftConsumedReadyOrMutated")
	}
	// Canonical pre-admission GETs are required and permitted. No mutation/new
	// attempt or Ready epoch transition may occur after same-UID policy drift.
	envelope := preparationEnvelope(t, input)
	if envelope.Admission == nil || envelope.Admission.State != journal.AdmissionReady || len(envelope.Attempts) != 1 {
		t.Fatal("PreparationDriftReadyAdmissionChanged")
	}
	preparationAssertCounters(t, capture, []string{"Namespace", "ServiceAccount", "Deployment", "Service"})
	preparationPublic(t, "plan-wait")
}
