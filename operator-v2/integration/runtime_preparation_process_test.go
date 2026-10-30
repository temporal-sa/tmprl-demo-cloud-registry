//go:build integration && preparation_integration

package integration

// Private supervisor/process scaffolding. No Plan/Pair/Context is marshaled;
// the explicit accepted bootstrap is private stdin only, never public history.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/candidateprep"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

const preparationPrivateMarker = "private-preparation-original-literal"

// Duplicated deliberately in the host release parser and checked by a pure
// source contract. Only these authored assertion codes may cross private stdio.
var preparationFailureCodes = []string{
	"ChildExecutionFailed", "PreparationPureBootstrapDenialMismatch", "PreparationAcceptedBootstrapUnavailable", "PreparationCheckpointInvalid", "PreparationWorkerAdmissionFailed", "PreparationSADriftAdmitted", "PreparationDesiredUnavailable", "PreparationNamespaceAcceptanceFailed", "PreparationBoundPairRenderFailed", "PreparationTypedPairUnavailable", "PreparationSAObservationFaultMissing", "PreparationSAHistoricalReceiptOrUnknownCurrentPolicyLost", "PreparationSAFaultKindLost", "PreparationSAFaultTargetLost", "PreparationCurrentMinimalSAPolicyUnproven", "PreparationDeploymentApplyFailed", "PreparationServiceAllocationUnproven", "PreparationDeploymentObservationFailed", "PreparationTaintDidNotPreventReadiness", "PreparationCurrentReadyControllerFactsUnproven", "PreparationFinalJournalUnavailable", "PreparationOriginalSAFaultLost", "PreparationCanonicalWriteUnproven", "PreparationJournalUnavailable", "PreparationQuiesceFailed", "PreparationPrivateReceiptInvalid", "PreparationRawPayloadEscapedReceipt", "PreparationPrivateInputInvalid",
}

func preparationChildAssertion(raw []byte) string {
	pattern := regexp.MustCompile(`^\s*(?:runtime_preparation_process_test|zzzzz_candidate_preparation_test|zzzzz_native_serviceaccount_receipt_test)\.go:[0-9]+: ([A-Za-z]+)$`)
	for _, line := range strings.Split(string(raw), "\n") {
		match := pattern.FindStringSubmatch(line)
		if len(match) != 2 {
			continue
		}
		for _, code := range preparationFailureCodes {
			if match[1] == code {
				return code
			}
		}
	}
	return "ChildExecutionFailed"
}

const preparationDocument = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: preparation-example
spec:
  source:
    repo: https://github.com/temporal-sa/private-preparation-source
  builds:
    app:
      context: .
      dockerfile: Dockerfile
  components:
    backend:
      build: app
      serviceName: backend-api
      replicas: 1
      command: [/fixture]
      args: [--revision, private-preparation-original-literal]
      automountServiceAccountToken: false
      env:
        - name: PRIVATE_LITERAL
          value: private-preparation-original-literal
      ports:
        - name: http
          containerPort: 8080
`

type preparationBootstrap struct {
	Raw                []byte
	Authority          ownership.Config
	Subject            rollout.Subject
	Generation         int64
	Attempt, Component string
	Profile            candidateprep.Profile
	Images             map[string]string
}

type preparationProcessInput struct {
	Base             privateRuntimeProcessInput
	Accepted         []byte
	ExpectedViewHash string
}

type preparationProcessOutput struct {
	Checkpoint []byte
	ViewHash   string
	State      string
	Denied     string
	Namespace  runtimeconfig.Receipt
	SA         runtimeconfig.ServiceAccountReceipt
	Deployment runtimeconfig.DeploymentReceipt
	Service    runtimeconfig.ServiceReceipt
	Writes     map[string]journal.Write
	Primary    *diagnostics.DomainFailure
}

func preparationViewHash(plan candidateprep.Plan) string {
	data, _ := json.Marshal(plan.View())
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func acceptedPreparation(input preparationProcessInput) (candidateprep.Plan, string) {
	if len(input.Accepted) == 0 {
		return candidateprep.Plan{}, "PreparationBootstrapMissing"
	}
	if len(input.Accepted) > 16384 || len(input.ExpectedViewHash) != 64 {
		return candidateprep.Plan{}, "PreparationBootstrapInvalid"
	}
	var bootstrap preparationBootstrap
	decoder := json.NewDecoder(bytes.NewReader(input.Accepted))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&bootstrap) != nil || decoder.Decode(new(any)) != io.EOF || bootstrap.Subject != input.Base.Pin.Subject || len(bootstrap.Images) != 1 || bootstrap.Images["app"] != local.ImageDigest {
		return candidateprep.Plan{}, "PreparationBootstrapInvalid"
	}
	document, err := project.Decode(bootstrap.Raw)
	if err != nil {
		return candidateprep.Plan{}, "PreparationBootstrapInvalid"
	}
	authority, err := ownership.NewContext(bootstrap.Authority)
	if err != nil {
		return candidateprep.Plan{}, "PreparationBootstrapInvalid"
	}
	scope, err := authority.Scope(ownership.Kubernetes)
	if err != nil || scope.ClusterID != local.RunID || scope.Namespace != "" || authority.Owner().RunID != local.RunID {
		return candidateprep.Plan{}, "PreparationBootstrapInvalid"
	}
	plan, err := candidateprep.New(candidateprep.Config{Document: document, Authority: authority, Subject: bootstrap.Subject, Generation: bootstrap.Generation, AttemptID: bootstrap.Attempt, Component: bootstrap.Component, Images: bootstrap.Images, Profile: bootstrap.Profile})
	if err != nil {
		return candidateprep.Plan{}, "PreparationBootstrapInvalid"
	}
	if preparationViewHash(plan) != input.ExpectedViewHash {
		return candidateprep.Plan{}, "PreparationBootstrapChanged"
	}
	return plan, ""
}

func newPreparationProcessFixture(t *testing.T, suffix string) (preparationProcessInput, preparationBootstrap, *runtimeinputs.Store) {
	t.Helper()
	pin := runtimeControlPin(t, "preparation-"+suffix)
	owner := ownership.Owner{EnvironmentID: pin.Subject.Environment, InstallationID: pin.Subject.Installation, ProjectUID: pin.Subject.ProjectUID, Lifetime: ownership.Disposable, RunID: local.RunID}
	bootstrap := preparationBootstrap{Raw: []byte(preparationDocument), Authority: ownership.Config{Owner: owner, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}}, Subject: pin.Subject, Generation: 1, Attempt: runtimeAttempt(pin), Component: "backend", Profile: candidateprep.LocalLiteralPairV1, Images: map[string]string{"app": local.ImageDigest}}
	document, err := project.Decode(bootstrap.Raw)
	if err != nil {
		t.Fatal("PreparationFixtureDocumentInvalid")
	}
	authority, err := ownership.NewContext(bootstrap.Authority)
	if err != nil {
		t.Fatal("PreparationFixtureAuthorityInvalid")
	}
	plan, err := candidateprep.New(candidateprep.Config{Document: document, Authority: authority, Subject: bootstrap.Subject, Generation: bootstrap.Generation, AttemptID: bootstrap.Attempt, Component: bootstrap.Component, Images: bootstrap.Images, Profile: bootstrap.Profile})
	if err != nil {
		t.Fatal("PreparationFixturePlanInvalid")
	}
	// This encoding is the ONE explicit private supervisor freeze, before any
	// candidate namespace acceptance. It carries Config, never private Context.
	accepted, err := json.Marshal(bootstrap)
	if err != nil {
		t.Fatal("PreparationFixtureBootstrapInvalid")
	}
	directory := t.TempDir()
	if os.Chmod(directory, 0700) != nil {
		t.Fatal("PreparationPrivateRootInvalid")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal("PreparationPrivateRootUnavailable")
	}
	t.Cleanup(func() { root.Close() })
	var nonce [32]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal("PreparationPrivateNonceUnavailable")
	}
	storeConfig := runtimeinputs.Config{BootstrapUID: hex.EncodeToString(nonce[:]), Owner: owner, CellUID: pin.Cell.Record.Ref.ProviderID, MaximumEntries: 8, ServiceAccounts: true}
	store, storePin, err := runtimeinputs.Bootstrap(context.Background(), root, storeConfig)
	if err != nil {
		t.Fatal("PreparationSAInputStoreUnavailable")
	}
	base := privateRuntimeProcessInput{Pin: pin, Directory: directory, StoreConfig: storeConfig, StorePin: storePin}
	return preparationProcessInput{Base: base, Accepted: append([]byte(nil), accepted...), ExpectedViewHash: preparationViewHash(plan)}, bootstrap, store
}

func decodePreparationInput(t *testing.T) preparationProcessInput {
	t.Helper()
	var input preparationProcessInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32769))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		t.Fatal("PreparationPrivateInputInvalid")
	}
	return input
}

func emitPreparationPrivate(t *testing.T, output preparationProcessOutput) {
	t.Helper()
	data, err := json.Marshal(output)
	if err != nil || len(data) > 32768 {
		t.Fatal("PreparationPrivateReceiptInvalid")
	}
	for _, forbidden := range []string{preparationPrivateMarker, "https://github.com/temporal-sa/private-preparation-source", local.ImageDigest} {
		if forbidden != "" && bytes.Contains(data, []byte(forbidden)) {
			t.Fatal("PreparationRawPayloadEscapedReceipt")
		}
	}
	_, _ = io.WriteString(os.Stdout, "PREPARATION-PRIVATE "+string(data)+"\n")
}

func runPreparationChild(t *testing.T, name, phase string, input preparationProcessInput) preparationProcessOutput {
	t.Helper()
	if name != "TestNativePreparationPlanAcrossCleanWaitHandoff" && name != "TestRuntimeServiceAccountReceiptBeforeFailedObservation" {
		t.Fatal("PreparationChildCaseInvalid")
	}
	allowed := map[string]bool{"wait": true, "ready": true, "missing-bootstrap": true, "changed-bootstrap": true, "sa-drift": true, "sa-fault": true, "sa-inspect": true}
	if !allowed[phase] {
		t.Fatal("PreparationChildPhaseInvalid")
	}
	caseName := "plan-wait"
	if name == "TestRuntimeServiceAccountReceiptBeforeFailedObservation" {
		caseName = "sa403"
	}
	progress, _ := json.Marshal(map[string]string{"case": caseName, "phase": phase})
	_, _ = io.WriteString(os.Stdout, "PREPARATION-PROGRESS "+string(progress)+"\n")
	data, err := json.Marshal(input)
	if err != nil || len(data) > 32768 {
		t.Fatal("PreparationPrivateInputOverflow")
	}
	command := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.v", "-test.timeout=100s")
	command.Env = []string{"PATH=/unavailable", "HOME=/tmp/owned-preparation", "TMPDIR=/tmp", "OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, "OPERATOR_LOCAL_PREPARATION_FIXTURE=1", "OPERATOR_PREPARATION_PHASE=" + phase}
	command.Stdin = bytes.NewReader(data)
	raw, runErr := command.CombinedOutput() // Returns only after actual Wait/reap.
	if runErr != nil || command.ProcessState == nil || !command.ProcessState.Exited() || !command.ProcessState.Success() || len(raw) > 1<<20 {
		// Do NOT echo private child output or thrown/provider errors.
		code := "ChildExecutionFailed"
		if len(raw) <= 1<<20 {
			code = preparationChildAssertion(raw)
		}
		failure, _ := json.Marshal(map[string]string{"case": caseName, "phase": phase, "code": code})
		_, _ = io.WriteString(os.Stdout, "PREPARATION-FAILURE "+string(failure)+"\n")
		t.Fatal("PreparationPrivateChildFailed")
	}
	var output preparationProcessOutput
	count := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "PREPARATION-PRIVATE ") {
			count++
			decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(line, "PREPARATION-PRIVATE ")))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&output) != nil || decoder.Decode(new(any)) != io.EOF {
				t.Fatal("PreparationPrivateReceiptInvalid")
			}
		}
	}
	if count != 1 {
		t.Fatal("PreparationPrivateReceiptCountInvalid")
	}
	return output
}

func preparationRuntime(t *testing.T, input preparationProcessInput) (runtimeconfig.Config, *journal.Repository) {
	t.Helper()
	config, repository := retirementConfig(t, input.Base.Pin, input.Base.Endpoint, input.Base.ProxyCA)
	store, _ := openRuntimeInputs(t, input.Base)
	config.DeploymentInputs, config.ServiceInputs, config.ServiceAccountInputs = store, store, store
	config.Diagnostics = &runtimeconfig.DiagnosticConfig{ProjectName: "preparation-example"}
	return config, repository
}

func preparationWrites(t *testing.T, repository *journal.Repository) map[string]journal.Write {
	t.Helper()
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal("PreparationJournalUnavailable")
	}
	writes := map[string]journal.Write{}
	for _, write := range snapshot.Envelope.Writes {
		kind := write.Preconditions.Kind
		if writes[kind].ID != "" || write.State != journal.Committed || write.ResultUID == "" || write.ResultVersion == "" || kind != "Namespace" && !write.Input.MatchesKind(kind) {
			t.Fatal("PreparationCanonicalWriteUnproven")
		}
		writes[kind] = write
	}
	return writes
}

func preparationFinish(t *testing.T, worker *runtimeconfig.Worker, repository *journal.Repository, output *preparationProcessOutput) {
	t.Helper()
	output.Writes = preparationWrites(t, repository)
	checkpoint, err := worker.Quiesce(context.Background())
	if err != nil {
		t.Fatal("PreparationQuiesceFailed")
	}
	output.Checkpoint, err = checkpoint.JSON()
	if err != nil {
		t.Fatal("PreparationCheckpointInvalid")
	}
	emitPreparationPrivate(t, *output)
}

func preparationProcess(t *testing.T, phase string) {
	t.Helper()
	input := decodePreparationInput(t)
	plan, denied := acceptedPreparation(input)
	if phase == "missing-bootstrap" || phase == "changed-bootstrap" {
		expected := "PreparationBootstrapMissing"
		if phase == "changed-bootstrap" {
			expected = "PreparationBootstrapChanged"
		}
		if denied != expected {
			t.Fatal("PreparationPureBootstrapDenialMismatch")
		}
		emitPreparationPrivate(t, preparationProcessOutput{Denied: denied})
		return
	}
	if denied != "" {
		t.Fatal("PreparationAcceptedBootstrapUnavailable")
	}
	// Pure accepted-input validation above precedes any provider client/RPC.
	TestOwnedAPIAndNetworkBoundary(t)
	config, repository := preparationRuntime(t, input)
	var worker *runtimeconfig.Worker
	var err error
	if phase == "wait" || phase == "sa-fault" {
		worker, err = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		checkpoint, e := runtimeconfig.ParseCheckpoint(input.Base.Checkpoint)
		if e != nil {
			t.Fatal("PreparationCheckpointInvalid")
		}
		worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
	}
	if phase == "sa-drift" {
		var changed *runtimeconfig.Error
		if !errors.As(err, &changed) || changed.Code != runtimeconfig.Changed {
			t.Fatal("PreparationSADriftAdmitted")
		}
		emitPreparationPrivate(t, preparationProcessOutput{Denied: string(changed.Code)})
		return
	}
	if err != nil {
		t.Fatal("PreparationWorkerAdmissionFailed")
	}
	desired, err := plan.Desired()
	if err != nil {
		t.Fatal("PreparationDesiredUnavailable")
	}
	var session *runtimeconfig.Session
	if phase == "sa-inspect" {
		session, err = worker.ObserveNamespace(context.Background(), desired, plan.View().OperationID)
	} else {
		session, err = worker.EnsureNamespace(context.Background(), desired, plan.View().OperationID)
	}
	if err != nil {
		t.Fatal("PreparationNamespaceAcceptanceFailed")
	}
	pair, err := plan.Render(session.Context())
	if err != nil {
		t.Fatal("PreparationBoundPairRenderFailed")
	}
	sa, e1 := pair.ServiceAccount()
	deployment, e2 := pair.Deployment()
	service, e3 := pair.Service()
	if e1 != nil || e2 != nil || e3 != nil {
		t.Fatal("PreparationTypedPairUnavailable")
	}
	output := preparationProcessOutput{ViewHash: preparationViewHash(plan)}
	if phase == "sa-inspect" {
		_, e := session.ApplyServiceAccount(context.Background(), sa)
		requireNativeMutationBlocked(t, e)
		output.SA, err = session.ObserveServiceAccount(context.Background(), sa)
	} else {
		output.SA, err = session.ApplyServiceAccount(context.Background(), sa)
	}
	if phase == "sa-fault" {
		var failure *runtimeconfig.Error
		if !errors.As(err, &failure) || failure.Fault == nil || !failure.Fault.Valid() {
			t.Fatal("PreparationSAObservationFaultMissing")
		}
		v := failure.Fault.View()
		if v.Dispatch != runtimefault.Applied || v.Condition != runtimefault.ConditionForbidden || v.Step != runtimefault.Observe || v.JournalDelivery.State != runtimefault.Acknowledged || v.DiagnosticDelivery.State != runtimefault.Acknowledged || output.SA.Record.Record.Ref.ProviderID == "" || output.SA.CreationResourceVersion == "" || output.SA.ResourceVersion != "" || output.SA.ObservedMinimalPolicy {
			t.Fatal("PreparationSAHistoricalReceiptOrUnknownCurrentPolicyLost")
		}
		if v.Failure.Code != string(provider.ChildUnavailable) {
			t.Fatal("PreparationSAFaultKindLost")
		}
		if v.Target.Kind != runtimefault.ServiceAccount || v.Target.Purpose != "service-account" || v.Target.Name != output.SA.Record.Record.Ref.Name || v.Target.NamespaceUID != output.SA.Record.Parent.UID {
			t.Fatal("PreparationSAFaultTargetLost")
		}
		_, e := session.ApplyDeployment(context.Background(), deployment)
		requireNativeMutationBlocked(t, e)
		_, e = session.ApplyService(context.Background(), service)
		requireNativeMutationBlocked(t, e)
		output.State = "Forbidden"
	} else {
		if err != nil || !output.SA.ObservedMinimalPolicy || output.SA.ResourceVersion == "" || output.SA.CreationResourceVersion == "" {
			t.Fatal("PreparationCurrentMinimalSAPolicyUnproven")
		}
		if phase == "sa-inspect" {
			_, e := session.ApplyDeployment(context.Background(), deployment)
			requireNativeMutationBlocked(t, e)
			_, e = session.ApplyService(context.Background(), service)
			requireNativeMutationBlocked(t, e)
			output.State = "Inspected"
		} else {
			output.Deployment, err = session.ApplyDeployment(context.Background(), deployment)
			if err != nil {
				t.Fatal("PreparationDeploymentApplyFailed")
			}
			output.Service, err = session.ApplyService(context.Background(), service)
			if err != nil || !output.Service.ObservedAllocated || output.Service.AllocatedSpecDigest == "" {
				t.Fatal("PreparationServiceAllocationUnproven")
			}
			expected := "Waiting"
			if phase == "ready" {
				expected = "Ready"
			}
			wait(t, func() bool {
				output.Deployment, err = session.ObserveDeployment(context.Background(), deployment)
				if err != nil {
					t.Fatal("PreparationDeploymentObservationFailed")
				}
				v := output.Deployment.Readiness
				return v.State == expected && v.Generation > 0 && v.ObservedGeneration == v.Generation
			})
			v := output.Deployment.Readiness
			if phase == "wait" && (v.Ready != 0 || v.Available != 0) {
				t.Fatal("PreparationTaintDidNotPreventReadiness")
			}
			if phase == "ready" && (v.Ready != v.Desired || v.Available != v.Desired || v.Desired != 1) {
				t.Fatal("PreparationCurrentReadyControllerFactsUnproven")
			}
			output.State = expected
		}
	}
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal("PreparationFinalJournalUnavailable")
	}
	if snapshot.Envelope.Projection != nil {
		output.Primary = snapshot.Envelope.Projection.Failure
	}
	if (phase == "sa-fault" || phase == "sa-inspect") && output.Primary == nil {
		t.Fatal("PreparationOriginalSAFaultLost")
	}
	preparationFinish(t, worker, repository, &output)
}

func preparationPublic(t *testing.T, caseName string) {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"case": caseName, "waiting": caseName == "plan-wait", "processExited": true, "canonical": true, "noRepost": true, "promotion": candidateprep.PromotionNotAuthorized})
	_, _ = io.WriteString(os.Stdout, "PREPARATION-PUBLIC "+string(data)+"\n")
}
