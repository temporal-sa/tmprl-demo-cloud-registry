//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

type nativeProcessInput struct {
	Base privateRuntimeProcessInput
	Kind runtimefault.Kind
}
type nativeProcessOutput struct {
	Checkpoint      []byte
	Fault           []byte
	SecondaryFault  []byte
	Write           journal.Write
	Record          ownership.NamespacedRecord
	ResourceVersion string
	Primary         diagnostics.DomainFailure
}

func nativeWrite(t *testing.T, repository *journal.Repository, kind runtimefault.Kind) journal.Write {
	t.Helper()
	snapshot, e := repository.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for _, write := range snapshot.Envelope.Writes {
		if write.Preconditions.Kind == string(kind) {
			if write.State != journal.Committed || write.ResultUID == "" || write.ResultVersion == "" || !write.Input.MatchesKind(string(kind)) {
				t.Fatal("native creation result not fully committed")
			}
			return write
		}
	}
	t.Fatal("native create journal row missing")
	return journal.Write{}
}
func nativeStoredObject(t *testing.T, input nativeProcessInput, store *runtimeinputs.Store, session *runtimeconfig.Session, write journal.Write) renderer.DesiredObject {
	t.Helper()
	p := write.Preconditions
	d := desiredRuntimeDeployment(input.Base.Pin)
	target := runtimeinputs.Target{CellUID: input.Base.Pin.Cell.Record.Ref.ProviderID, Parent: session.Context().Parent(), AttemptID: runtimeAttempt(input.Base.Pin), Revision: d.Revision, DesiredDigest: journal.DesiredDigest(d), Name: p.Name, Fingerprint: p.Fingerprint}
	intent, e := session.Context().Intent(map[runtimefault.Kind]string{runtimefault.Deployment: "deployment", runtimefault.Service: "service"}[input.Kind], p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
	if e != nil {
		t.Fatal(e)
	}
	// Resolve only the original accepted private input; never derive desired from GET.
	if input.Kind == runtimefault.Deployment {
		payload, e := store.Resolve(context.Background(), write.Input, target)
		if e != nil {
			t.Fatal(e)
		}
		return renderer.DesiredObject{Object: payload.ObjectForProvider(), Intent: intent}
	}
	payload, e := store.ResolveService(context.Background(), write.Input, target)
	if e != nil {
		t.Fatal(e)
	}
	return renderer.DesiredObject{Object: payload.ObjectForProvider(), Intent: intent}
}
func runtimeNativeProcess(t *testing.T, phase string) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input nativeProcessInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Kind != runtimefault.Deployment && input.Kind != runtimefault.Service {
		t.Fatal("invalid owned native receipt input")
	}
	config, repository := retirementConfig(t, input.Base.Pin, input.Base.Endpoint, input.Base.ProxyCA)
	config.Diagnostics = &runtimeconfig.DiagnosticConfig{ProjectName: "runtime-example"}
	store, _ := openRuntimeInputs(t, input.Base)
	config.DeploymentInputs, config.ServiceInputs = store, store
	var worker *runtimeconfig.Worker
	var e error
	if phase == "initial" {
		worker, e = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		checkpoint, err := runtimeconfig.ParseCheckpoint(input.Base.Checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		worker, e = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
	}
	if e != nil {
		t.Fatal(e)
	}
	desired := desiredRuntimeDeployment(input.Base.Pin)
	var session *runtimeconfig.Session
	if phase == "initial" {
		session, e = worker.EnsureNamespace(context.Background(), desired, runtimeAttempt(input.Base.Pin))
	} else {
		_, err := worker.EnsureNamespace(context.Background(), desired, runtimeAttempt(input.Base.Pin))
		requireNativeMutationBlocked(t, err)
		session, e = worker.ObserveNamespace(context.Background(), desired, runtimeAttempt(input.Base.Pin))
	}
	if e != nil {
		t.Fatal(e)
	}
	var object renderer.DesiredObject
	if phase == "initial" {
		if input.Kind == runtimefault.Deployment {
			// Explicit fixture administrator prerequisite, not runtime bundle parity.
			request(t, "POST", "/api/v1/namespaces/"+session.Context().NamespaceName()+"/serviceaccounts", objectMapServiceAccount(), 201)
			object = deploymentFromFixture(t, session, input.Base.Pin, false)
		} else {
			object = serviceFromRuntimeFixture(t, session, input.Base.Pin, false)
		}
	} else {
		object = nativeStoredObject(t, input, store, session, nativeWrite(t, repository, input.Kind))
	}
	var output nativeProcessOutput
	if input.Kind == runtimefault.Deployment {
		if phase == "initial" {
			_, e = session.ApplyDeployment(context.Background(), object)
		} else {
			_, err := session.ApplyDeployment(context.Background(), object)
			requireNativeMutationBlocked(t, err)
			receipt, err := session.ObserveDeployment(context.Background(), object)
			e = err
			output.Record, output.ResourceVersion = receipt.Record, receipt.ResourceVersion
		}
	} else {
		if phase == "initial" {
			_, e = session.ApplyService(context.Background(), object)
		} else {
			_, err := session.ApplyService(context.Background(), object)
			requireNativeMutationBlocked(t, err)
			receipt, err := session.ObserveService(context.Background(), object)
			e = err
			output.Record, output.ResourceVersion = receipt.Record, receipt.ResourceVersion
		}
	}
	if phase == "initial" {
		var failure *runtimeconfig.Error
		if !errors.As(e, &failure) || failure.Fault == nil || !failure.Fault.Valid() {
			t.Fatal("known POST observation lost original fault", e)
		}
		fault := failure.Fault.View()
		if fault.Dispatch != runtimefault.Applied || fault.Step != runtimefault.Observe || fault.Failure.Code != string(provider.ChildUnavailable) || fault.JournalDelivery.State != runtimefault.Acknowledged || fault.DiagnosticDelivery.State != runtimefault.Acknowledged {
			t.Fatal("known POST relabeled as unknown mutation")
		}
		output.Fault, e = json.Marshal(*failure.Fault)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = runtimefault.ParseFault(output.Fault); e != nil {
			t.Fatal(e)
		}
		if bytes.Contains(output.Fault, []byte(runtimeServicePrivateMarker)) || bytes.Contains(output.Fault, []byte(runtimeDeploymentPrivateMarker)) || bytes.Contains(output.Fault, []byte(input.Base.Directory)) {
			t.Fatal("raw payload/path escaped native observation fault")
		}
		if fault.Condition != runtimefault.ConditionForbidden {
			t.Fatal("first observation lost finite forbidden condition")
		}
		before, err := repository.Load(context.Background())
		if err != nil || before.Envelope.Projection == nil {
			t.Fatal("first acknowledged cause absent", err)
		}
		original := *before.Envelope.Projection
		originalWrite := nativeWrite(t, repository, input.Kind)
		var readErr error
		if input.Kind == runtimefault.Deployment {
			_, readErr = session.ObserveDeployment(context.Background(), object)
		} else {
			_, readErr = session.ObserveService(context.Background(), object)
		}
		var secondary *runtimeconfig.Error
		if !errors.As(readErr, &secondary) || secondary.Fault == nil || !secondary.Fault.Valid() {
			t.Fatal("second inspection lost safe fault", readErr)
		}
		second := secondary.Fault.View()
		if second.Dispatch != runtimefault.Applied || second.Condition != runtimefault.ConditionNotFound || second.JournalDelivery.State != runtimefault.Acknowledged || second.DiagnosticDelivery.State != runtimefault.Acknowledged {
			t.Fatal("second read fault not independently acknowledged")
		}
		output.SecondaryFault, err = json.Marshal(*secondary.Fault)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = runtimefault.ParseFault(output.SecondaryFault); err != nil {
			t.Fatal(err)
		}
		for _, marker := range []string{runtimeServicePrivateMarker, runtimeDeploymentPrivateMarker, input.Base.Directory} {
			if bytes.Contains(output.SecondaryFault, []byte(marker)) {
				t.Fatal("private second-read data escaped activity fault")
			}
		}
		after, err := repository.Load(context.Background())
		if err != nil || after.Envelope.Projection == nil {
			t.Fatal("secondary projection unavailable", err)
		}
		projection := after.Envelope.Projection
		if !reflect.DeepEqual(projection.Failure, original.Failure) || projection.State != original.State || projection.Step != original.Step || !reflect.DeepEqual(projection.Retry, original.Retry) || !reflect.DeepEqual(projection.Availability, original.Availability) || nativeWrite(t, repository, input.Kind) != originalWrite {
			t.Fatal("second read fault changed original cause/state/retry/committed receipt")
		}
		raw, err := projection.JSON()
		if err != nil {
			t.Fatal(err)
		}
		var persisted diagnostics.OperationProjection
		if json.Unmarshal(raw, &persisted) != nil {
			t.Fatal("actual projection wire unavailable")
		}
		visible := false
		for _, warning := range persisted.Warnings {
			if canonicalFailureWireEqual(warning, second.Failure) {
				visible = true
			}
		}
		if !visible {
			t.Fatal("acknowledged second read fault was silently dropped from actual projection JSON")
		}
	} else if e != nil {
		t.Fatal(e)
	}
	output.Write = nativeWrite(t, repository, input.Kind)
	snapshot, e := repository.Load(context.Background())
	if e != nil || snapshot.Envelope.Projection == nil || snapshot.Envelope.Projection.Failure == nil {
		t.Fatal("original observation cause absent", e)
	}
	output.Primary = *snapshot.Envelope.Projection.Failure
	checkpoint, e := worker.Quiesce(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	output.Checkpoint, e = checkpoint.JSON()
	if e != nil {
		t.Fatal(e)
	}
	data, e := json.Marshal(output)
	if e != nil {
		t.Fatal(e)
	}
	fmt.Println("NATIVE-CREATE-PROCESS " + string(data))
}
func objectMapServiceAccount() object {
	return object{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": object{"name": "app", "labels": object{"operator.local/run-id": local.RunID}}, "automountServiceAccountToken": false}
}
func runNativeProcess(t *testing.T, input nativeProcessInput, phase string) nativeProcessOutput {
	t.Helper()
	var output nativeProcessOutput
	runPrivateRuntimeProcess(t, "TestRuntimeNativeReceiptPrecedesObservationAcrossCleanProcesses", "OPERATOR_NATIVE_CREATE_PROCESS", "NATIVE-CREATE-PROCESS ", phase, input, &output, runtimeServicePrivateMarker, runtimeDeploymentPrivateMarker, input.Base.Directory)
	return output
}
func TestRuntimeNativeReceiptPrecedesObservationAcrossCleanProcesses(t *testing.T) {
	if phase := os.Getenv("OPERATOR_NATIVE_CREATE_PROCESS"); phase != "" {
		runtimeNativeProcess(t, phase)
		return
	}
	for _, kind := range []runtimefault.Kind{runtimefault.Deployment, runtimefault.Service} {
		t.Run(string(kind), func(t *testing.T) {
			base, _, _ := runtimeInputProcessFixture(t, "native", strings.ToLower(string(kind)))
			input := nativeProcessInput{Base: base, Kind: kind}
			endpoint, ca, capture := nativeCreationProxy(t, input)
			input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
			first := runNativeProcess(t, input, "initial")
			created, posts, checked := capture.snapshot()
			if posts != 1 || checked < 2 || len(first.Checkpoint) == 0 || first.Write.ResultUID != created.UID || first.Write.ResultVersion != created.Version {
				t.Fatal("native receipt was learned after GET or create repeated")
			}
			if kind == runtimefault.Service && (first.Write.ResultSpecDigest != created.AllocatedDigest || first.Write.ResultSpecContract != created.SpecContract) {
				t.Fatal("Service allocation provenance not committed before GET")
			}
			input.Base.Checkpoint = first.Checkpoint
			second := runNativeProcess(t, input, "replay")
			_, posts, _ = capture.snapshot()
			if posts != 1 || second.Write != first.Write || second.Record.Record.Ref.ProviderID != created.UID || second.ResourceVersion == "" || !reflect.DeepEqual(second.Primary, first.Primary) {
				t.Fatal("fresh read-only process changed canonical receipt/cause or reposted")
			}
		})
	}
}

func requireNativeMutationBlocked(t *testing.T, err error) {
	t.Helper()
	var denied *runtimeconfig.Error
	if !errors.As(err, &denied) || denied.Code != runtimeconfig.Blocked {
		t.Fatal("failed operation did not explicitly block mutation", err)
	}
}
