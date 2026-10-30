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
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	corev1 "k8s.io/api/core/v1"
)

// This is owned harness bootstrap, never activity/history data. Only the finite
// Outcome below crosses the diagnostic activity/data boundary.
type diagnosticProcessInput struct {
	Base            privateRuntimeProcessInput
	Mode            string
	Desired         ownership.NamespacedRecord
	ExpectedFailure string
}
type diagnosticProcessOutput struct {
	Outcome    []byte
	Checkpoint []byte
	Error      runtimeconfig.Code
	Receipt    runtimeconfig.ServiceReceipt
	Input      journal.InputReference
	Wire       []byte
}

func diagnosticIdentity(input diagnosticProcessInput) diagnostics.Identity {
	d := desiredRuntimeDeployment(input.Base.Pin)
	return diagnostics.Identity{Environment: d.Subject.Environment, Installation: d.Subject.Installation, ProjectUID: d.Subject.ProjectUID, ProjectName: "runtime-example", OperationID: runtimeAttempt(input.Base.Pin), ReconcileKey: d.ReconcileKey, Generation: d.Generation}
}
func fillDiagnosticStore(t *testing.T, store *runtimeinputs.Store, input diagnosticProcessInput, session *runtimeconfig.Session, body *corev1.Service) {
	t.Helper()
	slot := body.DeepCopy()
	slot.Name = "retained-owned-slot"
	fp, err := provider.ServiceFingerprint(slot)
	if err != nil {
		t.Fatal(err)
	}
	d := desiredRuntimeDeployment(input.Base.Pin)
	ref, err := store.RetainService(context.Background(), runtimeinputs.ServiceCapture{Target: runtimeinputs.Target{CellUID: input.Base.Pin.Cell.Record.Ref.ProviderID, Parent: session.Context().Parent(), AttemptID: runtimeAttempt(input.Base.Pin), Revision: d.Revision, DesiredDigest: journal.DesiredDigest(d), Name: slot.Name, Fingerprint: fp}, Desired: slot})
	if err != nil || !ref.MatchesKind("Service") {
		t.Fatal("owned capacity fixture did not retain its one slot", err)
	}
}
func encodeDiagnosticOutcome(t *testing.T, input diagnosticProcessInput, err error, sequence uint64, phase runtimefault.Phase) []byte {
	t.Helper()
	var cause *runtimeconfig.Error
	if !errors.As(err, &cause) || cause.Fault == nil || !cause.Fault.Valid() {
		t.Fatal("runtime lost finite original fault", err)
	}
	v := cause.Fault.View()
	if v.Failure.Code != input.ExpectedFailure || v.Dispatch != runtimefault.NotDispatched || v.Target.Kind != runtimefault.Service || v.Target.NamespaceUID == "" {
		t.Fatal("original private-input context/dispatch lost")
	}
	outcome, e := runtimefault.NewOutcome(diagnosticIdentity(input), "owned-activity-"+v.Failure.ID, sequence, time.Now().UTC(), phase, *cause.Fault)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(outcome)
	if e != nil {
		t.Fatal(e)
	}
	// Check decoded activity bytes before the harness encodes its []byte field.
	for _, marker := range []string{runtimeServicePrivateMarker, runtimeDeploymentPrivateMarker, input.Base.Directory} {
		if marker != "" && bytes.Contains(raw, []byte(marker)) {
			t.Fatal("private payload/path escaped safe activity outcome")
		}
	}
	parsed, e := runtimefault.ParseOutcome(raw)
	if e != nil || !reflect.DeepEqual(parsed.View(), outcome.View()) {
		t.Fatal("activity boundary changed safe outcome", e)
	}
	return raw
}
func runtimeDiagnosticProcess(t *testing.T, phase string) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input diagnosticProcessInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		t.Fatal("invalid owned diagnostic input")
	}
	var output diagnosticProcessOutput
	if phase == "read" {
		output.Wire = diagnosticHTTPWire(t, input)
	} else {
		config, repository := retirementConfig(t, input.Base.Pin, input.Base.Endpoint, input.Base.ProxyCA)
		config.Diagnostics = &runtimeconfig.DiagnosticConfig{ProjectName: "runtime-example"}
		store, _ := openRuntimeInputs(t, input.Base)
		if input.Mode != "missing-store" {
			config.ServiceInputs = store
		}
		var worker *runtimeconfig.Worker
		var err error
		if phase == "initial" || phase == "success" || phase == "blocked" {
			worker, err = runtimeconfig.OpenInitial(context.Background(), config)
		} else {
			checkpoint, e := runtimeconfig.ParseCheckpoint(input.Base.Checkpoint)
			if e != nil {
				t.Fatal(e)
			}
			worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
		}
		if input.Base.WantOpenError != "" {
			var denied *runtimeconfig.Error
			if !errors.As(err, &denied) || denied.Code != input.Base.WantOpenError {
				t.Fatalf("diagnostic open wanted %s, got %v", input.Base.WantOpenError, err)
			}
			output.Error = denied.Code
			if phase != "blocked" {
				output.Outcome = encodeDiagnosticOutcome(t, input, err, 2, runtimefault.BeforeAdmission)
			}
		} else if phase == "resume" {
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, e := worker.Quiesce(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			output.Checkpoint, e = checkpoint.JSON()
			if e != nil {
				t.Fatal(e)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			session, e := worker.EnsureNamespace(context.Background(), desiredRuntimeDeployment(input.Base.Pin), runtimeAttempt(input.Base.Pin))
			if e != nil {
				t.Fatal(e)
			}
			object := serviceFromRuntimeFixture(t, session, input.Base.Pin, false)
			if input.Mode == "capacity" || input.Mode == "delivery-unknown" {
				fillDiagnosticStore(t, store, input, session, object.Object.(*corev1.Service))
			}
			before, e := repository.Load(context.Background())
			if e != nil || before.Envelope.Projection == nil {
				t.Fatal("accepted diagnostic identity absent", e)
			}
			output.Receipt, err = session.ApplyService(context.Background(), object)
			if phase == "success" {
				if err != nil {
					t.Fatal(err)
				}
				snapshot, e := repository.Load(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				for _, write := range snapshot.Envelope.Writes {
					if write.Preconditions.Kind == "Service" {
						output.Input = write.Input
					}
				}
				if !output.Input.MatchesKind("Service") {
					t.Fatal("successful input reference absent")
				}
			} else {
				output.Outcome = encodeDiagnosticOutcome(t, input, err, before.Envelope.Projection.Sequence+1, runtimefault.Admitted)
				var failure *runtimeconfig.Error
				_ = errors.As(err, &failure)
				output.Error = failure.Code
				v := failure.Fault.View()
				if input.Mode == "delivery-unknown" {
					if output.Error != runtimeconfig.Uncertain || v.JournalDelivery.State != runtimefault.DeliveryUnknown || v.DiagnosticDelivery.State != runtimefault.DeliveryUnknown {
						t.Fatal("diagnostic uncertainty replaced original cause")
					}
					if _, e := session.ApplyService(context.Background(), object); e == nil {
						t.Fatal("uncertain projection reopened provider dispatch")
					}
					if _, e := worker.Quiesce(context.Background()); e == nil {
						t.Fatal("uncertain projection emitted clean handoff")
					}
				} else {
					if v.JournalDelivery.State != runtimefault.Acknowledged || v.DiagnosticDelivery.State != runtimefault.Acknowledged {
						t.Fatal("fault projection not acknowledged")
					}
					snapshot, e := repository.Load(context.Background())
					if e != nil || len(snapshot.Envelope.Writes) != 1 || snapshot.Envelope.Writes[0].Preconditions.Kind != "Namespace" || snapshot.Envelope.Projection == nil || snapshot.Envelope.Projection.Failure == nil || snapshot.Envelope.Projection.Failure.Code != input.ExpectedFailure {
						t.Fatal("pre-Pending cause was not independently durable", e)
					}
				}
			}
			if input.Mode != "delivery-unknown" {
				checkpoint, e := worker.Quiesce(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				output.Checkpoint, e = checkpoint.JSON()
				if e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	data, e := json.Marshal(output)
	if e != nil {
		t.Fatal(e)
	}
	fmt.Println("DIAGNOSTIC-PROCESS " + string(data))
}
func runDiagnosticProcess(t *testing.T, input diagnosticProcessInput, phase string) diagnosticProcessOutput {
	t.Helper()
	var output diagnosticProcessOutput
	runPrivateRuntimeProcess(t, "TestRuntimePrePendingFaultSurvivesProcessExitAndAuthorizedUIWire", "OPERATOR_DIAGNOSTIC_PROCESS", "DIAGNOSTIC-PROCESS ", phase, input, &output, runtimeServicePrivateMarker, runtimeDeploymentPrivateMarker, input.Base.Directory)
	return output
}
func diagnosticProcessFixture(t *testing.T, mode string, capacity int) (diagnosticProcessInput, *runtimeinputs.Store, *os.Root) {
	t.Helper()
	base, store, root := runtimeInputProcessFixtureWithCapacity(t, "diag", mode, capacity)
	input := diagnosticProcessInput{Base: base, Mode: mode, ExpectedFailure: "AcceptedInputCapacityReached"}
	if mode == "missing-store" {
		input.ExpectedFailure = "AcceptedInputStoreNotConfigured"
	}
	input.Desired = bootstrapDiagnosticDesired(t, input)
	return input, store, root
}
func TestRuntimePrePendingFaultSurvivesProcessExitAndAuthorizedUIWire(t *testing.T) {
	if phase := os.Getenv("OPERATOR_DIAGNOSTIC_PROCESS"); phase != "" {
		runtimeDiagnosticProcess(t, phase)
		return
	}
	for _, mode := range []string{"capacity", "missing-store"} {
		t.Run(mode, func(t *testing.T) {
			input, _, _ := diagnosticProcessFixture(t, mode, 1)
			endpoint, ca, posts, _, _ := diagnosticDispatchProxy(t, input, false)
			input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
			result := runDiagnosticProcess(t, input, "initial")
			if len(result.Checkpoint) == 0 || posts.Load() != 0 {
				t.Fatal("pre-Pending failure dispatched or lost clean checkpoint")
			}
			// The returned old worker has exited/reaped before this independent reader.
			read := runDiagnosticProcess(t, input, "read")
			if len(read.Wire) == 0 || posts.Load() != 0 {
				t.Fatal("fresh diagnostic reader replayed provider work")
			}
		})
	}
}
func TestRuntimeDiagnosticCommitUncertaintyKeepsOriginalFaultWithoutProviderReplay(t *testing.T) {
	input, _, _ := diagnosticProcessFixture(t, "delivery-unknown", 1)
	endpoint, ca, posts, updates, blockReads := diagnosticDispatchProxy(t, input, true)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	result := runDiagnosticProcess(t, input, "initial")
	if result.Error != runtimeconfig.Uncertain || len(result.Checkpoint) != 0 || posts.Load() != 0 || updates.Load() != 1 {
		t.Fatal("diagnostic ACK uncertainty reopened dispatch or dropped original fault")
	}
	// Independent owned fixture observation becomes available after old process
	// exit. It cannot convert the old actor's Unknown acknowledgement into ACK.
	blockReads.Store(false)
	// Existing admitted history reaches BeginInitial ErrChanged; there is no Unknown provider row.
	input.Base.WantOpenError = runtimeconfig.Changed
	runDiagnosticProcess(t, input, "blocked")
	input.Base.WantOpenError = ""
	read := runDiagnosticProcess(t, input, "read")
	original, e := runtimefault.ParseOutcome(result.Outcome)
	if e != nil || original.View().Fault.JournalDelivery.State != runtimefault.DeliveryUnknown || original.View().Fault.DiagnosticDelivery.State != runtimefault.DeliveryUnknown || len(read.Wire) == 0 || posts.Load() != 0 {
		t.Fatal("later read laundered original uncertain ACK", e)
	}
}
func TestRuntimePreAdmissionInputFaultDoesNotWriteOrConsumeReadyCheckpoint(t *testing.T) {
	input, _, root := diagnosticProcessFixture(t, "missing-body", 8)
	endpoint, ca, posts, _, _ := diagnosticDispatchProxy(t, input, false)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	first := runDiagnosticProcess(t, input, "success")
	if !first.Input.MatchesKind("Service") || posts.Load() != 1 {
		t.Fatal("missing-input fixture did not create canonical Service")
	}
	_, repository := retirementConfig(t, input.Base.Pin, "", nil)
	before, e := repository.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	name := first.Input.UID + ".input.json"
	original, e := root.ReadFile(name)
	if e != nil {
		t.Fatal(e)
	}
	if root.Remove(name) != nil {
		t.Fatal("owned fixture body removal failed")
	}
	input.ExpectedFailure = "AcceptedInputUnproven"
	input.Base.Checkpoint = first.Checkpoint
	input.Base.WantOpenError = runtimeconfig.Unavailable
	denied := runDiagnosticProcess(t, input, "deny")
	outcome, e := runtimefault.ParseOutcome(denied.Outcome)
	if e != nil || outcome.View().Phase != runtimefault.BeforeAdmission || outcome.View().Fault.JournalDelivery.State != runtimefault.NotAttempted || outcome.View().Fault.DiagnosticDelivery.State != runtimefault.NotAttempted {
		t.Fatal("pre-admission data invented delivery authority", e)
	}
	after, e := repository.Load(context.Background())
	if e != nil || after.Version != before.Version || after.Envelope.Digest != before.Envelope.Digest || after.Envelope.Admission == nil || after.Envelope.Admission.State != journal.AdmissionReady || posts.Load() != 1 {
		t.Fatal("read-only preflight altered Ready journal or dispatched", e)
	}
	if _, e := root.Lstat(name); !os.IsNotExist(e) {
		t.Fatal("runtime repaired missing input")
	}
	// Explicit fixture-admin restoration solely proves checkpoint was unconsumed.
	if root.WriteFile(name, original, 0600) != nil {
		t.Fatal("fixture byte restore failed")
	}
	input.Base.WantOpenError = ""
	resumed := runDiagnosticProcess(t, input, "resume")
	if len(resumed.Checkpoint) == 0 || bytes.Equal(resumed.Checkpoint, first.Checkpoint) || posts.Load() != 1 {
		t.Fatal("pre-admission failure consumed checkpoint or replayed Service")
	}
}
