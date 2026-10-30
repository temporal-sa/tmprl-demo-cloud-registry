//go:build integration

package integration

// Test-only clean supervisor. Accepted raw render inputs/desired identities are
// frozen once in the parent and transferred privately, never Context or Gate.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	render "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

const retirementSupersessionTest = "TestRuntimeSupersessionRetiresCandidateAcrossCleanProcessHandoff"
const retirementFinalizationTest = "TestRuntimeProjectFinalizationRemainsClosedAcrossCleanProcessHandoff"

func retirementReasonChildCode(output []byte) string {
	allowed := map[string]bool{"RetirementReasonPrivateBootstrapInvalid": true, "RetirementReasonCheckpointInvalid": true, "RetirementReasonAdmissionFailed": true, "RetirementReasonNamespaceApplyFailed": true, "RetirementReasonMetadataApplyFailed": true, "RetirementReasonOriginalDocumentInvalid": true, "RetirementReasonOriginalRenderInvalid": true, "RetirementReasonUnexpectedOriginalFailure": true, "RetirementReasonAcceptedEventInvalid": true, "RetirementReasonDrainFailed": true, "RetirementReasonExpectedFiniteDenial": true, "RetirementReasonStickyRefusalChangedJournal": true, "RetirementReasonResumeFailed": true, "RetirementReasonCleanupFailed": true, "RetirementReasonCanonicalAbsenceTimedOut": true, "RetirementReasonFinalAcknowledgementFailed": true, "RetirementReasonFinalStatusLostFacts": true, "RetirementReasonReplacementAdmissionFailed": true, "RetirementReasonIdempotentCompletionFailed": true, "RetirementReasonFinalizedSubjectReopened": true, "RetirementReasonQuiesceFailed": true, "RetirementReasonPrivateReceiptInvalid": true, "RetirementReasonJournalUnavailable": true}
	pattern := regexp.MustCompile(`^\s*(?:runtime_retirement_reason_process_test|zzzz_runtime_retirement_reasons_test)\.go:[0-9]+: ([A-Za-z]+)$`)
	for _, line := range strings.Split(string(output), "\n") {
		match := pattern.FindStringSubmatch(line)
		if len(match) == 2 && allowed[match[1]] {
			return match[1]
		}
	}
	return "ChildExecutionFailed"
}

type retirementReasonInput struct {
	Pin                              runtimePin
	Endpoint                         string
	ProxyCA, Checkpoint, RawDocument []byte
	Reason                           journal.DrainReason
	Original, Replacement            rollout.Desired
	Attempt, ReplacementAttempt      string
}
type retirementReasonOutput struct {
	Checkpoint []byte
	Receipt    runtimeconfig.Receipt
}

func retirementReasonState(reason journal.DrainReason) (diagnostics.State, string) {
	if reason == journal.DrainSuperseded {
		return diagnostics.Cancelled, "runtime.superseded"
	}
	return diagnostics.Finalizing, "runtime.project-finalizing"
}
func retirementReasonCode(t *testing.T, err error, code runtimeconfig.Code) {
	t.Helper()
	var actual *runtimeconfig.Error
	if !errors.As(err, &actual) || actual.Code != code {
		t.Fatal("RetirementReasonExpectedFiniteDenial")
	}
}
func retirementReasonLoad(t *testing.T, repository *journal.Repository) journal.Snapshot {
	t.Helper()
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal("RetirementReasonJournalUnavailable")
	}
	return snapshot
}
func retirementReasonProjection(t *testing.T, snapshot journal.Snapshot) []byte {
	t.Helper()
	if snapshot.Envelope.Projection == nil {
		t.Fatal("RetirementReasonProjectionMissing")
	}
	raw, err := snapshot.Envelope.Projection.JSON()
	if err != nil {
		t.Fatal("RetirementReasonProjectionInvalid")
	}
	return raw
}
func retirementReasonApply(t *testing.T, input retirementReasonInput, session *runtimeconfig.Session, desired rollout.Desired) runtimeconfig.Receipt {
	t.Helper()
	document, err := project.Decode(input.RawDocument)
	if err != nil {
		t.Fatal("RetirementReasonOriginalDocumentInvalid")
	}
	bundle, err := render.RenderCandidate(document, render.Bindings{Namespace: session.Context(), Revision: desired.Revision, OperationID: session.Context().Parent().CreationOperationID, Images: desired.Images})
	if err != nil {
		t.Fatal("RetirementReasonOriginalRenderInvalid")
	}
	for _, object := range bundle.Objects {
		if object.Object.GetName() == "platform-project-metadata" {
			receipt, e := session.ApplyConfigMap(context.Background(), object)
			if e != nil {
				t.Fatal("RetirementReasonMetadataApplyFailed")
			}
			return receipt
		}
	}
	t.Fatal("RetirementReasonMetadataMissing")
	return runtimeconfig.Receipt{}
}

func retirementReasonChild(t *testing.T, phase string) {
	t.Helper()
	var input retirementReasonInput
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32769))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Reason != journal.DrainSuperseded && input.Reason != journal.DrainProjectFinalization || input.Original.Subject != input.Pin.Subject || input.Replacement.Subject != input.Pin.Subject || input.Replacement.Generation != input.Original.Generation+1 || input.Attempt == input.ReplacementAttempt {
		t.Fatal("RetirementReasonPrivateBootstrapInvalid")
	}
	TestOwnedAPIAndNetworkBoundary(t)
	config, repository := retirementConfig(t, input.Pin, input.Endpoint, input.ProxyCA)
	config.Diagnostics = &runtimeconfig.DiagnosticConfig{ProjectName: "runtime-example"}
	var worker *runtimeconfig.Worker
	var err error
	if phase == "drain" {
		worker, err = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		checkpoint, e := runtimeconfig.ParseCheckpoint(input.Checkpoint)
		if e != nil {
			t.Fatal("RetirementReasonCheckpointInvalid")
		}
		worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
	}
	if err != nil {
		t.Fatal("RetirementReasonAdmissionFailed")
	}
	var output retirementReasonOutput
	switch phase {
	case "drain":
		session, e := worker.EnsureNamespace(context.Background(), input.Original, input.Attempt)
		if e != nil {
			t.Fatal("RetirementReasonNamespaceApplyFailed")
		}
		output.Receipt = retirementReasonApply(t, input, session, input.Original)
		snapshot := retirementReasonLoad(t, repository)
		p := snapshot.Envelope.Projection
		if p == nil || p.Failure != nil {
			t.Fatal("RetirementReasonUnexpectedOriginalFailure")
		}
		state, step := retirementReasonState(input.Reason)
		updated, e := p.Apply(diagnostics.OperationEvent{Identity: p.Identity, Sequence: p.Sequence + 1, OccurredAt: time.Now().UTC(), State: state, Step: step, Retry: &p.Retry, Availability: &p.Availability})
		if e != nil {
			t.Fatal("RetirementReasonAcceptedEventInvalid")
		}
		_, e = worker.BeginRetirement(context.Background(), runtimeconfig.RetirementRequest{AttemptID: input.Attempt, Reason: input.Reason, Projection: updated})
		if e != nil {
			t.Fatal("RetirementReasonDrainFailed")
		}
		if input.Reason == journal.DrainProjectFinalization {
			retirementReasonCode(t, worker.CompleteRuntimeFinalization(context.Background()), runtimeconfig.Blocked)
		}
	case "retire":
		_, e := worker.EnsureNamespace(context.Background(), input.Original, input.Attempt)
		retirementReasonCode(t, e, runtimeconfig.Blocked)
		_, e = worker.EnsureNamespace(context.Background(), input.Replacement, input.ReplacementAttempt)
		retirementReasonCode(t, e, runtimeconfig.Blocked)
		before := retirementReasonLoad(t, repository)
		wrong := journal.DrainCandidateFailed
		_, e = worker.BeginRetirement(context.Background(), runtimeconfig.RetirementRequest{AttemptID: input.Attempt, Reason: wrong, Projection: *before.Envelope.Projection})
		retirementReasonCode(t, e, runtimeconfig.Changed)
		after := retirementReasonLoad(t, repository)
		if before.Version != after.Version || before.Envelope.Digest != after.Envelope.Digest {
			t.Fatal("RetirementReasonStickyRefusalChangedJournal")
		}
		retirement, e := worker.BeginRetirement(context.Background(), runtimeconfig.RetirementRequest{AttemptID: input.Attempt, Reason: input.Reason, Projection: *after.Envelope.Projection})
		if e != nil {
			t.Fatal("RetirementReasonResumeFailed")
		}
		deadline := time.Now().Add(30 * time.Second)
		conflicts := 0
		for {
			progress, e := retirement.Advance(context.Background())
			if e != nil {
				var rejected *runtimeconfig.Error
				if !errors.As(e, &rejected) || rejected.Code != runtimeconfig.Rejected || rejected.Fault == nil || rejected.Fault.View().Failure.Code != string(provider.ConditionalConflict) || rejected.Fault.View().Dispatch != runtimefault.Rejected || conflicts >= 3 {
					t.Fatal("RetirementReasonCleanupFailed")
				}
				conflicts++
			}
			if progress.Consumed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("RetirementReasonCanonicalAbsenceTimedOut")
			}
			time.Sleep(100 * time.Millisecond)
		}
		if input.Reason == journal.DrainProjectFinalization {
			before := retirementReasonLoad(t, repository)
			old := *before.Envelope.Projection
			if old.State != diagnostics.Finalizing || worker.CompleteRuntimeFinalization(context.Background()) != nil {
				t.Fatal("RetirementReasonFinalAcknowledgementFailed")
			}
			after := retirementReasonLoad(t, repository)
			p := after.Envelope.Projection
			if p == nil || p.State != diagnostics.Finalized || p.Step != "runtime-finalized" || p.Sequence != old.Sequence+1 || p.Failure != nil || !reflect.DeepEqual(p.Recovery, old.Recovery) || !reflect.DeepEqual(p.Retry, old.Retry) || p.Availability != old.Availability {
				t.Fatal("RetirementReasonFinalStatusLostFacts")
			}
		}
	case "after":
		if input.Reason == journal.DrainSuperseded {
			_, e := worker.EnsureNamespace(context.Background(), input.Original, input.Attempt)
			retirementReasonCode(t, e, runtimeconfig.Consumed)
			session, e := worker.EnsureNamespace(context.Background(), input.Replacement, input.ReplacementAttempt)
			if e != nil {
				t.Fatal("RetirementReasonReplacementAdmissionFailed")
			}
			output.Receipt = retirementReasonApply(t, input, session, input.Replacement)
		} else {
			before := retirementReasonLoad(t, repository)
			raw := retirementReasonProjection(t, before)
			if worker.CompleteRuntimeFinalization(context.Background()) != nil {
				t.Fatal("RetirementReasonIdempotentCompletionFailed")
			}
			_, e := worker.EnsureNamespace(context.Background(), input.Original, input.Attempt)
			retirementReasonCode(t, e, runtimeconfig.Blocked)
			_, e = worker.EnsureNamespace(context.Background(), input.Replacement, input.ReplacementAttempt)
			retirementReasonCode(t, e, runtimeconfig.Blocked)
			after := retirementReasonLoad(t, repository)
			if before.Version != after.Version || !bytes.Equal(raw, retirementReasonProjection(t, after)) || len(after.Envelope.Attempts) != 1 {
				t.Fatal("RetirementReasonFinalizedSubjectReopened")
			}
		}
	default:
		t.Fatal("RetirementReasonPhaseInvalid")
	}
	checkpoint, err := worker.Quiesce(context.Background())
	if err != nil {
		t.Fatal("RetirementReasonQuiesceFailed")
	}
	output.Checkpoint, err = checkpoint.JSON()
	if err != nil {
		t.Fatal("RetirementReasonCheckpointInvalid")
	}
	raw, err := json.Marshal(output)
	if err != nil || len(raw) > 32768 {
		t.Fatal("RetirementReasonPrivateReceiptInvalid")
	}
	fmt.Println("RETIREMENT-REASON-PRIVATE " + string(raw))
}

func runRetirementReasonChild(t *testing.T, input retirementReasonInput, phase string) retirementReasonOutput {
	t.Helper()
	if input.Reason != journal.DrainSuperseded && input.Reason != journal.DrainProjectFinalization {
		t.Fatal("RetirementReasonSelectorInvalid")
	}
	name := retirementSupersessionTest
	if input.Reason == journal.DrainProjectFinalization {
		name = retirementFinalizationTest
	}
	if phase != "drain" && phase != "retire" && phase != "after" {
		t.Fatal("RetirementReasonPhaseInvalid")
	}
	fmt.Printf("RETIREMENT-REASON-PROGRESS reason=%s phase=%s\n", input.Reason, phase)
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > 32768 {
		t.Fatal("RetirementReasonPrivateBootstrapOverflow")
	}
	command := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.v", "-test.timeout=90s")
	command.Env = []string{"PATH=/unavailable", "HOME=/tmp/owned-retirement-reasons", "TMPDIR=/tmp", "OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, "OPERATOR_RETIREMENT_REASON_PHASE=" + phase}
	command.Stdin = bytes.NewReader(raw)
	output, err := command.CombinedOutput() // Actual Wait/reap before successor.
	if err != nil || len(output) > 1<<20 || command.ProcessState == nil || !command.ProcessState.Exited() || !command.ProcessState.Success() {
		code := "ChildExecutionFailed"
		if len(output) <= 1<<20 {
			code = retirementReasonChildCode(output)
		}
		t.Fatalf("RetirementReasonPrivateChildFailed reason=%s phase=%s code=%s", input.Reason, phase, code)
	}
	var value retirementReasonOutput
	count := 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "RETIREMENT-REASON-PRIVATE ") {
			count++
			decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(line, "RETIREMENT-REASON-PRIVATE ")))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF {
				t.Fatal("RetirementReasonPrivateReceiptInvalid")
			}
		}
	}
	if count != 1 {
		t.Fatal("RetirementReasonPrivateReceiptCountInvalid")
	}
	return value
}
