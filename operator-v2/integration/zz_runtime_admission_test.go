//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
)

type runtimeProcessResult struct {
	output  string
	err     error
	success bool
}
type runtimeProcessOutput struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	tail    string
	notices chan string
}

func (out *runtimeProcessOutput) Write(data []byte) (int, error) {
	out.mu.Lock()
	defer out.mu.Unlock()
	n, err := out.buffer.Write(data)
	out.tail += string(data)
	for {
		index := strings.IndexByte(out.tail, '\n')
		if index < 0 {
			break
		}
		line := out.tail[:index]
		out.tail = out.tail[index+1:]
		if line == "RUNTIME-CLOSED" {
			select {
			case out.notices <- line:
			default:
			}
		}
	}
	return n, err
}
func (out *runtimeProcessOutput) String() string {
	out.mu.Lock()
	defer out.mu.Unlock()
	return out.buffer.String()
}

type runtimeProcess struct {
	command *exec.Cmd
	input   io.WriteCloser
	done    chan runtimeProcessResult
	result  *runtimeProcessResult
	notices <-chan string
}

func startRuntimeProcess(t *testing.T, input runtimeChildInput) *runtimeProcess {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestRuntimeConfigCleanProcessHandoff$", "-test.v", "-test.timeout=90s")
	command.Env = []string{"OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, "OPERATOR_RUNTIME_CHILD=resume"}
	output := &runtimeProcessOutput{notices: make(chan string, 1)}
	command.Stdout, command.Stderr = output, output
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &runtimeProcess{command: command, input: stdin, done: make(chan runtimeProcessResult, 1), notices: output.notices}
	go func() {
		err := command.Wait()
		process.done <- runtimeProcessResult{output: output.String(), err: err, success: command.ProcessState != nil && command.ProcessState.Exited() && command.ProcessState.Success()}
	}()
	t.Cleanup(func() {
		if process.result == nil {
			_ = command.Process.Kill()
			result := <-process.done
			process.result = &result
		}
		_ = stdin.Close()
	})
	if err := json.NewEncoder(stdin).Encode(input); err != nil {
		t.Fatal(err)
	}
	return process
}
func (process *runtimeProcess) wait(t *testing.T) runtimeChildOutput {
	t.Helper()
	if process.result == nil {
		result := <-process.done
		process.result = &result
	}
	result := process.result
	if result.err != nil || !result.success {
		t.Fatalf("owned competing process failed to exit cleanly: %v\n%s", result.err, result.output)
	}
	for _, line := range strings.Split(result.output, "\n") {
		if strings.HasPrefix(line, "RUNTIME-CHILD ") {
			var value runtimeChildOutput
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RUNTIME-CHILD ")), &value); err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("missing competing process result: %s", result.output)
	return runtimeChildOutput{}
}
func runtimeEnvelope(t *testing.T, pin runtimePin) journal.Envelope {
	t.Helper()
	path := "/api/v1/namespaces/" + pin.Parent.Ref.Name + "/configmaps/" + pin.Cell.Record.Ref.Name
	object := request(t, "GET", path, nil, 200)
	if text(metadata(object), "uid") != pin.Cell.Record.Ref.ProviderID {
		t.Fatal("control cell UID changed")
	}
	envelope, err := journal.Decode([]byte(text(field(object, "data"), kubejournal.DataKey)))
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}
func armRuntimeClaimRace(t *testing.T, proof *runtimeProxy, sequence uint64) (<-chan struct{}, func()) {
	t.Helper()
	arrivals, release := make(chan struct{}, 2), make(chan struct{})
	proof.mu.Lock()
	proof.claimArrivals, proof.releaseClaims = arrivals, release
	proof.mu.Unlock()
	proof.claimSequence.Store(sequence)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return arrivals, unblock
}
func waitRuntimeClaimArrivals(t *testing.T, arrivals <-chan struct{}, first, second *runtimeProcess) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for n := 0; n < 2; n++ {
		select {
		case <-arrivals:
		case result := <-first.done:
			first.result = &result
			t.Fatalf("first process stopped before both conditional claims: %v\n%s", result.err, result.output)
		case result := <-second.done:
			second.result = &result
			t.Fatalf("second process stopped before both conditional claims: %v\n%s", result.err, result.output)
		case <-deadline.C:
			t.Fatal("two independent conditional admission requests did not arrive")
		}
	}
}

func TestRuntimeAdmissionCompetingProcessesConsumeHandoffOnce(t *testing.T) {
	pin := runtimeControlPin(t, "claim-race")
	proof := runtimeJournalProxy(t, pin)
	input := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, EnsureOnly: true}
	// This parent has reaped the old worker before either successor process starts.
	old := runRuntimeChild(t, input, true)
	envelope := runtimeEnvelope(t, pin)
	if envelope.Admission == nil || envelope.Admission.State != journal.AdmissionReady {
		t.Fatal("old child did not durably drain to HandoffReady")
	}
	arrivals, unblock := armRuntimeClaimRace(t, proof, envelope.Sequence+1)
	input.Checkpoint = old.Checkpoint
	input.AllowChanged = true
	input.HoldBeforeDispatch = true
	first, second := startRuntimeProcess(t, input), startRuntimeProcess(t, input)
	waitRuntimeClaimArrivals(t, arrivals, first, second)
	unblock()
	wait(t, func() bool {
		state := runtimeEnvelope(t, pin).Admission
		return state != nil && state.State == journal.AdmissionActive && state.Generation == 2
	})
	// The winner is deliberately held after its admitted OpenClean and before any
	// project dispatch/handoff. A separate fresh process cannot replay old bytes.
	stale := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, Checkpoint: old.Checkpoint, EnsureOnly: true, WantCode: runtimeconfig.Changed, OpenMustFail: true}
	runRuntimeChild(t, stale, false)
	for _, process := range []*runtimeProcess{first, second} {
		_ = json.NewEncoder(process.input).Encode(struct{ Exit bool }{true})
		_ = process.input.Close()
	}
	outcomes := []runtimeChildOutput{first.wait(t), second.wait(t)}
	var winner runtimeChildOutput
	accepted, rejected := 0, 0
	for _, outcome := range outcomes {
		if len(outcome.Checkpoint) > 0 && outcome.ErrorCode == "" {
			accepted++
			winner = outcome
		} else if outcome.ErrorCode == runtimeconfig.Changed {
			rejected++
		} else {
			t.Fatalf("unexpected admission outcome %+v", outcome)
		}
	}
	if accepted != 1 || rejected != 1 || proof.claims.Load() != 2 {
		t.Fatalf("accepted=%d rejected=%d conditional claims=%d", accepted, rejected, proof.claims.Load())
	}
	runRuntimeChild(t, stale, false)
	next := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, Checkpoint: winner.Checkpoint, EnsureOnly: true}
	resumed := runRuntimeChild(t, next, false)
	if len(resumed.Checkpoint) == 0 {
		t.Fatal("winning handoff did not support a clean successor")
	}
	requireRuntimeProxy(t, proof, 1, 0)
}

func TestRuntimeAdmissionLostClaimAcknowledgementNeverDispatchesProject(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(fmt.Sprint(resume), func(t *testing.T) {
			pin := runtimeControlPin(t, fmt.Sprintf("claim-unknown-%t", resume))
			proof := runtimeJournalProxy(t, pin)
			input := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, EnsureOnly: true}
			namespaces := int64(0)
			if resume {
				old := runRuntimeChild(t, input, true)
				input.Checkpoint = old.Checkpoint
				namespaces = 1
			}
			before := runtimeEnvelope(t, pin)
			proof.claimSequence.Store(before.Sequence + 1)
			proof.loseClaim.Store(true)
			input.WantCode = runtimeconfig.Uncertain
			input.OpenMustFail = true
			lost := runRuntimeChild(t, input, !resume)
			if len(lost.Checkpoint) != 0 || lost.Receipt.Record.Record.Ref.ProviderID != "" {
				t.Fatal("unacknowledged claim returned runtime authority")
			}
			after := runtimeEnvelope(t, pin)
			if after.Admission == nil || after.Admission.State != journal.AdmissionActive || proof.claims.Load() != 1 {
				t.Fatal("fault fixture did not commit exactly one actual admission")
			}
			requireRuntimeProxy(t, proof, namespaces, 0)
			proof.blockClaimRead.Store(false)
			input.WantCode = runtimeconfig.Changed
			runRuntimeChild(t, input, !resume)
			requireRuntimeProxy(t, proof, namespaces, 0)
			if proof.claims.Load() != 1 {
				t.Fatal("stale admission was resubmitted after accepted unknown claim")
			}
		})
	}
}

func TestRuntimeAdmissionClosedRecoveryAcknowledgesOnlyExactResult(t *testing.T) {
	pin := runtimeControlPin(t, "result-recovery")
	proof := runtimeJournalProxy(t, pin)
	input := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, EnsureOnly: true}
	old := runRuntimeChild(t, input, true)
	proof.loseResult.Store(true)
	input.Checkpoint = old.Checkpoint
	input.EnsureOnly = false
	input.RecoverClose = true
	input.WantCode = runtimeconfig.Uncertain
	process := startRuntimeProcess(t, input)
	select {
	case <-process.notices:
	case result := <-process.done:
		process.result = &result
		t.Fatalf("worker exited before closed recovery barrier: %v\n%s", result.err, result.output)
	case <-time.After(30 * time.Second):
		t.Fatal("closed worker recovery notice did not arrive")
	}
	if !proof.blockClaimRead.Load() {
		t.Fatal("journal result acknowledgement was not actually lost")
	}
	requireRuntimeProxy(t, proof, 1, 1)
	proof.loseResult.Store(false)
	proof.blockClaimRead.Store(false)
	if err := json.NewEncoder(process.input).Encode(struct{ Exit bool }{true}); err != nil {
		t.Fatal(err)
	}
	_ = process.input.Close()
	recovered := process.wait(t)
	if len(recovered.Checkpoint) == 0 {
		t.Fatal("exact closed result recovery did not produce acknowledged ready handoff")
	}
	if proof.claims.Load() != 1 {
		t.Fatal("closed recovery repeated an already committed conditional result write")
	}
	next := runtimeChildInput{Pin: pin, Endpoint: proof.endpoint, ProxyCA: proof.ca, Checkpoint: recovered.Checkpoint}
	resumed := runRuntimeChild(t, next, false)
	if resumed.Receipt.Record.Record.Ref.ProviderID == "" {
		t.Fatal("clean successor did not observe canonical committed ConfigMap")
	}
	requireRuntimeProxy(t, proof, 1, 1)
}
