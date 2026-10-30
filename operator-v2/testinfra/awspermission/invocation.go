package awspermission

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

const InvocationContract = "registry-aws-readproof-invocation-v1"
const JournalPinContract = "registry-aws-readproof-journal-pin-v1"
const JournalInitContract = "registry-aws-readproof-journal-init-v1"

// Invocation is a canonical private control file, never a credential source.
// Source/Binary SHA fields are externally checked audit references. They do not
// attest this running executable or authenticate approval.
type Invocation struct {
	Version                                            int
	Contract, Account, Region, ProofID                 string
	SourceFreezeSHA256, BinarySHA256, JournalPinSHA256 string
	Config                                             InvocationConfig
}
type InvocationConfig struct {
	Phase                                                 Phase
	Bindings                                              fixtureplan.ReadProofBindings
	SourceAdminRoleID                                     string
	FixtureExpiresAt, DenyFixtureExpiresAt, ApprovedUntil time.Time
	ApprovalSHA256                                        string
}

func (c InvocationConfig) config(now func() time.Time) Config {
	return Config{Phase: c.Phase, Bindings: c.Bindings, SourceAdminRoleID: c.SourceAdminRoleID, FixtureExpiresAt: c.FixtureExpiresAt, DenyFixtureExpiresAt: c.DenyFixtureExpiresAt, ApprovedUntil: c.ApprovedUntil, ApprovalSHA256: c.ApprovalSHA256, Now: now}
}

type InvocationOptions struct {
	InvocationPath, ExpectedInvocationSHA256                   string
	JournalDirectory, JournalPinPath, ExpectedJournalPinSHA256 string
}
type JournalPin struct {
	Version  int
	Contract string
	Binding  JournalBinding
}
type InitOptions struct{ JournalDirectory, PinPath, BootstrapUID, ConfigurationSHA256 string }
type InitReceipt struct {
	Contract, Outcome string
	PinSHA256         string `json:",omitempty"`
}

type invocationReader struct {
	io.Reader
	failed bool
}

func (r *invocationReader) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	if e != nil && e != io.EOF {
		r.failed = true
	}
	return n, e
}

type invocationDependencies struct {
	now    func() time.Time
	decode func(io.Reader, func() time.Time, time.Time) (Credentials, error)
	run    func(context.Context, Config, Credentials, *Journal) (Result, error)
}

func finiteInvocationError(e error) error {
	if e == nil {
		return nil
	}
	if code, ok := e.(Error); ok {
		switch code {
		case Invalid, Denied, Unproven, UnexpectedAllowed, WrongIdentity, MetadataChanged, ProviderFailure, PrivacyDenied, AlreadyAttempted:
			return code
		}
	}
	return Unproven
}
func utcTimes(c Config) bool {
	values := []time.Time{c.FixtureExpiresAt, c.DenyFixtureExpiresAt, c.ApprovedUntil, c.Bindings.AllowSecretCreatedAt, c.Bindings.ReaderRoleCreatedAt, c.Bindings.TrapRoleCreatedAt}
	for _, v := range values {
		if v.IsZero() || !strings.HasSuffix(v.Format(time.RFC3339Nano), "Z") {
			return false
		}
	}
	return true
}
func validateInvocation(v Invocation, o InvocationOptions, now func() time.Time) (Config, error) {
	c := v.Config.config(now)
	if v.Version != 1 || v.Contract != InvocationContract || v.Account != fixtureplan.Account || v.Region != fixtureplan.Region || v.ProofID != fixtureplan.ReadProofID || !shaPattern.MatchString(v.SourceFreezeSHA256) || !shaPattern.MatchString(v.BinarySHA256) || v.JournalPinSHA256 != o.ExpectedJournalPinSHA256 || !utcTimes(c) || validateConfig(c) != nil {
		return Config{}, Invalid
	}
	return c, nil
}
func pipeDeadline(ctx context.Context, pipe *os.File, now time.Time, until time.Time) error {
	if pipe == nil || ctx.Err() != nil {
		return Denied
	}
	i, e := pipe.Stat()
	if e != nil || i.Mode()&os.ModeNamedPipe == 0 {
		return Denied
	}
	native, ok := i.Sys().(*syscall.Stat_t)
	if !ok || native.Uid != uint32(os.Getuid()) {
		return Denied
	}
	deadline := now.Add(10 * time.Second)
	if until.Before(deadline) {
		deadline = until
	}
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if !deadline.After(now) || pipe.SetReadDeadline(deadline) != nil {
		return Denied
	}
	return nil
}
func expectedSteps(phase Phase) []Step {
	if phase == Seed {
		return []Step{SourceIdentity, BootstrapAllowMetadata, BootstrapDenyMetadata, SeedAllowV1, SeedAllowV2, SeedDenyV1}
	}
	return []Step{SourceIdentity, BootstrapAllowMetadata, BootstrapDenyMetadata, AssumeReader, ReaderIdentity, AllowMetadata, ReadV1, ReadV2, ReadDeny, GenerateDenyKey, AssumeTrap}
}
func validateInvocationResult(r Result, c Config, runErr error) error {
	steps := expectedSteps(c.Phase)
	if r.Contract != fixtureplan.ReadProofContract || r.SpecSHA256 != c.ApprovalSHA256 || len(r.Steps) > len(steps) {
		return Unproven
	}
	if runErr == nil {
		want := "ScopedEffectiveCallsMatched"
		if c.Phase == Seed {
			want = "SeededSyntheticVersions"
		}
		if r.Outcome != want || len(r.Steps) != len(steps) {
			return Unproven
		}
	} else if r.Outcome != "Unproven" {
		return Unproven
	}
	for i, step := range r.Steps {
		if step.Step != steps[i] || step.SpecSHA256 != c.ApprovalSHA256 || !shaPattern.MatchString(step.RequestSHA256) || !shaPattern.MatchString(step.TargetSHA256) || !safeCode(step.Code) || step.At.IsZero() || step.RequestID != "" && !requestIDPattern.MatchString(step.RequestID) {
			return Unproven
		}
		switch step.Outcome {
		case "Allowed", "Denied", "Unproven", "UnexpectedAllowed":
		default:
			return Unproven
		}
		expected := "Allowed"
		switch step.Step {
		case ReadV2, ReadDeny, GenerateDenyKey, AssumeTrap:
			expected = "Denied"
		}
		if runErr == nil && (step.Outcome != expected || step.Persistence != "Acknowledged") {
			return Unproven
		}
		if step.Outcome == "Allowed" && step.Code != "" || step.Outcome == "Denied" && step.Code != "AccessDenied" && step.Code != "AccessDeniedException" || step.Outcome == "UnexpectedAllowed" && step.Code != string(UnexpectedAllowed) {
			return Unproven
		}
		switch step.Persistence {
		case "Acknowledged", "Unknown", "NotAttempted":
		default:
			return Unproven
		}
	}
	raw, e := json.Marshal(r)
	if e != nil || len(raw) > 16384 {
		return Unproven
	}
	return nil
}

// ExecuteInvocation validates all private metadata/pins before reading the
// explicitly inherited anonymous pipe. The caller transfers ownership of pipe;
// it closes on every outcome. There is no loader/spawn/fallback/retry. The
// parent must independently verify source/binary/account/policy and authorize
// this exact invocation. Matching SHA fields are equality checks, not approval.
func ExecuteInvocation(ctx context.Context, o InvocationOptions, credentialPipe *os.File) (Result, error) {
	return executeInvocation(ctx, o, credentialPipe, invocationDependencies{now: time.Now, decode: DecodeBootstrapCredentials, run: func(ctx context.Context, c Config, m Credentials, j *Journal) (Result, error) {
		h, e := New(c, m, j)
		if e != nil {
			return Result{Contract: fixtureplan.ReadProofContract, SpecSHA256: c.ApprovalSHA256, Outcome: "Unproven"}, e
		}
		return h.Run(ctx)
	}})
}
func executeInvocation(ctx context.Context, o InvocationOptions, pipe *os.File, deps invocationDependencies) (result Result, err error) {
	result = Result{Contract: fixtureplan.ReadProofContract, Outcome: "Unproven"}
	if pipe != nil {
		defer pipe.Close()
	}
	defer func() {
		if recover() != nil {
			result = Result{Contract: fixtureplan.ReadProofContract, Outcome: "Unproven"}
			err = Unproven
		}
		err = finiteInvocationError(err)
	}()
	if deps.now == nil || deps.decode == nil || deps.run == nil || ctx.Err() != nil || !shaPattern.MatchString(o.ExpectedInvocationSHA256) || !shaPattern.MatchString(o.ExpectedJournalPinSHA256) || !outside(o.JournalDirectory, o.InvocationPath) || !outside(o.JournalDirectory, o.JournalPinPath) {
		return result, Invalid
	}
	raw, e := readMetadata(o.InvocationPath, o.ExpectedInvocationSHA256, 32768)
	if e != nil {
		return result, e
	}
	var invocation Invocation
	if e = decodeCanonical(raw, &invocation); e != nil {
		return result, e
	}
	c, e := validateInvocation(invocation, o, deps.now)
	if e != nil {
		return result, e
	}
	duration := c.ApprovedUntil.Sub(deps.now())
	if duration > 5*time.Minute {
		duration = 5 * time.Minute
	}
	if duration <= 0 {
		return result, Denied
	}
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	pinRaw, e := readMetadata(o.JournalPinPath, o.ExpectedJournalPinSHA256, 8192)
	if e != nil {
		return result, e
	}
	var pin JournalPin
	if decodeCanonical(pinRaw, &pin) != nil || pin.Version != 1 || pin.Contract != JournalPinContract || pin.Binding.SpecSHA256 != c.ApprovalSHA256 {
		return result, Invalid
	}
	root, e := openJournalRoot(o.JournalDirectory)
	if e != nil {
		return result, e
	}
	defer root.Close()
	journal, e := OpenJournal(root, pin.Binding)
	if e != nil {
		return result, e
	}
	if e = freshInvocationJournal(journal); e != nil {
		return result, e
	}
	if e = pipeDeadline(ctx, pipe, deps.now(), c.ApprovedUntil); e != nil {
		return result, e
	}
	reader := &invocationReader{Reader: pipe}
	material, e := deps.decode(reader, deps.now, c.ApprovedUntil)
	if e != nil {
		if ctx.Err() != nil {
			return result, Denied
		}
		if reader.failed {
			return result, Unproven
		}
		return result, e
	}
	defer material.Close()
	if material.private == nil {
		return result, Invalid
	}
	durationDeadline := deps.now().Add(5 * time.Minute)
	if c.ApprovedUntil.Before(durationDeadline) {
		durationDeadline = c.ApprovedUntil
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(durationDeadline) {
		durationDeadline = deadline
	}
	material.private.mu.Lock()
	sufficient := !material.private.closed && !material.private.expires.Before(durationDeadline)
	material.private.mu.Unlock()
	if !sufficient || ctx.Err() != nil || validateConfig(c) != nil {
		return result, Denied
	}
	if e = freshInvocationJournal(journal); e != nil {
		return result, e
	}
	result, e = deps.run(ctx, c, material, journal)
	if ve := validateInvocationResult(result, c, e); ve != nil {
		return Result{Contract: fixtureplan.ReadProofContract, SpecSHA256: c.ApprovalSHA256, Outcome: "Unproven"}, ve
	}
	return result, e
}

// InitializeInvocationJournal is OFFLINE initialization, never a run fallback.
// Its private root must already exist and be empty. An outside pin must not
// exist. Partial outcomes remain in place; there is no reset/adoption/replay.
func InitializeInvocationJournal(ctx context.Context, o InitOptions) (receipt InitReceipt, err error) {
	return initializeInvocationJournal(ctx, o, writePin)
}
func initializeInvocationJournal(ctx context.Context, o InitOptions, write func(string, []byte) error) (receipt InitReceipt, err error) {
	receipt = InitReceipt{Contract: JournalInitContract, Outcome: "Unproven"}
	defer func() {
		if recover() != nil {
			receipt = InitReceipt{Contract: JournalInitContract, Outcome: "Unproven"}
			err = Unproven
		}
		err = finiteInvocationError(err)
	}()
	if ctx.Err() != nil || write == nil || !shaPattern.MatchString(o.BootstrapUID) || !shaPattern.MatchString(o.ConfigurationSHA256) || !outside(o.JournalDirectory, o.PinPath) || absentPrivatePin(o.PinPath) != nil {
		return receipt, Invalid
	}
	root, e := openJournalRoot(o.JournalDirectory)
	if e != nil {
		return receipt, e
	}
	defer root.Close()
	_, binding, e := BootstrapJournal(root, o.BootstrapUID, o.ConfigurationSHA256)
	if e != nil {
		return receipt, e
	}
	if ctx.Err() != nil {
		return receipt, Unproven
	}
	raw := encode(JournalPin{Version: 1, Contract: JournalPinContract, Binding: binding})
	if e = write(o.PinPath, raw); e != nil {
		return receipt, Unproven
	}
	if ctx.Err() != nil {
		return receipt, Unproven
	}
	receipt.Outcome = "Initialized"
	receipt.PinSHA256 = bytesHash(raw)
	return receipt, nil
}
