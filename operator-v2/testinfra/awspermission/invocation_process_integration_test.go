//go:build linux && awspermission_process_integration

package awspermission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

const fd3Top = "TestOwnedInvocationFD3ProcessBoundary"
const fd3Run = "/aws-readproof/run"
const fd3Init = "/aws-readproof/init"
const fd3Key = "ASIA2222222222222222"
const fd3Secret = "fd3-synthetic-secret-222222222222222222222222222"
const fd3Token = "fd3-synthetic-session-222222222222222222222222222"

type fd3Build struct{ CompilerInputSHA256, RunSHA256, InitSHA256 string }
type fd3Counters struct {
	InitSuccess, InitRefused, RunProcesses, SourceIdentity, ReplayDenied, PreReadDenied        int
	RawNonblockPreserved, OrdinaryFlagCleared, StalledDeadline, MaterialAbsent, ChildrenReaped bool
}
type fd3Driver struct {
	base                        string
	t                           *testing.T
	build                       fd3Build
	initProcesses, runProcesses int
}
type fd3Fixture struct {
	dir, root, pin, invocation string
	options                    InvocationOptions
	config                     Config
	identity, pinBytes         []byte
}
type fd3Child struct {
	stdout, stderr []byte
	exit           int
	elapsed        time.Duration
	nonblock       bool
}
type fd3Capture struct {
	mu     sync.Mutex
	data   []byte
	over   bool
	cancel context.CancelFunc
}

func (c *fd3Capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data)+len(p) > 32768 {
		c.over = true
		c.cancel()
		return 0, io.ErrShortBuffer
	}
	c.data = append(c.data, p...)
	return len(p), nil
}
func (c *fd3Capture) snapshot() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.data...), c.over
}
func fd3IO(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal("FD3OwnedFixtureIOFailed")
	}
}
func fd3HashFile(t *testing.T, path string) string {
	t.Helper()
	i, e := os.Lstat(path)
	fd3IO(t, e)
	if !i.Mode().IsRegular() || i.Mode()&os.ModeSymlink != 0 || i.Size() <= 0 || i.Size() > 256<<20 {
		t.Fatal("FD3BinaryIdentityInvalid")
	}
	f, e := os.Open(path)
	fd3IO(t, e)
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, io.LimitReader(f, (256<<20)+1))
	fd3IO(t, e)
	if n != i.Size() {
		t.Fatal("FD3BinarySizeChanged")
	}
	return hex.EncodeToString(h.Sum(nil))
}
func fd3Private(t *testing.T, raw []byte) {
	t.Helper()
	for _, needle := range []string{fd3Key, fd3Secret, fd3Token} {
		if bytes.Contains(raw, []byte(needle)) {
			t.Fatal("FD3MaterialEscaped")
		}
	}
}
func fd3JSON(t *testing.T, raw []byte, v any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		t.Fatal("FD3FiniteOutputInvalid")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		t.Fatal("FD3FiniteOutputTrailing")
	}
}
func fd3Flags(f *os.File, command int) (int, error) {
	raw, e := f.SyscallConn()
	if e != nil {
		return 0, e
	}
	flags := 0
	var nativeErr error
	e = raw.Control(func(fd uintptr) {
		v, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(command), 0)
		if errno != 0 {
			nativeErr = errno
		} else {
			flags = int(v)
		}
	})
	if e != nil {
		return 0, e
	}
	return flags, nativeErr
}
func fd3Pipe(t *testing.T, ordinary bool) (*os.File, *os.File) {
	t.Helper()
	var reader, writer *os.File
	if ordinary {
		var e error
		reader, writer, e = os.Pipe()
		fd3IO(t, e)
	} else {
		fds := make([]int, 2)
		fd3IO(t, syscall.Pipe2(fds, syscall.O_CLOEXEC))
		if e := syscall.SetNonblock(fds[0], true); e != nil {
			syscall.Close(fds[0])
			syscall.Close(fds[1])
			t.Fatal("FD3RawNonblockFailed")
		}
		reader = os.NewFile(uintptr(fds[0]), "owned-preexisting-nonblock-read")
		writer = os.NewFile(uintptr(fds[1]), "owned-cloexec-write")
	}
	if reader == nil || writer == nil {
		if reader != nil {
			reader.Close()
		}
		if writer != nil {
			writer.Close()
		}
		t.Fatal("FD3OwnedPipeUnavailable")
	}
	t.Cleanup(func() { reader.Close(); writer.Close() })
	for _, f := range []*os.File{reader, writer} {
		flags, e := fd3Flags(f, syscall.F_GETFD)
		fd3IO(t, e)
		if flags&syscall.FD_CLOEXEC == 0 {
			reader.Close()
			writer.Close()
			t.Fatal("FD3CloseOnExecMissing")
		}
	}
	flags, e := fd3Flags(reader, syscall.F_GETFL)
	fd3IO(t, e)
	if flags&syscall.O_NONBLOCK == 0 {
		reader.Close()
		writer.Close()
		t.Fatal("FD3InitialNonblockMissing")
	}
	return reader, writer
}
func (d *fd3Driver) child(kind string, args []string, reader *os.File, retain bool) fd3Child {
	t := d.t
	t.Helper()
	path := fd3Run
	switch kind {
	case "init":
		path = fd3Init
		d.initProcesses++
		if d.initProcesses > 3 {
			t.Fatal("FD3InitBudgetExceeded")
		}
	case "run":
		d.runProcesses++
		if d.runProcesses > 7 {
			t.Fatal("FD3RunBudgetExceeded")
		}
	default:
		t.Fatal("FD3CommandSelectorInvalid")
	}
	envDir, e := os.MkdirTemp(d.base, "child-env-")
	fd3IO(t, e)
	home, tmp := filepath.Join(envDir, "home"), filepath.Join(envDir, "tmp")
	fd3IO(t, os.Mkdir(home, 0700))
	fd3IO(t, os.Mkdir(tmp, 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stdout, stderr := &fd3Capture{cancel: cancel}, &fd3Capture{cancel: cancel}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = []string{"PATH=/unavailable", "HOME=" + home, "TMPDIR=" + tmp}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = 2 * time.Second
	if reader != nil {
		cmd.ExtraFiles = []*os.File{reader}
	}
	started, waited := false, false
	defer func() {
		if started && !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
		if reader != nil && !retain {
			reader.Close()
		}
	}()
	begin := time.Now()
	if e := cmd.Start(); e != nil {
		t.Fatal("FD3ChildStartFailed")
	}
	started = true
	nonblock := false
	var flagsErr error
	if reader != nil {
		var flags int
		flags, flagsErr = fd3Flags(reader, syscall.F_GETFL)
		nonblock = flags&syscall.O_NONBLOCK != 0
		if !retain {
			reader.Close()
		}
	}
	waitErr := cmd.Wait()
	waited = true
	elapsed := time.Since(begin)
	out, overOut := stdout.snapshot()
	errBytes, overErr := stderr.snapshot()
	fd3Private(t, out)
	fd3Private(t, errBytes)
	if overOut || overErr || ctx.Err() != nil || flagsErr != nil || cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
		t.Fatal("FD3ChildBoundOrReapFailed")
	}
	if waitErr != nil {
		var exit *exec.ExitError
		if !errors.As(waitErr, &exit) {
			t.Fatal("FD3ChildWaitUnproven")
		}
	}
	return fd3Child{stdout: out, stderr: errBytes, exit: cmd.ProcessState.ExitCode(), elapsed: elapsed, nonblock: nonblock}
}
func (d *fd3Driver) initialize() fd3Fixture {
	t := d.t
	t.Helper()
	dir, e := os.MkdirTemp(d.base, "journal-fixture-")
	fd3IO(t, e)
	root := filepath.Join(dir, "journal")
	fd3IO(t, os.Mkdir(root, 0700))
	now := time.Now().UTC()
	names, e := fixtureplan.ReadProofResourceNames()
	fd3IO(t, e)
	c := Config{Phase: Seed, Bindings: fixtureplan.ReadProofBindings{AllowSecretARN: "arn:aws:secretsmanager:" + fixtureplan.Region + ":" + fixtureplan.Account + ":secret:" + names.Secret + "-Fd3ABC", AllowSecretCreatedAt: now.Add(-time.Minute), AllowSecretCreationReceiptSHA256: bytesHash([]byte("synthetic-owned-create")), ReaderRoleID: "AROA0123456789ABCDEFG", ReaderRoleCreatedAt: now.Add(-time.Minute), ReaderCreationReceiptSHA256: bytesHash([]byte("synthetic-reader-create")), TrapRoleID: "AROA1234567890ABCDEFG", TrapRoleCreatedAt: now.Add(-time.Minute), TrapCreationReceiptSHA256: bytesHash([]byte("synthetic-trap-create"))}, SourceAdminRoleID: "AROA2345678901ABCDEFG", FixtureExpiresAt: now.Add(time.Hour), DenyFixtureExpiresAt: now.Add(time.Hour), ApprovedUntil: now.Add(10 * time.Minute), Now: time.Now}
	c.ApprovalSHA256 = ConfigurationDigest(c)
	pin := filepath.Join(dir, "pin.json")
	args := []string{"--journal-root", root, "--pin", pin, "--bootstrap-uid", bytesHash([]byte(dir)), "--configuration-sha256", c.ApprovalSHA256}
	child := d.child("init", args, nil, false)
	var report struct {
		Receipt    InitReceipt `json:"receipt"`
		Diagnostic string      `json:"diagnostic,omitempty"`
	}
	fd3JSON(t, child.stdout, &report)
	if child.exit != 0 || len(child.stderr) != 0 || report.Diagnostic != "" || report.Receipt.Contract != JournalInitContract || report.Receipt.Outcome != "Initialized" {
		t.Fatal("FD3ActualInitFailed")
	}
	pinBytes, e := os.ReadFile(pin)
	fd3IO(t, e)
	if bytesHash(pinBytes) != report.Receipt.PinSHA256 {
		t.Fatal("FD3PinHashChanged")
	}
	var binding JournalPin
	if decodeCanonical(pinBytes, &binding) != nil || binding.Version != 1 || binding.Contract != JournalPinContract || binding.Binding.SpecSHA256 != c.ApprovalSHA256 || binding.Binding.BootstrapUID != bytesHash([]byte(dir)) {
		t.Fatal("FD3PinCanonicalInvalid")
	}
	r, e := os.OpenRoot(root)
	fd3IO(t, e)
	_, e = OpenJournal(r, binding.Binding)
	r.Close()
	fd3IO(t, e)
	invocation := Invocation{Version: 1, Contract: InvocationContract, Account: fixtureplan.Account, Region: fixtureplan.Region, ProofID: fixtureplan.ReadProofID, SourceFreezeSHA256: d.build.CompilerInputSHA256, BinarySHA256: d.build.RunSHA256, JournalPinSHA256: report.Receipt.PinSHA256, Config: InvocationConfig{Phase: c.Phase, Bindings: c.Bindings, SourceAdminRoleID: c.SourceAdminRoleID, FixtureExpiresAt: c.FixtureExpiresAt, DenyFixtureExpiresAt: c.DenyFixtureExpiresAt, ApprovedUntil: c.ApprovedUntil, ApprovalSHA256: c.ApprovalSHA256}}
	invocationPath := filepath.Join(dir, "invocation.json")
	raw := encode(invocation)
	fd3Private(t, raw)
	fd3Private(t, pinBytes)
	fd3IO(t, os.WriteFile(invocationPath, raw, 0600))
	identity, e := os.ReadFile(filepath.Join(root, "identity.json"))
	fd3IO(t, e)
	return fd3Fixture{dir: dir, root: root, pin: pin, invocation: invocationPath, config: c, identity: identity, pinBytes: pinBytes, options: InvocationOptions{InvocationPath: invocationPath, ExpectedInvocationSHA256: bytesHash(raw), JournalDirectory: root, JournalPinPath: pin, ExpectedJournalPinSHA256: report.Receipt.PinSHA256}}
}
func (f fd3Fixture) args() []string {
	return []string{"--invocation", f.options.InvocationPath, "--invocation-sha256", f.options.ExpectedInvocationSHA256, "--journal-root", f.root, "--journal-pin", f.pin, "--journal-pin-sha256", f.options.ExpectedJournalPinSHA256}
}
func fd3Body(t *testing.T) []byte {
	t.Helper()
	raw, e := json.Marshal(struct {
		Version                                    int
		AccessKeyId, SecretAccessKey, SessionToken string
		Expiration                                 time.Time
	}{1, fd3Key, fd3Secret, fd3Token, time.Now().UTC().Add(time.Hour)})
	fd3IO(t, e)
	return raw
}
func fd3Feed(t *testing.T, body []byte, ordinary bool) (*os.File, []byte) {
	t.Helper()
	r, w := fd3Pipe(t, ordinary)
	if len(body) > 4096 {
		r.Close()
		w.Close()
		t.Fatal("FD3SyntheticBodyTooLarge")
	}
	_, e := w.Write(body)
	fd3IO(t, e)
	fd3IO(t, w.Close())
	return r, body
}
func fd3Unread(t *testing.T, r *os.File, expected []byte) {
	t.Helper()
	defer r.Close()
	fd3IO(t, r.SetReadDeadline(time.Now().Add(time.Second)))
	raw, e := io.ReadAll(io.LimitReader(r, 4097))
	fd3IO(t, e)
	defer clear(raw)
	if !bytes.Equal(raw, expected) {
		t.Fatal("FD3PreflightConsumedCredentialBytes")
	}
}
func fd3IdentityOnly(t *testing.T, f fd3Fixture) {
	t.Helper()
	entries, e := os.ReadDir(f.root)
	fd3IO(t, e)
	if len(entries) != 1 || entries[0].Name() != "identity.json" {
		t.Fatal("FD3UnexpectedProviderDispatch")
	}
	raw, e := os.ReadFile(filepath.Join(f.root, "identity.json"))
	fd3IO(t, e)
	if !bytes.Equal(raw, f.identity) {
		t.Fatal("FD3IdentityChanged")
	}
}
func fd3Denied(t *testing.T, child fd3Child, code Error) {
	t.Helper()
	var report struct {
		Result     Result `json:"result"`
		Diagnostic string `json:"diagnostic,omitempty"`
	}
	fd3JSON(t, child.stdout, &report)
	if child.exit != 1 || len(child.stderr) != 0 || report.Diagnostic != string(code) || report.Result.Contract != fixtureplan.ReadProofContract || report.Result.Outcome != "Unproven" || len(report.Result.Steps) != 0 {
		t.Fatal("FD3FiniteDenialMismatch")
	}
}
func fd3Snapshot(t *testing.T, f fd3Fixture) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	entries, e := os.ReadDir(f.root)
	fd3IO(t, e)
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			t.Fatal("FD3FrameNotRegular")
		}
		raw, e := os.ReadFile(filepath.Join(f.root, entry.Name()))
		fd3IO(t, e)
		fd3Private(t, raw)
		out[entry.Name()] = raw
	}
	return out
}

func TestOwnedInvocationFD3ProcessBoundary(t *testing.T) {
	if t.Name() != fd3Top {
		t.Fatal("FD3ExactTopRequired")
	}
	if os.Getenv("REGISTRY_AWS_INVOCATION_PROCESS_TIER") != "owned-isolated" || runtime.GOOS != "linux" || runtime.Version() != "go1.27.3" {
		t.Fatal("FD3OwnedNativeTierRequired")
	}
	raw, e := os.ReadFile("/aws-readproof/binaries.json")
	fd3IO(t, e)
	if len(raw) > 4096 {
		t.Fatal("FD3BuildMetadataOverflow")
	}
	var build fd3Build
	fd3JSON(t, raw, &build)
	if !shaPattern.MatchString(build.CompilerInputSHA256) || fd3HashFile(t, fd3Run) != build.RunSHA256 || fd3HashFile(t, fd3Init) != build.InitSHA256 {
		t.Fatal("FD3BinaryHashUnproven")
	}
	d := fd3Driver{t: t, base: t.TempDir(), build: build}
	counters := fd3Counters{MaterialAbsent: true, ChildrenReaped: true}
	var attempted, fresh fd3Fixture
	subcase := func(name string, fn func(*testing.T)) {
		if !t.Run(name, fn) {
			t.Fatal("FD3FiniteSequenceStopped")
		}
	}
	subcase("real-init-and-preserved-fd3", func(t *testing.T) {
		d.t = t
		attempted = d.initialize()
		counters.InitSuccess++
		body := fd3Body(t)
		defer clear(body)
		reader, _ := fd3Feed(t, body, false)
		child := d.child("run", attempted.args(), reader, false)
		var report struct {
			Result     Result `json:"result"`
			Diagnostic string `json:"diagnostic,omitempty"`
		}
		fd3JSON(t, child.stdout, &report)
		if !child.nonblock || child.exit != 1 || len(child.stderr) != 0 || report.Diagnostic != string(ProviderFailure) || report.Result.Contract != fixtureplan.ReadProofContract || report.Result.SpecSHA256 != attempted.config.ApprovalSHA256 || report.Result.Outcome != "Unproven" || len(report.Result.Steps) != 1 {
			t.Fatal("FD3RealReceiverDidNotReachBoundedProviderPath")
		}
		step := report.Result.Steps[0]
		if step.Step != SourceIdentity || step.Outcome != "Unproven" || step.Code != string(ProviderFailure) || step.Persistence != "Acknowledged" {
			t.Fatal("FD3SourceReceiptUnproven")
		}
		frames := fd3Snapshot(t, attempted)
		if len(frames) != 3 || frames[string(SourceIdentity)+".requested.json"] == nil || frames[string(SourceIdentity)+".receipt.json"] == nil {
			t.Fatal("FD3ProviderJournalBoundaryWrong")
		}
		var requested, terminal Receipt
		if decodeCanonical(frames[string(SourceIdentity)+".requested.json"], &requested) != nil || decodeCanonical(frames[string(SourceIdentity)+".receipt.json"], &terminal) != nil {
			t.Fatal("FD3DurableReceiptInvalid")
		}
		if requested.Step != SourceIdentity || requested.SpecSHA256 != attempted.config.ApprovalSHA256 || requested.Outcome != "Requested" || requested.RequestSHA256 != step.RequestSHA256 || requested.TargetSHA256 != step.TargetSHA256 || terminal.Step != SourceIdentity || terminal.SpecSHA256 != requested.SpecSHA256 || terminal.RequestSHA256 != step.RequestSHA256 || terminal.TargetSHA256 != step.TargetSHA256 || terminal.Outcome != "Unproven" || terminal.Code != string(ProviderFailure) || terminal.Persistence != "" {
			t.Fatal("FD3DurableReceiptMismatch")
		}
		counters.SourceIdentity++
		counters.RawNonblockPreserved = true
	})
	subcase("ordinary-go-pipe-clears-flag", func(t *testing.T) {
		d.t = t
		fresh = d.initialize()
		counters.InitSuccess++
		body := fd3Body(t)
		defer clear(body)
		reader, _ := fd3Feed(t, body, true)
		child := d.child("run", fresh.args(), reader, false)
		if child.nonblock {
			t.Fatal("FD3OrdinaryPipeControlDidNotClearFlag")
		}
		fd3Denied(t, child, Denied)
		fd3IdentityOnly(t, fresh)
		counters.OrdinaryFlagCleared = true
		counters.PreReadDenied++
	})
	subcase("absent-fd3", func(t *testing.T) {
		d.t = t
		fd3IdentityOnly(t, fresh)
		fd3Denied(t, d.child("run", fresh.args(), nil, false), Denied)
		fd3IdentityOnly(t, fresh)
		counters.PreReadDenied++
	})
	subcase("malformed-stream", func(t *testing.T) {
		d.t = t
		fd3IdentityOnly(t, fresh)
		reader, _ := fd3Feed(t, []byte("malformed-owned-synthetic-json"), false)
		fd3Denied(t, d.child("run", fresh.args(), reader, false), Invalid)
		fd3IdentityOnly(t, fresh)
	})
	subcase("stalled-writer-deadline", func(t *testing.T) {
		d.t = t
		fd3IdentityOnly(t, fresh)
		reader, writer := fd3Pipe(t, false)
		defer writer.Close()
		child := d.child("run", fresh.args(), reader, false)
		fd3Denied(t, child, Unproven)
		if child.elapsed < 8*time.Second || child.elapsed > 18*time.Second {
			t.Fatal("FD3ReceiverDeadlineNotProven")
		}
		fd3IdentityOnly(t, fresh)
		counters.StalledDeadline = true
	})
	subcase("private-metadata-error", func(t *testing.T) {
		d.t = t
		fd3IdentityOnly(t, fresh)
		body := fd3Body(t)
		defer clear(body)
		reader, _ := fd3Feed(t, body, false)
		args := fresh.args()
		args[1] = filepath.Join(fresh.dir, "PRIVATE-OWNED-INPUT-MISSING")
		child := d.child("run", args, reader, true)
		fd3Denied(t, child, Denied)
		if bytes.Contains(child.stdout, []byte("PRIVATE-OWNED-INPUT-MISSING")) || bytes.Contains(child.stderr, []byte("PRIVATE-OWNED-INPUT-MISSING")) {
			t.Fatal("FD3PrivatePathEscaped")
		}
		fd3Unread(t, reader, body)
		fd3IdentityOnly(t, fresh)
		counters.PreReadDenied++
	})
	subcase("replay-and-init-no-reset", func(t *testing.T) {
		d.t = t
		before := fd3Snapshot(t, attempted)
		body := fd3Body(t)
		defer clear(body)
		reader, _ := fd3Feed(t, body, false)
		fd3Denied(t, d.child("run", attempted.args(), reader, true), AlreadyAttempted)
		fd3Unread(t, reader, body)
		after := fd3Snapshot(t, attempted)
		if len(before) != len(after) {
			t.Fatal("FD3ReplayChangedFrames")
		}
		for name, raw := range before {
			if !bytes.Equal(raw, after[name]) {
				t.Fatal("FD3ReplayChangedFrame")
			}
		}
		child := d.child("init", []string{"--journal-root", attempted.root, "--pin", attempted.pin, "--bootstrap-uid", bytesHash([]byte(attempted.dir)), "--configuration-sha256", attempted.config.ApprovalSHA256}, nil, false)
		var report struct {
			Receipt    InitReceipt `json:"receipt"`
			Diagnostic string      `json:"diagnostic,omitempty"`
		}
		fd3JSON(t, child.stdout, &report)
		if child.exit != 1 || len(child.stderr) != 0 || report.Diagnostic != string(Invalid) || report.Receipt.Outcome != "Unproven" || report.Receipt.PinSHA256 != "" {
			t.Fatal("FD3InitResetAllowed")
		}
		pin, e := os.ReadFile(attempted.pin)
		fd3IO(t, e)
		if !bytes.Equal(pin, attempted.pinBytes) {
			t.Fatal("FD3PinOverwritten")
		}
		afterReset := fd3Snapshot(t, attempted)
		if len(afterReset) != len(before) {
			t.Fatal("FD3ResetRefusalChangedFrames")
		}
		for name, raw := range before {
			if !bytes.Equal(raw, afterReset[name]) {
				t.Fatal("FD3ResetRefusalChangedFrame")
			}
		}
		counters.InitRefused++
		counters.ReplayDenied++
		counters.PreReadDenied++
	})
	counters.RunProcesses = d.runProcesses
	if d.initProcesses != 3 || d.runProcesses != 7 {
		t.Fatal("FD3ExactProcessBudgetMismatch")
	}
	public, e := json.Marshal(counters)
	fd3IO(t, e)
	fmt.Println("AWSINVOCATION-PUBLIC " + string(public))
}
