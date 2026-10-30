package awspermission

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

type invocationFixture struct {
	fixture               *fakeFixture
	options               InvocationOptions
	invocation            Invocation
	base                  string
	decodeCalls, runCalls int
	material              Credentials
}

func newInvocationFixture(t *testing.T, phase Phase) *invocationFixture {
	t.Helper()
	f := fixture(t, phase)
	f.now = time.Now().UTC().Truncate(time.Second)
	f.config.Bindings.AllowSecretCreatedAt = f.now.Add(-time.Minute)
	f.config.Bindings.ReaderRoleCreatedAt = f.now.Add(-time.Minute)
	f.config.Bindings.TrapRoleCreatedAt = f.now.Add(-time.Minute)
	f.config.FixtureExpiresAt = f.now.Add(time.Hour)
	f.config.DenyFixtureExpiresAt = f.now.Add(2 * time.Hour)
	f.config.ApprovedUntil = f.now.Add(10 * time.Minute)
	f.config.ApprovalSHA256 = ConfigurationDigest(f.config)
	base, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(base, 0700); e != nil {
		t.Fatal(e)
	}
	root := filepath.Join(base, "journal")
	if e = os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	pin := filepath.Join(base, "pin.json")
	init, e := InitializeInvocationJournal(context.Background(), InitOptions{JournalDirectory: root, PinPath: pin, BootstrapUID: hash("separately-retained-bootstrap"), ConfigurationSHA256: f.config.ApprovalSHA256})
	if e != nil || init.Outcome != "Initialized" {
		t.Fatal("synthetic journal init failed")
	}
	x := &invocationFixture{fixture: f, base: base, options: InvocationOptions{InvocationPath: filepath.Join(base, "invocation.json"), JournalDirectory: root, JournalPinPath: pin, ExpectedJournalPinSHA256: init.PinSHA256}}
	c := f.config
	x.invocation = Invocation{Version: 1, Contract: InvocationContract, Account: fixtureplan.Account, Region: fixtureplan.Region, ProofID: fixtureplan.ReadProofID, SourceFreezeSHA256: hash("reviewed-source"), BinarySHA256: hash("independently-reviewed-binary"), JournalPinSHA256: init.PinSHA256, Config: InvocationConfig{Phase: c.Phase, Bindings: c.Bindings, SourceAdminRoleID: c.SourceAdminRoleID, FixtureExpiresAt: c.FixtureExpiresAt, DenyFixtureExpiresAt: c.DenyFixtureExpiresAt, ApprovedUntil: c.ApprovedUntil, ApprovalSHA256: c.ApprovalSHA256}}
	x.save(t)
	return x
}
func (x *invocationFixture) save(t *testing.T) {
	t.Helper()
	raw := encode(x.invocation)
	if e := os.WriteFile(x.options.InvocationPath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	x.options.ExpectedInvocationSHA256 = bytesHash(raw)
}
func (x *invocationFixture) deps() invocationDependencies {
	return invocationDependencies{now: x.fixture.config.Now, decode: func(r io.Reader, now func() time.Time, until time.Time) (Credentials, error) {
		x.decodeCalls++
		m, e := DecodeBootstrapCredentials(r, now, until)
		x.material = m
		return m, e
	}, run: func(ctx context.Context, c Config, m Credentials, j *Journal) (Result, error) {
		x.runCalls++
		if c.ApprovalSHA256 != x.fixture.config.ApprovalSHA256 || !m.contains(bootstrapSecret) {
			return Result{}, Invalid
		}
		result := Result{Contract: fixtureplan.ReadProofContract, SpecSHA256: c.ApprovalSHA256, Outcome: "ScopedEffectiveCallsMatched"}
		if c.Phase == Seed {
			result.Outcome = "SeededSyntheticVersions"
		}
		for _, step := range expectedSteps(c.Phase) {
			r := Receipt{Step: step, SpecSHA256: c.ApprovalSHA256, RequestSHA256: hash(step), TargetSHA256: hash(struct{ Step Step }{step}), Outcome: "Requested", At: x.fixture.now}
			if e := j.begin(ctx, r); e != nil {
				return Result{}, e
			}
			r.Outcome = "Allowed"
			switch step {
			case ReadV2, ReadDeny, GenerateDenyKey, AssumeTrap:
				r.Outcome = "Denied"
				r.Code = "AccessDenied"
			}
			if e := j.finish(ctx, r); e != nil {
				return Result{}, e
			}
			r.Persistence = "Acknowledged"
			result.Steps = append(result.Steps, r)
		}
		return result, nil
	}}
}
func suppliedPipe(t *testing.T, body []byte) *os.File {
	t.Helper()
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Write(body); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func (x *invocationFixture) pipe(t *testing.T) *os.File {
	return suppliedPipe(t, credentialBody(t, x.fixture))
}
func TestInvocationSyntheticSeedAndProbeUsePinnedExistingJournal(t *testing.T) {
	for _, phase := range []Phase{Seed, Probe} {
		t.Run(string(phase), func(t *testing.T) {
			x := newInvocationFixture(t, phase)
			r, e := executeInvocation(context.Background(), x.options, x.pipe(t), x.deps())
			if e != nil || x.decodeCalls != 1 || x.runCalls != 1 || len(r.Steps) != len(expectedSteps(phase)) {
				t.Fatalf("synthetic finite invocation failed: %v", e)
			}
			assertPrivate(t, r)
			if x.material.private == nil || !x.material.private.closed {
				t.Fatal("material retained after invocation")
			}
			// Existing provider request markers, not a credential-read tombstone, stop
			// replay before consuming a second separately explicit credential stream.
			r, e = executeInvocation(context.Background(), x.options, x.pipe(t), x.deps())
			if e != AlreadyAttempted || x.decodeCalls != 1 || x.runCalls != 1 {
				t.Fatal("journal replay consumed credentials or dispatched")
			}
			assertPrivate(t, r)
		})
	}
}
func TestInvocationAllPinsAndScopeValidatedBeforeCredentialRead(t *testing.T) {
	for _, kind := range []string{"invocation-hash", "pin-hash", "phase", "account", "region", "proof", "source-hash", "binary-hash", "expired", "overlong", "future-birth", "parent-expiry", "role-conflict", "manifest", "wrong-root", "pin-uid", "pin-spec", "pin-inode", "pin-manifest", "missing-pin", "prior-request", "unknown-entry"} {
		t.Run(kind, func(t *testing.T) {
			x := newInvocationFixture(t, Seed)
			switch kind {
			case "invocation-hash":
				x.options.ExpectedInvocationSHA256 = hash("wrong")
			case "pin-hash":
				x.options.ExpectedJournalPinSHA256 = hash("wrong")
			case "phase":
				x.invocation.Config.Phase = "AnyAction"
				x.invocation.Config.ApprovalSHA256 = ConfigurationDigest(x.invocation.Config.config(x.fixture.config.Now))
				x.save(t)
			case "account":
				x.invocation.Account = "000000000000"
				x.save(t)
			case "region":
				x.invocation.Region = "us-east-1"
				x.save(t)
			case "proof":
				x.invocation.ProofID = "unreviewed"
				x.save(t)
			case "source-hash":
				x.invocation.SourceFreezeSHA256 = ""
				x.save(t)
			case "binary-hash":
				x.invocation.BinarySHA256 = ""
				x.save(t)
			case "expired":
				x.invocation.Config.ApprovedUntil = x.fixture.now
				x.invocation.Config.ApprovalSHA256 = ConfigurationDigest(x.invocation.Config.config(x.fixture.config.Now))
				x.save(t)
			case "overlong":
				x.invocation.Config.ApprovedUntil = x.fixture.now.Add(16 * time.Minute)
				x.invocation.Config.ApprovalSHA256 = ConfigurationDigest(x.invocation.Config.config(x.fixture.config.Now))
				x.save(t)
			case "future-birth":
				x.invocation.Config.Bindings.AllowSecretCreatedAt = x.fixture.now.Add(time.Minute)
				x.invocation.Config.ApprovalSHA256 = ConfigurationDigest(x.invocation.Config.config(x.fixture.config.Now))
				x.save(t)
			case "parent-expiry":
				x.invocation.Config.FixtureExpiresAt = x.fixture.now.Add(time.Minute)
				x.invocation.Config.ApprovalSHA256 = ConfigurationDigest(x.invocation.Config.config(x.fixture.config.Now))
				x.save(t)
			case "role-conflict":
				x.invocation.Config.SourceAdminRoleID = x.invocation.Config.Bindings.ReaderRoleID
				x.invocation.Config.ApprovalSHA256 = ConfigurationDigest(x.invocation.Config.config(x.fixture.config.Now))
				x.save(t)
			case "manifest":
				if e := os.WriteFile(filepath.Join(x.options.JournalDirectory, "identity.json"), []byte("changed"), 0600); e != nil {
					t.Fatal(e)
				}
			case "wrong-root":
				replacement := filepath.Join(x.base, "foreign-root")
				mustInvocationIO(t, os.Mkdir(replacement, 0700))
				identity, e := os.ReadFile(filepath.Join(x.options.JournalDirectory, "identity.json"))
				mustInvocationIO(t, e)
				mustInvocationIO(t, os.WriteFile(filepath.Join(replacement, "identity.json"), identity, 0600))
				x.options.JournalDirectory = replacement
			case "pin-uid", "pin-spec", "pin-inode", "pin-manifest":
				raw, e := os.ReadFile(x.options.JournalPinPath)
				mustInvocationIO(t, e)
				var pin JournalPin
				mustInvocationIO(t, decodeCanonical(raw, &pin))
				switch kind {
				case "pin-uid":
					pin.Binding.BootstrapUID = hash("foreign-bootstrap")
				case "pin-spec":
					pin.Binding.SpecSHA256 = hash("foreign-spec")
				case "pin-inode":
					pin.Binding.Directory.Inode++
				case "pin-manifest":
					pin.Binding.ManifestSHA256 = hash("foreign-manifest")
				}
				raw = encode(pin)
				mustInvocationIO(t, os.WriteFile(x.options.JournalPinPath, raw, 0600))
				x.options.ExpectedJournalPinSHA256 = bytesHash(raw)
				x.invocation.JournalPinSHA256 = bytesHash(raw)
				x.save(t)
			case "missing-pin":
				mustInvocationIO(t, os.Remove(x.options.JournalPinPath))
			case "prior-request":
				mustInvocationIO(t, os.WriteFile(filepath.Join(x.options.JournalDirectory, string(SourceIdentity)+".requested.json"), []byte("retained unknown"), 0600))
			case "unknown-entry":
				mustInvocationIO(t, os.WriteFile(filepath.Join(x.options.JournalDirectory, "unexpected"), []byte("owned harmless fixture"), 0600))
			}
			r, e := executeInvocation(context.Background(), x.options, x.pipe(t), x.deps())
			if e == nil || x.decodeCalls != 0 || x.runCalls != 0 {
				t.Fatal("invalid metadata read credentials/dispatched")
			}
			assertPrivate(t, r)
		})
	}
}
func TestCanonicalInvocationRejectsNestedAmbiguityAndTrailingBytes(t *testing.T) {
	for _, kind := range []string{"duplicate", "case-alias", "unknown-nested", "trailing", "pretty", "pin-duplicate", "pin-case-alias"} {
		t.Run(kind, func(t *testing.T) {
			x := newInvocationFixture(t, Seed)
			path := x.options.InvocationPath
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "duplicate":
				raw = bytes.Replace(raw, []byte(`"Phase":"Seed"`), []byte(`"Phase":"Seed","Phase":"Seed"`), 1)
			case "case-alias":
				raw = bytes.Replace(raw, []byte(`"SourceAdminRoleID"`), []byte(`"sourceAdminRoleID"`), 1)
			case "unknown-nested":
				raw = bytes.Replace(raw, []byte(`"Bindings":{`), []byte(`"Bindings":{"Unexpected":1,`), 1)
			case "trailing":
				raw = append(raw, []byte("{}")...)
			case "pretty":
				raw = bytes.Replace(raw, []byte(`"Version":1`), []byte(`"Version": 1`), 1)
			case "pin-duplicate", "pin-case-alias":
				path = x.options.JournalPinPath
				raw, e = os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				if kind == "pin-duplicate" {
					raw = bytes.Replace(raw, []byte(`"UID":`), []byte(`"UID":0,"UID":`), 1)
				} else {
					raw = bytes.Replace(raw, []byte(`"Directory"`), []byte(`"directory"`), 1)
				}
			}
			if e = os.WriteFile(path, raw, 0600); e != nil {
				t.Fatal(e)
			}
			if path == x.options.InvocationPath {
				x.options.ExpectedInvocationSHA256 = bytesHash(raw)
			} else {
				x.options.ExpectedJournalPinSHA256 = bytesHash(raw)
				x.invocation.JournalPinSHA256 = bytesHash(raw)
				x.save(t)
			}
			_, e = executeInvocation(context.Background(), x.options, x.pipe(t), x.deps())
			if e == nil || x.decodeCalls != 0 {
				t.Fatal("ambiguous control JSON consumed credentials")
			}
		})
	}
}
func TestInvocationPrivateFilesRejectUnsafeMetadata(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "loose-file", "loose-parent", "fifo", "oversize", "inside-pin", "symlink-parent", "root-symlink"} {
		t.Run(kind, func(t *testing.T) {
			x := newInvocationFixture(t, Seed)
			switch kind {
			case "symlink":
				old := filepath.Join(x.base, "old-invocation")
				mustInvocationIO(t, os.Rename(x.options.InvocationPath, old))
				mustInvocationIO(t, os.Symlink(old, x.options.InvocationPath))
			case "hardlink":
				mustInvocationIO(t, os.Link(x.options.InvocationPath, filepath.Join(x.base, "linked")))
			case "loose-file":
				mustInvocationIO(t, os.Chmod(x.options.InvocationPath, 0644))
			case "loose-parent":
				mustInvocationIO(t, os.Chmod(x.base, 0755))
			case "fifo":
				mustInvocationIO(t, os.Remove(x.options.InvocationPath))
				if e := syscall.Mkfifo(x.options.InvocationPath, 0600); e != nil {
					t.Fatal(e)
				}
			case "oversize":
				raw := bytes.Repeat([]byte("x"), 32769)
				mustInvocationIO(t, os.WriteFile(x.options.InvocationPath, raw, 0600))
				x.options.ExpectedInvocationSHA256 = bytesHash(raw)
			case "inside-pin":
				x.options.JournalPinPath = filepath.Join(x.options.JournalDirectory, "pin.json")
			case "symlink-parent":
				link := filepath.Join(x.base, "linked-parent")
				mustInvocationIO(t, os.Symlink(x.base, link))
				x.options.InvocationPath = filepath.Join(link, "invocation.json")
			case "root-symlink":
				link := filepath.Join(x.base, "linked-root")
				mustInvocationIO(t, os.Symlink(x.options.JournalDirectory, link))
				x.options.JournalDirectory = link
			}
			_, e := executeInvocation(context.Background(), x.options, x.pipe(t), x.deps())
			if e == nil || x.decodeCalls != 0 || x.runCalls != 0 {
				t.Fatal("unsafe metadata/pipe reached decode")
			}
		})
	}
}
func TestInvocationRequiresExplicitDeadlinePipeAndEOF(t *testing.T) {
	for _, kind := range []string{"nil", "regular-file", "malformed", "trailing", "expired-credentials", "short-lived-credentials"} {
		t.Run(kind, func(t *testing.T) {
			x := newInvocationFixture(t, Seed)
			var pipe *os.File
			switch kind {
			case "nil":
			case "regular-file":
				var e error
				pipe, e = os.Open(x.options.InvocationPath)
				if e != nil {
					t.Fatal(e)
				}
			case "malformed":
				pipe = suppliedPipe(t, []byte("not JSON"))
			case "trailing":
				body := credentialBody(t, x.fixture)
				pipe = suppliedPipe(t, append(body, body...))
			case "expired-credentials", "short-lived-credentials":
				body := credentialBody(t, x.fixture)
				expiry := x.fixture.now.Add(-time.Minute)
				if kind == "short-lived-credentials" {
					expiry = x.fixture.now.Add(time.Minute)
				}
				body = bytes.Replace(body, []byte(x.fixture.now.Add(time.Hour).Format(time.RFC3339Nano)), []byte(expiry.Format(time.RFC3339Nano)), 1)
				pipe = suppliedPipe(t, body)
			}
			r, e := executeInvocation(context.Background(), x.options, pipe, x.deps())
			if e == nil || x.runCalls != 0 {
				t.Fatal("invalid credential stream dispatched")
			}
			assertPrivate(t, r)
		})
	}
}
func TestInvocationPipeDeadlineStopsStalledProducerWithoutRunner(t *testing.T) {
	x := newInvocationFixture(t, Seed)
	reader, writer, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	r, e := executeInvocation(ctx, x.options, reader, x.deps())
	if e == nil || x.decodeCalls != 1 || x.runCalls != 0 || time.Since(start) > 2*time.Second {
		t.Fatal("stalled producer unbounded/dispatched")
	}
	assertPrivate(t, r)
}
func TestInvocationCancellationAndUnsafeRunnerFailuresRemainFinite(t *testing.T) {
	for _, kind := range []string{"canceled", "provider-error", "panic", "bad-result", "false-success"} {
		t.Run(kind, func(t *testing.T) {
			x := newInvocationFixture(t, Seed)
			deps := x.deps()
			ctx := context.Background()
			switch kind {
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "provider-error":
				deps.run = func(context.Context, Config, Credentials, *Journal) (Result, error) {
					return Result{Contract: fixtureplan.ReadProofContract, SpecSHA256: x.fixture.config.ApprovalSHA256, Outcome: "Unproven"}, errors.New(bootstrapSecret)
				}
			case "panic":
				deps.run = func(context.Context, Config, Credentials, *Journal) (Result, error) { panic(bootstrapToken) }
			case "bad-result":
				deps.run = func(context.Context, Config, Credentials, *Journal) (Result, error) {
					return Result{Contract: bootstrapSecret}, nil
				}
			case "false-success":
				realRun := deps.run
				deps.run = func(ctx context.Context, c Config, m Credentials, j *Journal) (Result, error) {
					r, e := realRun(ctx, c, m, j)
					r.Steps[0].Outcome = "UnexpectedAllowed"
					r.Steps[0].Code = string(UnexpectedAllowed)
					return r, e
				}
			}
			r, e := executeInvocation(ctx, x.options, x.pipe(t), deps)
			if e == nil {
				t.Fatal("unsafe output/failed run accepted")
			}
			if strings.Contains(e.Error(), bootstrapSecret) || strings.Contains(e.Error(), bootstrapToken) {
				t.Fatal("private error escaped")
			}
			assertPrivate(t, r)
			if kind == "canceled" && (x.decodeCalls != 0 || x.runCalls != 0) {
				t.Fatal("canceled preflight consumed credential bytes")
			}
			if x.material.private != nil && !x.material.private.closed {
				t.Fatal("failure retained material")
			}
		})
	}
}
func TestOfflineInitExclusiveOutsidePinAndRetainedUnknown(t *testing.T) {
	for _, kind := range []string{"success", "inside-pin", "existing-pin", "nonempty-root", "missing-root", "lost-pin-ack"} {
		t.Run(kind, func(t *testing.T) {
			base, e := filepath.EvalSymlinks(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			mustInvocationIO(t, os.Chmod(base, 0700))
			root := filepath.Join(base, "journal")
			mustInvocationIO(t, os.Mkdir(root, 0700))
			pin := filepath.Join(base, "pin.json")
			o := InitOptions{JournalDirectory: root, PinPath: pin, BootstrapUID: hash("offline-pinned-uid"), ConfigurationSHA256: hash("externally-reviewed-config")}
			write := writePin
			switch kind {
			case "inside-pin":
				o.PinPath = filepath.Join(root, "pin.json")
			case "existing-pin":
				mustInvocationIO(t, os.WriteFile(pin, []byte("existing owned pin"), 0600))
			case "nonempty-root":
				mustInvocationIO(t, os.WriteFile(filepath.Join(root, "existing"), []byte("retained"), 0600))
			case "missing-root":
				mustInvocationIO(t, os.Remove(root))
			case "lost-pin-ack":
				write = func(path string, b []byte) error {
					if e := writePin(path, b); e != nil {
						return e
					}
					return errors.New(bootstrapSecret)
				}
			}
			r, e := initializeInvocationJournal(context.Background(), o, write)
			assertPrivate(t, r)
			if kind == "success" {
				if e != nil || r.Outcome != "Initialized" || !shaPattern.MatchString(r.PinSHA256) {
					t.Fatal("offline init failed")
				}
				if _, e = InitializeInvocationJournal(context.Background(), o); e == nil {
					t.Fatal("init overwrote existing pin/root")
				}
			} else if e == nil || r.Outcome != "Unproven" || r.PinSHA256 != "" {
				t.Fatal("unsafe/unknown init acknowledged")
			}
			if kind == "lost-pin-ack" {
				if _, e = os.Stat(filepath.Join(root, "identity.json")); e != nil {
					t.Fatal("unknown init identity lost")
				}
				if _, e = os.Stat(pin); e != nil {
					t.Fatal("unknown pin lost")
				}
				if _, e = InitializeInvocationJournal(context.Background(), o); e == nil {
					t.Fatal("unknown init automatically adopted/reset")
				}
			}
		})
	}
}

func mustInvocationIO(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal("owned fixture setup failed")
	}
}

func TestInvocationRefusesInheritedBlockingPipeWithoutDeadlineBeforeRead(t *testing.T) {
	x := newInvocationFixture(t, Seed)
	fds := make([]int, 2)
	mustInvocationIO(t, syscall.Pipe(fds))
	reader := os.NewFile(uintptr(fds[0]), "synthetic-inherited-blocking-read")
	writer := os.NewFile(uintptr(fds[1]), "synthetic-inherited-blocking-write")
	if reader == nil || writer == nil {
		t.Fatal("raw owned pipe fixture unavailable")
	}
	defer reader.Close()
	defer writer.Close()
	if e := reader.SetReadDeadline(time.Now().Add(time.Second)); e == nil {
		t.Fatal("blocking raw-pipe fixture unexpectedly supports deadlines")
	}
	_, e := writer.Write(credentialBody(t, x.fixture))
	mustInvocationIO(t, e)
	mustInvocationIO(t, writer.Close())
	_, e = executeInvocation(context.Background(), x.options, reader, x.deps())
	if e != Denied || x.decodeCalls != 0 || x.runCalls != 0 {
		t.Fatal("unsupported deadline consumed credential bytes")
	}
}
