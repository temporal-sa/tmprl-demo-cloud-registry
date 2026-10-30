package credentialstaging

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type denyReadClient struct{ calls int }

func (c *denyReadClient) DescribeSecret(context.Context, *secretsmanager.DescribeSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	c.calls++
	return nil, errors.New(material)
}
func (c *denyReadClient) GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	c.calls++
	return nil, errors.New(material)
}

func publicReadSettings(t *testing.T) ObservationSettings {
	t.Helper()
	key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-read-ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der := must(x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key))
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return ObservationSettings{ProjectID: "owned-project-id", TrustBindingID: "owned-read-public-trust", ReaderBindingID: "independently-reviewed-reader", Cloud: observation.Endpoint{Address: "cloud.example.test:443", ServerName: "cloud.example.test", RootCAPEM: ca}, Data: observation.Endpoint{Address: "data.example.test:443", ServerName: "data.example.test", RootCAPEM: ca}, Timeout: 2 * time.Second}
}
func freshReadBootstrap(t *testing.T, f *fixture, settings ObservationSettings, client observation.SecretClient) {
	t.Helper()
	// New root before any actor claim/stage is fresh explicit bootstrap. Never
	// modify/adopt the already created zero-binding registry to enable reads.
	r, path := root(t)
	config := f.config.RegistryConfig
	config.BootstrapUID = "fresh-read-enabled-bootstrap"
	config.ObservationBindingDigest = must(ObservationSettingsFingerprint(settings, f.config.KMS))
	f.config.RegistryRoot = r
	f.config.RegistryConfig = config
	f.config.RegistryBinding = must(BootstrapRegistry(background, r, config))
	f.registryPath = path
	f.config.Observation = &ObservationRuntime{Settings: settings, SecretReader: client}
}
func setupRead(t *testing.T) (*fixture, *denyReadClient) {
	t.Helper()
	f := setup(t, nil)
	client := &denyReadClient{}
	freshReadBootstrap(t, f, publicReadSettings(t), client)
	return f, client
}
func readRequest(v tc.CredentialState) ObserveRequest {
	return ObserveRequest{StageID: v.Intent.StageID, WriteID: v.CandidateWriteID, CredentialJournalVersion: v.Version}
}
func stageForRead(t *testing.T, a *Actor, f *fixture) tc.CredentialState {
	t.Helper()
	var v tc.CredentialState
	for i := 0; i < 3; i++ {
		var e error
		v, e = a.Advance(background, f.request)
		if e != nil {
			t.Fatal(e)
		}
	}
	if v.Phase != tc.CredentialStaged {
		t.Fatal("owned stage fixture not staged")
	}
	return v
}

func TestStageOnlyRegistryBytesAndDisabledReadArePreserved(t *testing.T) {
	f := setup(t, nil)
	raw := must(f.config.RegistryRoot.ReadFile("reservations.json"))
	if !strings.Contains(string(raw), `"Schema":"staging-reservations-v1"`) || strings.Contains(string(raw), "ObservationBindingDigest") {
		t.Fatal("stage-only v1 bytes changed")
	}
	a := must(OpenInitial(background, f.config))
	_, e := a.ObserveStaged(background, ObserveRequest{})
	if !errors.Is(e, ObservationDisabled) || f.cloud.reads != 0 || f.aws.creates != 0 {
		t.Fatal("stage-only actor gained observation authority")
	}
}

func TestReadConfigurationRequiresFreshPinnedSettingsAndExactKMS(t *testing.T) {
	for _, mode := range []string{"implicit-enable", "implicit-disable", "reader-binding", "project", "trust", "endpoint", "kms-arn", "kms-provenance", "missing-reader"} {
		t.Run(mode, func(t *testing.T) {
			var f *fixture
			if mode == "implicit-enable" {
				f = setup(t, nil)
				f.config.Observation = &ObservationRuntime{Settings: publicReadSettings(t), SecretReader: &denyReadClient{}}
			} else {
				f, _ = setupRead(t)
			}
			switch mode {
			case "implicit-disable":
				f.config.Observation = nil
			case "reader-binding":
				f.config.Observation.Settings.ReaderBindingID = "different-reader"
			case "project":
				f.config.Observation.Settings.ProjectID = "different-project"
			case "trust":
				f.config.Observation.Settings.TrustBindingID = "different-trust"
			case "endpoint":
				f.config.Observation.Settings.Cloud.Address = "other.example.test:443"
			case "kms-arn":
				f.config.KMS.ARN = "arn:aws:kms:us-west-2:123456789012:key/22222222-2222-4222-8222-222222222222"
			case "kms-provenance":
				f.config.KMS.ProvenanceID = "different-kms-incarnation"
			case "missing-reader":
				f.config.Observation.SecretReader = nil
			}
			if _, e := OpenInitial(background, f.config); e == nil {
				t.Fatal("unpinned read configuration claimed actor")
			}
			if f.control.version != 1 || f.cloud.reads != 0 || f.cloud.creates != 0 || f.aws.creates != 0 {
				t.Fatal("invalid read configuration caused admission/provider I/O")
			}
		})
	}
}

func TestReadEnabledConstructorUsesOpaqueActualObserverAndCopiedPublicTrust(t *testing.T) {
	f, reader := setupRead(t)
	a := must(OpenInitial(background, f.config))
	if a.observer == nil || a.readSettings == nil || a.config.Observation.SecretReader != nil || reader.calls != 0 || f.cloud.reads != 0 {
		t.Fatal("constructor fetched material or retained printable reader")
	}
	original := append([]byte(nil), a.readSettings.Cloud.RootCAPEM...)
	f.config.Observation.Settings.Cloud.RootCAPEM[0] ^= 1
	if string(original) != string(a.readSettings.Cloud.RootCAPEM) {
		t.Fatal("actor trusted caller-mutable CA bytes")
	}
	var m manifest
	if json.Unmarshal(must(f.config.RegistryRoot.ReadFile("reservations.json")), &m) != nil || m.Schema != "staging-observation-reservations-v1" || m.Config.ObservationBindingDigest == "" {
		t.Fatal("read-enabled registry schema/binding missing")
	}
}

func TestObserveStagedDenialsNeverFetchMaterialOrBorrowNamespaceAuthority(t *testing.T) {
	for _, mode := range []string{"stage", "write", "version", "non-staged", "unknown-store", "uncertain-history", "closed", "canceled", "admin-header", "namespace-header", "writer"} {
		t.Run(mode, func(t *testing.T) {
			f, reader := setupRead(t)
			a := must(OpenInitial(background, f.config))
			var state tc.CredentialState
			if mode == "unknown-store" {
				f.aws.lost = true
				for i := 0; i < 2; i++ {
					_, _ = a.Advance(background, f.request)
				}
				state, _ = a.Advance(background, f.request)
			} else if mode == "non-staged" {
				state, _ = a.Advance(background, f.request)
			} else {
				state = stageForRead(t, a, f)
			}
			r := readRequest(state)
			if r.WriteID == "" {
				r.WriteID = strings.Repeat("a", 64)
			}
			ctx := background
			switch mode {
			case "stage":
				r.StageID = strings.Repeat("b", 64)
			case "write":
				r.WriteID = strings.Repeat("c", 64)
			case "version":
				r.CredentialJournalVersion = strings.Repeat("d", 64)
			case "uncertain-history":
				h := must(a.metadata.History(background))
				if e := f.config.MetadataRoot.WriteFile(fmt.Sprintf("tx-%06d.intent.json", len(h)+1), []byte("{"), 0600); e != nil {
					t.Fatal(e)
				}
			case "closed":
				_, _ = a.Quiesce(background)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "admin-header":
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("Authorization", "unrelated-admin"))
			case "namespace-header":
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("temporal-namespace", "foreign"))
			case "writer":
				w := f.config.Writer()
				w.Epoch++
				a.config.Writer = func() ownership.Writer { return w }
			}
			reads := f.cloud.reads
			_, e := a.ObserveStaged(ctx, r)
			if e == nil || strings.Contains(e.Error(), material) || reader.calls != 0 || f.cloud.reads != reads || f.cloud.deletes != 0 {
				t.Fatal("denied observation fetched material or borrowed provider authority")
			}
		})
	}
}

func TestPrivateReadCapabilityRejectsForeignSessionReferenceAndCreateContext(t *testing.T) {
	f, reader := setupRead(t)
	a := must(OpenInitial(background, f.config))
	state := stageForRead(t, a, f)
	r := readRequest(state)
	staged, receipt, e := a.stagedReadSnapshot(background, r)
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"expired", "used", "session", "epoch", "reference", "actor", "create-context"} {
		t.Run(mode, func(t *testing.T) {
			call := &readInvocation{actor: a, request: r, state: staged, outcome: receipt, writer: f.config.Writer(), session: *a.snapshot.Envelope.Admission, cell: a.snapshot.Version, live: true}
			a.reading = call
			ref := state.Candidate
			ctx := context.WithValue(background, readInvocationKey{}, call)
			switch mode {
			case "expired":
				call.live = false
			case "used":
				call.used = true
			case "session":
				call.session.Generation++
			case "epoch":
				call.writer.Epoch++
			case "reference":
				ref.KeyID = "foreign-key"
			case "actor":
				call.actor = nil
			case "create-context":
				ctx = context.WithValue(background, invocationKey{}, &invocation{actor: a, live: true})
			}
			called := false
			e := (actorReadGuard{actor: a}).Run(ctx, ref, func(context.Context) error { called = true; return nil })
			if e == nil || called || reader.calls != 0 {
				t.Fatal("foreign private read capability authorized callback")
			}
			call.live = false
			a.reading = nil
		})
	}
}

func TestZeroReadGuardCannotUseRetainedPrivateContext(t *testing.T) {
	f, reader := setupRead(t)
	a := must(OpenInitial(background, f.config))
	state := stageForRead(t, a, f)
	call := &readInvocation{actor: a, state: state, live: true}
	ctx := context.WithValue(background, readInvocationKey{}, call)
	for _, guard := range []actorReadGuard{{}, {actor: &Actor{}}} {
		called := false
		if e := guard.Run(ctx, state.Candidate, func(context.Context) error { called = true; return nil }); !errors.Is(e, NotAuthorized) || called || reader.calls != 0 {
			t.Fatal("zero guard accepted retained authority")
		}
	}
}

// This manually advanced parent context deterministically produces a deadline
// ONLY after the namespace pre-read has been reached; no wall-clock sleep or
// earlier gate timeout can accidentally satisfy the regression.
type readDeadlineContext struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func (c *readDeadlineContext) Done() <-chan struct{}       { return c.done }
func (c *readDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *readDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
func (c *readDeadlineContext) expire() { c.once.Do(func() { close(c.done) }) }

type namespaceReadFault struct {
	tc.Transport
	enabled bool
	reached bool
	expire  func()
}

func (f *namespaceReadFault) GetNamespace(ctx context.Context, r *cloud.GetNamespaceRequest, opts ...grpc.CallOption) (*cloud.GetNamespaceResponse, error) {
	if f.enabled {
		f.reached = true
		f.expire()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.Transport.GetNamespace(ctx, r, opts...)
}
func TestNamespacePreReadPreservesCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "deadline"}[deadline], func(t *testing.T) {
			f, reader := setupRead(t)
			transport := &namespaceReadFault{Transport: f.cloud}
			f.config.NamespaceTransport = transport
			a := must(OpenInitial(background, f.config))
			state := stageForRead(t, a, f)
			ctx := background
			want := observation.Canceled
			if deadline {
				manual := &readDeadlineContext{Context: background, done: make(chan struct{})}
				ctx = manual
				transport.expire = manual.expire
				want = observation.Deadline
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				transport.expire = cancel
			}
			transport.enabled = true
			_, e := a.ObserveStaged(ctx, readRequest(state))
			typed, ok := e.(*observation.Error)
			if !transport.reached || !ok || typed.Code != want || reader.calls != 0 || a.closed {
				t.Fatal("namespace pre-read cancellation was misclassified or fetched material")
			}
		})
	}
}
