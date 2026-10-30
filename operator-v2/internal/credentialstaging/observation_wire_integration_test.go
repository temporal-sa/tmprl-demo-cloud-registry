//go:build credentialobservation_integration

package credentialstaging

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	enums "go.temporal.io/api/enums/v1"
	namespaceinfo "go.temporal.io/api/namespace/v1"
	workflow "go.temporal.io/api/workflowservice/v1"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// All socket code is opt-in and never host-run. This shares the accepted
// scratch/loopback proof strategy, not a production fake/token factory.
func requireActorReadWireTier(t *testing.T) {
	t.Helper()
	if os.Getenv("REGISTRY_CREDENTIAL_OBSERVATION_TLS_TIER") != "owned-isolated" {
		t.Fatal("actor observation wire proof requires owned isolated runner")
	}
	interfaces, e := net.Interfaces()
	if e != nil {
		t.Fatal("owned network boundary unavailable")
	}
	loopback := false
	for _, i := range interfaces {
		if i.Flags&net.FlagUp == 0 {
			continue
		}
		if i.Flags&net.FlagLoopback == 0 {
			t.Fatal("external interface in actor wire tier")
		}
		loopback = true
	}
	if !loopback {
		t.Fatal("owned loopback unavailable")
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "TEMPORAL_API_KEY", "TEMPORAL_CLOUD_API_KEY"} {
		if os.Getenv(name) != "" {
			t.Fatal("ambient credential in actor wire tier")
		}
	}
}

// Synthetic memory storage, not an AWS encryption/IAM proof. The wrapper copies
// creation material only inside the actual store's synchronous SDK call, then
// supplies it only for the exact independently receipted ARN/version.
type actorWireSecrets struct {
	base    *fakeAWS
	mu      sync.Mutex
	key     []byte
	version string
	reads   int
	last    []byte
}

func (s *actorWireSecrets) CreateSecret(ctx context.Context, r *secretsmanager.CreateSecretInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	v, e := s.base.CreateSecret(ctx, r, opts...)
	if e == nil {
		s.mu.Lock()
		s.key = append([]byte(nil), r.SecretBinary...)
		s.version = aws.ToString(r.ClientRequestToken)
		s.mu.Unlock()
	}
	return v, e
}
func (s *actorWireSecrets) DescribeSecret(ctx context.Context, r *secretsmanager.DescribeSecretInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	return s.base.DescribeSecret(ctx, r, opts...)
}
func (s *actorWireSecrets) GetSecretValue(_ context.Context, r *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.base.metadata == nil || aws.ToString(r.SecretId) != aws.ToString(s.base.metadata.ARN) || aws.ToString(r.VersionId) != s.version || r.VersionStage != nil {
		return nil, errors.New(material)
	}
	s.last = append([]byte(nil), s.key...)
	return &secretsmanager.GetSecretValueOutput{ARN: s.base.metadata.ARN, Name: s.base.metadata.Name, VersionId: aws.String(s.version), SecretBinary: s.last}, nil
}

type actorWireListener struct {
	net.Listener
	accepted, closed atomic.Int32
	changed          chan struct{}
}
type actorWireConn struct {
	net.Conn
	owner *actorWireListener
	once  sync.Once
}

func (c *actorWireConn) Close() error {
	e := c.Conn.Close()
	c.once.Do(func() {
		c.owner.closed.Add(1)
		select {
		case c.owner.changed <- struct{}{}:
		default:
		}
	})
	return e
}
func (l *actorWireListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	l.accepted.Add(1)
	return &actorWireConn{Conn: c, owner: l}, nil
}
func (l *actorWireListener) waitClosed(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for l.closed.Load() != l.accepted.Load() {
		select {
		case <-l.changed:
		case <-timer.C:
			t.Fatal("actor retained candidate TLS connection after return")
		}
	}
}
func actorWireCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	clock := time.Now()
	key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-actor-read-ca"}, NotBefore: clock.Add(-time.Hour), NotAfter: clock.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER := must(x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key))
	ca = must(x509.ParseCertificate(caDER))
	leafKey := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"localhost"}, NotBefore: clock.Add(-time.Hour), NotAfter: clock.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der := must(x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key))
	return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: leafKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}

type actorWireAudit struct {
	mu      sync.Mutex
	methods map[string]int
	bad     bool
}

func (a *actorWireAudit) record(ctx context.Context, method, id string, data bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.methods[method]++
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer "+material {
		a.bad = true
	}
	if data {
		v := md.Get("temporal-namespace")
		if method != workflow.WorkflowService_DescribeNamespace_FullMethodName || len(v) != 1 || v[0] != id || len(md.Get("temporal-cloud-api-version")) != 0 {
			a.bad = true
		}
	} else {
		v := md.Get("temporal-cloud-api-version")
		if method != cloud.CloudService_GetCurrentIdentity_FullMethodName && method != cloud.CloudService_GetNamespace_FullMethodName || len(v) != 1 || v[0] != tc.SupportedAPIVersion || len(md.Get("temporal-namespace")) != 0 {
			a.bad = true
		}
	}
}
func (a *actorWireAudit) check(t *testing.T, want map[string]int) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.bad || sum(a.methods) != sum(want) {
		t.Fatal("actor TLS auth/routing/method counters mismatch")
	}
}

type actorReadCloudServer struct {
	cloud.UnimplementedCloudServiceServer
	f       *fixture
	foreign bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *actorReadCloudServer) GetCurrentIdentity(ctx context.Context, _ *cloud.GetCurrentIdentityRequest) (*cloud.GetCurrentIdentityResponse, error) {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, "owned read canceled")
		}
	}
	sa, e := s.f.cloud.GetServiceAccount(ctx, &cloud.GetServiceAccountRequest{ServiceAccountId: s.f.config.MetadataConfig.PreprovisionedServiceAccount.ID})
	if e != nil {
		return nil, status.Error(codes.Internal, "owned SA metadata unavailable")
	}
	if s.foreign {
		sa.ServiceAccount.Id = "other-owned-fixture-sa"
	}
	return &cloud.GetCurrentIdentityResponse{Principal: &cloud.GetCurrentIdentityResponse_ServiceAccount{ServiceAccount: sa.ServiceAccount}, PrincipalApiKey: proto.Clone(s.f.cloud.key).(*identity.ApiKey)}, nil
}
func (s *actorReadCloudServer) GetNamespace(ctx context.Context, r *cloud.GetNamespaceRequest) (*cloud.GetNamespaceResponse, error) {
	v, e := s.f.cloud.GetNamespace(ctx, r)
	if e != nil {
		return nil, status.Error(codes.Internal, "owned namespace unavailable")
	}
	v.Namespace.ProjectId = "owned-read-project"
	return v, nil
}

type actorReadDataServer struct {
	workflow.UnimplementedWorkflowServiceServer
	f *fixture
}

func (s *actorReadDataServer) DescribeNamespace(_ context.Context, r *workflow.DescribeNamespaceRequest) (*workflow.DescribeNamespaceResponse, error) {
	if !proto.Equal(r, &workflow.DescribeNamespaceRequest{Namespace: s.f.config.Namespace.NamespaceID}) {
		return nil, status.Error(codes.PermissionDenied, "owned route rejected")
	}
	return &workflow.DescribeNamespaceResponse{NamespaceInfo: &namespaceinfo.NamespaceInfo{Name: r.Namespace, Id: "owned-observed-data-id", State: enums.NAMESPACE_STATE_REGISTERED}}, nil
}
func startActorWire(t *testing.T, f *fixture, data bool, cloudServer *actorReadCloudServer) (*actorWireListener, *actorWireAudit, observation.Endpoint) {
	t.Helper()
	cert, ca := actorWireCertificate(t)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("owned actor TLS listener unavailable")
	}
	l := &actorWireListener{Listener: listener, changed: make(chan struct{}, 1)}
	audit := &actorWireAudit{methods: map[string]int{}}
	interceptor := func(ctx context.Context, r any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		audit.record(ctx, info.FullMethod, f.config.Namespace.NamespaceID, data)
		return next(ctx, r)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})), grpc.ConnectionTimeout(2*time.Second), grpc.UnaryInterceptor(interceptor))
	if data {
		workflow.RegisterWorkflowServiceServer(s, &actorReadDataServer{f: f})
	} else {
		cloud.RegisterCloudServiceServer(s, cloudServer)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Serve(l) }()
	t.Cleanup(func() { s.Stop(); _ = l.Close(); <-done })
	return l, audit, observation.Endpoint{Address: net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), ServerName: "localhost", RootCAPEM: ca}
}

type actorReadWireFixture struct {
	f                     *fixture
	a                     *Actor
	secret                *actorWireSecrets
	server                *actorReadCloudServer
	cloud, data           *actorWireListener
	cloudAudit, dataAudit *actorWireAudit
	clock                 *time.Time
}

func newActorReadWire(t *testing.T) *actorReadWireFixture {
	return newActorReadWireMode(t, false)
}
func newActorReadWireMode(t *testing.T, receipts bool) *actorReadWireFixture {
	t.Helper()
	f := setup(t, nil)
	store := &actorWireSecrets{base: f.aws}
	f.config.AWSClient = store
	server := &actorReadCloudServer{f: f}
	cl, ca, ce := startActorWire(t, f, false, server)
	dl, da, de := startActorWire(t, f, true, server)
	settings := ObservationSettings{ProjectID: "owned-read-project", TrustBindingID: "owned-wire-public-trust", ReaderBindingID: "independently-reviewed-fake-reader", Cloud: ce, Data: de, Timeout: 5 * time.Second}
	clock := now
	if receipts {
		freshReceiptBootstrap(t, f, settings, store, nil)
		f.config.Now = func() time.Time { return clock }
		writer := f.config.Writer
		f.config.Writer = func() ownership.Writer { w := writer(); w.ExpiresAt = clock.Add(time.Hour); return w }
	} else {
		freshReadBootstrap(t, f, settings, store)
	}
	a := must(OpenInitial(background, f.config))
	t.Cleanup(func() { store.mu.Lock(); clear(store.key); store.key = nil; clear(store.last); store.mu.Unlock() })
	return &actorReadWireFixture{f: f, a: a, secret: store, server: server, cloud: cl, data: dl, cloudAudit: ca, dataAudit: da, clock: &clock}
}

func (w *actorReadWireFixture) checkReceiptRead(t *testing.T, state tc.CredentialState, outcome awsstore.Outcome, result RecordObservationResult) {
	t.Helper()
	if !equalReceiptMetadata(t, w.a, state, outcome) || w.f.cloud.creates != 1 || w.f.aws.creates != 1 || w.f.cloud.deletes != 0 {
		t.Fatal("record observation changed staged credentials")
	}
	if bytes.Contains(must(json.Marshal(result.View())), []byte(material)) {
		t.Fatal("record observation disclosed synthetic material")
	}
	w.secret.mu.Lock()
	cleared := bytes.Equal(w.secret.last, make([]byte, len(w.secret.last)))
	w.secret.mu.Unlock()
	if !cleared {
		t.Fatal("record observation retained SDK payload")
	}
	w.cloud.waitClosed(t)
	w.data.waitClosed(t)
}
func (w *actorReadWireFixture) checkRead(t *testing.T, before string, p observation.Observation, e error) {
	t.Helper()
	raw, _ := json.Marshal(p)
	if bytes.Contains(raw, []byte(material)) || e != nil && strings.Contains(e.Error(), material) {
		t.Fatal("actor read disclosed synthetic key")
	}
	if sum(must(w.a.metadata.History(background))) != before {
		t.Fatal("observation changed credential/outcome history")
	}
	if w.f.cloud.creates != 1 || w.f.aws.creates != 1 || w.f.cloud.deletes != 0 {
		t.Fatal("observation created/revoked credentials")
	}
	w.secret.mu.Lock()
	cleared := bytes.Equal(w.secret.last, make([]byte, len(w.secret.last)))
	w.secret.mu.Unlock()
	if !cleared {
		t.Fatal("actor read retained SDK response material")
	}
	w.cloud.waitClosed(t)
	w.data.waitClosed(t)
}

func TestActorStagedObservationLocalTLSLifecycle(t *testing.T) {
	requireActorReadWireTier(t)
	t.Run("staged-read-and-clean-handoff-no-metadata-writes", func(t *testing.T) {
		w := newActorReadWire(t)
		state := stageForRead(t, w.a, w.f)
		before := sum(must(w.a.metadata.History(background)))
		proof, e := w.a.ObserveStaged(background, readRequest(state))
		w.checkRead(t, before, proof, e)
		if e != nil || proof.Contract != observation.Contract {
			t.Fatal("actual actor-owned observer failed")
		}
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1, cloud.CloudService_GetNamespace_FullMethodName: 2})
		w.dataAudit.check(t, map[string]int{workflow.WorkflowService_DescribeNamespace_FullMethodName: 1})
		checkpoint := must(w.a.Quiesce(background))
		w.a = must(OpenClean(background, w.f.config, must(ParseCheckpoint(must(checkpoint.JSON())))))
		proof, e = w.a.ObserveStaged(background, readRequest(state))
		w.checkRead(t, before, proof, e)
		if e != nil {
			t.Fatal("clean read-enabled handoff failed")
		}
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 2, cloud.CloudService_GetNamespace_FullMethodName: 4})
		w.dataAudit.check(t, map[string]int{workflow.WorkflowService_DescribeNamespace_FullMethodName: 2})
	})
	t.Run("failed-identity-retains-staged-history", func(t *testing.T) {
		w := newActorReadWire(t)
		state := stageForRead(t, w.a, w.f)
		before := sum(must(w.a.metadata.History(background)))
		w.server.foreign = true
		proof, e := w.a.ObserveStaged(background, readRequest(state))
		w.checkRead(t, before, proof, e)
		if e == nil {
			t.Fatal("foreign identity accepted")
		}
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1})
		w.dataAudit.check(t, map[string]int{})
	})
	t.Run("shutdown-drains-actual-TLS-before-handoff", func(t *testing.T) {
		w := newActorReadWire(t)
		state := stageForRead(t, w.a, w.f)
		before := sum(must(w.a.metadata.History(background)))
		w.server.entered = make(chan struct{})
		w.server.release = make(chan struct{})
		type result struct {
			p observation.Observation
			e error
		}
		read := make(chan result, 1)
		go func() { p, e := w.a.ObserveStaged(background, readRequest(state)); read <- result{p, e} }()
		select {
		case <-w.server.entered:
		case <-time.After(6 * time.Second):
			t.Fatal("owned actor read did not reach real TLS handler")
		}
		closed := make(chan error, 1)
		started := make(chan struct{})
		go func() { close(started); _, e := w.a.Quiesce(background); closed <- e }()
		<-started
		select {
		case <-closed:
			t.Fatal("handoff escaped active TLS read")
		case <-time.After(20 * time.Millisecond):
		}
		close(w.server.release)
		r := <-read
		if e := <-closed; e != nil {
			t.Fatal("drained actor handoff failed")
		}
		w.checkRead(t, before, r.p, r.e)
		if r.e != nil {
			t.Fatal("read failed while shutdown waited")
		}
		w.secret.mu.Lock()
		reads := w.secret.reads
		w.secret.mu.Unlock()
		_, e := w.a.ObserveStaged(background, readRequest(state))
		if !errors.Is(e, Closed) {
			t.Fatal("closed actor read reopened")
		}
		w.secret.mu.Lock()
		defer w.secret.mu.Unlock()
		if w.secret.reads != reads {
			t.Fatal("closed actor fetched material")
		}
	})
	t.Run("canceled-read-keeps-staged-and-actor-open", func(t *testing.T) {
		w := newActorReadWire(t)
		state := stageForRead(t, w.a, w.f)
		before := sum(must(w.a.metadata.History(background)))
		w.server.entered = make(chan struct{})
		w.server.release = make(chan struct{})
		ctx, cancel := context.WithCancel(background)
		type result struct {
			p observation.Observation
			e error
		}
		done := make(chan result, 1)
		go func() { p, e := w.a.ObserveStaged(ctx, readRequest(state)); done <- result{p, e} }()
		select {
		case <-w.server.entered:
		case <-time.After(6 * time.Second):
			cancel()
			t.Fatal("owned canceled read did not reach TLS")
		}
		cancel()
		r := <-done
		w.checkRead(t, before, r.p, r.e)
		if r.e == nil || w.a.closed {
			t.Fatal("canceled diagnostic mutated actor lifecycle")
		}
	})
	t.Run("changed-read-binding-cannot-clean-resume", func(t *testing.T) {
		w := newActorReadWire(t)
		stageForRead(t, w.a, w.f)
		checkpoint := must(w.a.Quiesce(background))
		config := w.f.config
		copyRuntime := *config.Observation
		copyRuntime.Settings.ReaderBindingID = "different-reader"
		config.Observation = &copyRuntime
		if _, e := OpenClean(background, config, checkpoint); e == nil {
			t.Fatal("clean checkpoint accepted changed read authority")
		}
		if w.secret.reads != 0 {
			t.Fatal("changed binding fetched material")
		}
	})
	t.Run("terminal-auth-receipt-and-clean-historical-replay", func(t *testing.T) {
		w := newActorReadWireMode(t, true)
		state := stageForRead(t, w.a, w.f)
		outcome := mustOutcome(t, w.a, state.CandidateWriteID)
		before := len(must(w.a.metadata.History(background)))
		request := recordRequest(state, "actual-record-success")
		result, e := w.a.RecordAuthObservation(background, request)
		if e != nil || result.private.Acknowledgement != ReceiptRecorded || result.private.Receipt == nil || result.private.Receipt.Kind != localmetadata.AuthObserved || !equalReceiptMetadata(t, w.a, state, outcome) || len(must(w.a.metadata.History(background))) != before+1 {
			t.Fatal("actual actor observation did not record terminal evidence")
		}
		w.checkReceiptRead(t, state, outcome, result)
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1, cloud.CloudService_GetNamespace_FullMethodName: 2})
		w.dataAudit.check(t, map[string]int{workflow.WorkflowService_DescribeNamespace_FullMethodName: 1})
		original := result.private.Receipt.Authority.SessionID
		checkpoint := must(w.a.Quiesce(background))
		*w.clock = now.Add(2 * time.Hour)
		w.a = must(OpenClean(background, w.f.config, must(ParseCheckpoint(must(checkpoint.JSON())))))
		historical, e := w.a.RecordAuthObservation(background, request)
		if e != nil || historical.private.Acknowledgement != ReceiptHistorical || historical.private.Receipt.Authority.SessionID != original || w.a.snapshot.Envelope.Admission.SessionID == original || !historical.private.Receipt.RecordedAt.Equal(result.private.Receipt.RecordedAt) {
			t.Fatal("clean successor replaced history or failed expired lookup")
		}
		w.checkReceiptRead(t, state, outcome, historical)
		w.secret.mu.Lock()
		reads := w.secret.reads
		w.secret.mu.Unlock()
		if reads != 1 {
			t.Fatal("historical replay fetched secret")
		}
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1, cloud.CloudService_GetNamespace_FullMethodName: 2})
		w.dataAudit.check(t, map[string]int{workflow.WorkflowService_DescribeNamespace_FullMethodName: 1})
	})
	t.Run("terminal-known-expiry-zero-sdk-tls", func(t *testing.T) {
		w := newActorReadWireMode(t, true)
		state := stageForRead(t, w.a, w.f)
		outcome := mustOutcome(t, w.a, state.CandidateWriteID)
		reads := w.f.cloud.reads
		*w.clock = now.Add(2 * time.Hour)
		result, e := w.a.RecordAuthObservation(background, recordRequest(state, "actual-expired-noauth"))
		failure := finiteObservationFailure(e)
		if failure == nil || failure.Code != observation.Expired || result.private.Acknowledgement != ReceiptRecorded || result.private.Receipt.Kind != localmetadata.AuthNotEstablished || w.secret.reads != 0 || w.f.cloud.reads != reads {
			t.Fatal("known expiry authenticated candidate or lost finite record")
		}
		w.checkReceiptRead(t, state, outcome, result)
		w.cloudAudit.check(t, map[string]int{})
		w.dataAudit.check(t, map[string]int{})
	})
	t.Run("terminal-identity-failure-retains-original-cause", func(t *testing.T) {
		w := newActorReadWireMode(t, true)
		state := stageForRead(t, w.a, w.f)
		outcome := mustOutcome(t, w.a, state.CandidateWriteID)
		w.server.foreign = true
		request := recordRequest(state, "actual-identity-failure")
		result, e := w.a.RecordAuthObservation(background, request)
		failure := finiteObservationFailure(e)
		if failure == nil || failure.Code != observation.IdentityUnproven || result.private.Acknowledgement != ReceiptRecorded || result.private.Receipt.Failure.Code != failure.Code || result.private.ObservationFailure.Code != failure.Code {
			t.Fatal("auth failure replaced by receipt outcome")
		}
		w.checkReceiptRead(t, state, outcome, result)
		historical, e := w.a.RecordAuthObservation(background, request)
		if e != nil || historical.private.Acknowledgement != ReceiptHistorical || historical.private.ObservationFailure.Code != failure.Code {
			t.Fatal("failed receipt not historical")
		}
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1})
		w.dataAudit.check(t, map[string]int{})
	})
	t.Run("terminal-record-drains-before-ready", func(t *testing.T) {
		w := newActorReadWireMode(t, true)
		state := stageForRead(t, w.a, w.f)
		outcome := mustOutcome(t, w.a, state.CandidateWriteID)
		w.server.entered = make(chan struct{})
		w.server.release = make(chan struct{})
		type recorded struct {
			r RecordObservationResult
			e error
		}
		done := make(chan recorded, 1)
		go func() {
			r, e := w.a.RecordAuthObservation(background, recordRequest(state, "actual-record-drain"))
			done <- recorded{r, e}
		}()
		select {
		case <-w.server.entered:
		case <-time.After(6 * time.Second):
			t.Fatal("record did not reach TLS")
		}
		ready := make(chan error, 1)
		started := make(chan struct{})
		go func() { close(started); _, e := w.a.Quiesce(background); ready <- e }()
		<-started
		select {
		case <-ready:
			t.Fatal("ready escaped active record")
		case <-time.After(20 * time.Millisecond):
		}
		close(w.server.release)
		r := <-done
		if e := <-ready; e != nil || r.e != nil || r.r.private.Acknowledgement != ReceiptRecorded {
			t.Fatal("record drain/ack failed")
		}
		w.checkReceiptRead(t, state, outcome, r.r)
		if _, e := w.a.RecordAuthObservation(background, recordRequest(state, "after-closed")); !errors.Is(e, Closed) {
			t.Fatal("closed recording reopened")
		}
	})
	t.Run("terminal-capacity-prevents-another-tls-read", func(t *testing.T) {
		w := newActorReadWireMode(t, true)
		state := stageForRead(t, w.a, w.f)
		outcome := mustOutcome(t, w.a, state.CandidateWriteID)
		for _, id := range []string{"actual-cap-one", "actual-cap-two"} {
			r, e := w.a.RecordAuthObservation(background, recordRequest(state, id))
			if e != nil || r.private.Acknowledgement != ReceiptRecorded {
				t.Fatal("fixture did not fill receipt capacity")
			}
		}
		r, e := w.a.RecordAuthObservation(background, recordRequest(state, "actual-cap-three"))
		if !errors.Is(e, AuthReceiptCapacity) || r.private.Acknowledgement != ReceiptRejected || w.secret.reads != 2 {
			t.Fatal("capacity allowed another TLS/secret read")
		}
		w.checkReceiptRead(t, state, outcome, r)
		w.cloudAudit.check(t, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 2, cloud.CloudService_GetNamespace_FullMethodName: 4})
		w.dataAudit.check(t, map[string]int{workflow.WorkflowService_DescribeNamespace_FullMethodName: 2})
	})
	t.Run("terminal-caller-cancellation-no-fabricated-receipt", func(t *testing.T) {
		w := newActorReadWireMode(t, true)
		state := stageForRead(t, w.a, w.f)
		outcome := mustOutcome(t, w.a, state.CandidateWriteID)
		before := sum(must(w.a.metadata.History(background)))
		w.server.entered = make(chan struct{})
		w.server.release = make(chan struct{})
		ctx, cancel := context.WithCancel(background)
		type recorded struct {
			r RecordObservationResult
			e error
		}
		done := make(chan recorded, 1)
		go func() {
			r, e := w.a.RecordAuthObservation(ctx, recordRequest(state, "actual-record-canceled"))
			done <- recorded{r, e}
		}()
		select {
		case <-w.server.entered:
		case <-time.After(6 * time.Second):
			cancel()
			t.Fatal("record cancellation did not reach TLS")
		}
		cancel()
		r := <-done
		if r.e == nil || r.r.private.Acknowledgement != ReceiptNotAttempted || r.r.private.Receipt != nil || r.r.private.ObservationFailure == nil || r.r.private.ObservationFailure.Code != observation.Canceled || sum(must(w.a.metadata.History(background))) != before {
			t.Fatal("canceled record fabricated receipt or lost cause")
		}
		w.checkReceiptRead(t, state, outcome, r.r)
	})
}
