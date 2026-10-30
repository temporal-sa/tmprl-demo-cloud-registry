//go:build credentialobservation_integration

package credentialobservation

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
	"io"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	workflow "go.temporal.io/api/workflowservice/v1"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const wireTierEnvironment = "REGISTRY_CREDENTIAL_OBSERVATION_TLS_TIER"
const wireTierValue = "owned-isolated"

// This tier is NEVER host-run. Default builds omit this complete file. The
// runner supplies a finite exact selection plus marker only in its no-network
// scratch container; the independent boundary binary verifies the full kernel,
// mount and credential contract. The checks here fail before creating sockets.
func requireOwnedWireTier(t *testing.T) {
	t.Helper()
	if os.Getenv(wireTierEnvironment) != wireTierValue {
		t.Fatal("credential observation TLS proof requires the owned isolated runner")
	}
	interfaces, e := net.Interfaces()
	if e != nil {
		t.Fatal("cannot verify owned network boundary")
	}
	loopback := false
	for _, i := range interfaces {
		if i.Flags&net.FlagUp == 0 {
			continue
		}
		if i.Flags&net.FlagLoopback == 0 {
			t.Fatal("non-loopback interface in TLS proof tier")
		}
		loopback = true
	}
	if !loopback {
		t.Fatal("owned loopback unavailable")
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "TEMPORAL_API_KEY", "TEMPORAL_CLOUD_API_KEY"} {
		if os.Getenv(name) != "" {
			t.Fatal("ambient credential variable in TLS proof tier")
		}
	}
}

type wireCertificate struct {
	certificate tls.Certificate
	ca          []byte
}

func newWireCertificate(t *testing.T, name string) wireCertificate {
	t.Helper()
	now := time.Now()
	caKey := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-loopback-ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	rootDER := must(x509.CreateCertificate(rand.Reader, root, root, &caKey.PublicKey, caKey))
	root = must(x509.ParseCertificate(rootDER))
	leafKey := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER := must(x509.CreateCertificate(rand.Reader, leaf, root, &leafKey.PublicKey, caKey))
	return wireCertificate{certificate: tls.Certificate{Certificate: [][]byte{leafDER, rootDER}, PrivateKey: leafKey}, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})}
}

type wireCounts struct {
	mu      sync.Mutex
	methods map[string]int
	bad     bool
}

func (w *wireCounts) record(method string, md metadata.MD, namespace string, data bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.methods[method]++
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer "+sentinel {
		w.bad = true
	}
	if len(md.Get("wire-private-trace")) != 0 {
		w.bad = true
	}
	if data {
		values := md.Get("temporal-namespace")
		if method != workflow.WorkflowService_DescribeNamespace_FullMethodName || len(values) != 1 || values[0] != namespace || len(md.Get("temporal-cloud-api-version")) != 0 {
			w.bad = true
		}
	} else {
		values := md.Get("temporal-cloud-api-version")
		if method != cloud.CloudService_GetCurrentIdentity_FullMethodName && method != cloud.CloudService_GetNamespace_FullMethodName || len(values) != 1 || values[0] != tc.SupportedAPIVersion || len(md.Get("temporal-namespace")) != 0 {
			w.bad = true
		}
	}
}
func (w *wireCounts) snapshot() (map[string]int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := map[string]int{}
	for k, v := range w.methods {
		m[k] = v
	}
	return m, w.bad
}

type wireListener struct {
	net.Listener
	accepted, closed atomic.Int32
	changed          chan struct{}
}
type wireConn struct {
	net.Conn
	owner *wireListener
	once  sync.Once
}

func (c *wireConn) Close() error {
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
func (l *wireListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	l.accepted.Add(1)
	return &wireConn{Conn: c, owner: l}, nil
}
func (l *wireListener) endpoint(ca []byte) Endpoint {
	return Endpoint{Address: net.JoinHostPort("localhost", strconv.Itoa(l.Addr().(*net.TCPAddr).Port)), ServerName: "localhost", RootCAPEM: ca}
}
func (l *wireListener) waitClosed(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for l.closed.Load() != l.accepted.Load() {
		select {
		case <-l.changed:
		case <-deadline.C:
			t.Fatal("observer retained an owned TLS connection after return")
		}
	}
}
func newWireListener(t *testing.T) *wireListener {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("owned loopback listener unavailable")
	}
	return &wireListener{Listener: l, changed: make(chan struct{}, 1)}
}

type wireCloudServer struct {
	cloud.UnimplementedCloudServiceServer
	fixture *fixture
	foreign bool
	entered chan struct{}
	once    sync.Once
	block   bool
}

func (s *wireCloudServer) GetCurrentIdentity(ctx context.Context, _ *cloud.GetCurrentIdentityRequest) (*cloud.GetCurrentIdentityResponse, error) {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
	}
	if s.block {
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, "owned request canceled")
	}
	r := proto.Clone(s.fixture.probes.identity).(*cloud.GetCurrentIdentityResponse)
	if s.foreign {
		r.GetServiceAccount().Id = "foreign-owned-fixture-sa"
	}
	return r, nil
}
func (s *wireCloudServer) GetNamespace(_ context.Context, r *cloud.GetNamespaceRequest) (*cloud.GetNamespaceResponse, error) {
	if !proto.Equal(r, &cloud.GetNamespaceRequest{Namespace: s.fixture.config.Namespace.NamespaceID}) {
		return nil, status.Error(codes.PermissionDenied, "owned request rejected")
	}
	return &cloud.GetNamespaceResponse{Namespace: proto.Clone(s.fixture.probes.namespace).(*namespace.Namespace)}, nil
}

type wireDataServer struct {
	workflow.UnimplementedWorkflowServiceServer
	fixture *fixture
}

func (s *wireDataServer) DescribeNamespace(_ context.Context, r *workflow.DescribeNamespaceRequest) (*workflow.DescribeNamespaceResponse, error) {
	if !proto.Equal(r, &workflow.DescribeNamespaceRequest{Namespace: s.fixture.config.Namespace.NamespaceID}) {
		return nil, status.Error(codes.PermissionDenied, "owned request rejected")
	}
	return proto.Clone(s.fixture.probes.data).(*workflow.DescribeNamespaceResponse), nil
}

func startWireServer(t *testing.T, f *fixture, isData bool, cert wireCertificate, server *wireCloudServer) (*wireListener, *wireCounts) {
	t.Helper()
	listener := newWireListener(t)
	audit := &wireCounts{methods: map[string]int{}}
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		audit.record(info.FullMethod, md, f.config.Namespace.NamespaceID, isData)
		return next(ctx, req)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert.certificate}, MinVersion: tls.VersionTLS12})), grpc.ConnectionTimeout(2*time.Second), grpc.UnaryInterceptor(interceptor))
	if isData {
		workflow.RegisterWorkflowServiceServer(s, &wireDataServer{fixture: f})
	} else {
		if server == nil {
			server = &wireCloudServer{fixture: f}
		}
		cloud.RegisterCloudServiceServer(s, server)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Serve(listener) }()
	t.Cleanup(func() { s.Stop(); _ = listener.Close(); <-done })
	return listener, audit
}

type wireFixture struct {
	f                     *fixture
	observer              *Observer
	cloud, data           *wireListener
	cloudAudit, dataAudit *wireCounts
}

func publicWireFixture(t *testing.T, cloudName string, server *wireCloudServer) *wireFixture {
	t.Helper()
	// Reuse only immutable metadata and narrow AWS/state/Guard fixtures. The
	// default private fake observer is discarded without ever being invoked.
	f := setup(t)
	f.observer = nil
	cloudCert, dataCert := newWireCertificate(t, cloudName), newWireCertificate(t, "localhost")
	if server != nil {
		server.fixture = f
	}
	cl, ca := startWireServer(t, f, false, cloudCert, server)
	dl, da := startWireServer(t, f, true, dataCert, nil)
	f.config.Cloud = cl.endpoint(cloudCert.ca)
	f.config.Data = dl.endpoint(dataCert.ca)
	f.config.Timeout = 5 * time.Second
	observer, e := New(f.config, f.records, outcomeReader{f.records}, f.secrets, f.guard)
	if e != nil {
		t.Fatal("public observer construction failed")
	}
	return &wireFixture{f: f, observer: observer, cloud: cl, data: dl, cloudAudit: ca, dataAudit: da}
}
func safeWireResult(t *testing.T, w *wireFixture, p Observation, e error) {
	t.Helper()
	raw, _ := json.Marshal(p)
	if bytes.Contains(raw, []byte(sentinel)) || e != nil && strings.Contains(e.Error(), sentinel) {
		t.Fatal("synthetic material escaped TLS observation result")
	}
	if e != nil {
		var typed *Error
		if !errors.As(e, &typed) {
			t.Fatal("unbounded provider error escaped TLS observation")
		}
	}
	if w.f.records.state.Phase != tc.CredentialStaged {
		t.Fatal("wire observation mutated credential phase")
	}
	if w.f.secrets.reads > 0 && !bytes.Equal(w.f.secrets.lastBytes, make([]byte, len(w.f.secrets.lastBytes))) {
		t.Fatal("wire observation retained SDK material buffer")
	}
	w.cloud.waitClosed(t)
	w.data.waitClosed(t)
}
func requireWireCounts(t *testing.T, w *wireCounts, expected map[string]int) {
	t.Helper()
	actual, bad := w.snapshot()
	if bad || !same(actual, expected) {
		t.Fatal("TLS wire authentication, routing or finite method count mismatch")
	}
}

// refusedWirePeer is an actual owned TLS HTTP/2 endpoint. Production's public
// constructor fixes zero retry buffering. The proof is one authenticated wire
// request/no replay and closure, not forced second-credential-budget execution.
func refusedWirePeer(t *testing.T, f *fixture, isData bool) (*wireListener, []byte, *wireCounts, *atomic.Int32) {
	t.Helper()
	cert := newWireCertificate(t, "localhost")
	listener := newWireListener(t)
	tl := tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{cert.certificate}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}})
	audit := &wireCounts{methods: map[string]int{}}
	resets := &atomic.Int32{}
	var wg sync.WaitGroup
	var sockets sync.Map
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, e := tl.Accept()
			if e != nil {
				return
			}
			sockets.Store(conn, true)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				defer sockets.Delete(conn)
				_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, e := io.ReadFull(conn, preface); e != nil || string(preface) != http2.ClientPreface {
					return
				}
				framer := http2.NewFramer(conn, conn)
				framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
				framer.MaxHeaderListSize = 16384
				if framer.WriteSettings() != nil {
					return
				}
				for {
					frame, e := framer.ReadFrame()
					if e != nil {
						return
					}
					switch v := frame.(type) {
					case *http2.SettingsFrame:
						if !v.IsAck() {
							_ = framer.WriteSettingsAck()
						}
					case *http2.MetaHeadersFrame:
						md := metadata.MD{}
						method := ""
						for _, h := range v.Fields {
							if h.Name == ":path" {
								method = h.Value
							} else {
								md.Append(h.Name, h.Value)
							}
						}
						audit.record(method, md, f.config.Namespace.NamespaceID, isData)
						if v.Truncated {
							audit.mu.Lock()
							audit.bad = true
							audit.mu.Unlock()
						}
						if framer.WriteRSTStream(v.StreamID, http2.ErrCodeRefusedStream) != nil {
							return
						}
						resets.Add(1)
					case *http2.PingFrame:
						if !v.IsAck() {
							_ = framer.WritePing(true, v.Data)
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = tl.Close()
		<-acceptDone
		sockets.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		wg.Wait()
	})
	return listener, cert.ca, audit, resets
}

type afterReadSecret struct {
	SecretClient
	after func()
}

func (s afterReadSecret) GetSecretValue(ctx context.Context, r *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	v, e := s.SecretClient.GetSecretValue(ctx, r, opts...)
	s.after()
	return v, e
}

func TestCandidateObservationLocalTLSWireBoundary(t *testing.T) {
	requireOwnedWireTier(t)
	t.Run("candidate-only-success-and-connection-closure", func(t *testing.T) {
		w := publicWireFixture(t, "localhost", nil)
		ctx := metadata.NewOutgoingContext(bg, metadata.Pairs("wire-private-trace", sentinel))
		proof, e := w.observer.Observe(ctx, w.f.ref)
		safeWireResult(t, w, proof, e)
		if e != nil || proof.Contract != Contract {
			t.Fatal("public generated TLS observation failed")
		}
		requireWireCounts(t, w.cloudAudit, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1, cloud.CloudService_GetNamespace_FullMethodName: 2})
		requireWireCounts(t, w.dataAudit, map[string]int{workflow.WorkflowService_DescribeNamespace_FullMethodName: 1})
		if w.cloud.accepted.Load() != 1 || w.data.accepted.Load() != 1 {
			t.Fatal("unexpected TLS connection reuse/budget")
		}
		t.Log("wire identity=1 namespace=2 data=1; both owned clients closed; no promotion evidence")
	})
	for _, mode := range []string{"wrong-trust", "wrong-SNI", "foreign-identity", "denied-metadata", "canceled-before-dispatch", "inherited-admin-auth", "inherited-namespace-route"} {
		t.Run(mode, func(t *testing.T) {
			name := "localhost"
			if mode == "wrong-SNI" {
				name = "other-owned-name.local"
			}
			server := &wireCloudServer{foreign: mode == "foreign-identity"}
			w := publicWireFixture(t, name, server)
			ctx := bg
			switch mode {
			case "wrong-trust":
				w.f.config.Cloud.RootCAPEM = newWireCertificate(t, "localhost").ca
				w.observer = must(New(w.f.config, w.f.records, outcomeReader{w.f.records}, w.f.secrets, w.f.guard))
			case "denied-metadata":
				w.f.records.fail = true
			case "canceled-before-dispatch":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "inherited-admin-auth":
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "unrelated-owned-admin"))
			case "inherited-namespace-route":
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("temporal-namespace", "other-owned-namespace"))
			}
			proof, e := w.observer.Observe(ctx, w.f.ref)
			safeWireResult(t, w, proof, e)
			if e == nil {
				t.Fatal("invalid TLS/authority observation accepted")
			}
			expected := map[string]int{}
			if mode == "foreign-identity" {
				expected[cloud.CloudService_GetCurrentIdentity_FullMethodName] = 1
			}
			requireWireCounts(t, w.cloudAudit, expected)
			requireWireCounts(t, w.dataAudit, map[string]int{})
			if (mode == "wrong-trust" || mode == "wrong-SNI") && w.cloud.accepted.Load() == 0 {
				t.Fatal("TLS negative did not reach its real owned listener")
			}
			if mode == "denied-metadata" || mode == "canceled-before-dispatch" || strings.HasPrefix(mode, "inherited-") {
				if w.f.secrets.reads != 0 || w.cloud.accepted.Load() != 0 || w.data.accepted.Load() != 0 {
					t.Fatal("denied authority reached secret or TLS transport")
				}
			}
		})
	}
	t.Run("cancel-in-flight-drains-generated-client", func(t *testing.T) {
		entered := make(chan struct{})
		server := &wireCloudServer{entered: entered, block: true}
		w := publicWireFixture(t, "localhost", server)
		ctx, cancel := context.WithCancel(bg)
		type result struct {
			proof Observation
			err   error
		}
		done := make(chan result, 1)
		go func() { p, e := w.observer.Observe(ctx, w.f.ref); done <- result{p, e} }()
		select {
		case <-entered:
		case <-time.After(6 * time.Second):
			cancel()
			t.Fatal("owned TLS call did not reach handler")
		}
		cancel()
		r := <-done
		safeWireResult(t, w, r.proof, r.err)
		if r.err == nil {
			t.Fatal("canceled TLS request returned evidence")
		}
		requireWireCounts(t, w.cloudAudit, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1})
		requireWireCounts(t, w.dataAudit, map[string]int{})
	})
	t.Run("expired-before-first-authenticated-headers", func(t *testing.T) {
		w := publicWireFixture(t, "localhost", nil)
		var advanced atomic.Bool
		w.f.config.Now = func() time.Time {
			if advanced.Load() {
				return at.Add(10 * time.Second)
			}
			return at
		}
		secret := afterReadSecret{SecretClient: w.f.secrets, after: func() { advanced.Store(true) }}
		w.observer = must(New(w.f.config, w.f.records, outcomeReader{w.f.records}, secret, w.f.guard))
		proof, e := w.observer.Observe(bg, w.f.ref)
		safeWireResult(t, w, proof, e)
		if e == nil {
			t.Fatal("late credential attempt returned observation")
		}
		if w.f.secrets.reads != 1 {
			t.Fatal("expiry proof did not reach material boundary")
		}
		requireWireCounts(t, w.cloudAudit, map[string]int{})
		requireWireCounts(t, w.dataAudit, map[string]int{})
	})
	for _, isData := range []bool{false, true} {
		name := "refused-cloud-no-authenticated-replay"
		if isData {
			name = "refused-data-no-authenticated-replay"
		}
		t.Run(name, func(t *testing.T) {
			w := publicWireFixture(t, "localhost", nil)
			peer, roots, audit, resets := refusedWirePeer(t, w.f, isData)
			if isData {
				w.f.config.Data = peer.endpoint(roots)
			} else {
				w.f.config.Cloud = peer.endpoint(roots)
			}
			w.observer = must(New(w.f.config, w.f.records, outcomeReader{w.f.records}, w.f.secrets, w.f.guard))
			proof, e := w.observer.Observe(bg, w.f.ref)
			safeWireResult(t, w, proof, e)
			peer.waitClosed(t)
			if e == nil {
				t.Fatal("REFUSED_STREAM returned observation")
			}
			expected := cloud.CloudService_GetCurrentIdentity_FullMethodName
			if isData {
				expected = workflow.WorkflowService_DescribeNamespace_FullMethodName
				requireWireCounts(t, w.cloudAudit, map[string]int{cloud.CloudService_GetCurrentIdentity_FullMethodName: 1, cloud.CloudService_GetNamespace_FullMethodName: 1})
			}
			requireWireCounts(t, audit, map[string]int{expected: 1})
			if resets.Load() != 1 {
				t.Fatal("owned peer did not send exactly one REFUSED_STREAM")
			}
			t.Log("REFUSED_STREAM: exactly one authenticated wire request; closed connection; zero-buffer transport does not require a second credential attempt")
		})
	}
}
