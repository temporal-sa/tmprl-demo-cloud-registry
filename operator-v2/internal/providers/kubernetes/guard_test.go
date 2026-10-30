package kubernetes

import (
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
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func requireCode(t *testing.T, err error, code Code) {
	t.Helper()
	var observed *Error
	if !errors.As(err, &observed) || observed.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

type fixture struct {
	base      ownership.Context
	intent    ownership.Intent
	writer    ownership.Writer
	parent    ownership.Record
	namespace *corev1.Namespace
	options   Connection
	requests  atomic.Int64
	writes    atomic.Int64
	mu        sync.RWMutex
}

func tlsServer(t *testing.T, handler http.Handler) (*httptest.Server, Credentials) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "local-test-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certificate := func(serial int64, usage x509.ExtKeyUsage) ([]byte, []byte) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "local-test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
	}
	serverCert, serverKey := certificate(2, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := certificate(3, x509.ExtKeyUsageClientAuth)
	serverTLS, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverTLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	t.Cleanup(server.Close)
	credentials, err := NewCredentials(caPEM, clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return server, credentials
}

func guardFixture(t *testing.T) *fixture {
	t.Helper()
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "fixture", ProjectUID: "project-one", Lifetime: ownership.Disposable, RunID: "run-one"}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "owned-local"}}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := base.Intent(ownership.Kubernetes, "runtime-namespace", "attempt-one", "revision-one", "namespace-create-one", "namespace-spec-one", 63)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "writer-one", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	annotations := CreationAnnotations(intent, writer)
	annotations[AttemptAnnotation] = "attempt-one"
	namespace := &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: intent.Name, UID: types.UID("namespace-uid-one"), ResourceVersion: "100", Annotations: annotations}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	parent := ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: string(namespace.UID)}, Owner: intent.Owner, CreationOperationID: intent.OperationID, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch}
	f := &fixture{base: base, intent: intent, writer: writer, parent: parent, namespace: namespace}
	server, credentials := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		if r.Method != "GET" {
			f.writes.Add(1)
			t.Error("read-only fixture received a write")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/kube-system":
			_ = json.NewEncoder(w).Encode(&corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "owned-cluster-uid", ResourceVersion: "1"}})
		case "/api/v1/namespaces/" + intent.Name:
			f.mu.RLock()
			snapshot := f.namespace.DeepCopy()
			f.mu.RUnlock()
			_ = json.NewEncoder(w).Encode(snapshot)
		default:
			http.NotFound(w, r)
		}
	}))
	f.options = Connection{Mode: LocalSerialized, Endpoint: server.URL, Identity: ClusterIdentity{ID: "owned-local", KubeSystemUID: "owned-cluster-uid"}, Credentials: credentials, CallTimeout: time.Second}
	return f
}

func TestClientRequiresExplicitAuthorityAndRejectsUnsupportedModes(t *testing.T) {
	for _, change := range []func(*Connection){
		func(c *Connection) { c.Mode = "production" }, func(c *Connection) { c.Mode = "distributed" }, func(c *Connection) { c.Endpoint = "https://localhost:6443" }, func(c *Connection) { c.Endpoint = "http://127.0.0.1:6443" }, func(c *Connection) { c.Endpoint += "?token=secret" }, func(c *Connection) { c.Credentials = Credentials{} }, func(c *Connection) { c.Identity.KubeSystemUID = "" }, func(c *Connection) { c.CallTimeout = 0 },
	} {
		f := guardFixture(t)
		options := f.options
		change(&options)
		if _, err := New(context.Background(), options); err == nil {
			t.Fatal("invalid authority accepted")
		}
		if f.requests.Load() != 0 {
			t.Fatal("invalid configuration reached API")
		}
	}
}

func TestClientPinsActualClusterUIDAndIgnoresAmbientProxy(t *testing.T) {
	f := guardFixture(t)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9")
	t.Setenv("KUBECONFIG", "/unreadable/production-config")
	client, err := New(context.Background(), f.options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.BindNamespace(context.Background(), f.base, f.parent, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	f.options.Identity.KubeSystemUID = "another-cluster"
	_, err = New(context.Background(), f.options)
	requireCode(t, err, ClusterMismatch)
	if f.writes.Load() != 0 {
		t.Fatal("cluster observation wrote resources")
	}
}

func TestTypedGVKScopeDoesNotTrustNamespaceField(t *testing.T) {
	for _, gvk := range []schema.GroupVersionKind{{Group: "", Version: "v1", Kind: "ConfigMap"}, {Group: "", Version: "v1", Kind: "Namespace"}} {
		namespaced, err := ClassifyScope(gvk)
		if err != nil || namespaced != (gvk.Kind == "ConfigMap") {
			t.Fatal("scope classification failed")
		}
	}
	for _, gvk := range []schema.GroupVersionKind{{Version: "v2", Kind: "ConfigMap"}, {Group: "foreign", Version: "v1", Kind: "ConfigMap"}, {Version: "v1", Kind: "ClusterRole"}, {Version: "v1", Kind: "Secret"}} {
		_, err := ClassifyScope(gvk)
		requireCode(t, err, UnsupportedKind)
	}
}

func TestParentObservationRejectsTerminationOwnershipAndReplacement(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*fixture)
		code   Code
	}{
		{"terminating", func(f *fixture) { value := metav1.Now(); f.namespace.DeletionTimestamp = &value }, ParentTerminating},
		{"terminating phase", func(f *fixture) { f.namespace.Status.Phase = corev1.NamespaceTerminating }, ParentTerminating},
		{"unknown phase", func(f *fixture) { f.namespace.Status.Phase = "" }, ParentUnavailable},
		{"recreated UID", func(f *fixture) { f.namespace.UID = "replacement-uid" }, OwnershipRejected},
		{"missing owner", func(f *fixture) { delete(f.namespace.Annotations, OwnerAnnotation) }, OwnershipRejected},
		{"foreign owner", func(f *fixture) { f.namespace.Annotations[OwnerAnnotation] = `{"environmentId":"production"}` }, OwnershipRejected},
		{"missing creation operation", func(f *fixture) { delete(f.namespace.Annotations, CreationOperationAnnotation) }, OwnershipRejected},
		{"foreign creation key", func(f *fixture) { f.namespace.Annotations[CreationKeyAnnotation] = "another-operation" }, OwnershipRejected},
		{"foreign attempt", func(f *fixture) { f.namespace.Annotations[AttemptAnnotation] = "attempt-two" }, OwnershipRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := guardFixture(t)
			client, err := New(context.Background(), f.options)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			test.mutate(f)
			f.mu.Unlock()
			_, err = client.BindNamespace(context.Background(), f.base, f.parent, "attempt-one")
			requireCode(t, err, test.code)
			if f.writes.Load() != 0 {
				t.Fatal("rejected observation wrote resources")
			}
		})
	}
}

func TestExistingBindingRechecksParentIncarnation(t *testing.T) {
	f := guardFixture(t)
	client, err := New(context.Background(), f.options)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := client.BindNamespace(context.Background(), f.base, f.parent, "attempt-one")
	if err != nil {
		t.Fatal(err)
	}
	if bound.Context().NamespaceName() != f.intent.Name {
		t.Fatal("bound another namespace")
	}
	f.mu.Lock()
	f.namespace.UID = "replacement-uid"
	f.mu.Unlock()
	_, err = bound.Refresh(context.Background())
	requireCode(t, err, OwnershipRejected)
}

func TestObservedNamespaceNameCannotBindAnotherAttempt(t *testing.T) {
	f := guardFixture(t)
	client, err := New(context.Background(), f.options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.BindNamespace(context.Background(), f.base, f.parent, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.namespace.UID = "replacement-uid"
	f.namespace.Annotations[AttemptAnnotation] = "attempt-two"
	f.mu.Unlock()
	parent := f.parent
	parent.Ref.ProviderID = "replacement-uid"
	_, err = client.BindNamespace(context.Background(), f.base, parent, "attempt-two")
	requireCode(t, err, AttemptConsumed)
}

func TestCredentialsAndErrorsDoNotPrintTLSMaterial(t *testing.T) {
	f := guardFixture(t)
	values := []any{f.options.Credentials, failure(OwnershipRejected, fmt.Errorf("secret-provider-body"))}
	for _, value := range values {
		encoded, _ := json.Marshal(value)
		printed := fmt.Sprintf("%v %+v %#v %s", value, value, value, encoded)
		if strings.Contains(printed, "PRIVATE KEY") || strings.Contains(printed, "secret-provider-body") {
			t.Fatal("credential/provider body leaked")
		}
	}
}
