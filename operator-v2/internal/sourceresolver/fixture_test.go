package sourceresolver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

type authFunc func(context.Context, AuthRequest) (Credential, error)

func (f authFunc) Credential(ctx context.Context, request AuthRequest) (Credential, error) {
	return f(ctx, request)
}

type sourceFixture struct {
	mu       sync.Mutex
	branch   plumbing.Hash
	packs    map[plumbing.Hash][]byte
	requests int
	wants    []plumbing.Hash
	private  bool
	redirect bool
	post     func(plumbing.Hash)
}

func newFixture() *sourceFixture {
	objects, commit := snapshotObjects(map[string]fixtureFile{"Dockerfile": {data: "FROM scratch\n"}, "private-source-file": {data: "PRIVATE-SOURCE-CONTENT"}, "bin/start": {data: "#!/bin/sh\nexit 0\n", mode: "100755"}})
	return &sourceFixture{branch: commit, packs: map[plumbing.Hash][]byte{commit: packSnapshot(objects)}}
}
func (f *sourceFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests++
	branch, private, redirect := f.branch, f.private, f.redirect
	f.mu.Unlock()
	if private {
		user, password, ok := r.BasicAuth()
		if !ok || user != "source-user" || password != "PRIVATE-AUTH-SENTINEL" {
			w.WriteHeader(401)
			_, _ = w.Write([]byte("password=PRIVATE-AUTH-SENTINEL"))
			return
		}
	}
	if redirect {
		w.Header().Set("Location", "https://foreign.invalid/leak?password=PRIVATE-AUTH-SENTINEL")
		w.WriteHeader(302)
		return
	}
	switch {
	case r.Method == "GET" && r.URL.Path == "/repo.git/info/refs" && r.URL.RawQuery == "service=git-upload-pack":
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		encoder := pktline.NewEncoder(w)
		_ = encoder.EncodeString("# service=git-upload-pack\n")
		_ = encoder.Flush()
		refs := packp.NewAdvRefs()
		refs.Head = &branch
		refs.References["refs/heads/main"] = branch
		_ = refs.Encode(w)
	case r.Method == "POST" && r.URL.Path == "/repo.git/git-upload-pack":
		request := packp.NewUploadPackRequest()
		if request.Decode(r.Body) != nil || len(request.Wants) != 1 {
			w.WriteHeader(400)
			return
		}
		wanted := request.Wants[0]
		f.mu.Lock()
		f.wants = append(f.wants, wanted)
		data, found := f.packs[wanted]
		hook := f.post
		f.mu.Unlock()
		if hook != nil {
			hook(wanted)
		}
		if !found {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		response := packp.NewUploadPackResponseWithPackfile(request, io.NopCloser(bytes.NewReader(data)))
		_ = response.Encode(w)
	default:
		w.WriteHeader(404)
	}
}
func (f *sourceFixture) count() int { f.mu.Lock(); defer f.mu.Unlock(); return f.requests }
func fixtureRoots(t *testing.T) *x509.CertPool {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-local-source-fixture"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Unix(0, 0), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return pool
}
func resolverFixture(t *testing.T, repo string, roots *x509.CertPool, handler http.Handler) (*Resolver, project.Document, buildplan.Bindings, *LocalStore) {
	t.Helper()
	d, err := project.Decode([]byte(`apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: source-example
spec:
  source:
    repo: https://github.com/temporal-sa/example
  builds:
    app:
      context: .
      dockerfile: Dockerfile
  components:
    app:
      build: app
`))
	if err != nil {
		t.Fatal(err)
	}
	d.Spec.Source.GitRepository = &project.SourceGitRepository{URL: &repo}
	owner := ownership.Owner{EnvironmentID: "local", InstallationID: "test", ProjectUID: "source-project", Lifetime: ownership.Disposable, RunID: "owned-run"}
	authority, err := ownership.NewContext(ownership.Config{Owner: owner, ResourceRoot: "rgtest", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "owned-local", Namespace: "sources"}}})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := buildplan.SelectionHash(d)
	if err != nil {
		t.Fatal(err)
	}
	b := buildplan.Bindings{Authority: authority, Source: buildplan.SourceBinding{AuthoredRepository: d.Spec.Source.Repo, CheckoutURL: repo, Ref: "main", AuthPolicy: "None", PolicyRevision: "approved-1", SelectionHash: selection}}
	directory := t.TempDir()
	if err = os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	store, _, err := BootstrapLocalStore(context.Background(), root, owner, repo)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := New(TransportPolicy{Owner: owner, Repository: repo, RootCAs: roots, AllowedIPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, AllowLoopback: true, Timeout: 5 * time.Second}, nil, store, BoundedLimits(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if handler != nil {
		resolver.client = func(policy TransportPolicy, limits Limits, expires time.Time, now func() time.Time) (*http.Client, func(), error) {
			client, close, err := newHTTPPolicy(policy, limits, expires, now)
			if err != nil {
				return nil, nil, err
			}
			client.Transport.(*scopedTransport).transport = memoryRoundTripper{handler: handler}
			return client, close, nil
		}
	}
	return resolver, d, b, store
}

type memoryRoundTripper struct{ handler http.Handler }

func (m memoryRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	m.handler.ServeHTTP(recorder, request)
	response := recorder.Result()
	response.Request = request
	return response, nil
}
func makePrivate(t *testing.T, r *Resolver, d *project.Document, b *buildplan.Bindings) {
	t.Helper()
	name := project.DNSLabel("source-auth")
	d.Spec.Source.GitRepository.AuthSecret = &name
	b.Source.AuthPolicy = "LogicalSecret"
	b.Source.AuthLogicalName = string(name)
	b.Source.AuthVersionRef = "immutable-version-1"
	var err error
	b.Source.SelectionHash, err = buildplan.SelectionHash(*d)
	if err != nil {
		t.Fatal(err)
	}
	r.auth = authFunc(func(_ context.Context, request AuthRequest) (Credential, error) {
		return NewCredential(request, "source-user", "PRIVATE-AUTH-SENTINEL", time.Now().Add(time.Minute))
	})
}
func assertPrivateAbsent(t *testing.T, value string) {
	t.Helper()
	for _, secret := range []string{"PRIVATE-AUTH-SENTINEL", "PRIVATE-SOURCE-CONTENT"} {
		if strings.Contains(value, secret) {
			t.Fatal("private source/auth material entered diagnostic view")
		}
	}
}
