package sourceresolver

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

func TestMemorySmartHTTPResolvesActualObjectsAndOwnedRecord(t *testing.T) {
	fixture := newFixture()
	resolver, d, b, store := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
	artifact, err := resolver.Resolve(context.Background(), d, b)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.count() != 2 || artifact.Source().Commit != fixture.branch.String() || artifact.Source().SourceUID != store.uid || artifact.Source().SourceVersion != "1" || artifact.Summary().WatchProfile != WatchProfile {
		t.Fatal("wire/record provenance missing")
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, safe := range []string{string(encoded), artifact.String(), fmt.Sprintf("%#v", artifact)} {
		assertPrivateAbsent(t, safe)
	}
	for _, verb := range []string{"%d", "%x", "%q", "%p", "%032.4v"} {
		assertPrivateAbsent(t, fmt.Sprintf(verb, artifact))
	}
	files := map[string]string{}
	reader := tar.NewReader(bytes.NewReader(artifact.PrivateArchive()))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(reader)
		files[header.Name] = string(data)
	}
	if files["private-source-file"] != "PRIVATE-SOURCE-CONTENT" || artifact.Summary().ArchiveFormat != ArchiveFormat {
		t.Fatal("private full archive did not preserve source")
	}
	again, err := resolver.Resolve(context.Background(), d, b)
	if err != nil || again.Source() != artifact.Source() || !bytes.Equal(again.PrivateArchive(), artifact.PrivateArchive()) {
		t.Fatal("identical source did not retain deterministic artifact/version")
	}
}
func TestPrivateAuthAndPolicyErrorsExposeNoCredentials(t *testing.T) {
	fixture := newFixture()
	fixture.private = true
	resolver, d, b, _ := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
	makePrivate(t, resolver, &d, &b)
	artifact, err := resolver.Resolve(context.Background(), d, b)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(artifact)
	assertPrivateAbsent(t, string(encoded))
	if artifact.Source().BindingHash != buildplan.SourceBindingHash(b.Source) {
		t.Fatal("credential policy not bound to provenance")
	}
	fixture.redirect = true
	before := fixture.count()
	if _, err = resolver.Resolve(context.Background(), d, b); err == nil {
		t.Fatal("redirect followed")
	}
	assertPrivateAbsent(t, err.Error())
	if fixture.count() != before+1 {
		t.Fatal("redirect used another endpoint")
	}
	fixture.redirect = false
	resolver.auth = authFunc(func(_ context.Context, r AuthRequest) (Credential, error) {
		return Credential{}, errors.New("password=PRIVATE-AUTH-SENTINEL")
	})
	before = fixture.count()
	if _, err = resolver.Resolve(context.Background(), d, b); err == nil {
		t.Fatal("raw credential provider error accepted")
	}
	assertPrivateAbsent(t, err.Error())
	if fixture.count() != before {
		t.Fatal("denied auth contacted source")
	}
	for _, change := range []func(*AuthRequest, *time.Time){func(r *AuthRequest, _ *time.Time) { r.private.owner.ProjectUID = "foreign" }, func(r *AuthRequest, _ *time.Time) { r.private.versionRef = "wrong-version" }, func(_ *AuthRequest, expires *time.Time) { *expires = time.Now().Add(-time.Second) }} {
		resolver.auth = authFunc(func(_ context.Context, r AuthRequest) (Credential, error) {
			expires := time.Now().Add(time.Minute)
			copied := r.value()
			r.private = &copied
			change(&r, &expires)
			return NewCredential(r, "source-user", "PRIVATE-AUTH-SENTINEL", expires)
		})
		before = fixture.count()
		if _, err = resolver.Resolve(context.Background(), d, b); err == nil || fixture.count() != before {
			t.Fatal("foreign/expired credential reached source")
		}
	}
}

func TestRemoteAuthenticationDenialDoesNotExposeErrorBodyOrUseFallback(t *testing.T) {
	fixture := newFixture()
	fixture.private = true
	resolver, d, b, _ := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
	if _, err := resolver.Resolve(context.Background(), d, b); err == nil {
		t.Fatal("private repo silently used another credential")
	} else {
		assertPrivateAbsent(t, err.Error())
	}
	if fixture.count() != 1 {
		t.Fatal("source authentication denial attempted a fallback")
	}
}

func TestActivityCredentialsHaveSafeFormattingForAllCommonVerbs(t *testing.T) {
	request := AuthRequest{private: &authRequestData{repository: "https://fixture.invalid/PRIVATE-SOURCE-CONTENT"}}
	credential, err := NewCredential(request, "source-user", "PRIVATE-AUTH-SENTINEL", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q", "%p", "%032.4v"} {
		assertPrivateAbsent(t, fmt.Sprintf(verb, request))
		assertPrivateAbsent(t, fmt.Sprintf(verb, credential))
	}
}
func TestSourceScopeRecordAndSelectionDenyBeforeAuthOrNetwork(t *testing.T) {
	for _, kind := range []string{"owner", "repo", "ref", "filter", "missing-pin", "prohibited-empty", "unsupported-filter", "unsupported-ref", "sha256", "override-fast-path"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newFixture()
			resolver, d, b, store := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
			makePrivate(t, resolver, &d, &b)
			authCalls := 0
			resolver.auth = authFunc(func(context.Context, AuthRequest) (Credential, error) {
				authCalls++
				return Credential{}, errors.New("must not call")
			})
			switch kind {
			case "owner":
				resolver.policy.Owner.ProjectUID = "foreign"
			case "repo":
				b.Source.CheckoutURL = "https://foreign.invalid/repo.git"
			case "ref":
				b.Source.Ref = "unapproved"
			case "filter":
				d.Spec.Source.GitRepository.SparseCheckout = []string{"private-path"}
			case "missing-pin":
				if err := store.root.Remove(recordName); err != nil {
					t.Fatal(err)
				}
			case "prohibited-empty":
				component := d.Spec.Components["app"]
				component.Serverless = &project.Serverless{Provider: "aws-lambda"}
				component.Secrets = []project.DNSLabel{}
				d.Spec.Components["app"] = component
			case "unsupported-filter":
				pattern := "[unsupported]"
				d.Spec.Source.GitRepository.Ignore = &pattern
				b.Source.SelectionHash, _ = buildplan.SelectionHash(d)
			case "unsupported-ref":
				branch := "refs/tags/release"
				d.Spec.Source.Branch = &branch
				b.Source.Ref = branch
				b.Source.SelectionHash, _ = buildplan.SelectionHash(d)
			case "sha256":
				pin := strings.Repeat("a", 64)
				d.Spec.Rollout = &project.Rollout{SourceCommitSha: &pin}
				b.Source.Ref = pin
				b.Source.SelectionHash, _ = buildplan.SelectionHash(d)
			case "override-fast-path":
				d.Spec.Rollout = &project.Rollout{ImageOverrides: map[string]string{"app": "example.invalid/app@sha256:" + strings.Repeat("a", 64)}}
				b.Source.Ref = "foreign-ref"
			}
			if _, err := resolver.Resolve(context.Background(), d, b); err == nil || authCalls != 0 || fixture.count() != 0 {
				t.Fatal("invalid authoring/policy/pin crossed source boundary")
			}
		})
	}
}
func TestPinnedCommitAndBranchMovementUseObservedFullCommit(t *testing.T) {
	fixture := newFixture()
	resolver, d, b, _ := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
	old := fixture.branch
	objects, newCommit := snapshotObjects(map[string]fixtureFile{"Dockerfile": {data: "FROM scratch\n# newer\n"}})
	fixture.packs[newCommit] = packSnapshot(objects)
	fixture.post = func(wanted plumbing.Hash) {
		fixture.mu.Lock()
		fixture.branch = newCommit
		fixture.mu.Unlock()
		if wanted != old {
			t.Error("build fetch used moving branch instead of observed commit")
		}
	}
	first, err := resolver.Resolve(context.Background(), d, b)
	if err != nil || first.Source().Commit != old.String() {
		t.Fatal("branch movement changed pinned fetch", err)
	}
	fixture.post = nil
	second, err := resolver.Resolve(context.Background(), d, b)
	if err != nil || second.Source().Commit != newCommit.String() || second.Source().SourceVersion != "2" {
		t.Fatal("new branch observation not versioned", err)
	}
	pin := old.String()
	d.Spec.Rollout = &project.Rollout{SourceCommitSha: &pin}
	b.Source.Ref = pin
	var errHash error
	b.Source.SelectionHash, errHash = buildplan.SelectionHash(d)
	if errHash != nil {
		t.Fatal(errHash)
	}
	pinned, err := resolver.Resolve(context.Background(), d, b)
	if err != nil || pinned.Source().Commit != pin {
		t.Fatal("full explicit commit pin ignored", err)
	}
}
func TestSlowOldFetchCannotOverwriteNewerSourceRecord(t *testing.T) {
	fixture := newFixture()
	resolver, d, b, store := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
	old := fixture.branch
	objects, newCommit := snapshotObjects(map[string]fixtureFile{"Dockerfile": {data: "FROM scratch\n# new\n"}})
	fixture.packs[newCommit] = packSnapshot(objects)
	oldStarted, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	fixture.post = func(wanted plumbing.Hash) {
		if wanted == old {
			close(oldStarted)
			<-release
		}
	}
	finished := make(chan error, 1)
	go func() { _, err := resolver.Resolve(context.Background(), d, b); finished <- err }()
	<-oldStarted
	fixture.mu.Lock()
	fixture.branch = newCommit
	fixture.mu.Unlock()
	newer, err := resolver.Resolve(context.Background(), d, b)
	if err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	if err = <-finished; err == nil {
		t.Fatal("slow stale fetch overwrote accepted new source")
	}
	record, err := store.readRecord()
	if err != nil || record.Facts.Commit != newer.Source().Commit || record.Version != 1 {
		t.Fatal("source record regressed")
	}
}
func TestTransportPolicyIgnoresAmbientProxyAndRejectsOtherOriginsWithoutDial(t *testing.T) {
	fixture := newFixture()
	resolver, d, b, _ := resolverFixture(t, "https://fixture.invalid/repo.git", fixtureRoots(t), fixture)
	t.Setenv("HTTPS_PROXY", "http://foreign-proxy.invalid:9000")
	t.Setenv("HTTP_PROXY", "http://foreign-proxy.invalid:9000")
	t.Setenv("GIT_CONFIG_GLOBAL", "/nonexistent-private-config")
	t.Setenv("GIT_SSH_COMMAND", "must-not-execute")
	t.Setenv("SSH_AUTH_SOCK", "/nonexistent-private-agent")
	if _, err := resolver.Resolve(context.Background(), d, b); err != nil {
		t.Fatal(err)
	}
	client, close, err := newHTTPPolicy(resolver.policy, BoundedLimits(), time.Time{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	scoped := client.Transport.(*scopedTransport)
	native := scoped.transport.(*http.Transport)
	if native.Proxy != nil || native.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("ambient proxy/TLS bypass enabled")
	}
	if connection, err := native.DialContext(context.Background(), "tcp", "foreign.invalid:443"); err == nil || connection != nil {
		if connection != nil {
			connection.Close()
		}
		t.Fatal("foreign address reached native dial")
	}
	for _, target := range []string{"http://fixture.invalid/repo.git/info/refs?service=git-upload-pack", "https://foreign.invalid/repo.git/info/refs?service=git-upload-pack", "https://fixture.invalid/other/info/refs?service=git-upload-pack", "https://fixture.invalid/repo.git/info/refs?service=git-receive-pack"} {
		request, _ := http.NewRequest("GET", target, nil)
		if _, err := scoped.RoundTrip(request); !errors.Is(err, errTransport) {
			t.Fatal("out-of-policy request allowed")
		}
	}
	policy := resolver.policy
	policy.AllowedIPs = []netip.Addr{netip.MustParseAddr("::ffff:169.254.169.254")}
	if _, _, err := newHTTPPolicy(policy, BoundedLimits(), time.Time{}, time.Now); err == nil {
		t.Fatal("mapped metadata address permitted")
	}
	policy = resolver.policy
	policy.AllowLoopback = false
	policy.AllowedIPs = []netip.Addr{netip.MustParseAddr("::ffff:127.0.0.1")}
	if _, _, err := newHTTPPolicy(policy, BoundedLimits(), time.Time{}, time.Now); err == nil {
		t.Fatal("mapped loopback bypassed explicit policy")
	}
}
