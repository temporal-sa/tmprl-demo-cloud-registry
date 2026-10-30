package buildplan

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

const fixture = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: example
spec:
  source:
    repo: https://github.com/temporal-sa/example
  builds:
    web:
      context: .
      dockerfile: Dockerfile
  components:
    web:
      build: web
`

func ptr[T any](v T) *T { return &v }
func setup(t *testing.T, document project.Document) (Bindings, ResolvedSource) {
	t.Helper()
	context, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "project-uid", Lifetime: ownership.Disposable, RunID: "test-run"}, ResourceRoot: "rgtest", Scopes: []ownership.Scope{{Provider: ownership.AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-2"}}})
	if err != nil {
		t.Fatal(err)
	}
	d, err := project.Normalize(document)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := SelectionHash(document)
	if err != nil {
		data, _ := json.Marshal(d)
		_, specific := project.Decode(data)
		t.Fatalf("selection validation: %v; canonical decode: %v", err, specific)
	}
	source := SourceBinding{AuthoredRepository: d.Spec.Source.Repo, CheckoutURL: d.Spec.Source.Repo, Ref: *d.Spec.Source.Branch, AuthPolicy: "None", PolicyRevision: "policy-v1", SelectionHash: selection}
	if git := d.Spec.Source.GitRepository; git != nil {
		if git.URL != nil {
			source.CheckoutURL = *git.URL
		}
		if git.AuthSecret != nil {
			source.AuthLogicalName, source.AuthVersionRef, source.AuthPolicy = string(*git.AuthSecret), "immutable-secret-version", "LogicalSecret"
		}
	}
	if d.Spec.Rollout != nil && d.Spec.Rollout.SourceCommitSha != nil {
		source.Ref = *d.Spec.Rollout.SourceCommitSha
	}
	name, _ := context.Name(ownership.AWS, "codebuild", "images", 63)
	role, _ := context.Name(ownership.AWS, "build-role", "service", 64)
	execution := &ExecutionBinding{ProjectARN: "arn:aws:codebuild:us-west-2:123456789012:project/" + name, ServiceRoleARN: "arn:aws:iam::123456789012:role/" + role, ToolchainDigest: "sha256:" + strings.Repeat("a", 64), Targets: map[string]Target{}}
	for build := range d.Spec.Builds {
		repo, _ := context.Name(ownership.AWS, "ecr", build, 63)
		execution.Targets[build] = Target{repo, "arn:aws:ecr:us-west-2:123456789012:repository/" + repo, "123456789012.dkr.ecr.us-west-2.amazonaws.com/" + repo}
	}
	b := Bindings{Authority: context, Source: source, Execution: execution, OverrideRepositories: map[string][]string{}}
	observation := ResolvedSource{Owner: context.Owner(), CheckoutURL: source.CheckoutURL, RequestedRef: source.Ref, PolicyRevision: source.PolicyRevision, BindingHash: SourceBindingHash(source), Commit: strings.Repeat("b", 40), ArtifactDigest: "sha256:" + strings.Repeat("c", 64), SourceUID: "source-uid", SourceVersion: "17"}
	return b, observation
}

func document(t *testing.T) project.Document {
	t.Helper()
	d, err := project.Decode([]byte(fixture))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var safe *diagnostics.FailureError
	if !errors.As(err, &safe) || safe.Diagnostic().Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

func TestResolveBeforeBuildAndStableIdentity(t *testing.T) {
	d := document(t)
	b, observation := setup(t, d)
	p, err := Compute(d, b, nil, nil)
	if err != nil || p.Action != "ResolveSource" || len(p.Builds) != 0 {
		t.Fatalf("mutable ref entered executor: %+v %v", p, err)
	}
	p, err = Compute(d, b, &observation, nil)
	if err != nil || p.Action != "Build" || len(p.Builds) != 1 || p.Source.Commit != observation.Commit {
		t.Fatalf("bad build plan: %+v %v", p, err)
	}
	again, _ := Compute(d, b, &observation, nil)
	if !reflect.DeepEqual(p, again) {
		t.Fatal("identical input changed idempotency")
	}
	observation.Commit = strings.Repeat("d", 40)
	changed, err := Compute(d, b, &observation, nil)
	if err != nil || changed.Builds[0].IdempotencyKey == p.Builds[0].IdempotencyKey {
		t.Fatal("source movement reused build identity")
	}
	// Metadata annotations are not authoritative source or image observations.
	d.Metadata.Annotations = map[string]string{"registry.tmprl-demo.cloud/source-commit-sha": "injected", "registry.tmprl-demo.cloud/image-overrides": `{"web":"prod:latest"}`}
	annotated, err := Compute(d, b, &observation, nil)
	if err != nil || annotated.Source.Commit != observation.Commit || len(annotated.Builds) != 1 {
		t.Fatal("annotation became source authority")
	}
}

func TestAttestationRejectsForeignAndChangedSelection(t *testing.T) {
	mutations := map[string]func(*ResolvedSource){
		"owner":    func(o *ResolvedSource) { o.Owner.ProjectUID = "foreign" },
		"repo":     func(o *ResolvedSource) { o.CheckoutURL = "https://github.com/other/repo" },
		"ref":      func(o *ResolvedSource) { o.RequestedRef = "other" },
		"commit":   func(o *ResolvedSource) { o.Commit = "bbbbbbb" },
		"artifact": func(o *ResolvedSource) { o.ArtifactDigest = "mutable" },
		"uid":      func(o *ResolvedSource) { o.SourceUID = "" },
		"version":  func(o *ResolvedSource) { o.SourceVersion = "" },
		"policy":   func(o *ResolvedSource) { o.PolicyRevision = "old" },
		"binding":  func(o *ResolvedSource) { o.BindingHash = "old" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			d := document(t)
			b, o := setup(t, d)
			mutate(&o)
			_, err := Compute(d, b, &o, nil)
			requireCode(t, err, "SourceAttestationMismatch")
		})
	}
	d := document(t)
	b, o := setup(t, d)
	d.Spec.Source.GitRepository = &project.SourceGitRepository{SparseCheckout: []string{"subdir"}}
	_, err := Compute(d, b, &o, nil)
	requireCode(t, err, "SourcePolicyMismatch")
	b.Source.CheckoutURL = "https://user:private-source-token@github.com/a/b"
	_, err = Compute(document(t), b, &o, nil)
	requireCode(t, err, "SourcePolicyMismatch")
	if strings.Contains(err.Error(), "private-source-token") {
		t.Fatal("unsafe source URL echoed")
	}
}

func TestPinnedCommitAndPrivateCredentialPolicy(t *testing.T) {
	d := document(t)
	d.Spec.Rollout = &project.Rollout{SourceCommitSha: ptr(strings.Repeat("a", 40))}
	b, o := setup(t, d)
	_, err := Compute(d, b, &o, nil)
	requireCode(t, err, "SourceAttestationMismatch")
	o.Commit = *d.Spec.Rollout.SourceCommitSha
	if _, err = Compute(d, b, &o, nil); err != nil {
		t.Fatal(err)
	}
	d.Spec.Rollout.SourceCommitSha = ptr("aaaaaaa")
	b, o = setup(t, d)
	_, err = Compute(d, b, &o, nil)
	requireCode(t, err, "FullSourceCommitRequired")
	d = document(t)
	d.Spec.Source.GitRepository = &project.SourceGitRepository{AuthSecret: ptr(project.DNSLabel("source-auth"))}
	b, o = setup(t, d)
	if _, err = Compute(d, b, &o, nil); err != nil {
		t.Fatal(err)
	}
	b.Source.AuthVersionRef = ""
	_, err = Compute(d, b, &o, nil)
	requireCode(t, err, "SourceCredentialPolicyMismatch")
	d = document(t)
	b, o = setup(t, d)
	b.Source.AuthPolicy, b.Source.AuthVersionRef = "Installation", "approved-install-version"
	o.BindingHash = SourceBindingHash(b.Source)
	if _, err = Compute(d, b, &o, nil); err != nil {
		t.Fatal(err)
	}
}

func TestImpossibleAndUnboundedRefsAreRejectedBeforeResolution(t *testing.T) {
	for _, ref := range []string{"-argument", "bad..ref", "part/.hidden", "part.lock", "refs//heads", "trailing/", "branch@{1}", "bad\nref", strings.Repeat("x", 256)} {
		d := document(t)
		d.Spec.Source.Branch = ptr(ref)
		b, _ := setup(t, d)
		_, err := Compute(d, b, nil, nil)
		requireCode(t, err, "InvalidSourceRef")
	}
}

func TestDigestOverridesRequireExplicitRepositoryApproval(t *testing.T) {
	d := document(t)
	b, _ := setup(t, d)
	d.Spec.Rollout = &project.Rollout{ImageOverrides: map[string]string{"web": "example.invalid/team/web@sha256:" + strings.Repeat("f", 64)}}
	_, err := Compute(d, b, nil, nil)
	requireCode(t, err, "ImageOverrideDenied")
	b.OverrideRepositories["web"] = []string{"example.invalid/team/web"}
	b.Execution = nil
	p, err := Compute(d, b, nil, nil)
	if err != nil || p.Action != "Resolved" || p.Source != nil || len(p.Builds) != 0 {
		t.Fatalf("bad override plan %+v %v", p, err)
	}
	d.Spec.Rollout.ImageOverrides["web"] = "example.invalid/team/web:git-abcdef1"
	_, err = Compute(d, b, nil, nil)
	requireCode(t, err, "ImageOverrideDenied")
}

func TestSourcePolicyCannotBeSkippedByImageOverrideFastPath(t *testing.T) {
	d := document(t)
	b, _ := setup(t, d)
	d.Spec.Rollout = &project.Rollout{ImageOverrides: map[string]string{"web": "example.invalid/team/web@sha256:" + strings.Repeat("f", 64)}}
	b.OverrideRepositories["web"] = []string{"example.invalid/team/web"}
	b.Source.CheckoutURL = "https://unapproved.invalid/repo"
	// Image resolution may finish without source, but a source adapter must
	// explicitly validate all source selection and credential bindings itself.
	if p, err := Compute(d, b, nil, nil); err != nil || p.Action != "Resolved" {
		t.Fatal(err)
	}
	b.Source.AuthoredRepository = "different"
	if _, err := PrepareSource(d, b); err == nil {
		t.Fatal("source fast path became authorization")
	}
	b, _ = setup(t, d)
	selection, err := PrepareSource(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if selection.RequestedRef != "main" || selection.PinnedCommit || selection.Owner != b.Authority.Owner() {
		t.Fatal("wrong validated source selection")
	}
	selection.Source.Branch = ptr("mutated")
	if d.Spec.Source.Branch != nil {
		t.Fatal("source preparation aliased authoring")
	}
}

func TestCacheReuseRequiresExactProvenanceDefinitionTargetAndDigest(t *testing.T) {
	d := document(t)
	b, o := setup(t, d)
	p, err := Compute(d, b, &o, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache := Cache{Source: o, DefinitionHash: p.DefinitionHash, ToolchainDigest: b.Execution.ToolchainDigest, Targets: b.Execution.Targets, Images: map[string]string{"web": b.Execution.Targets["web"].URI + "@sha256:" + strings.Repeat("f", 64)}}
	reused, err := Compute(d, b, &o, &cache)
	if err != nil || reused.Action != "Resolved" {
		t.Fatalf("cache missed %v %+v", err, reused)
	}
	for _, mutate := range []func(*Cache){
		func(c *Cache) { c.DefinitionHash = "old" }, func(c *Cache) { c.Source.Commit = strings.Repeat("d", 40) }, func(c *Cache) { c.ToolchainDigest = "old" },
		func(c *Cache) {
			c.Images = map[string]string{"web": "prod.invalid/other@sha256:" + strings.Repeat("f", 64)}
		},
		func(c *Cache) { c.Images = map[string]string{"web": b.Execution.Targets["web"].URI + ":latest"} }, func(c *Cache) { c.Targets = nil },
	} {
		copy := cache
		mutate(&copy)
		p, err := Compute(d, b, &o, &copy)
		if err != nil || p.Action != "Build" {
			t.Fatal("stale cache accepted")
		}
	}
	build := d.Spec.Builds["web"]
	build.Env = map[string]string{"BUILD_MODE": "changed"}
	d.Spec.Builds["web"] = build
	changed, err := Compute(d, b, &o, &cache)
	if err != nil || changed.Action != "Build" || changed.DefinitionHash == p.DefinitionHash {
		t.Fatal("build definition change reused cache")
	}
}

func TestExactExecutorRoleAndTargetAuthority(t *testing.T) {
	for _, mutate := range []func(*Bindings){func(b *Bindings) { b.Execution.ProjectARN = "arn:aws:codebuild:us-west-2:123456789012:project/prod" }, func(b *Bindings) { b.Execution.ServiceRoleARN = "arn:aws:iam::123456789012:role/prod" }} {
		d := document(t)
		b, o := setup(t, d)
		mutate(&b)
		_, err := Compute(d, b, &o, nil)
		requireCode(t, err, "ForeignBuildExecutor")
	}
	d := document(t)
	b, o := setup(t, d)
	target := b.Execution.Targets["web"]
	target.ARN = strings.Replace(target.ARN, "123456789012", "999999999999", 1)
	b.Execution.Targets["web"] = target
	_, err := Compute(d, b, &o, nil)
	requireCode(t, err, "ForeignBuildTarget")
}

func TestLambdaArchitectureInvalidatesCacheAndConflictsFail(t *testing.T) {
	d := document(t)
	c := d.Spec.Components["web"]
	c.Serverless = &project.Serverless{Provider: "aws-lambda"}
	d.Spec.Components["web"] = c
	b, o := setup(t, d)
	p, err := Compute(d, b, &o, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache := &Cache{Source: o, DefinitionHash: p.DefinitionHash, ToolchainDigest: b.Execution.ToolchainDigest, Targets: b.Execution.Targets, Images: map[string]string{"web": b.Execution.Targets["web"].URI + "@sha256:" + strings.Repeat("f", 64)}}
	c.Serverless.Architecture = ptr("arm64")
	d.Spec.Components["web"] = c
	changed, err := Compute(d, b, &o, cache)
	if err != nil || changed.Action != "Build" || changed.DefinitionHash == p.DefinitionHash || changed.Builds[0].IdempotencyKey == p.Builds[0].IdempotencyKey {
		t.Fatal("changed Lambda architecture reused an image")
	}
	other := c
	other.Serverless = &project.Serverless{Provider: "aws-lambda", Architecture: ptr("x86_64")}
	d.Spec.Components["other"] = other
	_, err = Compute(d, b, &o, nil)
	requireCode(t, err, "ConflictingLambdaBuildArchitectures")
}

func TestProgrammaticForbiddenEmptyPresenceIsNotLost(t *testing.T) {
	d := document(t)
	b, o := setup(t, d)
	c := d.Spec.Components["web"]
	c.Serverless = &project.Serverless{Provider: "aws-lambda"}
	c.Secrets = []project.DNSLabel{}
	d.Spec.Components["web"] = c
	_, err := Compute(d, b, &o, nil)
	requireCode(t, err, "InvalidBuildProject")
}

func TestCurrentFixturesPlanWithoutCopyingBuildConfigToDurableDTO(t *testing.T) {
	files, _ := filepath.Glob("../../../projects/demo/*.yaml")
	if len(files) < 20 {
		t.Fatal("fixture inventory missing")
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			d, err := project.Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			b, o := setup(t, d)
			p, err := Compute(d, b, &o, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Builds) != len(d.Spec.Builds) {
				t.Fatal("lost build")
			}
		})
	}
	d := document(t)
	build := d.Spec.Builds["web"]
	build.Env = map[string]string{"EXAMPLE_CONFIG": "private-build-value-sentinel"}
	d.Spec.Builds["web"] = build
	b, o := setup(t, d)
	p, err := Compute(d, b, &o, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(p)
	if strings.Contains(string(encoded), "private-build-value-sentinel") {
		t.Fatal("raw build env entered durable plan")
	}
}
