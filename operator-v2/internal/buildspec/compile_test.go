package buildspec

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
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
    app:
      context: .
      dockerfile: Dockerfile
  components:
    app:
      build: app
`

func ptr[T any](v T) *T { return &v }
func setup(t *testing.T) (project.Document, buildplan.Bindings, buildplan.ResolvedSource, Tools) {
	t.Helper()
	d, err := project.Decode([]byte(fixture))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "project-uid", Lifetime: ownership.Disposable, RunID: "test-run"}, ResourceRoot: "rgtest", Scopes: []ownership.Scope{{Provider: ownership.AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-2"}}})
	if err != nil {
		t.Fatal(err)
	}
	bin := func(name string) Binary {
		return Binary{Path: "/opt/registry-tools/" + name, SHA256: strings.Repeat("a", 64)}
	}
	tools := Tools{RunnerImage: "registry.invalid/tools/runner@sha256:" + strings.Repeat("1", 64), RunnerPlatform: "linux/amd64", Docker: bin("docker"), Python: bin("python3"), Pack: bin("pack"), Wrappers: map[string]Binary{"amd64": bin("wrapper-amd64"), "arm64": bin("wrapper-arm64")}, Builders: map[string]string{"builder:approved": "registry.invalid/builders/base@sha256:" + strings.Repeat("2", 64)}}
	digest, err := tools.Digest()
	if err != nil {
		t.Fatal(err)
	}
	selection, err := buildplan.SelectionHash(d)
	if err != nil {
		t.Fatal(err)
	}
	s := buildplan.SourceBinding{AuthoredRepository: d.Spec.Source.Repo, CheckoutURL: d.Spec.Source.Repo, Ref: "main", AuthPolicy: "None", PolicyRevision: "policy-v1", SelectionHash: selection}
	projectName, _ := authority.Name(ownership.AWS, "codebuild", "images", 63)
	roleName, _ := authority.Name(ownership.AWS, "build-role", "service", 64)
	repoName, _ := authority.Name(ownership.AWS, "ecr", "app", 63)
	b := buildplan.Bindings{Authority: authority, Source: s, Execution: &buildplan.ExecutionBinding{ProjectARN: "arn:aws:codebuild:us-west-2:123456789012:project/" + projectName, ServiceRoleARN: "arn:aws:iam::123456789012:role/" + roleName, ToolchainDigest: digest, Targets: map[string]buildplan.Target{"app": {Name: repoName, ARN: "arn:aws:ecr:us-west-2:123456789012:repository/" + repoName, URI: "123456789012.dkr.ecr.us-west-2.amazonaws.com/" + repoName}}}}
	source := buildplan.ResolvedSource{Owner: authority.Owner(), CheckoutURL: s.CheckoutURL, RequestedRef: s.Ref, PolicyRevision: s.PolicyRevision, BindingHash: buildplan.SourceBindingHash(s), Commit: strings.Repeat("b", 40), ArtifactDigest: "sha256:" + strings.Repeat("c", 64), SourceUID: "source-uid", SourceVersion: "17"}
	return d, b, source, tools
}
func config(t *testing.T, a Artifact) executionConfig {
	t.Helper()
	var c executionConfig
	if err := json.Unmarshal(a.SensitiveFiles()[a.Requirements().ExecutionRoot+"/config.json"], &c); err != nil {
		t.Fatal(err)
	}
	return c
}
func TestDeterministicBoundedCompilerAndAdapterRequirements(t *testing.T) {
	d, b, s, tools := setup(t)
	a, err := Compile(d, b, &s, nil, tools)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Compile(d, b, &s, nil, tools)
	if err != nil || !reflect.DeepEqual(a, again) {
		t.Fatal("unstable compilation")
	}
	var spec map[string]any
	if json.Unmarshal(a.Buildspec(), &spec) != nil || spec["version"] != "0.2" {
		t.Fatal("invalid JSON buildspec")
	}
	if !strings.Contains(string(a.Buildspec()), "set -euo pipefail") || strings.Contains(string(a.Buildspec()), "git clone") {
		t.Fatal("missing fail-closed script")
	}
	c := config(t, a)
	r := a.Requirements()
	if len(c.Builds) != 1 || c.SourceDigest != s.ArtifactDigest || !r.VerifySourceProvenance || !r.VerifyExecutorAuthority || !r.ResolveOwnedImageDigests || r.Source.Commit != s.Commit || len(r.Outputs) != 1 || !strings.HasPrefix(c.Builds[0].Image, b.Execution.Targets["app"].URI+":build-") {
		t.Fatal("lost execution obligations")
	}
	a.SensitiveFiles()[r.ExecutionRoot+"/config.json"][0] = 'x'
	if config(t, a).Builds[0].Name != "app" {
		t.Fatal("file accessor aliased")
	}
	r.Source.Commit = "forged"
	r.Outputs[0].Tag = "forged"
	if a.Requirements().Source.Commit != s.Commit || a.Requirements().Outputs[0].Tag == "forged" {
		t.Fatal("requirements accessor aliased")
	}
}
func TestPrivateBuildpackValuesNeverEnterSafeDTOOrBuildspec(t *testing.T) {
	d, b, s, tools := setup(t)
	build := d.Spec.Builds["app"]
	build.Strategy = ptr("buildpack")
	build.Dockerfile = nil
	build.Builder = ptr("builder:approved")
	build.Context = "folder ' $(touch SHOULD_NOT_RUN)"
	build.Env = map[string]string{"PRIVATE_SETTING": "PRIVATE-VALUE-'\n$(touch SHOULD_NOT_RUN)"}
	build.Processes = map[string]string{"web": "exec app --arg='PRIVATE-PROCESS-😀'"}
	d.Spec.Builds["app"] = build
	if _, err := project.Prepare(d); err != nil {
		t.Fatalf("test authoring invalid: %v", err)
	}
	a, err := Compile(d, b, &s, nil, tools)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	views := []string{string(encoded), a.String(), fmt.Sprintf("%#v", a), fmt.Sprintf("%+v", a), string(a.Buildspec())}
	for _, verb := range []string{"%d", "%x", "%q", "%p", "%032.4v"} {
		v := fmt.Sprintf(verb, a)
		if strings.Contains(v, "PRIVATE-") || strings.Contains(v, "SHOULD_NOT_RUN") {
			t.Fatal("format fallback exposed private execution fields")
		}
	}
	for _, v := range views {
		if strings.Contains(v, "PRIVATE-") || strings.Contains(v, "SHOULD_NOT_RUN") {
			t.Fatal("private authored data entered metadata/history view")
		}
	}
	c := config(t, a)
	if c.Builds[0].Definition.Env["PRIVATE_SETTING"] != build.Env["PRIVATE_SETTING"] || c.Builds[0].Definition.Processes["web"] != build.Processes["web"] || c.Builds[0].Builder != tools.Builders["builder:approved"] {
		t.Fatal("private execution config lost intent")
	}
}
func TestLambdaArchitectureSelectionAndMissingWrapper(t *testing.T) {
	for _, arch := range []string{"x86_64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			d, b, s, tools := setup(t)
			c := d.Spec.Components["app"]
			c.Serverless = &project.Serverless{Provider: "aws-lambda", Architecture: ptr(arch)}
			d.Spec.Components["app"] = c
			a, err := Compile(d, b, &s, nil, tools)
			if err != nil {
				t.Fatal(err)
			}
			expected := "amd64"
			if arch == "arm64" {
				expected = "arm64"
			}
			if config(t, a).Builds[0].Goarch != expected {
				t.Fatal("wrong wrapper architecture")
			}
			delete(tools.Wrappers, expected)
			b.Execution.ToolchainDigest, _ = tools.Digest()
			if _, err = Compile(d, b, &s, nil, tools); err == nil {
				t.Fatal("missing wrapper accepted")
			}
		})
	}
}
func TestMismatchAndUnsafeInputsFailWithoutEcho(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*project.Document, *buildplan.Bindings, *buildplan.ResolvedSource, *Tools)
	}{
		{"toolchain", func(_ *project.Document, _ *buildplan.Bindings, _ *buildplan.ResolvedSource, t *Tools) {
			t.Pack.SHA256 = strings.Repeat("d", 64)
		}},
		{"unpinned runner", func(_ *project.Document, _ *buildplan.Bindings, _ *buildplan.ResolvedSource, t *Tools) {
			t.RunnerImage = "registry.invalid/runner:latest"
		}},
		{"foreign target", func(_ *project.Document, b *buildplan.Bindings, _ *buildplan.ResolvedSource, _ *Tools) {
			target := b.Execution.Targets["app"]
			target.ARN = "arn:foreign"
			b.Execution.Targets["app"] = target
		}},
		{"source", func(_ *project.Document, _ *buildplan.Bindings, s *buildplan.ResolvedSource, _ *Tools) {
			s.Commit = "short"
		}},
		{"escape path", func(d *project.Document, _ *buildplan.Bindings, _ *buildplan.ResolvedSource, _ *Tools) {
			build := d.Spec.Builds["app"]
			build.Context = "../PRIVATE-SENTINEL"
			d.Spec.Builds["app"] = build
		}},
		{"dockerfile env", func(d *project.Document, _ *buildplan.Bindings, _ *buildplan.ResolvedSource, _ *Tools) {
			build := d.Spec.Builds["app"]
			build.Env = map[string]string{"IGNORED": "PRIVATE-SENTINEL"}
			d.Spec.Builds["app"] = build
		}},
		{"present empty prohibited", func(d *project.Document, _ *buildplan.Bindings, _ *buildplan.ResolvedSource, _ *Tools) {
			c := d.Spec.Components["app"]
			c.Serverless = &project.Serverless{Provider: "aws-lambda"}
			c.Secrets = []project.DNSLabel{}
			d.Spec.Components["app"] = c
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, b, s, tools := setup(t)
			tc.mutate(&d, &b, &s, &tools)
			a, err := Compile(d, b, &s, nil, tools)
			if err == nil || strings.Contains(err.Error(), "PRIVATE-SENTINEL") || len(a.Buildspec()) != 0 {
				t.Fatalf("unsafe acceptance/error: %v", err)
			}
		})
	}
}
func TestNoWorkAndCacheStillRequireIndependentImageVerification(t *testing.T) {
	d, b, s, tools := setup(t)
	p, err := buildplan.Compute(d, b, &s, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache := &buildplan.Cache{Source: s, DefinitionHash: p.DefinitionHash, ToolchainDigest: b.Execution.ToolchainDigest, Targets: b.Execution.Targets, Images: map[string]string{"app": b.Execution.Targets["app"].URI + "@sha256:" + strings.Repeat("f", 64)}}
	a, err := Compile(d, b, &s, cache, tools)
	if err != nil || a.Summary().Action != "Resolved" || len(a.SensitiveFiles()) != 0 || !a.Requirements().VerifyCachedImages || len(a.Requirements().ReusedImages) != 1 {
		t.Fatal("cache fast path lost attestation obligation")
	}
	a, err = Compile(d, b, nil, nil, tools)
	if err != nil || a.Summary().Action != "ResolveSource" || len(a.Buildspec()) != 0 {
		t.Fatal("unresolved source entered compiler")
	}
}
func TestPathAndQuoteContracts(t *testing.T) {
	for _, p := range []string{"../escape", "/absolute", "foo/../../escape", "foo\\bar", "bad\npath"} {
		if relativeSourcePath(p) {
			t.Fatalf("unsafe path %q", p)
		}
	}
	if !relativeSourcePath("folder ' $(unexecuted)/app") || quote("a'b") != "'a'\"'\"'b'" {
		t.Fatal("path/quoting contract")
	}
}
