package buildspec

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"unicode"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

//go:embed runner.py
var runner []byte

type executionBuild struct {
	Name, Image, Goarch, Builder string
	Definition                   project.Build
}
type executionConfig struct {
	Tools                             Tools
	Root, SourceArchive, SourceDigest string
	Builds                            []executionBuild
	MaxFiles                          int
	MaxExpandedBytes                  int64
	RegistryAuthPath                  string
	RegistryEndpoints                 []string
}

// Compile recomputes the planner contract from raw authoring and trusted
// installation bindings. A caller-supplied Plan is not an authorization permit.
// Inputs called observations/cache must come from separately verified adapters.
func Compile(authored project.Document, b buildplan.Bindings, source *buildplan.ResolvedSource, cache *buildplan.Cache, tools Tools) (Artifact, error) {
	plan, err := buildplan.Compute(authored, b, source, cache)
	if err != nil {
		return Artifact{}, err
	}
	a := Artifact{private: &artifactData{summary: Summary{Compiler: CompilerVersion, Action: plan.Action, DefinitionHash: plan.DefinitionHash, ToolchainDigest: plan.ToolchainDigest, BuildCount: len(plan.Builds)}, files: map[string][]byte{}}}
	a.private.requirements = Requirements{Source: plan.Source, DefinitionHash: plan.DefinitionHash, ToolchainDigest: plan.ToolchainDigest, VerifyCachedImages: len(plan.Images) > 0, ReusedImages: plan.Images, VerifySourceProvenance: plan.Source != nil}
	if plan.Action != "Build" {
		return a, nil
	}
	manifest, err := tools.Digest()
	if err != nil {
		return Artifact{}, err
	}
	if plan.ToolchainDigest != manifest {
		return Artifact{}, invalid("BuildToolchainMismatch", "The bound planner toolchain does not match the full compiler/tool manifest.")
	}
	d, err := project.Prepare(authored)
	if err != nil {
		return Artifact{}, invalid("InvalidBuildProject", "Project authoring is invalid before execution compilation.")
	}
	// All names and execution roots are compiler-generated stable identities.
	keys := []string{}
	for _, build := range plan.Builds {
		keys = append(keys, build.IdempotencyKey)
	}
	root := "/opt/registry-execution/" + hash(keys)
	config := executionConfig{Tools: tools, Root: root, SourceArchive: "/opt/registry-input/source.tar", SourceDigest: plan.Source.ArtifactDigest, MaxFiles: 50000, MaxExpandedBytes: 1024 * 1024 * 1024, RegistryAuthPath: root + "/docker-auth"}
	registries := map[string]bool{}
	for _, build := range plan.Builds {
		definition := d.Spec.Builds[build.Name]
		if !relativeSourcePath(definition.Context) || definition.Dockerfile != nil && !relativeSourcePath(*definition.Dockerfile) {
			return Artifact{}, invalid("UnsafeBuildPath", "Build paths must stay within the verified extracted source tree.")
		}
		e := executionBuild{Name: build.Name, Image: build.Target.URI + ":build-" + build.IdempotencyKey, Definition: definition}
		registries[strings.SplitN(build.Target.URI, "/", 2)[0]] = true
		a.private.requirements.Outputs = append(a.private.requirements.Outputs, Output{BuildName: build.Name, Tag: e.Image, IdempotencyKey: build.IdempotencyKey, Target: build.Target})
		if (definition.Strategy == nil || *definition.Strategy == "dockerfile") && len(definition.Env) > 0 {
			return Artifact{}, invalid("UnsupportedDockerfileEnv", "Build environment values are supported only by buildpack builds; Dockerfile build arguments require a separate reviewed contract.")
		}
		for _, c := range d.Spec.Components {
			if string(c.Build) == build.Name && c.Serverless != nil {
				e.Goarch = "amd64"
				if c.Serverless.Architecture != nil && *c.Serverless.Architecture == "arm64" {
					e.Goarch = "arm64"
				}
			}
		}
		if e.Goarch != "" {
			if !validBinary(tools.Wrappers[e.Goarch]) {
				return Artifact{}, invalid("LambdaWrapperBindingRequired", "The selected Lambda architecture lacks a pinned preinstalled wrapper.")
			}
		}
		if definition.Strategy != nil && *definition.Strategy == "buildpack" {
			if !validBinary(tools.Pack) {
				return Artifact{}, invalid("PackBindingRequired", "Buildpacks require a checksum-pinned preinstalled pack tool.")
			}
			if definition.Builder == nil {
				return Artifact{}, invalid("BuilderBindingRequired", "The buildpack builder identifier is missing.")
			}
			e.Builder = tools.Builders[*definition.Builder]
			if !imageDigest.MatchString(e.Builder) {
				return Artifact{}, invalid("BuilderBindingRequired", "The requested buildpack builder lacks an approved immutable image resolution.")
			}
		}
		config.Builds = append(config.Builds, e)
	}
	config.RegistryEndpoints = sorted(registries)
	configBytes, err := json.Marshal(config)
	if err != nil {
		return Artifact{}, invalid("BuildExecutionLimit", "The private build configuration cannot be encoded.")
	}
	if len(configBytes)+len(runner) > MaxExecutionBytes {
		return Artifact{}, invalid("BuildExecutionLimit", "The private build execution artifact exceeds its byte budget.")
	}
	a.private.files[root+"/config.json"] = configBytes
	a.private.files[root+"/runner.py"] = append([]byte(nil), runner...)
	// CodeBuild echoes command text. Keep all authored env/process values in
	// separately staged private assets, never inline shell/base64 commands.
	commands := []string{"set -euo pipefail; set +x; umask 077; " + checksum(configBytes, root+"/config.json") + "; " + checksum(runner, root+"/runner.py") + "; " + checksumExpected(tools.Python.SHA256, tools.Python.Path) + "; exec " + quote(tools.Python.Path) + " -I -S " + quote(root+"/runner.py") + " " + quote(root+"/config.json")}
	spec := map[string]any{"version": "0.2", "env": map[string]string{"shell": "bash"}, "phases": map[string]any{"build": map[string]any{"commands": commands}}}
	a.private.buildspec, err = json.Marshal(spec)
	if err != nil {
		return Artifact{}, invalid("BuildExecutionLimit", "The execution buildspec cannot be encoded.")
	}
	a.private.requirements.SourceArchivePath = config.SourceArchive
	a.private.requirements.RunnerImage = tools.RunnerImage
	a.private.requirements.RunnerPlatform = tools.RunnerPlatform
	a.private.requirements.ProjectARN = plan.ProjectARN
	a.private.requirements.ServiceRoleARN = plan.ServiceRoleARN
	a.private.requirements.ExecutionRoot = root
	a.private.requirements.VerifyExecutorAuthority = true
	a.private.requirements.ResolveOwnedImageDigests = true
	a.private.requirements.RegistryAuthPath = config.RegistryAuthPath
	a.private.requirements.RegistryEndpoints = append([]string(nil), config.RegistryEndpoints...)
	a.private.requirements.RequireMetadataNetworkIsolation = true
	a.private.summary.SourceCommit = plan.Source.Commit
	a.private.summary.SourceArtifactDigest = plan.Source.ArtifactDigest
	a.private.summary.ArtifactDigest = "sha256:" + hash(struct {
		Spec  []byte
		Files map[string][]byte
	}{a.private.buildspec, a.private.files})
	return a, nil
}
func relativeSourcePath(value string) bool {
	if value == "" || len(value) > 4096 || path.IsAbs(value) || strings.ContainsRune(value, '\\') || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return false
	}
	cleaned := path.Clean(value)
	return cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}
func quote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
func checksum(data []byte, p string) string {
	sum := sha256.Sum256(data)
	return checksumExpected(hex.EncodeToString(sum[:]), p)
}
func checksumExpected(sum, p string) string {
	return "printf '%s  %s\\n' " + quote(sum) + " " + quote(p) + " | /usr/bin/sha256sum -c - >/dev/null"
}
func hash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func sorted[V any](m map[string]V) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
