// Package buildspec compiles execution-only inputs. It never starts a process,
// contacts a provider, loads credentials or authenticates an input DTO.
package buildspec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

const CompilerVersion = "registry-buildspec-v1"
const MaxExecutionBytes = 2 * 1024 * 1024

var hexDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)
var imageDigest = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[a-f0-9]{64}$`)

type Binary struct{ Path, SHA256 string }

var toolPath = regexp.MustCompile(`^/opt/registry-tools/[A-Za-z0-9/_.-]+$`)

// Tools is trusted installation policy. The executor must independently verify
// the actual runner, Docker daemon and tool provenance; these DTOs prove none.
// Pack and architecture-specific wrappers are preinstalled, never downloaded.
// Builders maps exact authored builder identifiers to approved digest images.
type Tools struct {
	RunnerImage    string
	RunnerPlatform string
	Docker         Binary
	Python         Binary
	Pack           Binary
	Wrappers       map[string]Binary
	Builders       map[string]string
}

func validBinary(b Binary) bool {
	return toolPath.MatchString(b.Path) && path.Clean(b.Path) == b.Path && hexDigest.MatchString(b.SHA256)
}
func (t Tools) Digest() (string, error) {
	if !imageDigest.MatchString(t.RunnerImage) || (t.RunnerPlatform != "linux/amd64" && t.RunnerPlatform != "linux/arm64") || !validBinary(t.Docker) || !validBinary(t.Python) {
		return "", invalid("UnpinnedBuildTools", "The build runner and core tools require exact pinned installation bindings.")
	}
	if t.Pack != (Binary{}) && !validBinary(t.Pack) {
		return "", invalid("UnpinnedBuildTools", "The pack tool requires an exact preinstalled checksum binding.")
	}
	for arch, b := range t.Wrappers {
		if arch != "amd64" && arch != "arm64" || !validBinary(b) {
			return "", invalid("UnpinnedBuildTools", "Lambda wrappers require explicit architecture and checksum bindings.")
		}
	}
	for authored, image := range t.Builders {
		if authored == "" || len(authored) > 512 || !imageDigest.MatchString(image) {
			return "", invalid("UnpinnedBuildTools", "Buildpack builders require explicit approved digest resolutions.")
		}
	}
	data, err := json.Marshal(struct {
		Compiler     string
		HelperDigest string
		Tools        Tools
	}{CompilerVersion, hash(runner), t})
	if err != nil || len(data) > 64*1024 {
		return "", invalid("BuildToolManifestLimit", "The trusted tool manifest exceeds its bounded contract.")
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
func invalid(code, message string) error {
	return diagnostics.NewError(diagnostics.DomainFailure{ID: "buildspec-invalid", Code: code, Category: diagnostics.Configuration, Message: message, Step: "compile-buildspec", NextAction: "Correct the explicit execution binding before starting a build."})
}

// Requirements are obligations for the real execution adapter. Exact equality
// cannot authenticate source/cache claims. Before execution it must verify the
// full commit->artifact mapping independently, stage only the expected archive,
// verify executor role/daemon/toolchain authority, and privately stage Files.
// After push it must resolve exact owned ECR image digests and record provenance
// against DefinitionHash/ToolchainDigest/source/target before cache reuse.
type Requirements struct {
	Source                          *buildplan.ResolvedSource
	SourceArchivePath               string
	RunnerImage                     string
	RunnerPlatform                  string
	ProjectARN, ServiceRoleARN      string
	DefinitionHash, ToolchainDigest string
	ExecutionRoot                   string
	VerifySourceProvenance          bool
	VerifyExecutorAuthority         bool
	VerifyCachedImages              bool
	ResolveOwnedImageDigests        bool
	RegistryAuthPath                string
	RegistryEndpoints               []string
	ReusedImages                    map[string]string
	Outputs                         []Output
	RequireMetadataNetworkIsolation bool
}
type Output struct {
	BuildName, Tag, IdempotencyKey string
	Target                         buildplan.Target
}
type Summary struct {
	Compiler, Action, DefinitionHash, ToolchainDigest, ArtifactDigest string
	BuildCount                                                        int
	SourceCommit, SourceArtifactDigest                                string
}

// Artifact is deliberately private. String and JSON expose only safe metadata.
// Buildspec and SensitiveFiles are explicit SDK/staging-boundary accessors;
// never put their returned bytes in Temporal history, diagnostics or logs.
// Keep payloads behind an opaque pointer as well as Formatter: %p bypasses
// formatting methods on value structs and must not reflect inline config bytes.
type Artifact struct{ private *artifactData }
type artifactData struct {
	summary      Summary
	requirements Requirements
	buildspec    []byte
	files        map[string][]byte
}

func (a Artifact) Summary() Summary {
	if a.private == nil {
		return Summary{}
	}
	return a.private.summary
}
func (a Artifact) Requirements() Requirements {
	if a.private == nil {
		return Requirements{}
	}
	r := a.private.requirements
	r.RegistryEndpoints = append([]string(nil), r.RegistryEndpoints...)
	r.Outputs = append([]Output(nil), r.Outputs...)
	r.ReusedImages = map[string]string{}
	for k, v := range a.private.requirements.ReusedImages {
		r.ReusedImages[k] = v
	}
	if r.Source != nil {
		s := *r.Source
		r.Source = &s
	}
	return r
}
func (a Artifact) Buildspec() []byte {
	if a.private == nil {
		return nil
	}
	return append([]byte(nil), a.private.buildspec...)
}
func (a Artifact) SensitiveFiles() map[string][]byte {
	m := map[string][]byte{}
	if a.private == nil {
		return m
	}
	for k, v := range a.private.files {
		m[k] = append([]byte(nil), v...)
	}
	return m
}
func (a Artifact) String() string                 { b, _ := json.Marshal(a.Summary()); return string(b) }
func (a Artifact) GoString() string               { return a.String() }
func (a Artifact) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, a.String()) }
func (a Artifact) MarshalJSON() ([]byte, error)   { return json.Marshal(a.Summary()) }
