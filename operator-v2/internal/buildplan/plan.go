// Package buildplan computes source/build work without credentials, subprocesses
// or provider I/O. Its trusted bindings are installation policy, not claims from
// a project. Actual adapters still verify provider provenance and IAM authority.
package buildplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

const CompilerVersion = "registry-build-v2-1"

type SourceBinding struct {
	// AuthoredRepository is the exact approved authoring identifier; CheckoutURL
	// is a separately approved credential-free transport URL. No URL conversion
	// or ambient Git/SSH credential lookup is performed here.
	AuthoredRepository string
	CheckoutURL        string
	Ref                string
	AuthLogicalName    string
	AuthVersionRef     string
	AuthPolicy         string // None, LogicalSecret, or Installation
	PolicyRevision     string
	SelectionHash      string
}

// SourceSelection is execution-only authoring/policy for a trusted source
// adapter. Source may include private filter/path identifiers; do not log or
// place this object in workflow history. It contains references, never resolved
// credential material. A resolver independently verifies its source facts.
type SourceSelection struct {
	Owner        ownership.Owner
	Binding      SourceBinding
	RequestedRef string
	PinnedCommit bool
	Source       project.Source
}

// PrepareSource applies source policy even when every image is overridden and
// Compute legitimately needs no build. Source watchers must not bypass policy
// by treating the build fast path as source authorization.
func PrepareSource(authored project.Document, b Bindings) (SourceSelection, error) {
	d, err := validate(authored)
	if err != nil {
		return SourceSelection{}, err
	}
	owner := b.Authority.Owner()
	if owner.ProjectUID == "" || d.Metadata.UID != "" && d.Metadata.UID != owner.ProjectUID {
		return SourceSelection{}, failure("BuildOwnerMismatch", "Source resolution requires the verified project owner.", "Bind the API project UID to trusted installation authority.")
	}
	ref, err := validateSource(d, b.Source)
	if err != nil {
		return SourceSelection{}, err
	}
	return SourceSelection{Owner: owner, Binding: b.Source, RequestedRef: ref, PinnedCommit: d.Spec.Rollout != nil && d.Spec.Rollout.SourceCommitSha != nil, Source: d.Spec.Source}, nil
}

type ResolvedSource struct {
	Owner          ownership.Owner `json:"owner"`
	CheckoutURL    string          `json:"checkoutURL"`
	RequestedRef   string          `json:"requestedRef"`
	PolicyRevision string          `json:"policyRevision"`
	BindingHash    string          `json:"bindingHash"`
	Commit         string          `json:"commit"`
	ArtifactDigest string          `json:"artifactDigest"`
	SourceUID      string          `json:"sourceUID"`
	SourceVersion  string          `json:"sourceVersion"`
}

// ResolvedSource and Cache are safe reference DTOs, not cryptographic proofs.
// Only trusted adapters that independently verify source objects/artifacts and
// digest-to-definition/toolchain/target evidence may produce them. Matching
// fields alone never authorize network access or prove a build role is isolated.

type ExecutionBinding struct {
	ProjectARN     string
	ServiceRoleARN string
	// ToolchainDigest identifies the full pinned tool manifest (runner image,
	// pack/wrapper artifacts and compiler version). The actual adapter verifies
	// the manifest, actual runner image and role's transitive privileges.
	ToolchainDigest string
	Targets         map[string]Target
}

type Target struct {
	Name string `json:"name"`
	ARN  string `json:"arn"`
	URI  string `json:"uri"`
}

type Bindings struct {
	Authority ownership.Context
	Source    SourceBinding
	Execution *ExecutionBinding
	// Overrides need explicit repository approval even when no build is needed.
	OverrideRepositories map[string][]string
}

type Cache struct {
	Source          ResolvedSource    `json:"source"`
	DefinitionHash  string            `json:"definitionHash"`
	ToolchainDigest string            `json:"toolchainDigest"`
	Targets         map[string]Target `json:"targets"`
	Images          map[string]string `json:"images"`
}

type Build struct {
	Name           string `json:"name"`
	Target         Target `json:"target"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type Plan struct {
	Action          string            `json:"action"`
	DefinitionHash  string            `json:"definitionHash"`
	Source          *ResolvedSource   `json:"source,omitempty"`
	ProjectARN      string            `json:"projectARN,omitempty"`
	ServiceRoleARN  string            `json:"serviceRoleARN,omitempty"`
	ToolchainDigest string            `json:"toolchainDigest,omitempty"`
	Images          map[string]string `json:"images"`
	Builds          []Build           `json:"builds,omitempty"`
}

var fullCommit = regexp.MustCompile(`^(?:[a-f0-9]{40}|[a-f0-9]{64})$`)
var digest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var repository = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+$`)

func failure(code, message, action string) error {
	return diagnostics.NewError(diagnostics.DomainFailure{ID: "build-plan-invalid", Code: code, Category: diagnostics.Configuration, Message: message, NextAction: action, Step: "plan-builds"})
}

// Compute returns only safe identities and references, never raw build env,
// source credential values, or provider messages. A missing source observation
// schedules resolution; a mutable branch is never passed to the build executor.
func Compute(authored project.Document, b Bindings, observed *ResolvedSource, cache *Cache) (Plan, error) {
	document, err := validate(authored)
	if err != nil {
		return Plan{}, err
	}
	owner := b.Authority.Owner()
	if owner.ProjectUID == "" || document.Metadata.UID != "" && document.Metadata.UID != owner.ProjectUID {
		return Plan{}, failure("BuildOwnerMismatch", "Build planning requires the verified project owner.", "Bind the API project UID to trusted installation authority.")
	}
	wrappers, err := lambdaArchitectures(document)
	if err != nil {
		return Plan{}, err
	}
	hash := definitionHash(document, wrappers)
	p := Plan{DefinitionHash: hash, Images: map[string]string{}}
	names := sortedKeys(document.Spec.Builds)
	for _, name := range names {
		if document.Spec.Rollout == nil {
			break
		}
		image, ok := document.Spec.Rollout.ImageOverrides[name]
		if !ok {
			continue
		}
		repo, valid := immutableImage(image)
		approved := false
		for _, allowed := range b.OverrideRepositories[name] {
			approved = approved || repo == allowed
		}
		if !valid || !approved {
			return Plan{}, failure("ImageOverrideDenied", "An override is mutable or outside its approved image repository.", "Supply an approved repository image with an exact sha256 digest.")
		}
		p.Images[name] = image
	}
	if len(p.Images) == len(names) {
		p.Action = "Resolved"
		return p, nil
	}
	ref, err := validateSource(document, b.Source)
	if err != nil {
		return Plan{}, err
	}
	if observed == nil {
		p.Action = "ResolveSource"
		return p, nil
	}
	source := *observed
	if source.Owner != owner || source.CheckoutURL != b.Source.CheckoutURL || source.RequestedRef != ref || source.PolicyRevision != b.Source.PolicyRevision || source.BindingHash != SourceBindingHash(b.Source) || !fullCommit.MatchString(source.Commit) || !digest.MatchString(source.ArtifactDigest) || !safeID(source.SourceUID) || !safeID(source.SourceVersion) || fullCommit.MatchString(ref) && ref != source.Commit {
		return Plan{}, failure("SourceAttestationMismatch", "The source observation does not match this owner, repository, ref and immutable artifact.", "Resolve the exact approved ref and independently verify its source UID, version, full commit and artifact digest.")
	}
	p.Source = &source
	if err = validateExecution(b, names); err != nil {
		return Plan{}, err
	}
	p.ProjectARN, p.ServiceRoleARN, p.ToolchainDigest = b.Execution.ProjectARN, b.Execution.ServiceRoleARN, b.Execution.ToolchainDigest
	for _, name := range names {
		if _, ok := p.Images[name]; ok {
			continue
		}
		target := b.Execution.Targets[name]
		if reusable(cache, source, hash, b.Execution.ToolchainDigest, name, target) {
			p.Images[name] = cache.Images[name]
			continue
		}
		key := hashValue(struct {
			Owner                            ownership.Owner
			Source                           ResolvedSource
			DefinitionHash, Toolchain, Build string
			Target                           Target
		}{owner, source, hash, b.Execution.ToolchainDigest, name, target})
		p.Builds = append(p.Builds, Build{Name: name, Target: target, IdempotencyKey: key})
	}
	p.Action = "Build"
	if len(p.Builds) == 0 {
		p.Action = "Resolved"
	}
	return p, nil
}

func validate(authored project.Document) (project.Effective, error) {
	effective, err := project.Prepare(authored)
	if err != nil {
		return project.Effective{}, failure("InvalidBuildProject", "The project build contract is invalid.", "Correct schema and semantic validation errors before planning builds.")
	}
	return effective, nil
}

func validateSource(d project.Effective, b SourceBinding) (string, error) {
	ref := *d.Spec.Source.Branch
	if d.Spec.Rollout != nil && d.Spec.Rollout.SourceCommitSha != nil {
		ref = *d.Spec.Rollout.SourceCommitSha
		if !fullCommit.MatchString(ref) {
			return "", failure("FullSourceCommitRequired", "Pinned source must use a full lowercase commit identity.", "Resolve abbreviated commits before submitting a build intent.")
		}
	}
	if !validRef(ref) {
		return "", failure("InvalidSourceRef", "The requested source ref is invalid or exceeds its size limit.", "Supply a bounded valid Git ref or a full pinned commit.")
	}
	parsed, err := url.Parse(b.CheckoutURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path == "" || strings.ContainsAny(b.CheckoutURL, "\r\n\t ") || b.AuthoredRepository != d.Spec.Source.Repo || b.Ref != ref || !safeID(b.PolicyRevision) || b.SelectionHash != selectionHash(d) {
		return "", failure("SourcePolicyMismatch", "The source lacks an exact approved repository, HTTPS transport, ref and policy revision.", "Bind source-watch policy explicitly; project annotations cannot supply source authority.")
	}
	auth := ""
	if git := d.Spec.Source.GitRepository; git != nil {
		if git.URL != nil && *git.URL != b.CheckoutURL {
			return "", failure("SourcePolicyMismatch", "The requested checkout URL differs from its approved transport.", "Approve the exact source transport through installation policy.")
		}
		if git.AuthSecret != nil {
			auth = string(*git.AuthSecret)
		}
	}
	validAuth := b.AuthPolicy == "None" && auth == "" && b.AuthLogicalName == "" && b.AuthVersionRef == "" || b.AuthPolicy == "LogicalSecret" && auth != "" && b.AuthLogicalName == auth && safeID(b.AuthVersionRef) || b.AuthPolicy == "Installation" && auth == "" && b.AuthLogicalName == "" && safeID(b.AuthVersionRef)
	if !validAuth {
		return "", failure("SourceCredentialPolicyMismatch", "Source authentication must have an exact approved logical name and immutable version reference.", "Bind private source credentials through the trusted secret boundary.")
	}
	return ref, nil
}

func validateExecution(b Bindings, names []string) error {
	scope, err := b.Authority.Scope(ownership.AWS)
	if err != nil || b.Execution == nil || !digest.MatchString(b.Execution.ToolchainDigest) {
		return failure("BuildExecutionBindingRequired", "An explicitly scoped build executor and pinned toolchain are required.", "Configure an isolated CodeBuild project, service role and ECR targets.")
	}
	projectName, _ := b.Authority.Name(ownership.AWS, "codebuild", "images", 63)
	roleName, _ := b.Authority.Name(ownership.AWS, "build-role", "service", 64)
	prefix := "arn:" + scope.Partition + ":"
	if b.Execution.ProjectARN != prefix+"codebuild:"+scope.Region+":"+scope.AccountID+":project/"+projectName || b.Execution.ServiceRoleARN != prefix+"iam::"+scope.AccountID+":role/"+roleName {
		return failure("ForeignBuildExecutor", "The executor or service role is outside this disjoint installation identity.", "Use independently verified test-only executor and role bindings.")
	}
	suffix := "amazonaws.com"
	if scope.Partition == "aws-cn" {
		suffix = "amazonaws.com.cn"
	} else if scope.Partition != "aws" && scope.Partition != "aws-us-gov" {
		return failure("UnsupportedAWSPartition", "The build endpoint partition requires a reviewed adapter.", "Add and verify the partition-specific ECR endpoint contract.")
	}
	for _, name := range names {
		target, ok := b.Execution.Targets[name]
		expected, nameErr := b.Authority.Name(ownership.AWS, "ecr", name, 63)
		if !ok || nameErr != nil || target.Name != expected || target.ARN != prefix+"ecr:"+scope.Region+":"+scope.AccountID+":repository/"+expected || target.URI != fmt.Sprintf("%s.dkr.ecr.%s.%s/%s", scope.AccountID, scope.Region, suffix, expected) {
			return failure("ForeignBuildTarget", "A build target is missing or outside its exact approved ECR identity.", "Bind every build to its verified owner, account, region and repository.")
		}
	}
	return nil
}

func reusable(c *Cache, s ResolvedSource, hash, toolchain, name string, target Target) bool {
	if c == nil || c.Source != s || c.DefinitionHash != hash || c.ToolchainDigest != toolchain || c.Targets[name] != target {
		return false
	}
	repo, valid := immutableImage(c.Images[name])
	return valid && repo == target.URI
}

func immutableImage(image string) (string, bool) {
	repo, sum, found := strings.Cut(image, "@")
	return repo, found && repository.MatchString(repo) && digest.MatchString(sum)
}

func definitionHash(d project.Effective, wrappers map[string]string) string {
	return hashValue(struct {
		Compiler       string
		Builds         map[string]project.Build
		LambdaWrappers map[string]string
	}{CompilerVersion, d.Spec.Builds, wrappers})
}

func lambdaArchitectures(d project.Effective) (map[string]string, error) {
	wrappers := map[string]string{}
	for _, name := range sortedKeys(d.Spec.Components) {
		component := d.Spec.Components[name]
		if component.Serverless == nil {
			continue
		}
		architecture := "x86_64"
		if component.Serverless.Architecture != nil {
			architecture = *component.Serverless.Architecture
		}
		build := string(component.Build)
		if previous, exists := wrappers[build]; exists && previous != architecture {
			return nil, failure("ConflictingLambdaBuildArchitectures", "Lambda components sharing one build require the same architecture.", "Use separate builds for different runtime architectures.")
		}
		wrappers[build] = architecture
	}
	return wrappers, nil
}

// SelectionHash covers source filter/auth/ref authoring, including sparse paths
// and ignore rules. A watch adapter must bind the independently observed source
// object to this selection before attesting its artifact.
func SelectionHash(authored project.Document) (string, error) {
	d, err := validate(authored)
	if err != nil {
		return "", err
	}
	return selectionHash(d), nil
}
func selectionHash(d project.Effective) string {
	var pinned *string
	if d.Spec.Rollout != nil {
		pinned = d.Spec.Rollout.SourceCommitSha
	}
	return hashValue(struct {
		Source project.Source
		Pinned *string
	}{d.Spec.Source, pinned})
}
func SourceBindingHash(b SourceBinding) string { return hashValue(b) }

func validRef(ref string) bool {
	if ref == "" || len(ref) > 255 || ref == "@" || strings.HasPrefix(ref, "-") || strings.HasPrefix(ref, "/") || strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".") || strings.Contains(ref, "..") || strings.Contains(ref, "//") || strings.Contains(ref, "@{") || strings.ContainsAny(ref, " ~^:?*[\\") || strings.IndexFunc(ref, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
		return false
	}
	for _, part := range strings.Split(ref, "/") {
		if strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

func hashValue(value any) string {
	data, _ := json.Marshal(value)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func safeID(value string) bool {
	return value != "" && len(value) <= 256 && strings.IndexFunc(value, func(r rune) bool { return r < 33 || r > 126 }) < 0
}
