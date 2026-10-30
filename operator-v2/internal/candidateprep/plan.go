// Package candidateprep freezes one private preparation plan without I/O.
// It grants no provider, process, prerequisite, readiness or promotion authority.
package candidateprep

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	render "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type Profile string

const (
	// LocalLiteralPairV1 selects exactly one ordinary component in a disposable
	// cluster scope. It is not the full renderer bundle or a Pod trust profile.
	LocalLiteralPairV1     Profile = "local-literal-deployment-service-v1-k8s1.37"
	PromotionNotAuthorized         = "PromotionNotAuthorized"
)

// Config is PRIVATE supervisor input. Never log, serialize into history, or
// construct its authority from project authoring. Image bindings are trusted
// bootstrap assertions, not authenticated build/registry provenance.
type Config struct {
	Document   project.Document
	Authority  ownership.Context
	Subject    rollout.Subject
	Generation int64
	// AttemptID is independently accepted and retained across same-attempt
	// retries. A new explicit attempt changes both derived plan identities.
	AttemptID string
	Component string
	Images    map[string]string
	Profile   Profile
}

type Plan struct{ state *planState }

type planState struct {
	document           project.Document
	authority          ownership.Context
	attempt, component string
	profile            Profile
	desired            rollout.Desired
}

// View is bounded descriptive identity only, never a write grant. It excludes
// raw commands, env, source and image locators and namespace/provider receipts.
type View struct {
	Profile      Profile         `json:"profile"`
	Subject      rollout.Subject `json:"subject"`
	Generation   int64           `json:"generation"`
	OperationID  string          `json:"operationID"`
	Revision     string          `json:"revision"`
	ReconcileKey string          `json:"reconcileKey"`
	Component    string          `json:"component"`
	Promotion    string          `json:"promotion"`
}

var (
	immutableImage = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[a-f0-9]{64}$`)
	identifier     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,255}$`)
)

func failure(code, message string) error {
	return diagnostics.NewError(diagnostics.DomainFailure{ID: "candidate-preparation-plan", Code: code, Category: diagnostics.Configuration, Step: "accept-candidate-plan", Message: message, NextAction: "Correct the accepted local preparation inputs before creating a namespace; retain the original private plan for this attempt."})
}

// New validates raw authoring before cloning. Its digest binds the full private
// canonical raw document, every supplied image binding, selection, finite
// profile, accepted attempt and trusted subject/owner/scope/naming context.
// Digests describe equality; they do not authenticate the bootstrap inputs.
func New(c Config) (Plan, error) {
	if c.Profile != LocalLiteralPairV1 {
		return Plan{}, failure("PreparationProfileUnsupported", "This preparation slice requires its explicit local literal Deployment and Service profile.")
	}
	scope, err := c.Authority.Scope(ownership.Kubernetes)
	owner := c.Authority.Owner()
	if err != nil || scope.Namespace != "" || owner.Lifetime != ownership.Disposable || owner.RunID == "" || c.Subject != (rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}) || c.Generation < 1 || !identifier.MatchString(c.AttemptID) {
		return Plan{}, failure("PreparationAuthorityMismatch", "Preparation requires explicit matching disposable project authority, cluster scope, generation and stable accepted attempt.")
	}
	// Name derivation includes Context's private resource root, which is not
	// otherwise exported. This stable binding makes a changed naming context
	// change the accepted plan without reflecting private Context internals.
	naming, err := c.Authority.Name(ownership.Kubernetes, "runtime-namespace", "accepted-preparation-profile", 63)
	if err != nil {
		return Plan{}, failure("PreparationAuthorityMismatch", "The accepted namespace naming context is invalid.")
	}
	effective, err := project.Prepare(c.Document)
	if err != nil {
		return Plan{}, failure("PreparationProjectInvalid", "Raw project authoring failed validation before preparation acceptance.")
	}
	if c.Document.Metadata.UID != "" && c.Document.Metadata.UID != c.Subject.ProjectUID || c.Document.Metadata.Generation != 0 && c.Document.Metadata.Generation != c.Generation {
		return Plan{}, failure("PreparationProjectIdentityMismatch", "Authored project identity differs from its independently accepted subject or generation.")
	}
	if err = supported(c.Document, effective, c.Component); err != nil {
		return Plan{}, err
	}
	images := make(map[string]string, len(c.Images))
	if len(c.Images) == 0 || len(c.Images) > len(c.Document.Spec.Builds) {
		return Plan{}, failure("PreparationImageBindingInvalid", "Explicit digest-pinned image bindings must identify declared builds.")
	}
	for name, image := range c.Images {
		if _, exists := c.Document.Spec.Builds[name]; !exists || len(image) > 512 || !immutableImage.MatchString(image) {
			return Plan{}, failure("PreparationImageBindingInvalid", "Every supplied image binding must identify a declared build and a credential-free immutable image digest.")
		}
		images[name] = image
	}
	build := string(c.Document.Spec.Components[c.Component].Build)
	if images[build] == "" {
		return Plan{}, failure("PreparationImageBindingMissing", "The selected component requires an explicit resolved immutable image binding.")
	}
	if overrides := c.Document.Spec.Rollout; overrides != nil {
		for name, image := range overrides.ImageOverrides {
			if images[name] != image {
				return Plan{}, failure("PreparationImageBindingMismatch", "An authored image override differs from its explicit trusted resolved binding.")
			}
		}
	}
	raw, err := json.Marshal(c.Document)
	if err != nil || len(raw) > project.MaxDocumentBytes {
		return Plan{}, failure("PreparationProjectInvalid", "The raw accepted project cannot be represented within the authoring byte bound.")
	}
	// Keep raw authoring, never cast Effective back into a Document.
	document, err := project.Decode(raw)
	if err != nil {
		return Plan{}, failure("PreparationProjectInvalid", "The independent raw accepted project copy failed validation.")
	}
	accepted := struct {
		Contract           string
		Raw                json.RawMessage
		Subject            rollout.Subject
		Owner              ownership.Owner
		Scope              ownership.Scope
		Naming             string
		Generation         int64
		Attempt, Component string
		Profile            Profile
		Images             map[string]string
	}{"candidate-preparation-accepted-plan-v1", raw, c.Subject, owner, scope, naming, c.Generation, c.AttemptID, c.Component, c.Profile, images}
	encoded, err := json.Marshal(accepted)
	if err != nil {
		return Plan{}, failure("PreparationProjectInvalid", "The private accepted plan cannot be encoded.")
	}
	hash := sha256.Sum256(encoded)
	digest := hex.EncodeToString(hash[:])
	desired := rollout.Desired{Subject: c.Subject, Generation: c.Generation, Revision: "prep-" + digest, ReconcileKey: "prep-reconcile-" + digest, RequiredBuilds: []string{build}, Images: images}
	return Plan{&planState{document: document, authority: c.Authority, attempt: c.AttemptID, component: c.Component, profile: c.Profile, desired: desired}}, nil
}

func supported(raw project.Document, effective project.Effective, name string) error {
	c, found := raw.Spec.Components[name]
	if !found || len(raw.Spec.Components) != 1 {
		return failure("PreparationSelectionUnsupported", "This preparation slice accepts exactly one explicitly selected ordinary component.")
	}
	// Inspect raw presence before defaulting can obscure unsupported authoring.
	// All fields of the selected Component not listed here are rendered by the
	// existing baseline (build/name/replicas/command/args/ports/literal env/
	// resources/grace). Explicit false token automount is supported.
	if c.IsolatedExecutor != nil || c.AWSIAMRoleARN != nil || c.TemporalAccess != nil || c.TemporalNamespace != nil || c.TemporalProxy != nil || c.TemporalMtls != nil || c.Serverless != nil || c.Worker != nil || c.Autoscaling != nil || len(c.Secrets) != 0 || c.AutomountServiceAccountToken != nil && *c.AutomountServiceAccountToken || len(raw.Spec.Secrets) != 0 || raw.Spec.Temporal != nil || raw.Spec.Ingress != nil || raw.Spec.SmokeCheck != nil {
		return failure("PreparationCapabilityUnsupported", "This preparation slice supports only literal ordinary workloads without credentials, workers, autoscaling, ingress or smoke execution.")
	}
	for _, env := range c.Env {
		if env.ValueFrom != nil || env.Value == nil {
			return failure("PreparationEnvironmentUnsupported", "The selected preparation component supports literal environment values only.")
		}
	}
	if len(effective.Spec.Components[name].Ports) == 0 {
		return failure("PreparationServiceRequired", "The selected component needs at least one declared port for its ClusterIP Service pair.")
	}
	if raw.Metadata.Annotations["registry.tmprl-demo.cloud/catalog"] == "true" {
		return failure("PreparationCapabilityUnsupported", "Platform self-management is outside this local preparation slice.")
	}
	return nil
}

func (p Plan) View() View {
	if p.state == nil {
		return View{Promotion: PromotionNotAuthorized}
	}
	d := p.state.desired
	return View{Profile: p.state.profile, Subject: d.Subject, Generation: d.Generation, OperationID: p.state.attempt, Revision: d.Revision, ReconcileKey: d.ReconcileKey, Component: p.state.component, Promotion: PromotionNotAuthorized}
}

// Desired returns a PRIVATE execution copy for the existing Worker acceptance
// boundary. It includes image locators: never log it or put it in public history.
func (p Plan) Desired() (rollout.Desired, error) {
	if p.state == nil {
		return rollout.Desired{}, failure("PreparationPlanMissing", "The original private accepted preparation plan is unavailable.")
	}
	d := p.state.desired
	d.Images = make(map[string]string, len(p.state.desired.Images))
	for key, value := range p.state.desired.Images {
		d.Images[key] = value
	}
	d.RequiredBuilds = append([]string(nil), d.RequiredBuilds...)
	sort.Strings(d.RequiredBuilds)
	return d, nil
}

func (p Plan) Format(s fmt.State, verb rune) {
	_, _ = io.WriteString(s, "[private accepted candidate preparation plan]")
}
func (p Plan) MarshalJSON() ([]byte, error) {
	return nil, failure("PreparationPrivateSerializationDenied", "Private accepted preparation plans cannot be serialized; use the safe descriptive view.")
}

// Namespace returns a fresh PRIVATE bootstrap render, not authority to apply or
// recreate an already observed namespace. The Worker enforces that lifecycle.
func (p Plan) Namespace() (render.NamespacePlan, error) {
	if p.state == nil {
		return render.NamespacePlan{}, failure("PreparationPlanMissing", "The original private accepted preparation plan is unavailable.")
	}
	// Match the existing Worker's exact namespace bootstrap contract. This
	// digest does not replace the complete private accepted-plan binding above.
	return render.RenderNamespace(p.state.authority, render.NamespaceRequest{Revision: p.state.desired.Revision, OperationID: p.state.attempt, Fingerprint: journal.DesiredDigest(p.state.desired)})
}
