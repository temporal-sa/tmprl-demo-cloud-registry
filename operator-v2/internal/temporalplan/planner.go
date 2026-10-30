package temporalplan

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

type Planner struct {
	policy  Policy
	scope   ownership.Scope
	regions map[string]bool
	auth    map[AuthMode]bool
	targets map[string]TargetBinding
}

var labelPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
var safeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+#-]{0,255}$`)
var regionPattern = regexp.MustCompile(`^(aws|gcp|azure)-[a-z0-9-]{1,58}$`)
var attributePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

func invalid() error { return &Error{Code: "TemporalPlanInvalidPolicy"} }

func New(policy Policy) (*Planner, error) {
	scope, err := policy.Ownership.Scope(ownership.Temporal)
	if err != nil || !labelPattern.MatchString(policy.ProjectName) || policy.DefaultRetentionDays < 1 || policy.MaximumRetentionDays < policy.DefaultRetentionDays || policy.MaximumRetentionDays > 90 {
		return nil, invalid()
	}
	p := &Planner{policy: policy, scope: scope, regions: map[string]bool{}, auth: map[AuthMode]bool{}, targets: map[string]TargetBinding{}}
	p.policy.Targets = nil
	p.policy.AllowedRegions = nil
	p.policy.AllowedAuthModes = nil
	for _, region := range policy.AllowedRegions {
		if !regionPattern.MatchString(region) || p.regions[region] {
			return nil, invalid()
		}
		p.regions[region] = true
	}
	if !p.regions[policy.DefaultRegion] || !p.regions[policy.ServerlessRegion] {
		return nil, invalid()
	}
	for _, mode := range policy.AllowedAuthModes {
		if mode != APIKey && mode != MTLS || p.auth[mode] {
			return nil, invalid()
		}
		p.auth[mode] = true
	}
	if len(p.auth) == 0 {
		return nil, invalid()
	}
	for _, binding := range policy.Targets {
		name, err := policy.Ownership.Name(ownership.Temporal, "namespace", binding.Label, 39)
		if err != nil || !labelPattern.MatchString(binding.Label) || binding.Label == "candidate" || binding.Label == "ca-candidate" || binding.Name != name || binding.Owner != policy.Ownership.Owner() || !p.regions[binding.RegionID] {
			return nil, invalid()
		}
		if _, exists := p.targets[binding.Label]; exists {
			return nil, invalid()
		}
		if binding.Incarnation != nil {
			copy := *binding.Incarnation
			if copy.NamespaceID != binding.Name+"."+scope.AccountID || copy.CreatedAt.IsZero() || !safeIDPattern.MatchString(copy.ProvenanceID) {
				return nil, invalid()
			}
			binding.Incarnation = &copy
		}
		p.targets[binding.Label] = binding
	}
	return p, nil
}

// Plan validates raw authoring again, then computes intent descriptors. Neither
// a CreateIntent action nor an expected version is a mutation capability.
func (p *Planner) Plan(authored project.Document, observed []Observation, managed []ManagedAttributes) (Plan, error) {
	d, err := project.Prepare(authored)
	if err != nil {
		return Plan{}, err
	}
	if d.Metadata.Name != p.policy.ProjectName || d.Metadata.UID != "" && d.Metadata.UID != p.policy.Ownership.Owner().ProjectUID {
		return Plan{}, &Error{Code: "TemporalPlanProjectIdentityMismatch"}
	}
	observations, err := p.observations(observed)
	if err != nil {
		return Plan{}, err
	}
	previous, err := p.managed(managed)
	if err != nil {
		return Plan{}, err
	}
	components, primary, serverless := requirements(d)
	labels := make([]string, 0, len(components)+1)
	if primary {
		labels = append(labels, "primary")
	}
	if d.Spec.Temporal != nil {
		for label := range d.Spec.Temporal.Namespaces {
			labels = append(labels, label)
		}
	}
	sort.Strings(labels)
	plan := Plan{}
	mode := APIKey
	if d.Spec.Temporal != nil && d.Spec.Temporal.Auth != nil && d.Spec.Temporal.Auth.Mode != nil {
		mode = AuthMode(*d.Spec.Temporal.Auth.Mode)
	}
	requested := map[string]bool{}
	for _, label := range labels {
		requested[label] = true
		binding, ok := p.targets[label]
		if !ok {
			plan.add("NamespaceBindingRequired", label, "binding", "The logical namespace needs an exact trusted target binding.", true)
			continue
		}
		ns := p.desired(d, label, binding, mode, serverless)
		ns.Components = slices.Clone(components[label])
		if !p.auth[mode] {
			plan.add("AuthCapabilityDisabled", label, "auth", "The requested authentication mode is outside installation capability.", true)
			ns.Action = Blocked
		}
		if mode == MTLS {
			plan.add("MTLSAuthorityRequired", label, "auth", "mTLS remains blocked until a separately reviewed public CA binding and lifecycle adapter exist.", true)
			ns.Action = Blocked
		}
		if ns.RegionID != binding.RegionID {
			plan.add("RegionBindingMismatch", label, "region", "The requested region differs from the exact approved logical target.", true)
			ns.Action = Blocked
		}
		if ns.RetentionDays < 1 || ns.RetentionDays > p.policy.MaximumRetentionDays {
			plan.add("RetentionOutsidePolicy", label, "retentionDays", "Retention exceeds installation limits.", true)
			ns.Action = Blocked
		}
		obs, found := observations[label]
		p.observe(&plan, &ns, binding, obs, found)
		var attrs map[string]project.TemporalSearchAttribute
		if label == "primary" && d.Spec.Temporal != nil {
			attrs = d.Spec.Temporal.SearchAttributes
		}
		p.attributes(&plan, &ns, attrs, obs, previous[label])
		plan.Namespaces = append(plan.Namespaces, ns)
	}
	// Removing authoring intent never silently turns retained namespaces or
	// their managed attribute ledger into a deletion request.
	for label, binding := range p.targets {
		if binding.Incarnation != nil && !requested[label] {
			obs, found := observations[label]
			retained := p.retained(&plan, binding, obs, found, previous[label])
			plan.RetainedNamespaces = append(plan.RetainedNamespaces, retained)
			plan.add("NamespaceRetained", label, "namespace", "The previously bound namespace is retained after authoring omission.", false)
		}
	}

	sort.Slice(plan.RetainedNamespaces, func(i, j int) bool { return plan.RetainedNamespaces[i].Label < plan.RetainedNamespaces[j].Label })
	sort.Slice(plan.Issues, func(i, j int) bool {
		a, b := plan.Issues[i], plan.Issues[j]
		if a.Label != b.Label {
			return a.Label < b.Label
		}
		if a.Field != b.Field {
			return a.Field < b.Field
		}
		return a.Code < b.Code
	})
	return plan, nil
}

// Omitted resources remain observable without producing mutation/delete intent.
// Retain-only policy must not hide missing or replaced namespace incarnations.
func (p *Planner) retained(plan *Plan, binding TargetBinding, obs Observation, found bool, managed map[string]AttributeType) RetainedNamespace {
	r := RetainedNamespace{Label: binding.Label, Name: binding.Name, NamespaceID: binding.Incarnation.NamespaceID, Reason: "NamespaceDeletionUnsupported", BoundIncarnation: *binding.Incarnation, ObservationState: RetainedUnknown}
	verifiedBirth := false
	if !found {
		plan.add("RetainedNamespaceObservationRequired", binding.Label, "incarnation", "The omitted bound namespace has no current observation; retention does not prove it still exists.", false)
	} else if !obs.Exists {
		r.ObservationState = RetainedMissing
		plan.add("RetainedNamespaceMissing", binding.Label, "incarnation", "The omitted bound namespace is absent; its creation history is retained and its name is never re-created.", false)
	} else if !obs.CreatedAt.Equal(binding.Incarnation.CreatedAt) || obs.ProvenanceID != binding.Incarnation.ProvenanceID {
		r.ObservationState = RetainedIncarnationMismatch
		plan.add("RetainedNamespaceIncarnationMismatch", binding.Label, "incarnation", "The retained namespace observation has another birth or creation provenance; do not adopt or clean it by name.", true)
	} else {
		verifiedBirth = true
		r.ObservedState = obs.State
		r.ObservedVersion = obs.ResourceVersion
		r.ObservationState = RetainedObserved
		if obs.State != Active {
			r.ObservationState = RetainedNotActive
			plan.add("RetainedNamespaceNotActive", binding.Label, "state", "The omitted bound namespace is not active; retain its lifecycle evidence for diagnosis.", false)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(managed)) {
		decision := AttributeDecision{Name: name, DesiredType: managed[name], Action: RetainedUnsupported}
		if verifiedBirth {
			decision.ObservedType = obs.SearchAttributes[name]
			if actual, present := obs.SearchAttributes[name]; present && actual != managed[name] {
				plan.add("RetainedManagedSearchAttributeTypeDrift", binding.Label, "searchAttributes/"+name, "A retained managed attribute has another observed type; keep the original ledger and do not replace it.", true)
			}
		}
		r.Attributes = append(r.Attributes, decision)
		plan.add("SearchAttributeRemovalUnsupported", binding.Label, "searchAttributes/"+name, "The omitted previously managed attribute remains tracked; provider removal is disabled.", false)
	}
	return r
}

func (plan *Plan) add(code, label, field, message string, blocking bool) {
	plan.Issues = append(plan.Issues, Issue{code, label, field, message, blocking})
}

func requirements(d project.Effective) (map[string][]string, bool, bool) {
	components := map[string][]string{}
	primary, serverless := false, false
	for name, c := range d.Spec.Components {
		isServerless := c.Serverless != nil && c.Serverless.Provider == "aws-lambda"
		serverless = serverless || isServerless
		needs := c.Worker != nil && c.Worker.Enabled || c.TemporalAccess != nil && *c.TemporalAccess || isServerless
		needs = needs || c.TemporalMtls != nil && c.TemporalMtls.CertFiles != nil && (c.TemporalMtls.CertFiles.Enabled == nil || *c.TemporalMtls.CertFiles.Enabled)
		for _, env := range c.Env {
			if env.ValueFrom != nil && strings.HasPrefix(env.ValueFrom.InputRef, "temporal.") {
				needs = true
			}
		}
		if !needs {
			continue
		}
		label := "primary"
		if c.TemporalNamespace != nil {
			label = string(*c.TemporalNamespace)
		}
		components[label] = append(components[label], name)
		primary = primary || label == "primary"
	}
	if t := d.Spec.Temporal; t != nil {
		primary = primary || len(t.SearchAttributes) > 0
		for _, endpoint := range t.NexusEndpoints {
			primary = primary || endpoint.TargetNamespace == nil || string(*endpoint.TargetNamespace) == "primary"
			for _, caller := range endpoint.AllowedCallerNamespaces {
				primary = primary || string(caller) == "primary"
			}
		}
	}
	for _, names := range components {
		sort.Strings(names)
	}
	return components, primary, serverless
}

func (p *Planner) desired(d project.Effective, label string, binding TargetBinding, mode AuthMode, serverless bool) Namespace {
	region := p.policy.DefaultRegion
	if serverless {
		region = p.policy.ServerlessRegion
	}
	retention := p.policy.DefaultRetentionDays
	fairness := false
	if t := d.Spec.Temporal; t != nil {
		if label == "primary" {
			if t.Region != nil {
				region = string(*t.Region)
			}
			fairness = t.Fairness != nil && bool(*t.Fairness)
		} else {
			spec := t.Namespaces[label]
			if spec.Region != nil {
				region = string(*spec.Region)
			}
			if spec.RetentionDays != nil {
				retention = int32(*spec.RetentionDays)
			}
			fairness = spec.Fairness != nil && bool(*spec.Fairness)
		}
	}
	ns := Namespace{Label: label, Name: binding.Name, NamespaceID: binding.Name + "." + p.scope.AccountID, RegionID: region, RetentionDays: retention, FairnessEnabled: fairness, AuthMode: mode, Action: Observe}
	if binding.Incarnation != nil {
		copy := *binding.Incarnation
		ns.Incarnation = &copy
	}
	return ns
}

func (p *Planner) observations(values []Observation) (map[string]Observation, error) {
	result := map[string]Observation{}
	for _, value := range values {
		binding, ok := p.targets[value.Label]
		if !ok {
			return nil, &Error{Code: "TemporalPlanUnknownObservation"}
		}
		if _, exists := result[value.Label]; exists {
			return nil, &Error{Code: "TemporalPlanDuplicateObservation"}
		}
		if value.Owner != binding.Owner || value.Name != binding.Name || value.NamespaceID != binding.Name+"."+p.scope.AccountID {
			return nil, &Error{Code: "TemporalPlanObservationIdentityMismatch"}
		}
		if value.Exists {
			if value.CreatedAt.IsZero() || !safeIDPattern.MatchString(value.ResourceVersion) || !safeIDPattern.MatchString(value.ProvenanceID) || !p.regions[value.RegionID] || !validAuth(value.AuthMode) || !validNamespaceState(value.State) {
				return nil, &Error{Code: "TemporalPlanInvalidObservation"}
			}
		} else if value.ResourceVersion != "" || len(value.SearchAttributes) > 0 {
			return nil, &Error{Code: "TemporalPlanInvalidObservation"}
		}
		for name, kind := range value.SearchAttributes {
			if !attributePattern.MatchString(name) || !validAttribute(kind) {
				return nil, &Error{Code: "TemporalPlanInvalidObservation"}
			}
		}
		value.SearchAttributes = maps.Clone(value.SearchAttributes)
		result[value.Label] = value
	}
	return result, nil
}
func (p *Planner) managed(values []ManagedAttributes) (map[string]map[string]AttributeType, error) {
	result := map[string]map[string]AttributeType{}
	for _, value := range values {
		binding, ok := p.targets[value.Label]
		if !ok || binding.Incarnation == nil || value.Owner != binding.Owner || value.NamespaceID != binding.Incarnation.NamespaceID || !value.CreatedAt.Equal(binding.Incarnation.CreatedAt) || value.ProvenanceID != binding.Incarnation.ProvenanceID {
			return nil, &Error{Code: "TemporalPlanManagedAttributeIdentityMismatch"}
		}
		if _, exists := result[value.Label]; exists {
			return nil, &Error{Code: "TemporalPlanDuplicateManagedAttributes"}
		}
		for name, kind := range value.Attributes {
			if !attributePattern.MatchString(name) || !validAttribute(kind) {
				return nil, &Error{Code: "TemporalPlanInvalidManagedAttribute"}
			}
		}
		result[value.Label] = maps.Clone(value.Attributes)
	}
	return result, nil
}

func (p *Planner) observe(plan *Plan, ns *Namespace, binding TargetBinding, obs Observation, found bool) {
	block := func(code, field, message string) { plan.add(code, ns.Label, field, message, true); ns.Action = Blocked }
	if !found {
		if binding.Incarnation != nil {
			block("BoundIncarnationObservationRequired", "incarnation", "Observe the exact previously bound namespace before planning mutations.")
		}
		return
	}
	if binding.Incarnation == nil {
		if obs.Exists {
			block("NamespaceOwnershipUnproven", "incarnation", "A matching name is not creation provenance; explicit reviewed binding is required.")
			return
		}
		if ns.Action != Blocked {
			ns.Action = CreateIntent
		}
		return
	}
	if !obs.Exists {
		block("BoundNamespaceMissing", "incarnation", "A previously bound incarnation is absent; this plan never re-creates its name.")
		return
	}
	if !obs.CreatedAt.Equal(binding.Incarnation.CreatedAt) || obs.ProvenanceID != binding.Incarnation.ProvenanceID {
		block("NamespaceIncarnationMismatch", "incarnation", "The observed birth or attested creation provenance differs from the bound incarnation.")
		return
	}
	ns.ExpectedVersion = obs.ResourceVersion
	if obs.State != Active {
		block("NamespaceNotActive", "state", "The bound namespace is not active; wait or diagnose its provider lifecycle before update.")
	}
	if obs.RegionID != ns.RegionID {
		block("RegionMigrationUnsupported", "region", "An existing namespace region differs; destructive region migration is outside this planner.")
	}
	if obs.AuthMode != ns.AuthMode {
		block("AuthMigrationUnsupported", "auth", "Authentication migration needs a separately reviewed credential/namespace lifecycle.")
	}
	ns.NeedsRetentionUpdate = obs.RetentionDays != ns.RetentionDays
	ns.NeedsFairnessUpdate = obs.FairnessEnabled != ns.FairnessEnabled
	if ns.Action != Blocked {
		ns.Action = Verify
		if ns.NeedsRetentionUpdate || ns.NeedsFairnessUpdate {
			ns.Action = UpdateIntent
		}
	}
}

func (p *Planner) attributes(plan *Plan, ns *Namespace, desired map[string]project.TemporalSearchAttribute, obs Observation, previous map[string]AttributeType) {
	names := map[string]bool{}
	for name := range desired {
		names[name] = true
	}
	for name := range previous {
		names[name] = true
	}
	for name := range obs.SearchAttributes {
		names[name] = true
	}
	for _, name := range slices.Sorted(maps.Keys(names)) {
		definition, wanted := desired[name]
		actual, present := obs.SearchAttributes[name]
		old, wasManaged := previous[name]
		decision := AttributeDecision{Name: name, ObservedType: actual}
		if wanted {
			kind := Keyword
			if definition.Type != nil {
				kind = AttributeType(*definition.Type)
				if kind == "keywordList" {
					kind = KeywordList
				}
			}
			decision.DesiredType = kind
			decision.Action = Add
			if wasManaged && old != kind {
				decision.Action = AttributeTypeConflict
				ns.Action = Blocked
				plan.add("ManagedSearchAttributeTypeConflict", ns.Label, "searchAttributes/"+name, "The requested type differs from the retained managed-attribute ledger; replacement is disabled.", true)
			} else if present && actual == kind {
				decision.Action = VerifyAttribute
			} else if present {
				decision.Action = AttributeTypeConflict
				ns.Action = Blocked
				plan.add("SearchAttributeTypeConflict", ns.Label, "searchAttributes/"+name, "The observed search attribute has another type; it is retained without replacement.", true)
			}
			if decision.Action == Add && !obs.Exists && ns.Action != CreateIntent {
				decision.Action = ObserveAttribute
			}
			if decision.Action == Add && ns.Action == Verify {
				ns.Action = UpdateIntent
			}
		} else if wasManaged {
			decision.DesiredType = old
			decision.Action = RetainedUnsupported
			plan.add("SearchAttributeRemovalUnsupported", ns.Label, "searchAttributes/"+name, "The omitted previously managed attribute remains tracked; provider removal is disabled.", false)
			if present && actual != old {
				ns.Action = Blocked
				plan.add("ManagedSearchAttributeTypeDrift", ns.Label, "searchAttributes/"+name, "A previously managed attribute has changed type; ownership history is retained.", true)
			}
		} else {
			decision.Action = RetainedUnmanaged
		}
		ns.Attributes = append(ns.Attributes, decision)
	}
}
func validAttribute(v AttributeType) bool {
	switch v {
	case Text, Keyword, Int, Double, Bool, Datetime, KeywordList:
		return true
	}
	return false
}
func validAuth(v AuthMode) bool { return v == APIKey || v == MTLS }

// String is deliberately a summary; it cannot accidentally print a project or
// transport response. Detailed output remains typed and credential-free.
func (p Plan) String() string {
	return fmt.Sprintf("Temporal namespace plan: %d targets, %d issues; mutation authorization required", len(p.Namespaces), len(p.Issues))
}

func validNamespaceState(state NamespaceState) bool {
	switch state {
	case Active, Activating, Deleting, ActivationFailed:
		return true
	}
	return false
}
