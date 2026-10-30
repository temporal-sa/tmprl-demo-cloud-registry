package temporalplan

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

const minimal = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
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
func document(t *testing.T) project.Document {
	t.Helper()
	d, err := project.Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func policy(t *testing.T, d project.Document) Policy {
	t.Helper()
	owner := ownership.Owner{EnvironmentID: "lab", InstallationID: "registry-v2", ProjectUID: "uid-" + d.Metadata.Name, Lifetime: ownership.Disposable, RunID: "run-one"}
	ctx, err := ownership.NewContext(ownership.Config{Owner: owner, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Temporal, AccountID: "fixture-account", Namespace: "fixture-authority"}}})
	if err != nil {
		t.Fatal(err)
	}
	regions := map[string]bool{"aws-ca-central-1": true, "aws-us-east-2": true}
	labels := []string{"primary"}
	if temporal := d.Spec.Temporal; temporal != nil {
		if temporal.Region != nil {
			regions[string(*temporal.Region)] = true
		}
		for label, spec := range temporal.Namespaces {
			labels = append(labels, label)
			if spec.Region != nil {
				regions[string(*spec.Region)] = true
			}
		}
	}
	var allowed []string
	for region := range regions {
		allowed = append(allowed, region)
	}
	sort.Strings(allowed)
	p := Policy{Ownership: ctx, ProjectName: d.Metadata.Name, DefaultRegion: "aws-ca-central-1", ServerlessRegion: "aws-us-east-2", AllowedRegions: allowed, DefaultRetentionDays: 7, MaximumRetentionDays: 90, AllowedAuthModes: []AuthMode{APIKey, MTLS}}
	effective, err := project.Prepare(d)
	if err != nil {
		t.Fatal(err)
	}
	_, _, serverless := requirements(effective)
	for _, label := range labels {
		if label == "primary" && len(p.Targets) > 0 {
			continue
		}
		name, err := ctx.Name(ownership.Temporal, "namespace", label, 39)
		if err != nil {
			t.Fatal(err)
		}
		region := p.DefaultRegion
		if serverless {
			region = p.ServerlessRegion
		}
		if temporal := d.Spec.Temporal; temporal != nil {
			if label == "primary" && temporal.Region != nil {
				region = string(*temporal.Region)
			} else if label != "primary" && temporal.Namespaces[label].Region != nil {
				region = string(*temporal.Namespaces[label].Region)
			}
		}
		p.Targets = append(p.Targets, TargetBinding{Label: label, Name: name, RegionID: region, Owner: owner})
	}
	return p
}
func planner(t *testing.T, p Policy) *Planner {
	t.Helper()
	value, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func plan(t *testing.T, p *Planner, d project.Document, observed []Observation, managed []ManagedAttributes) Plan {
	t.Helper()
	value, err := p.Plan(d, observed, managed)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func absent(p Policy, label string) Observation {
	for _, target := range p.Targets {
		if target.Label == label {
			return Observation{Label: label, Owner: target.Owner, Name: target.Name, NamespaceID: target.Name + ".fixture-account"}
		}
	}
	panic("fixture target missing")
}
func bind(p *Policy, label string) Observation {
	for i, target := range p.Targets {
		if target.Label != label {
			continue
		}
		birth := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
		inc := &Incarnation{NamespaceID: target.Name + ".fixture-account", CreatedAt: birth, ProvenanceID: "fixture-create-proof"}
		p.Targets[i].Incarnation = inc
		return Observation{Label: label, Exists: true, State: Active, Name: target.Name, NamespaceID: inc.NamespaceID, Owner: target.Owner, CreatedAt: birth, ProvenanceID: inc.ProvenanceID, ResourceVersion: "version-one", RegionID: target.RegionID, RetentionDays: 7, AuthMode: APIKey}
	}
	panic("fixture target missing")
}
func issue(p Plan, code string) bool {
	for _, value := range p.Issues {
		if value.Code == code {
			return true
		}
	}
	return false
}

func TestImplicitRequirementsAndEmptyTemporalSection(t *testing.T) {
	for name, edit := range map[string]func(*project.Document){
		"none":           func(*project.Document) {},
		"empty-temporal": func(d *project.Document) { d.Spec.Temporal = &project.Temporal{} },
		"worker": func(d *project.Document) {
			c := d.Spec.Components["app"]
			c.Worker = &project.Worker{Enabled: true}
			d.Spec.Components["app"] = c
		},
		"access": func(d *project.Document) {
			c := d.Spec.Components["app"]
			c.TemporalAccess = ptr(true)
			d.Spec.Components["app"] = c
		},
		"search-only": func(d *project.Document) {
			d.Spec.Temporal = &project.Temporal{SearchAttributes: map[string]project.TemporalSearchAttribute{"Indexed": {}}}
		},
		"input-only": func(d *project.Document) {
			c := d.Spec.Components["app"]
			c.Env = []project.EnvVar{{Name: "CUSTOM_NAMESPACE", ValueFrom: &project.EnvValueFrom{InputRef: "temporal.namespace"}}}
			d.Spec.Components["app"] = c
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := document(t)
			edit(&d)
			p := policy(t, d)
			result := plan(t, planner(t, p), d, []Observation{absent(p, "primary")}, nil)
			required := name != "none" && name != "empty-temporal"
			if (len(result.Namespaces) == 1) != required {
				t.Fatalf("wrong implicit requirement: %#v", result)
			}
			if required && result.Namespaces[0].Action != CreateIntent {
				t.Fatal("verified absence should require durable create intent")
			}
			if result.MutationAuthorized {
				t.Fatal("pure DTO granted mutation")
			}
		})
	}
}

func TestAdditionalNamespaceAndNexusPrimaryRequirements(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{Fairness: ptr(project.TemporalFairness(true)), Namespaces: map[string]project.TemporalNamespaceSpec{"secondary": {RetentionDays: ptr(14), Fairness: ptr(project.TemporalFairness(false))}}, NexusEndpoints: map[string]project.NexusEndpoint{"Demo": {TargetNamespace: ptr(project.NexusEndpointNamespaceRef("secondary")), TargetTaskQueue: "jobs", AllowedCallerNamespaces: []project.NexusEndpointNamespaceRef{"secondary"}}}}
	p := policy(t, d)
	result := plan(t, planner(t, p), d, nil, nil)
	if len(result.Namespaces) != 1 || result.Namespaces[0].Label != "secondary" || result.Namespaces[0].RetentionDays != 14 || result.Namespaces[0].FairnessEnabled {
		t.Fatal("additional declaration/default semantics changed")
	}
	endpoint := d.Spec.Temporal.NexusEndpoints["Demo"]
	endpoint.AllowedCallerNamespaces = []project.NexusEndpointNamespaceRef{"primary"}
	d.Spec.Temporal.NexusEndpoints["Demo"] = endpoint
	result = plan(t, planner(t, p), d, nil, nil)
	if len(result.Namespaces) != 2 || result.Namespaces[0].Label != "primary" || !result.Namespaces[0].FairnessEnabled {
		t.Fatal("Nexus caller primary context missing")
	}
}

func TestObservedIncarnationCannotBeAdoptedOrRecreated(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{SearchAttributes: map[string]project.TemporalSearchAttribute{"Indexed": {}}}
	p := policy(t, d)
	observation := bind(&p, "primary")
	bound := planner(t, p)
	result := plan(t, bound, d, []Observation{observation}, nil)
	if result.Namespaces[0].Action != UpdateIntent || result.Namespaces[0].ExpectedVersion != "version-one" {
		t.Fatal("bound update intent missing version")
	}
	missing := observation
	missing.Exists = false
	missing.ResourceVersion = ""
	missing.SearchAttributes = nil
	result = plan(t, bound, d, []Observation{missing}, nil)
	if result.Namespaces[0].Action != Blocked || !issue(result, "BoundNamespaceMissing") {
		t.Fatal("missing bound namespace was recreated")
	}
	result = plan(t, bound, d, nil, nil)
	if !issue(result, "BoundIncarnationObservationRequired") {
		t.Fatal("missing observation became absence")
	}
	changed := observation
	changed.CreatedAt = changed.CreatedAt.Add(time.Second)
	result = plan(t, bound, d, []Observation{changed}, nil)
	if !issue(result, "NamespaceIncarnationMismatch") {
		t.Fatal("reused name acquired authority")
	}
	unboundPolicy := p
	unboundPolicy.Targets = append([]TargetBinding(nil), p.Targets...)
	unboundPolicy.Targets[0].Incarnation = nil
	result = plan(t, planner(t, unboundPolicy), d, []Observation{observation}, nil)
	if !issue(result, "NamespaceOwnershipUnproven") {
		t.Fatal("name-only discovery was adopted")
	}
	observation.State = Activating
	result = plan(t, bound, d, []Observation{observation}, nil)
	if !issue(result, "NamespaceNotActive") {
		t.Fatal("inactive namespace became ready")
	}
}

func TestSearchAttributeAddVerifyTypeConflictAndVisibleRetention(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{SearchAttributes: map[string]project.TemporalSearchAttribute{"Existing": {}, "Drift": {}, "NewList": {Type: ptr("keywordList")}}}
	p := policy(t, d)
	obs := bind(&p, "primary")
	obs.SearchAttributes = map[string]AttributeType{"Existing": Keyword, "Drift": Int, "Retired": Text, "Manual": Bool}
	managed := ManagedAttributes{Label: "primary", NamespaceID: obs.NamespaceID, CreatedAt: obs.CreatedAt, Owner: obs.Owner, ProvenanceID: obs.ProvenanceID, Attributes: map[string]AttributeType{"Retired": Text}}
	result := plan(t, planner(t, p), d, []Observation{obs}, []ManagedAttributes{managed})
	if !issue(result, "SearchAttributeTypeConflict") || !issue(result, "SearchAttributeRemovalUnsupported") || result.Namespaces[0].Action != Blocked {
		t.Fatal("type drift/removal silently accepted")
	}
	decisions := map[string]AttributeDecision{}
	for _, entry := range result.Namespaces[0].Attributes {
		decisions[entry.Name] = entry
	}
	if decisions["Existing"].Action != VerifyAttribute || decisions["NewList"].DesiredType != KeywordList || decisions["NewList"].Action != Add || decisions["Retired"].Action != RetainedUnsupported || decisions["Manual"].Action != RetainedUnmanaged {
		t.Fatalf("wrong decisions: %#v", decisions)
	}
	// Removing all Temporal authoring still exposes retained attribute history.
	d.Spec.Temporal = nil
	result = plan(t, planner(t, p), d, []Observation{obs}, []ManagedAttributes{managed})
	if len(result.Namespaces) != 0 || len(result.RetainedNamespaces) != 1 || len(result.RetainedNamespaces[0].Attributes) != 1 || !issue(result, "SearchAttributeRemovalUnsupported") {
		t.Fatal("whole-block omission lost managed attribute history")
	}
}

func TestPolicyOwnershipRegionAuthAndTypeHistoryAreExplicit(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{SearchAttributes: map[string]project.TemporalSearchAttribute{"Indexed": {}}}
	p := policy(t, d)
	obs := bind(&p, "primary")
	for name, alter := range map[string]func(*Policy){"owner": func(p *Policy) { p.Targets[0].Owner.ProjectUID = "foreign" }, "name": func(p *Policy) { p.Targets[0].Name = "production-name" }, "region": func(p *Policy) { p.Targets[0].RegionID = "gcp-unapproved" }, "account": func(p *Policy) { p.Targets[0].Incarnation.NamespaceID = "other.account" }} {
		t.Run(name, func(t *testing.T) {
			copy := p
			copy.Targets = append([]TargetBinding(nil), p.Targets...)
			birth := *p.Targets[0].Incarnation
			copy.Targets[0].Incarnation = &birth
			alter(&copy)
			if _, err := New(copy); err == nil {
				t.Fatal("invalid trusted binding accepted")
			}
		})
	}
	p.Targets[0].RegionID = "aws-us-east-2"
	result := plan(t, planner(t, p), d, []Observation{obs}, nil)
	if !issue(result, "RegionBindingMismatch") || result.Namespaces[0].Action != Blocked {
		t.Fatal("requested region bypassed approval")
	}
	p.Targets[0].RegionID = obs.RegionID
	d.Spec.Temporal.Auth = &project.TemporalAuth{Mode: ptr("mtls")}
	result = plan(t, planner(t, p), d, []Observation{obs}, nil)
	if !issue(result, "MTLSAuthorityRequired") || !issue(result, "AuthMigrationUnsupported") {
		t.Fatal("mTLS transition invented authority")
	}
	d.Spec.Temporal.Auth = nil
	managed := ManagedAttributes{Label: "primary", NamespaceID: obs.NamespaceID, CreatedAt: obs.CreatedAt, Owner: obs.Owner, ProvenanceID: obs.ProvenanceID, Attributes: map[string]AttributeType{"Indexed": Text}}
	result = plan(t, planner(t, p), d, []Observation{obs}, []ManagedAttributes{managed})
	if !issue(result, "ManagedSearchAttributeTypeConflict") {
		t.Fatal("missing provider attr erased earlier managed type")
	}
}

func TestCopiesAndDeterministicOutput(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{SearchAttributes: map[string]project.TemporalSearchAttribute{"Zed": {}, "Alpha": {}}}
	p := policy(t, d)
	obs := bind(&p, "primary")
	obs.SearchAttributes = map[string]AttributeType{"Alpha": Keyword}
	value := planner(t, p)
	before, _ := json.Marshal(d)
	first := plan(t, value, d, []Observation{obs}, nil)
	p.Targets[0].Name = "mutated"
	p.Targets[0].Incarnation.ProvenanceID = "mutated"
	p.AllowedRegions[0] = "mutated"
	second := plan(t, value, d, []Observation{obs}, nil)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("constructor retained mutable caller policy")
	}
	first.Namespaces[0].Incarnation.ProvenanceID = "mutated"
	first.Namespaces[0].Attributes[0].Name = "mutated"
	third := plan(t, value, d, []Observation{obs}, nil)
	if !reflect.DeepEqual(second, third) {
		t.Fatal("output mutated planner bindings")
	}
	after, _ := json.Marshal(d)
	if string(before) != string(after) {
		t.Fatal("planner modified authoring document")
	}
}

func TestCurrentProjectFixturesPlanWithoutCredentials(t *testing.T) {
	var files []string
	for _, pattern := range []string{"../../../projects/demo/*.yaml", "../../../projects/operator/*.yaml"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
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
			p := policy(t, d)
			result := plan(t, planner(t, p), d, nil, nil)
			if result.MutationAuthorized {
				t.Fatal("fixture plan authorized Cloud writes")
			}
			for _, ns := range result.Namespaces {
				if ns.RetentionDays < 1 || ns.Name == "" || ns.NamespaceID == "" {
					t.Fatal("incomplete desired namespace")
				}
			}
		})
	}
}

func TestForeignProjectObservationAndManagedHistoryAreRejected(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{SearchAttributes: map[string]project.TemporalSearchAttribute{"Indexed": {}}}
	p := policy(t, d)
	obs := bind(&p, "primary")
	value := planner(t, p)
	for name, change := range map[string]func(*Observation){
		"owner":   func(o *Observation) { o.Owner.ProjectUID = "foreign" },
		"account": func(o *Observation) { o.NamespaceID = o.Name + ".foreign-account" },
		"name":    func(o *Observation) { o.Name = "foreign-name" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := obs
			change(&copy)
			if _, err := value.Plan(d, []Observation{copy}, nil); err == nil {
				t.Fatal("foreign observation accepted")
			}
		})
	}
	copy := d
	copy.Metadata.UID = "foreign-project-uid"
	if _, err := value.Plan(copy, nil, nil); err == nil {
		t.Fatal("foreign project UID accepted")
	}
	managed := ManagedAttributes{Label: "primary", NamespaceID: obs.NamespaceID, CreatedAt: obs.CreatedAt.Add(time.Second), Owner: obs.Owner, ProvenanceID: obs.ProvenanceID, Attributes: map[string]AttributeType{"Retired": Keyword}}
	if _, err := value.Plan(d, []Observation{obs}, []ManagedAttributes{managed}); err == nil {
		t.Fatal("another incarnation's managed attributes accepted")
	}
}

func TestFairnessAndRetentionProduceOnlyBoundedUpdateDescriptors(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{Fairness: ptr(project.TemporalFairness(true)), Namespaces: map[string]project.TemporalNamespaceSpec{"secondary": {RetentionDays: ptr(14)}}}
	c := d.Spec.Components["app"]
	c.Worker = &project.Worker{Enabled: true}
	c.TemporalNamespace = ptr(project.TemporalNamespaceLabel("secondary"))
	d.Spec.Components["app"] = c
	p := policy(t, d)
	obs := bind(&p, "secondary")
	result := plan(t, planner(t, p), d, []Observation{obs}, nil)
	if len(result.Namespaces) != 1 || result.Namespaces[0].Action != UpdateIntent || !result.Namespaces[0].NeedsRetentionUpdate || result.Namespaces[0].NeedsFairnessUpdate {
		t.Fatal("additional fairness incorrectly inherited from primary")
	}
	p.MaximumRetentionDays = 10
	result = plan(t, planner(t, p), d, []Observation{obs}, nil)
	if !issue(result, "RetentionOutsidePolicy") || result.Namespaces[0].Action != Blocked {
		t.Fatal("retention exceeded installation cap")
	}
}

func TestReservedPrimaryAdditionalLabelIsNotConflated(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{Namespaces: map[string]project.TemporalNamespaceSpec{"primary": {}}}
	// The shared authoring boundary rejects this before a namespace planner can
	// interpret the label differently from a component or Nexus reference.
	p := policy(t, document(t))
	_, err := planner(t, p).Plan(d, nil, nil)
	typed, ok := err.(*project.ValidationError)
	found := false
	if ok {
		for _, issue := range typed.Issues {
			found = found || issue.Code == "ReservedNamespaceLabel" && issue.Pointer == "/spec/temporal/namespaces/primary"
		}
	}
	if !found {
		t.Fatalf("ambiguous primary label accepted: %v", err)
	}
}

func TestServerlessImplicitAccessAndRegionPrecedence(t *testing.T) {
	d := document(t)
	c := d.Spec.Components["app"]
	c.Serverless = &project.Serverless{Provider: "aws-lambda"}
	d.Spec.Components["app"] = c
	p := policy(t, d)
	result := plan(t, planner(t, p), d, nil, nil)
	if len(result.Namespaces) != 1 || result.Namespaces[0].RegionID != "aws-us-east-2" || !reflect.DeepEqual(result.Namespaces[0].Components, []string{"app"}) {
		t.Fatal("serverless component did not implicitly require regional Temporal access")
	}
	d.Spec.Temporal = &project.Temporal{Region: ptr(project.TemporalRegion("aws-ca-central-1"))}
	p = policy(t, d)
	result = plan(t, planner(t, p), d, nil, nil)
	if result.Namespaces[0].RegionID != "aws-ca-central-1" {
		t.Fatal("explicit region did not override serverless default")
	}
}

func TestMTLSFileAccessIsExplicitAndIssuanceRemainsBlocked(t *testing.T) {
	d := document(t)
	d.Spec.Temporal = &project.Temporal{Auth: &project.TemporalAuth{Mode: ptr("mtls")}}
	c := d.Spec.Components["app"]
	c.TemporalMtls = &project.TemporalMtls{CertFiles: &project.TemporalMtlsCertFiles{}}
	d.Spec.Components["app"] = c
	p := policy(t, d)
	result := plan(t, planner(t, p), d, nil, nil)
	if len(result.Namespaces) != 1 || result.Namespaces[0].AuthMode != MTLS || !issue(result, "MTLSAuthorityRequired") || result.Namespaces[0].Action != Blocked {
		t.Fatal("mTLS files were silently ignored or issuance implied")
	}
	c.TemporalMtls.CertFiles.Enabled = ptr(false)
	d.Spec.Components["app"] = c
	result = plan(t, planner(t, p), d, nil, nil)
	if len(result.Namespaces) != 0 {
		t.Fatal("disabled certificate files created a namespace requirement")
	}
}

func TestOmittedBoundNamespaceStillReportsObservedLifecycle(t *testing.T) {
	d := document(t)
	p := policy(t, d)
	active := bind(&p, "primary")
	active.SearchAttributes = map[string]AttributeType{"Retired": Keyword}
	managed := ManagedAttributes{Label: "primary", NamespaceID: active.NamespaceID, CreatedAt: active.CreatedAt, Owner: active.Owner, ProvenanceID: active.ProvenanceID, Attributes: map[string]AttributeType{"Retired": Keyword}}
	for _, test := range []struct {
		name    string
		mutate  func(*Observation)
		missing bool
		state   RetainedObservationState
		code    string
	}{
		{name: "unknown", missing: true, state: RetainedUnknown, code: "RetainedNamespaceObservationRequired"},
		{name: "absent", mutate: func(o *Observation) { o.Exists = false; o.ResourceVersion = ""; o.SearchAttributes = nil }, state: RetainedMissing, code: "RetainedNamespaceMissing"},
		{name: "changed-birth", mutate: func(o *Observation) { o.CreatedAt = o.CreatedAt.Add(time.Second) }, state: RetainedIncarnationMismatch, code: "RetainedNamespaceIncarnationMismatch"},
		{name: "changed-provenance", mutate: func(o *Observation) { o.ProvenanceID = "another-operation" }, state: RetainedIncarnationMismatch, code: "RetainedNamespaceIncarnationMismatch"},
		{name: "deleting", mutate: func(o *Observation) { o.State = Deleting }, state: RetainedNotActive, code: "RetainedNamespaceNotActive"},
		{name: "active", state: RetainedObserved},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation := active
			if test.mutate != nil {
				test.mutate(&observation)
			}
			values := []Observation{observation}
			if test.missing {
				values = nil
			}
			result := plan(t, planner(t, p), d, values, []ManagedAttributes{managed})
			if len(result.Namespaces) != 0 || len(result.RetainedNamespaces) != 1 || result.MutationAuthorized {
				t.Fatal("retained resource generated mutation intent")
			}
			retained := result.RetainedNamespaces[0]
			if retained.ObservationState != test.state || !retained.BoundIncarnation.CreatedAt.Equal(active.CreatedAt) {
				t.Fatal("retained state/birth missing")
			}
			if test.code != "" && !issue(result, test.code) {
				t.Fatal("retained lifecycle issue hidden")
			}
			if len(retained.Attributes) != 1 || retained.Attributes[0].Action != RetainedUnsupported {
				t.Fatal("retained attribute history lost")
			}
			if test.state == RetainedIncarnationMismatch && (retained.ObservedVersion != "" || retained.Attributes[0].ObservedType != "") {
				t.Fatal("replacement metadata was attributed to retained owned incarnation")
			}
		})
	}
}

func TestOmittedNamespaceAttributeDriftRemainsVisibleWithoutReplacement(t *testing.T) {
	d := document(t)
	p := policy(t, d)
	obs := bind(&p, "primary")
	obs.SearchAttributes = map[string]AttributeType{"Retired": Int}
	managed := ManagedAttributes{Label: "primary", NamespaceID: obs.NamespaceID, CreatedAt: obs.CreatedAt, Owner: obs.Owner, ProvenanceID: obs.ProvenanceID, Attributes: map[string]AttributeType{"Retired": Keyword}}
	result := plan(t, planner(t, p), d, []Observation{obs}, []ManagedAttributes{managed})
	if !issue(result, "RetainedManagedSearchAttributeTypeDrift") || result.RetainedNamespaces[0].Attributes[0].ObservedType != Int || result.RetainedNamespaces[0].Attributes[0].DesiredType != Keyword || len(result.Namespaces) != 0 {
		t.Fatal("omission hid drift or generated replacement")
	}
}
