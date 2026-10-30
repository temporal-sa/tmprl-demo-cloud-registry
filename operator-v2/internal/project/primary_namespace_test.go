package project

import "testing"

func TestPrimaryNamespaceReferencesKeepTheirDeclaredContext(t *testing.T) {
	d := authored(t)
	changeComponent(&d, func(c *Component) { c.TemporalAccess = pointer(true) })
	if _, err := Prepare(d); err != nil {
		t.Fatal("implicit component primary rejected", err)
	}
	d.Spec.Temporal = &Temporal{NexusEndpoints: map[string]NexusEndpoint{
		"Endpoint": {TargetNamespace: pointer(NexusEndpointNamespaceRef("primary")), TargetTaskQueue: "jobs", AllowedCallerNamespaces: []NexusEndpointNamespaceRef{"primary"}},
	}}
	if _, err := Prepare(d); err != nil {
		t.Fatal("explicit Nexus implicit-primary target/caller rejected", err)
	}
	// Component selection remains an additional-namespace reference; it is not
	// silently reinterpreted or normalized into the implicit namespace.
	changeComponent(&d, func(c *Component) { c.TemporalNamespace = pointer(TemporalNamespaceLabel("primary")) })
	issues := ValidateSemantics(d)
	found := false
	for _, issue := range issues {
		found = found || issue.Code == "ReservedNamespaceLabel" && issue.Pointer == "/spec/components/web/temporalNamespace"
	}
	if !found {
		t.Fatal("component primary selector not rejected with its exact location", issues)
	}
	if _, err := Prepare(d); err == nil {
		t.Fatal("ambiguous component authoring crossed public Prepare")
	}
	changeComponent(&d, func(c *Component) { c.TemporalNamespace = nil })
	d.Spec.Temporal.Namespaces = map[string]TemporalNamespaceSpec{"primary": {}}
	if !containsCode(ValidateSemantics(d), "ReservedNamespaceLabel") {
		t.Fatal("additional primary declaration became a new namespace")
	}
	if _, err := Prepare(d); err == nil {
		t.Fatal("additional primary crossed public Prepare")
	}
	delete(d.Spec.Temporal.Namespaces, "primary")
	endpoint := d.Spec.Temporal.NexusEndpoints["Endpoint"]
	endpoint.TargetNamespace = pointer(NexusEndpointNamespaceRef("missing"))
	d.Spec.Temporal.NexusEndpoints["Endpoint"] = endpoint
	if !containsCode(ValidateSemantics(d), "UnknownTemporalNamespace") {
		t.Fatal("ordinary undeclared Nexus references lost validation")
	}
}
