package project

import (
	"fmt"
	"sort"
	"strings"
)

func validateCatalogMetadata(d Document, issues *issueSet) {
	if registry := d.Spec.Registry; registry != nil {
		for _, list := range []struct {
			name   string
			values RegistryLabels
		}{{"tags", registry.Tags}, {"temporalSdks", registry.TemporalSDKs}, {"aiSdks", registry.AISDKs}} {
			seen := map[string]bool{}
			for index, value := range list.values {
				key := strings.ToLower(string(value))
				if seen[key] {
					issues.add(fmt.Sprintf("/spec/registry/%s/%d", list.name, index), "DuplicateRegistryLabel", "registry labels must be unique ignoring case")
				}
				seen[key] = true
			}
		}
		if readiness := registry.Readiness; readiness != nil && readiness.Score > readiness.MaxScore {
			issues.add("/spec/registry/readiness/score", "InvalidReadinessScore", "readiness score cannot exceed maximum score")
		}
		if assets := registry.Assets; assets != nil {
			for _, group := range []struct {
				name   string
				values RegistryAssetList
			}{{"technical", assets.Technical}, {"nonTechnical", assets.NonTechnical}} {
				seen := map[string]bool{}
				for index, asset := range group.values {
					key := strings.ToLower(string(asset.Label))
					if seen[key] {
						issues.add(fmt.Sprintf("/spec/registry/assets/%s/%d/label", group.name, index), "DuplicateAssetLabel", "asset labels must be unique ignoring case")
					}
					seen[key] = true
				}
			}
		}
	}
	if ingress := d.Spec.Ingress; ingress != nil {
		catchAll := 0
		prefixes := map[string]bool{}
		for index, route := range ingress.Routes {
			base := fmt.Sprintf("/spec/ingress/routes/%d", index)
			if route.PathPrefix == nil {
				catchAll++
			} else {
				if prefixes[*route.PathPrefix] {
					issues.add(base+"/pathPrefix", "DuplicateRoutePrefix", "ingress route prefixes must be unique")
				}
				prefixes[*route.PathPrefix] = true
			}
			if truth(route.Public) && strings.Trim(text(route.PathPrefix, ""), "/") == "" {
				issues.add(base+"/public", "UnsafePublicCatchAll", "public routes require a non-root path prefix")
			}
		}
		if catchAll > 1 {
			issues.add("/spec/ingress/routes", "MultipleCatchAllRoutes", "ingress can contain only one catch-all route")
		}
	}
}

// ResourceClaim is a resolved, trusted naming claim used by catalog/migration
// validation. No legacy production naming is silently selected by this package.
type ResourceClaim struct {
	Project  string
	Provider string
	Scope    string
	Kind     string
	Name     string
}

type CatalogIssue struct {
	Project string `json:"project"`
	Issue
}

// ValidateCatalog detects cross-project collisions. Mutation admission must also
// supply all resolved naming claims from its trusted installation/ownership
// context; an authoring-only check with no claims does not prove name isolation.
func ValidateCatalog(documents []Document, claims []ResourceClaim) []CatalogIssue {
	var output []CatalogIssue
	names := map[string]bool{}
	for _, document := range documents {
		names[document.Metadata.Name] = true
	}
	seen := map[string]string{}
	claim := func(project, key, pointer, code string) {
		if previous, exists := seen[key]; exists {
			output = append(output, CatalogIssue{project, Issue{Pointer: pointer, Code: code, Message: "identity is claimed more than once in this catalog"}})
			output = append(output, CatalogIssue{previous, Issue{Pointer: pointer, Code: code, Message: "identity is claimed more than once in this catalog"}})
		} else {
			seen[key] = project
		}
	}
	for _, d := range documents {
		claim(d.Metadata.Name, "project/"+d.Metadata.Name, "/metadata/name", "DuplicateProjectName")
		if d.Spec.Ingress != nil {
			claim(d.Metadata.Name, "ingress/"+string(d.Spec.Ingress.Subdomain), "/spec/ingress/subdomain", "DuplicateSubdomain")
		}
		if temporal := d.Spec.Temporal; temporal != nil {
			for endpointName, endpoint := range temporal.NexusEndpoints {
				base := "/spec/temporal/nexusEndpoints/" + escapePointer(endpointName)
				claim(d.Metadata.Name, "nexus/"+endpointName, base, "DuplicateNexusName")
				for index, caller := range endpoint.AllowedCallerProjects {
					if !names[string(caller)] {
						output = append(output, CatalogIssue{d.Metadata.Name, Issue{Pointer: fmt.Sprintf("%s/allowedCallerProjects/%d", base, index), Code: "UnknownCallerProject", Message: "Nexus caller project is absent from this catalog"}})
					}
				}
			}
		}
	}
	for _, resource := range claims {
		if !names[resource.Project] || resource.Provider == "" || resource.Scope == "" || resource.Kind == "" || resource.Name == "" {
			output = append(output, CatalogIssue{resource.Project, Issue{Pointer: "/metadata/name", Code: "InvalidResourceClaim", Message: "resolved claim requires a known project and complete provider identity"}})
			continue
		}
		// Length-delimited keys avoid ambiguity in opaque provider scope/name IDs.
		key := fmt.Sprintf("resource/%q/%q/%q/%q", resource.Provider, resource.Scope, resource.Kind, resource.Name)
		claim(resource.Project, key, "/metadata/name", "DuplicateResolvedResource")
	}
	sort.Slice(output, func(i, j int) bool {
		if output[i].Project == output[j].Project {
			if output[i].Pointer == output[j].Pointer {
				return output[i].Code < output[j].Code
			}
			return output[i].Pointer < output[j].Pointer
		}
		return output[i].Project < output[j].Project
	})
	return output
}
