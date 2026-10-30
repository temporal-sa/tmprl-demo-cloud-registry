package project

import (
	"fmt"
	"sort"
	"strings"
)

// ValidateReferences checks local graph relationships. Account-wide references,
// capability permissions and cross-project catalog collisions require separate
// validators; passing this function never authorizes a provider operation.
func ValidateReferences(document Document) []Issue {
	var issues []Issue
	add := func(path, code, message string) {
		issues = append(issues, Issue{Pointer: path, Code: code, Message: message})
	}
	if strings.HasPrefix(document.Metadata.Name, "pv-") {
		add("/metadata/name", "ReservedIdentity", "preview project names require a trusted preview capability")
	}
	if document.Spec.Ingress != nil && strings.HasPrefix(string(document.Spec.Ingress.Subdomain), "pv-") {
		add("/spec/ingress/subdomain", "ReservedIdentity", "preview subdomains require a trusted preview capability")
	}
	for _, key := range []string{"registry.tmprl-demo.cloud/preview", "registry.tmprl-demo.cloud/preview-owner", "registry.tmprl-demo.cloud/secrets-from"} {
		if _, ok := document.Metadata.Labels[key]; ok {
			add("/metadata/labels/"+escapePointer(key), "ReservedMetadata", "metadata key is reserved for trusted platform capabilities")
		}
		if _, ok := document.Metadata.Annotations[key]; ok {
			add("/metadata/annotations/"+escapePointer(key), "ReservedMetadata", "metadata key is reserved for trusted platform capabilities")
		}
	}
	serviceNames := map[string]string{}
	for name, component := range document.Spec.Components {
		path := "/spec/components/" + escapePointer(name)
		if _, ok := document.Spec.Builds[string(component.Build)]; !ok {
			add(path+"/build", "UnknownBuild", "component references an undeclared build")
		}
		service := name
		if component.ServiceName != nil {
			service = string(*component.ServiceName)
		}
		if _, found := serviceNames[service]; found && len(component.Ports) > 0 {
			add(path+"/serviceName", "DuplicateServiceName", "components must have distinct stable service names")
		}
		if len(component.Ports) > 0 {
			serviceNames[service] = name
		}
		ports := map[DNSLabel]bool{}
		for index, port := range component.Ports {
			if port.Name == nil && len(component.Ports) > 1 {
				add(fmt.Sprintf("%s/ports/%d/name", path, index), "PortNameRequired", "multiple ports require explicit unique names")
			}
			if port.Name != nil {
				if ports[*port.Name] {
					add(fmt.Sprintf("%s/ports/%d/name", path, index), "DuplicatePortName", "port names must be unique")
				}
				ports[*port.Name] = true
			}
		}
		for index, secret := range component.Secrets {
			if _, ok := document.Spec.Secrets[string(secret)]; !ok {
				add(fmt.Sprintf("%s/secrets/%d", path, index), "UnknownSecret", "component references an undeclared project secret")
			}
		}
		if label := component.TemporalNamespace; label != nil {
			if *label == "primary" {
				add(path+"/temporalNamespace", "ReservedNamespaceLabel", "omit temporalNamespace to select the implicit primary namespace")
			} else if document.Spec.Temporal == nil {
				add(path+"/temporalNamespace", "UnknownTemporalNamespace", "component references an undeclared Temporal namespace")
			} else if _, ok := document.Spec.Temporal.Namespaces[string(*label)]; !ok {
				add(path+"/temporalNamespace", "UnknownTemporalNamespace", "component references an undeclared Temporal namespace")
			}
		}
	}
	if temporal := document.Spec.Temporal; temporal != nil {
		for label := range temporal.Namespaces {
			if label == "primary" {
				add("/spec/temporal/namespaces/primary", "ReservedNamespaceLabel", "primary is reserved for the implicit project namespace; additional namespaces need a distinct label")
			}
			if label == "candidate" || label == "ca-candidate" {
				add("/spec/temporal/namespaces/"+escapePointer(label), "ReservedNamespaceLabel", "namespace label collides with credential staging")
			}
		}
		declared := func(label string) bool {
			if label == "primary" {
				return true
			}
			_, ok := temporal.Namespaces[label]
			return ok
		}
		for name, endpoint := range temporal.NexusEndpoints {
			path := "/spec/temporal/nexusEndpoints/" + escapePointer(name)
			if endpoint.TargetNamespace != nil && !declared(string(*endpoint.TargetNamespace)) {
				add(path+"/targetNamespace", "UnknownTemporalNamespace", "endpoint target is not declared by this project")
			}
			for index, label := range endpoint.AllowedCallerNamespaces {
				if !declared(string(label)) {
					add(fmt.Sprintf("%s/allowedCallerNamespaces/%d", path, index), "UnknownTemporalNamespace", "endpoint caller is not declared by this project")
				}
			}
		}
	}
	if ingress := document.Spec.Ingress; ingress != nil {
		routes := ingress.Routes
		if len(routes) == 0 && ingress.Component != nil {
			routes = []IngressRoute{{Component: *ingress.Component}}
			if ingress.Port != nil {
				routes[0].Port = *ingress.Port
			}
		}
		for index, route := range routes {
			path := fmt.Sprintf("/spec/ingress/routes/%d", index)
			component, ok := document.Spec.Components[string(route.Component)]
			if !ok {
				add(path+"/component", "UnknownComponent", "ingress route references an undeclared component")
				continue
			}
			portName := route.Port
			if portName == "" {
				portName = "http"
			}
			found := false
			for _, port := range component.Ports {
				name := DNSLabel("")
				if port.Name != nil {
					name = *port.Name
				} else if len(component.Ports) == 1 {
					name = "http"
				}
				if name == portName {
					found = true
				}
			}
			if !found {
				add(path+"/port", "UnknownPort", "ingress route references an undeclared component port")
			}
		}
	}
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Pointer == issues[j].Pointer {
			return issues[i].Code < issues[j].Code
		}
		return issues[i].Pointer < issues[j].Pointer
	})
	return issues
}
