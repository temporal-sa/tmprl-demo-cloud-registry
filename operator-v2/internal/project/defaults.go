package project

import (
	"encoding/json"
	"fmt"
	"sort"
)

func pointer[T any](value T) *T { return &value }

// Normalize returns an independent effective copy. It never writes defaults
// back to a CR or treats project-supplied provider identities as trusted config.
func Normalize(document Document) (Effective, error) {
	if issues := ValidateSemantics(document); len(issues) > 0 {
		return Effective{}, &ValidationError{Issues: issues}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return Effective{}, err
	}
	var effective Document
	if err = json.Unmarshal(encoded, &effective); err != nil {
		return Effective{}, err
	}
	if effective.Spec.Source.Branch == nil {
		effective.Spec.Source.Branch = pointer("main")
	}
	for name, build := range effective.Spec.Builds {
		if build.Strategy == nil {
			build.Strategy = pointer("dockerfile")
		}
		if *build.Strategy == "dockerfile" && build.Dockerfile == nil {
			build.Dockerfile = pointer("Dockerfile")
		}
		effective.Spec.Builds[name] = build
	}
	for name, component := range effective.Spec.Components {
		if component.Replicas == nil && component.Serverless == nil && (component.Autoscaling == nil || explicitlyFalse(component.Autoscaling.Enabled)) {
			component.Replicas = pointer(1)
		}
		for index, port := range component.Ports {
			if len(component.Ports) == 1 && port.Name == nil {
				port.Name = pointer(DNSLabel("http"))
			}
			if port.ServicePort == nil {
				port.ServicePort = pointer(port.ContainerPort)
			}
			component.Ports[index] = port
		}
		effective.Spec.Components[name] = component
	}
	if smoke := effective.Spec.SmokeCheck; smoke != nil && smoke.TimeoutSeconds == nil {
		smoke.TimeoutSeconds = pointer(10)
	}
	if temporal := effective.Spec.Temporal; temporal != nil {
		if temporal.Auth == nil {
			temporal.Auth = &TemporalAuth{Mode: pointer("apiKey")}
		} else if temporal.Auth.Mode == nil {
			temporal.Auth.Mode = pointer("apiKey")
		}
		for name, endpoint := range temporal.NexusEndpoints {
			if endpoint.TargetNamespace == nil {
				endpoint.TargetNamespace = pointer(NexusEndpointNamespaceRef("primary"))
			}
			temporal.NexusEndpoints[name] = endpoint
		}
	}
	if ingress := effective.Spec.Ingress; ingress != nil && len(ingress.Routes) == 0 {
		if ingress.Component != nil {
			port := DNSLabel("http")
			if ingress.Port != nil {
				port = *ingress.Port
			}
			ingress.Routes = []IngressRoute{{Component: *ingress.Component, Port: port}}
		} else {
			var targets []string
			for name, component := range effective.Spec.Components {
				for _, port := range component.Ports {
					if port.ServicePort != nil && *port.ServicePort == 80 {
						targets = append(targets, name)
						break
					}
				}
			}
			sort.Strings(targets)
			if len(targets) != 1 {
				return Effective{}, issue("/spec/ingress/routes", "AmbiguousDefaultRoute", "ingress needs one service on port 80 or explicit routes")
			}
			component := effective.Spec.Components[targets[0]]
			for _, port := range component.Ports {
				if *port.ServicePort == 80 {
					ingress.Routes = []IngressRoute{{Component: DNSLabel(targets[0]), Port: *port.Name}}
					break
				}
			}
		}
	}
	if issues := ValidateReferences(effective); len(issues) > 0 {
		return Effective{}, &ValidationError{Issues: issues}
	}
	if effective.Kind != "DemoProject" {
		return Effective{}, fmt.Errorf("unsupported project kind")
	}
	return Effective(effective), nil
}
