package project

import "testing"

func authored(t *testing.T) Document {
	t.Helper()
	d, err := Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func changeComponent(d *Document, change func(*Component)) {
	c := d.Spec.Components["web"]
	change(&c)
	d.Spec.Components["web"] = c
}
func containsCode(issues []Issue, code string) bool {
	for _, i := range issues {
		if i.Code == code {
			return true
		}
	}
	return false
}

func TestSemanticRejectionsAcrossCapabilities(t *testing.T) {
	for _, scenario := range []struct {
		name, code string
		change     func(*Document)
	}{
		{"reserved preview subdomain", "ReservedIdentity", func(d *Document) { d.Spec.Ingress.Subdomain = "pv-private" }},
		{"unused secret", "UnusedSecret", func(d *Document) { d.Spec.Secrets = map[string]UserSecret{"token": {}} }},
		{"undeclared environment secret", "UnknownSecret", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Env = EnvList{{Name: "TOKEN", ValueFrom: &EnvValueFrom{InputRef: "secrets.token.TOKEN"}}}
			})
		}},
		{"invalid input namespace", "InvalidInputReference", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Env = EnvList{{Name: "TOKEN", ValueFrom: &EnvValueFrom{InputRef: "arbitrary.secret"}}}
			})
		}},
		{"proxy without access", "TemporalAccessRequired", func(d *Document) {
			changeComponent(d, func(c *Component) { c.TemporalProxy = &TemporalProxy{Enabled: true} })
		}},
		{"mtls direct key input", "AuthModeConflict", func(d *Document) {
			d.Spec.Temporal = &Temporal{Auth: &TemporalAuth{Mode: pointer("mtls")}}
			changeComponent(d, func(c *Component) {
				c.Env = EnvList{{Name: "TOKEN", ValueFrom: &EnvValueFrom{InputRef: "temporal.apiKey"}}}
			})
		}},
		{"mtls versioning", "AuthModeConflict", func(d *Document) {
			d.Spec.Temporal = &Temporal{Auth: &TemporalAuth{Mode: pointer("mtls")}}
			changeComponent(d, func(c *Component) { c.Worker = &Worker{Enabled: true, Versioning: &WorkerVersioning{Enabled: true}} })
		}},
		{"certificate root mount", "UnsafeCertificateMount", func(d *Document) {
			d.Spec.Temporal = &Temporal{Auth: &TemporalAuth{Mode: pointer("mtls")}}
			changeComponent(d, func(c *Component) {
				c.TemporalMtls = &TemporalMtls{CertFiles: &TemporalMtlsCertFiles{MountPath: pointer("/")}}
			})
		}},
		{"nonworker autoscaling", "WorkerRequired", func(d *Document) {
			changeComponent(d, func(c *Component) { c.Autoscaling = &Autoscaling{TaskQueue: pointer("jobs")} })
		}},
		{"fixed autoscaling replicas", "FixedReplicaConflict", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Worker = &Worker{Enabled: true}
				c.Autoscaling = &Autoscaling{TaskQueue: pointer("jobs")}
				c.Replicas = pointer(0)
			})
		}},
		{"missing task queue", "TaskQueueRequired", func(d *Document) {
			changeComponent(d, func(c *Component) { c.Worker = &Worker{Enabled: true}; c.Autoscaling = &Autoscaling{} })
		}},
		{"reversed autoscaling range", "InvalidReplicaRange", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Worker = &Worker{Enabled: true}
				c.Autoscaling = &Autoscaling{TaskQueue: pointer("jobs"), MinReplicas: pointer(4), MaxReplicas: pointer(2)}
			})
		}},
		{"Lambda explicit false field", "UnsupportedLambdaField", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Serverless = &Serverless{Provider: "aws-lambda"}
				c.Ports = nil
				c.AutomountServiceAccountToken = pointer(false)
			})
		}},
		{"Lambda empty secret list", "UnsupportedLambdaField", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Serverless = &Serverless{Provider: "aws-lambda"}
				c.Ports = nil
				c.Secrets = []DNSLabel{}
			})
		}},
		{"Lambda worker boolean false", "UnsupportedLambdaWorker", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Serverless = &Serverless{Provider: "aws-lambda"}
				c.Worker = &Worker{Enabled: false}
			})
		}},
		{"Lambda buildpack", "UnsupportedLambdaBuild", func(d *Document) {
			b := d.Spec.Builds["web"]
			b.Strategy = pointer("buildpack")
			d.Spec.Builds["web"] = b
			changeComponent(d, func(c *Component) { c.Serverless = &Serverless{Provider: "aws-lambda"} })
		}},
		{"Lambda duplicate identity env", "DuplicateWorkerIdentityEnv", func(d *Document) {
			changeComponent(d, func(c *Component) {
				c.Serverless = &Serverless{Provider: "aws-lambda", WorkerDeployment: &ServerlessWorkerDeployment{Env: &ServerlessWorkerDeploymentEnv{DeploymentName: []EnvName{"IDENTITY"}, BuildID: []EnvName{"IDENTITY"}}}}
			})
		}},
		{"unused additional namespace", "UnusedTemporalNamespace", func(d *Document) {
			d.Spec.Temporal = &Temporal{Namespaces: map[string]TemporalNamespaceSpec{"unused": {}}}
		}},
		{"Nexus missing caller", "NexusCallerRequired", func(d *Document) {
			d.Spec.Temporal = &Temporal{NexusEndpoints: map[string]NexusEndpoint{"Endpoint": {TargetTaskQueue: "jobs"}}}
		}},
		{"Nexus own project caller", "InvalidNexusCallerProject", func(d *Document) {
			d.Spec.Temporal = &Temporal{NexusEndpoints: map[string]NexusEndpoint{"Endpoint": {TargetTaskQueue: "jobs", AllowedCallerProjects: []DNSLabel{"example"}}}}
		}},
		{"public root route", "UnsafePublicCatchAll", func(d *Document) {
			d.Spec.Ingress.Routes = []IngressRoute{{Component: "web", Port: "http", PathPrefix: pointer("/"), Public: pointer(true)}}
		}},
		{"multiple catchalls", "MultipleCatchAllRoutes", func(d *Document) {
			d.Spec.Ingress.Routes = []IngressRoute{{Component: "web", Port: "http"}, {Component: "web", Port: "http"}}
		}},
		{"case duplicate tag", "DuplicateRegistryLabel", func(d *Document) { d.Spec.Registry = &Registry{Tags: RegistryLabels{"Temporal", "temporal"}} }},
		{"bad readiness score", "InvalidReadinessScore", func(d *Document) { d.Spec.Registry = &Registry{Readiness: &RegistryReadiness{Score: 11, MaxScore: 10}} }},
		{"isolated Lambda missing runtime", "LambdaRequired", func(d *Document) {
			d.Spec.Registry = &Registry{IsolatedLambda: &RegistryIsolatedLambda{Enabled: pointer(true), TaskQueueEnv: pointer("TASK_QUEUE")}}
		}},
		{"workspace mode conflict", "WorkspaceModeConflict", func(d *Document) {
			d.Spec.Registry = &Registry{IsolatedLambda: &RegistryIsolatedLambda{Enabled: pointer(true)}, Crashable: &RegistryCrashable{Enabled: pointer(true)}}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			d := authored(t)
			scenario.change(&d)
			issues := ValidateSemantics(d)
			if !containsCode(issues, scenario.code) {
				t.Fatalf("missing %s: %+v", scenario.code, issues)
			}
		})
	}
}

func TestEffectiveAutoscalerDoesNotInventFixedReplicas(t *testing.T) {
	d := authored(t)
	d.Spec.Registry = &Registry{Crashable: &RegistryCrashable{TaskQueueEnv: pointer("APP_QUEUE"), BaseTaskQueue: pointer("fallback")}}
	changeComponent(&d, func(c *Component) {
		c.Worker = &Worker{Enabled: true}
		c.Autoscaling = &Autoscaling{}
		c.Env = EnvList{{Name: "APP_QUEUE", Value: pointer("  actual-queue  ")}}
	})
	if TaskQueue(d, d.Spec.Components["web"]) != "actual-queue" {
		t.Fatal("queue resolution changed")
	}
	if issues := ValidateSemantics(d); len(issues) > 0 {
		t.Fatalf("valid autoscaling rejected: %+v", issues)
	}
	effective, err := Normalize(d)
	if err != nil {
		t.Fatal(err)
	}
	if effective.Spec.Components["web"].Replicas != nil {
		t.Fatal("defaulting invented fixed replicas for autoscaling worker")
	}
}

func TestCatalogCollisionsUseExplicitProviderClaims(t *testing.T) {
	a := authored(t)
	b := authored(t)
	b.Metadata.Name = "other"
	a.Spec.Temporal = &Temporal{NexusEndpoints: map[string]NexusEndpoint{"SharedEndpoint": {TargetTaskQueue: "a", AllowedCallerProjects: []DNSLabel{"absent"}}}}
	b.Spec.Temporal = &Temporal{NexusEndpoints: map[string]NexusEndpoint{"SharedEndpoint": {TargetTaskQueue: "b", AllowedCallerNamespaces: []NexusEndpointNamespaceRef{"primary"}}}}
	claims := []ResourceClaim{{Project: "example", Provider: "temporal", Scope: "account-test", Kind: "namespace", Name: "same-truncated-name"}, {Project: "other", Provider: "temporal", Scope: "account-test", Kind: "namespace", Name: "same-truncated-name"}}
	issues := ValidateCatalog([]Document{a, b}, claims)
	codes := map[string]bool{}
	for _, i := range issues {
		codes[i.Code] = true
	}
	for _, code := range []string{"DuplicateSubdomain", "DuplicateNexusName", "UnknownCallerProject", "DuplicateResolvedResource"} {
		if !codes[code] {
			t.Fatalf("missing collision %s: %+v", code, issues)
		}
	}
	claims[1].Scope = "different-account"
	for _, i := range ValidateCatalog([]Document{a, b}, claims) {
		if i.Code == "DuplicateResolvedResource" {
			t.Fatal("names in distinct explicit scopes collided")
		}
	}
}
