# Architecture

This repository is the platform-owned configuration source for project
onboarding.

## Flow

1. A project owner opens a PR adding `projects/<name>.yaml`.
2. CI validates the config against `schemas/project.schema.json`.
3. Platform automation renders managed assets.
4. Terraform or provider-specific automation creates external resources:
   ECR, Temporal Cloud namespace/API key, and AWS Secrets Manager values.
5. Flux reconciles Kubernetes resources from inside the cluster.

## Boundaries

Flux owns Kubernetes desired state. It should not need direct access to Temporal
Cloud admin credentials.

External control-plane assets are managed by a platform provisioner. Sensitive
generated values, especially Temporal API keys, are stored in AWS Secrets
Manager and projected into Kubernetes through External Secrets.

Project repositories provide source code and container build instructions. Build
workers should run with narrowly scoped credentials that can push only that
project's image repository.

## Hostnames

Project configs specify only `spec.ingress.subdomain`.

The platform always appends:

```text
tmprl-demo.cloud
```

For example, `agent-harness-demo.testing` becomes:

```text
agent-harness-demo.testing.tmprl-demo.cloud
```

## Ingress Target

Projects may declare multiple runtime services. `spec.ingress.target` is always
required so the platform knows which service and port should receive external
traffic.

This prevents accidental routing to a worker-only pod when a project has both UI
and worker services.
