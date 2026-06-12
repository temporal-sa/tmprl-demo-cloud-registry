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

Temporal Cloud namespaces and API keys are platform-derived from project
identity. Project owners do not provide Temporal namespace names or injected env
var names in their config. Worker components receive Temporal credentials
automatically; non-worker components opt in with `temporalAccess: true`.

User-managed secrets are constrained to platform-derived AWS Secrets Manager
paths. A project may declare a secret name such as `llm`, and the platform maps
it to `tmprl-cloud-demo/<project-name>/llm`. Components must explicitly list
which user secrets they receive. Every JSON key in that AWS secret is injected
as an environment variable for those components.

Per-project IAM roles should allow access only to that project's secret prefix,
for example `tmprl-cloud-demo/<project-name>/*`. This makes AWS IAM a second
enforcement layer if a project or ExternalSecret is misconfigured.

## Hostnames

Project configs specify only `spec.ingress.subdomain`.

The platform always appends:

```text
tmprl-demo.cloud
```

For example, `dejavu-tacos` becomes:

```text
dejavu-tacos.tmprl-demo.cloud
```

## Builds, Components, and Routes

Projects may declare multiple builds and runtime components. A component points
at one build, and components may either use separate Dockerfiles or share one
image when that is natural for the project.

Components that declare ports get generated Kubernetes Services and stable DNS.
Ingress routes must explicitly target a component and named port for each path.
This prevents accidental routing to a worker-only pod when a project has both UI
and worker components.

## Registry Landing Page

Project configs include a `registry` section for the public/internal demo
catalog. The platform should regenerate landing page data whenever project
configs are added or updated.

The landing page can derive operational links from other project fields:

- App UI from `ingress.subdomain`;
- source repository from `source.repo`;
- Temporal Cloud namespace from the platform-derived namespace name and
  platform account settings.

Project owners provide the human-facing catalog content: title, summary, tags,
Temporal SDKs, optional speaker notes, and supporting technical or
non-technical assets.

The registry landing page is itself a managed project in this repo. This keeps
the registry UI and its future environment-management worker on the same build,
deploy, auth, and Temporal credential path as other demos.
