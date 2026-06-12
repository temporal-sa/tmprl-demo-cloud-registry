# Project Plan

Last updated: 2026-06-12

This document is the steering record for the `tmprl-demo.cloud` GitOps platform.
Keep it current as phases move from design to implementation.

## Current Status

| Phase | Status | Notes |
| --- | --- | --- |
| 1. Validation | In progress | Validator, CI workflow, simplified config schema, and taco example are scaffolded on `build-platform-generator`; needs final review before this phase is complete. |
| 2. Generator | In progress | Initial deterministic generator writes cluster manifests, unapplied Deployment templates, build plan data, and registry landing page data under `clusters/sa-demo/generated/`; image refs remain placeholders. |
| 3. Platform Infra | Not started | Needs provider verification for Temporal Cloud resources and scoped API key support. |
| 4. Flux Bootstrap | Not started | Must reconcile generated Kubernetes state from inside the cluster. |
| 5. Build Pipeline | Not started | Must run untrusted Dockerfile builds with narrow per-project credentials. |
| 6. Worker Controller | Not started | Must verify current Temporal Worker Controller CRDs before implementation. |
| 7. Shared Auth | Design | New platform capability for optionally protecting project routes with Temporal employee Google auth. |
| 8. Registry App | Design | The registry landing page is itself a project in this repo and will later include a worker for demo environment management. |
| 9. Per-Project Temporal UI | Deferred | Explore after core deploy path works; likely self-hosted Temporal UI pointed at Temporal Cloud with read-only namespace credentials. |

## Decisions

- Use Flux, not Argo.
- Project source repositories must be in the `temporal-sa` GitHub organization.
- Project owners add one reviewed project config file under `projects/`.
- Project owners specify `metadata.name` as the project identity.
- Project owners do not specify a raw Kubernetes namespace.
- Project owners define build inputs, not output image repositories or image
  tags. ECR repositories and immutable tags are platform-derived.
- Project owners do not define Temporal Cloud namespace names or Temporal env
  var names. The platform derives the namespace from `metadata.name` and injects
  standard Temporal env vars.
- Projects may define multiple builds, including separate Dockerfiles for
  separate runtime components.
- Multiple runtime components may reuse one build when that is natural, but the
  platform must not require a single image with different command overrides.
- Runtime components produce stable Kubernetes Services when ports are declared.
  The default service name is the component name; an optional `serviceName`
  override is allowed when a project needs a different DNS name.
- Every project config includes registry display metadata for the demo landing
  page. The platform derives app, Temporal Cloud, and source repository links
  where possible; owners provide human-facing title, summary, tags, SDKs, and
  optional supporting assets.
- Kubernetes namespaces are derived by the platform as
  `tmprl-cloud-demo-<metadata.name>`.
- Derived namespaces must fit Kubernetes DNS label limits. If truncation is
  needed, use deterministic hash-suffix truncation rather than plain truncation
  so two long project names cannot silently collapse to the same namespace.
- Project configs specify only a subdomain. The platform appends
  `tmprl-demo.cloud`.
- Project configs must explicitly specify the ingress target service and port.
- Project owners should not need direct Kubernetes access for routine deploys.
- Project owners should not manually manage Temporal namespace or API key
  lifecycle.
- Generated destructive external resources, especially Temporal namespaces,
  should not be deleted by ordinary config removal.
- Temporal API keys injected into project pods must be scoped only to that
  project's Temporal namespace.
- Temporal credentials are injected automatically into worker components.
  Non-worker components that need Temporal credentials declare
  `temporalAccess: true`.
- User-managed secrets are constrained to platform-derived AWS Secrets Manager
  paths: `tmprl-cloud-demo/<metadata.name>/<secret-name>`. Components receive
  them only when they explicitly reference the secret. Every JSON key in the AWS
  secret is injected as an environment variable for those components.
- Project IAM roles must be scoped to the project secret prefix only, for
  example `tmprl-cloud-demo/<metadata.name>/*`, so a compromised project cannot
  read or mutate secrets belonging to other demos or non-demo systems.
- Initial Temporal API key lifetime can be one year, but key rotation and
  reconciliation must be designed before real provider automation is enabled.
- Project ingress auth is secure by default. If the auth field is omitted, the
  platform requires shared Temporal employee auth. Public demos must explicitly
  opt out with `temporalAuthRequired: false`.
- The registry landing page is managed as a project in this repo so it uses the
  same build, deploy, auth, and Temporal credential path as other demos.

## Phase 1: Validation

Goal: make project config review safe before any generator or external mutation
exists.

Implemented scaffold:

- `scripts/validate_projects.py`
- `.github/workflows/validate-projects.yaml`
- `requirements-dev.txt`
- README validation instructions

Remaining Phase 1 work:

- Review the simplified schema and `projects/dejavu-tacos.yaml` contract.
- Decide whether Phase 1 should include optional readiness/liveness probe
  fields or leave probes for the generator phase.
- Decide whether the standard Temporal env vars are exactly
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and
  `TEMPORAL_TLS`.
- Decide whether registry readiness score is useful enough to keep in the
  schema or should remain out of the first implementation.
- Keep CI PR/push-only with no cluster or external-provider mutations.

Proposed namespace derivation:

```text
prefix = tmprl-cloud-demo-
raw = prefix + metadata.name
if len(raw) <= 63:
  namespace = raw
else:
  namespace = raw[:54] + "-" + sha256(raw)[:8]
```

The exact truncation boundary should be implemented by code, not duplicated
manually in project configs.

Target owner-facing config shape:

```yaml
metadata:
  name: dejavu-tacos

registry:
  title: Deja Vu Tacos
  summary: Interactive taco shop demo showing Temporal workflows replacing fragile checkout orchestration with durable execution, retries, and recoverable failures.
  tags:
    - Durable Execution
    - Retries
    - Sagas
    - Compensation
    - Signals
    - Queries
  temporalSdks:
    - Python
    - Go
    - TypeScript
    - Java
    - .NET

source:
  repo: https://github.com/temporal-sa/taco-app-temporal-demo
  branch: main

builds:
  frontend:
    context: .
    dockerfile: docker/frontend.Dockerfile
  backend:
    context: .
    dockerfile: docker/backend.Dockerfile
  worker-python:
    context: .
    dockerfile: docker/worker-python.Dockerfile

components:
  frontend:
    build: frontend
    ports:
      - name: http
        containerPort: 5173
        servicePort: 80

  backend:
    build: backend
    serviceName: backend
    temporalAccess: true
    ports:
      - name: http
        containerPort: 8000
        servicePort: 8000
    env:
      - name: DEJAVU_RELOAD
        value: "0"
      - name: DEJAVU_WORKER_LANGUAGE
        value: python

  worker-python:
    build: worker-python
    worker:
      taskQueue: dejavu-tacos
    env:
      - name: DEJAVU_BACKEND_URL
        value: http://backend:8000

ingress:
  subdomain: dejavu-tacos
  routes:
    - pathPrefix: /api
      component: backend
      port: http
    - component: frontend
      port: http
```

Projects that use separate images define multiple builds and point each
component at the right build:

```yaml
builds:
  web:
    context: web
    dockerfile: Dockerfile
  worker:
    context: worker
    dockerfile: Dockerfile

components:
  web:
    build: web
  worker:
    build: worker
```

Projects can also reuse one build across multiple components when that is the
real application shape. For example, `projects/temporal-agent-harness.yaml`
uses one `app` build for `web`, `api`, and `worker`, with each component
providing its own command as an inline string array. The `api` and `worker`
components reference the user-managed `anthropic-credentials` secret, which
maps to:

```text
tmprl-cloud-demo/temporal-agent-harness/anthropic-credentials
```

Generated service DNS:

- default: `<component>.<namespace>.svc.cluster.local`
- same namespace shorthand: `http://<component>`
- optional override:

```yaml
components:
  api:
    serviceName: agent-api
```

Simple ingress shorthand:

```yaml
ingress:
  subdomain: demo.testing
  temporalAuthRequired: true
  component: web
  port: http
```

## Phase 2: Generator

Goal: render reviewed project intent into deterministic Kubernetes manifests
under `clusters/sa-demo/generated/<project>/`.

Initial generated resources:

- `Namespace`
- service account and namespace-scoped RBAC skeleton
- Traefik `IngressRoute`
- Kubernetes `Service` resources for components with ports
- ExternalSecret placeholder wiring
- ConfigMap and Secret names for injected environment
- unapplied Deployment templates for each component with placeholder image refs
- generated `build-plan.json` with source repo, context, Dockerfile, derived
  image repository/tag templates, and build consumers
- registry landing page data from project configs
- Worker Controller placeholder/template resources when enabled
- Optional auth middleware references when shared auth is required

Deployment templates are generated for review and for the future build pipeline,
but they are not referenced from generated `kustomization.yaml` files while
their image refs point at `registry.invalid`.

Out of scope for this phase:

- real Terraform/OpenTofu resources
- real Temporal Cloud namespace creation
- real API key creation
- real AWS Secrets Manager writes

## Phase 3: Platform Infra

Goal: provision external assets with deletion controls and least privilege.

Assets:

- ECR repo per project
- Temporal Cloud namespace per project
- Temporal service account/API key scoped only to that namespace
- AWS Secrets Manager secret for generated Temporal API key
- IAM/IRSA or access policy so External Secrets can read only that project's
  secrets
- per-project IAM permissions limited to the exact secret prefix
  `tmprl-cloud-demo/<metadata.name>/*`

Temporal API key rotation design:

- Initial TTL: one year.
- Platform must track key expiration.
- Reconcile before expiry, for example when less than 30-60 days remain.
- Write the replacement key to AWS Secrets Manager.
- Allow External Secrets to sync the new value into Kubernetes.
- Roll dependent pods after sync.
- Revoke the old key only after an overlap window.
- Failed rotation must alert and must not delete the currently working key.

Provider verification needed:

- current Temporal Terraform/OpenTofu provider resource names
- current support for namespace-scoped service accounts or API keys
- current API key expiry/rotation semantics

## Phase 4: Flux Bootstrap

Goal: have Flux reconcile generated Kubernetes state from inside the cluster.

Requirements:

- Flux source points at this repo.
- Flux reconciles the generated cluster path.
- Tenant resources use namespace-scoped service accounts where practical.
- Kubernetes generated resources may use prune.
- External destructive resources must not be pruned by routine project config
  removal.

## Phase 5: Build Pipeline

Goal: platform-owned image builds from project source repos.

Requirements:

- Build each declared build from the project config's source repo, branch,
  context, and Dockerfile.
- Support multiple build definitions per project so different components can
  use different Dockerfiles.
- Allow multiple components to reference the same build when they intentionally
  share an image.
- Push immutable image tags to platform-derived per-project or per-build ECR
  repos.
- Update generated deploy state with the image tag.
- Let Flux roll out from inside the cluster.
- Build credentials must be limited to that project's image repository.
- Build workers must not have Temporal admin privileges or broad AWS access.

Open choice:

- CodeBuild vs GitHub Actions vs in-AWS/cluster build worker.

## Phase 6: Temporal Worker Controller

Goal: generate Worker Controller resources for project workers.

Requirements:

- Verify current Worker Controller CRD names and shapes before implementation.
- Wire image tag, task queue, Temporal namespace, and Temporal API key env.
- Support rollout policy.
- Only generate worker resources for services marked `worker: true`.
- Worker versioning is platform intent, not app-authored static env values.
  Projects may map app-specific env names for versioning fields, but the
  platform derives the actual build/version values from image/build state.

## Phase 7: Shared Auth

Goal: let project owners choose whether a demo is public or protected by shared
Temporal employee Google auth.

Owner-facing intent:

```yaml
ingress:
  temporalAuthRequired: true
```

If `temporalAuthRequired` is omitted, it defaults to `true`. Public demos must
set it to `false` explicitly.

Design direction:

- Run a shared auth service at `auth.tmprl-demo.cloud`.
- Use Google sign-in restricted to Temporal employees.
- For protected projects, generated Traefik routing must call the shared auth
  service before proxying to the project service.
- The shared auth service validates the user's session and returns a
  platform-signed JWT or identity headers to Traefik.
- Traefik forwards the JWT to the downstream app, for example in
  `X-Temporal-Auth-JWT`.
- The platform should validate all protected route calls before they reach the
  project app.
- Downstream apps do not need to implement login to be private.
- Downstream apps may optionally consume and verify the JWT for user-aware
  behavior.
- Public demos remain possible by leaving auth disabled.

JWT requirements:

- Signed by a platform-owned key, not by individual project apps.
- Includes issuer, audience, subject, email, hosted domain, issued-at, expiry,
  and the project host or project name.
- Short-lived enough that stolen forwarded headers have limited value.
- Verification material should be published internally, for example through a
  JWKS endpoint, if apps are expected to validate the JWT themselves.

Important security notes:

- Protected apps must trust identity only from Traefik-injected headers, not
  from client-supplied headers.
- The route/middleware must strip inbound identity headers before auth runs.
- Auth bypass for health checks, static assets, or internal callbacks must be
  explicit, reviewed, and generated rather than project-defined free-form rules.

## Open Questions

- Should generated files be committed to this repo, or generated in CI and
  applied by Flux from an artifact or branch?
- Should project config support multiple environments in one file, or one file
  per environment?
- Should Temporal namespace deletion require an explicit `deletionApproved`
  field or a separate removal workflow?
- What is the final naming convention for Temporal Cloud namespaces?
- How should host collision be checked against existing Traefik routes outside
  this repo?
- Which build system should run untrusted project Dockerfiles?
- Do project repos provide only Dockerfiles, or optional deployment overlays?
- How much of app deployment should be generated versus project-owned?
- What exact shared-auth field name should the project schema expose?
- Should protected routes cover every path by default, or support generated
  exceptions for health/readiness/callback paths?
- What JWT header name and claims should be contractually supported for
  downstream apps?

## Phase 8: Registry App

Goal: build the registry landing page from project configs and host it through
the same platform path as other demos.

Initial project config:

- `projects/demo-registry.yaml`
- source repo: this repo
- ingress subdomain: `registry`
- components: `web` and `worker`

Registry web responsibilities:

- render project cards from reviewed project configs;
- expose derived links for app UI, source repository, and Temporal Cloud
  namespace;
- display owner-provided registry metadata such as title, summary, tags, SDKs,
  speaker notes, and assets;
- trigger or inspect registry worker workflows if the UI needs active demo
  environment operations.

Registry worker direction:

- generalize the temporary environment management currently being explored in
  `temporal-agent-harness-example`;
- provide lifecycle workflows for published demos rather than one-off
  project-specific environment code;
- create, inspect, refresh, and clean up temporary demo environments through
  reviewed platform APIs and narrow credentials;
- avoid giving project apps broad Kubernetes, AWS, or Temporal admin access.

Open registry questions:

- Should the landing page live at `registry.tmprl-demo.cloud` first, or should
  the platform support the apex `tmprl-demo.cloud` host for this project?
- Should registry page data be generated as static JSON committed under
  `clusters/sa-demo/generated`, generated into the registry image at build time,
  or loaded dynamically from this repo?

## Phase 9: Per-Project Temporal UI

Goal: optionally provide each demo with a Temporal UI view scoped to that
project's Temporal Cloud namespace.

Status: deferred until core validation, generation, infra, Flux, build, and
worker-controller flows are working.

Design direction:

- Generate a self-hosted `temporalio/ui` deployment per project only when
  explicitly enabled by platform config.
- Point the UI at the project's Temporal Cloud endpoint and default namespace.
- Protect the route with shared Temporal employee auth.
- Use a separate read-only Namespace-scoped Temporal service account/API key for
  the UI. Do not reuse the worker/API key.
- Disable UI write actions with Temporal UI configuration as an additional
  guardrail.
- Store the UI key under the project secret prefix, for example
  `tmprl-cloud-demo/<metadata.name>/temporal-ui`.

POC questions:

- Confirm the supported way for self-hosted Temporal UI to authenticate to
  Temporal Cloud with API keys. If direct API key config is not supported, test
  whether an in-cluster proxy can inject the required `Authorization` header and
  whether `TEMPORAL_FORWARD_HEADERS` is sufficient.
- Confirm Temporal Cloud read-only namespace permissions cover the UI workflows
  list/detail/history/query use cases without allowing mutation.
- Decide whether the route should be a project path such as `/temporal` or a
  separate derived host.
- Confirm payload codec behavior if a demo uses a codec server.
- Define key rotation behavior separately from worker credentials.
