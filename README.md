# Demo Cloud Registry

Shared onboarding configuration for projects deployed into Temporal demo
clusters through Flux.

The demo registry landing page and operator worker are also represented as a
project in this repo, but that project is a control-plane/self-management path,
not the ordinary template for adding demos.

Project owners add a reviewed `DemoProject` CR under `projects/demo/` or an
`InstruqtCourse` CR under `projects/instruqt/` — that one file is the only thing
they commit. The platform owns the registry operator, Flux installation,
cluster add-ons, and external resource provisioning needed to turn that CR into
managed infrastructure.

## Goals

- Project owners do not need direct Kubernetes API access for routine deploys.
- Projects provide a containerized source repository and deployment intent.
- The platform creates and manages the Kubernetes namespace, Flux wiring,
  Traefik route, Temporal Cloud namespace/API key, ECR repository, and secret
  injection.
- Flux applies Kubernetes desired state from inside the cluster network.

## Project Contract

Each `DemoProject` CR specifies only:

- a source repository in the `temporal-sa` GitHub organization;
- one or more container build contexts using Dockerfiles or explicit buildpack
  builds;
- runtime components that reference those builds;
- component ports when the platform should generate stable service DNS;
- a subdomain only — the platform always appends `tmprl-demo.cloud`;
- which non-worker components need Temporal credentials (`temporalAccess: true`).
- which Temporal-using components need the local Temporal Cloud proxy sidecar
  (`temporalProxy.enabled: true`) for demos that cannot read Cloud credentials
  directly.

Most else is optional and defaulted: `source.branch` (→ `main`), `ingress.routes`
(→ catch-all to the `servicePort: 80` component), route Host-header passthrough
(→ `ingress.routes[].passHostHeader: true`), `smokeCheck` (omitted → promote on
readiness), single-port names (→ `http`), and `spec.registry` catalog metadata.
Kubernetes namespaces, Temporal Cloud namespaces, Temporal API keys, ECR
repositories, image tags, and standard Temporal env vars are all derived by the
operator from `metadata.name`.

## Supported Onboarding Path

The current supported path for new demos is the thin `DemoProject` shape proven
by `projects/demo/banking-money-transfer.yaml`: a `temporal-sa` source repo,
Dockerfile-backed builds by default, optional explicit buildpack-backed builds
for repos without Dockerfiles, stateless long-running components,
platform-managed Temporal credentials, and ingress through the platform route.
Buildpack-backed demos can declare `builds.<name>.processes`; the operator
appends a generated process buildpack that contributes those entries to the CNB
launch metadata, and components launch a process with
`command: ["<process-name>"]`.

Demos that are written for a local Temporal dev server can opt into the
platform Temporal Cloud proxy sidecar instead of changing application code, as
long as the app can be pointed at a Temporal host through environment variables.
For example, code that connects with `TEMPORAL_HOST` and defaults
`TEMPORAL_NAMESPACE` to `default` can use `temporalProxy.enabled: true`; the app
container talks to `127.0.0.1:7233`, while the sidecar handles TLS, API-key auth,
and namespace rewriting to the project Temporal Cloud namespace. If the app
renders a Temporal UI link, set `TEMPORAL_UI_URL` explicitly from the known
Cloud namespace URL.

Supported additions use the shared temporal-sa read-only GitHub PAT at
`tmprl-dem-cld/platform/github-temporal-sa-readonly` for HTTPS source watches
and CodeBuild checkouts. Per-project source auth through
`source.gitRepository.*` remains available for explicit overrides such as SSH
deploy keys. Component secrets are also supported as project-scoped AWS Secrets
Manager JSON values; the values are created manually, and the operator verifies
declared `requiredProperties` when present.

The registry also has an initial `InstruqtCourse` resource type for
operator-owned courses. `spec.source` is the track config source, optional
`spec.content` can point at a separate exercise-code repo, and `spec.images`
declares container image bindings that should resolve to operator-built ECR
images. `spec.source.testingBranch` optionally enables a separate testing track
from a non-production branch; deleting that branch tells the operator to tear
down the testing side while `source.branch` continues to default to `main` for
production. The workflow reconciles ECR repositories for managed images, builds
those images with AWS CodeBuild, renders a local track artifact, injects or
overwrites matching `containers[].image` values, runs `instruqt track validate`,
then `track push --force`, `track test`, and `track deploy`. VM/lifecycle-only
configs are left alone. The worker image includes the Instruqt CLI and reports
CLI/token readiness from `tmprl-dem-cld/platform/instruqt-cli`. If the testing
branch is absent, the workflow deletes the managed testing track.

Worker Versioning is opt-in and advanced. Use `worker: true` for normal workers
unless the app already runs with SDK Worker Versioning and can satisfy Worker
Controller rollout expectations.

Project-owned Kubernetes, Helm, Flux overlays, and BYO image rollouts are not
the current onboarding path. Adapt those demos into platform-managed builds and
`components` first. `projects/demo/registry.yaml` is a special control-plane
project and should not be copied as the ordinary demo template.

See [projects/demo/registry.yaml](projects/demo/registry.yaml),
[projects/README.md](projects/README.md), and
[schemas/project.schema.json](schemas/project.schema.json).

See [docs/architecture.md](docs/architecture.md) for what the platform is, how
it works today, and the design intent for what is not built yet, and
[docs/operations.md](docs/operations.md) for the runbook and operational
gotchas.

See [docs/project-plan.md](docs/project-plan.md) for phase status, and
[BOOTSTRAP.md](BOOTSTRAP.md) for the cluster bootstrap checklist.

## Validation

Adding a demo or managed Instruqt course is a single step: drop one CR under
`projects/demo/` or `projects/instruqt/`. There is no generation/render step and
nothing else to commit — Flux delivers the CR and the registry operator
reconciles it.

Project config validation runs in GitHub Actions on pull requests and pushes.
Run it locally with:

```sh
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_projects.py
```

If the ambient Python environment is missing dependencies, run the validator in
an isolated environment:

```sh
uv run --isolated --with jsonschema --with pyyaml python scripts/validate_projects.py
```

The validator checks each `projects/**/*.yaml` CR against the matching
`DemoProject` or `InstruqtCourse` schema and also enforces cross-file and
cross-field rules: unique resource names, derived namespaces, ingress
subdomains, valid build/component references, valid ingress routes, stable
service DNS, registry display metadata, `temporal-sa` source repositories, and
subdomain-only ingress configuration.

## How a project becomes running resources

The operator is the single source of truth — it computes everything from
`metadata.name` at reconcile time (no values are baked into the CR):

- Kubernetes `Namespace` `tmprl-dem-cld-<name>`, service accounts, and RBAC;
- ECR repositories (prefix `tmprl-dem-cld`) and immutable image tags;
- Temporal Cloud namespace `tmprl-dem-cld-<name>` (hash-truncated to 39 chars
  when needed), a namespace-scoped service account, and a 30-day API key that is
  rotated 10 days before expiry;
- AWS Secrets Manager entries and External Secrets sync for the runtime key;
- Traefik `IngressRoute` for the requested subdomain;
- the catalog: the operator lists all `DemoProject` CRs and publishes a
  `registry-catalog` ConfigMap (the `registry.json` the web app mounts), plus a
  per-project live status in `registry-catalog-status` (`<project>.json`) with
  rollout phase, build/external-resource state, candidate readiness, smoke
  result, active version, and last error.

Flux's `registry-projects` Kustomization points at `projects/demo/`, and
`registry-instruqt-courses` points at `projects/instruqt/`. The
kustomize-controller auto-includes every CR in those directories — adding a demo
or course is just dropping one file.

Deletion of destructive external resources, especially Temporal namespaces, runs
through the finalizer-held teardown flow rather than accidental deletion from a
routine edit.
