# Demo Cloud Registry

Shared onboarding configuration for projects deployed into Temporal demo
clusters through Flux.

The demo registry landing page is also tracked as a project in this repo. Its
future worker will own shared demo environment management workflows for
published demos.

Project owners add a reviewed `DemoProject` CR under `projects/` — that one file
is the only thing they commit. The platform owns the registry operator, Flux
installation, cluster add-ons, and external resource provisioning needed to turn
that CR into managed infrastructure.

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
- one or more container build contexts and Dockerfiles;
- runtime components that reference those builds;
- component ports when the platform should generate stable service DNS;
- a subdomain only — the platform always appends `tmprl-demo.cloud`;
- which non-worker components need Temporal credentials (`temporalAccess: true`).

Most else is optional and defaulted: `source.branch` (→ `main`), `ingress.routes`
(→ catch-all to the `servicePort: 80` component), `smokeCheck` (omitted → promote
on readiness), single-port names (→ `http`), and `spec.registry` catalog
metadata. Kubernetes namespaces, Temporal Cloud namespaces, Temporal API keys,
ECR repositories, image tags, and standard Temporal env vars are all derived by
the operator from `metadata.name`.

See [projects/registry.yaml](projects/registry.yaml),
[projects/README.md](projects/README.md), and
[schemas/project.schema.json](schemas/project.schema.json).

See [docs/project-plan.md](docs/project-plan.md) for the phased implementation
plan, current decisions, and open steering questions.

See [BOOTSTRAP.md](BOOTSTRAP.md) for the cluster bootstrap checklist.

## Validation

Adding a demo is a single step: drop one `DemoProject` CR under `projects/`.
There is no generation/render step and nothing else to commit — Flux delivers
the CR and the registry operator reconciles it (deriving namespaces, host, and
prefixes; provisioning Temporal Cloud; building, deploying, smoke-testing, and
promoting).

Project config validation runs in GitHub Actions on pull requests and pushes.
Run it locally with:

```sh
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_projects.py
```

The validator checks each `projects/*.yaml` CR against the project schema and
also enforces cross-file and cross-field rules: unique project names, derived
namespaces, ingress subdomains, valid build/component references, valid ingress
routes, stable service DNS, registry display metadata, `temporal-sa` source
repositories, and subdomain-only host configuration.

## How a project becomes running resources

The operator is the single source of truth — it computes everything from
`metadata.name` at reconcile time (no values are baked into the CR):

- Kubernetes `Namespace` `tmprl-dem-cld-<name>`, service accounts, and RBAC;
- ECR repositories (prefix `tmprl-dem-cld`) and immutable image tags;
- Temporal Cloud namespace `tmprl-dem-cld-<name>` (hash-truncated to 39 chars
  when needed), a namespace-scoped service account, and an API key;
- AWS Secrets Manager entries and External Secrets sync for the runtime key;
- Traefik `IngressRoute` for the requested subdomain;
- the catalog: the operator lists all `DemoProject` CRs and publishes a
  `registry-catalog` ConfigMap (the `registry.json` the web app mounts), plus a
  per-project live status in `registry-catalog-status` (`<project>.json`) with
  rollout phase, build/external-resource state, candidate readiness, smoke
  result, active version, and last error.

Flux's `registry-projects` Kustomization points at `projects/`, so the
kustomize-controller auto-includes every CR there — adding a demo is just
dropping one file.

Deletion of destructive external resources, especially Temporal namespaces, runs
through the finalizer-held teardown flow rather than accidental deletion from a
routine edit.
