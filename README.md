# Demo Cloud Registry

Shared onboarding configuration for projects deployed into Temporal demo
clusters through Flux.

The demo registry landing page is also tracked as a project in this repo. Its
future worker will own shared demo environment management workflows for
published demos.

Project owners add a reviewed file under `projects/`. The platform owns the
generators, Flux installation, cluster add-ons, and external resource
provisioning needed to turn that project intent into managed infrastructure.

## Goals

- Project owners do not need direct Kubernetes API access for routine deploys.
- Projects provide a containerized source repository and deployment intent.
- The platform creates and manages the Kubernetes namespace, Flux wiring,
  Traefik route, Temporal Cloud namespace/API key, ECR repository, and secret
  injection.
- Flux applies Kubernetes desired state from inside the cluster network.

## Project Contract

Each project config must specify:

- a source repository in the `temporal-sa` GitHub organization;
- one or more container build contexts and Dockerfiles;
- runtime components that reference those builds;
- component ports when the platform should generate stable service DNS;
- ingress routes that explicitly target a component and named port;
- registry display metadata for the landing page;
- a subdomain only. The platform always appends `tmprl-demo.cloud`;
- which non-worker components need Temporal credentials.

Kubernetes namespaces, Temporal Cloud namespaces, Temporal API keys, ECR
repositories, image tags, and standard Temporal env vars are derived by the
platform.

See [projects/dejavu-tacos.yaml](projects/dejavu-tacos.yaml),
[projects/temporal-agent-harness.yaml](projects/temporal-agent-harness.yaml),
and [schemas/project.schema.json](schemas/project.schema.json).

See [docs/project-plan.md](docs/project-plan.md) for the phased implementation
plan, current decisions, and open steering questions.

See [BOOTSTRAP.md](BOOTSTRAP.md) for the manual setup checklist and first
`dejavu-tacos` deployment test path.

## Validation

Project config validation runs in GitHub Actions on pull requests and pushes.
To run the same validation and generation checks locally:

```sh
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_projects.py
python3 scripts/generate_projects.py --check
```

The validator checks each `projects/*.yaml` file against the project schema and
also enforces cross-file and cross-field rules, including unique project names,
derived namespaces, ingress subdomains, valid build/component references, valid
ingress routes, stable service DNS, registry display metadata, `temporal-sa`
source repositories, and subdomain-only host configuration.

## Generation

Generated cluster output lives under `clusters/sa-demo/generated/` and is
committed for review. Regenerate it after changing project configs:

```sh
python3 scripts/generate_projects.py
```

The generator writes the applied `DemoProject` CR set, Kubernetes
namespace/RBAC skeletons, Services, Traefik routes, ExternalSecret placeholders,
worker-controller placeholders, project metadata, registry landing page data,
Deployment templates, and deterministic build and external resources plans.

Generated Deployment templates use `registry.invalid` placeholders and are not
referenced from generated `kustomization.yaml` files until the platform build
pipeline can replace them with immutable ECR image tags.

The generated `build-plan.json` lists source repos, build contexts,
Dockerfiles, derived ECR repository names, tag templates, and the components
that consume each build. It is review data for now; it does not trigger image
builds.

The generated `codebuild-setup-plan.json` turns the same build declarations
into a manual AWS CodeBuild bootstrap plan: shared project settings, required
operator permissions, CodeBuild service-role permissions, source repositories,
and exact ECR repository ARN templates.

The generated `external-resources-plan.json` lists the ECR repositories,
Temporal Cloud namespace/API key intent, AWS Secrets Manager paths, and scoped
IAM access that future platform infra automation should provision.

Generated `registry.json` also includes a `catalogStatus` pointer per project.
The operator writes each live lifecycle summary to
`tmprl-dem-cld-registry-operator/registry-catalog-status` under `<project>.json` so a
registry UI can show rollout phase, build state, external-resource state,
candidate readiness, smoke-check result, active version, and last error.

Generated `demoprojects.yaml` is referenced from the generated
`kustomization.yaml` and is the desired state consumed by the registry
operator after bootstrap. Per-project `demoproject-template.yaml` files remain
review artifacts. The older generated Kubernetes manifests are not referenced
from the applied top-level kustomization; the operator reconciles those
resources from `DemoProject` status and spec.

## Generated Assets

The platform provisioner is expected to generate or maintain:

- Kubernetes `Namespace`;
- namespace-scoped Flux `Kustomization` resources;
- namespace-scoped service accounts and RBAC;
- ECR repository and build/push workflow wiring;
- Temporal Cloud namespace, service account, and API key;
- AWS Secrets Manager entries for generated sensitive values;
- External Secrets resources for Kubernetes secret sync;
- Traefik `IngressRoute` for the requested subdomain;
- registry landing page entries and derived links;
- Temporal worker controller resources when workers are declared.

Deletion of destructive external resources, especially Temporal namespaces,
should require an explicit reviewed removal flow rather than accidental deletion
from a routine edit.
