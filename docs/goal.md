# Goal-Mode Prompt: Finish End-to-End Registry Rollouts

Use this file as the prompt for a fresh goal-mode agent working in
`/Users/mjones/Source/cluster-gitops-config`.

## Goal

Create and pursue this goal:

```text
Bring tmprl-demo-cloud-demo-registry from the current manually bootstrapped
Deja Vu success to a true end-to-end registry rollout path: the Temporal-backed
registry operator should create or verify external resources, build immutable
images, create a previously absent Kubernetes namespace, deploy a candidate,
run smoke checks, promote Traefik routing only after success, and surface the
whole lifecycle on DemoProject status and the registry/catalog experience.
```

Work over multiple iterations. Continue until the repo contains a coherent,
validated implementation path for testing deployments, or until a real external
blocker requires user action. Commit coherent checkpoints with neutral commit
messages and no mention of the agent/tool that made them.

## Repo Context

- Repo path: `/Users/mjones/Source/cluster-gitops-config`
- GitHub repo: `temporal-sa/tmprl-demo-cloud-demo-registry`
- Branch: `build-platform-generator`
- Purpose: platform/registry for demos hosted under `tmprl-demo.cloud`
- Current state:
  - Flux is installed on the `sa-demo` EKS cluster and reconciles this branch
    from GitHub over SSH using a read-only deploy key.
  - External Secrets Operator is installed and
    `ClusterSecretStore/platform-secrets` is Ready.
  - The registry operator is running in `tmprl-dem-cld-registry-operator` as a Python
    Temporal worker on namespace `tmprl-demo-cloud-registry.a2dd6`.
  - Generated `DemoProject` resources are applied by Flux.
  - `DemoProject/dejavu-tacos` is `Promoted` and
    `https://dejavu-tacos.tmprl-demo.cloud/` plus
    `https://dejavu-tacos.tmprl-demo.cloud/api/health` return HTTP 200.
  - Deja Vu used manual image overrides and manually prepared Temporal/AWS
    secrets. The operator reconciled runtime Kubernetes resources and promoted
    routing after smoke check, but it did not yet build images or create
    external resources itself.
  - Deja Vu did not prove namespace creation from zero during the Flux/operator
    run because `tmprl-dem-cld-dejavu-tacos` already existed from an earlier
    manual registry test. The old manual namespace `temporal-dejavu-tacos` has
    been deleted.
  - `DemoProject/demo-registry` and `DemoProject/temporal-agent-harness` are
    `WaitingForImages`; only base namespace/RBAC/metadata resources exist for
    those projects.

Read these files first:

- `docs/project-plan.md`
- `README.md`
- `projects/README.md`
- `projects/registry.yaml`
- `projects/dejavu-tacos.yaml`
- `projects/temporal-agent-harness.yaml`
- `scripts/validate_projects.py`
- `scripts/generate_projects.py`
- `registry/worker/src/demo_registry_operator/workflows/demo_project.py`
- `registry/worker/src/demo_registry_operator/activities/reconcile.py`

Run the existing checks before and after implementation slices:

```sh
/tmp/cluster-gitops-config-venv/bin/python scripts/validate_projects.py
/tmp/cluster-gitops-config-venv/bin/python scripts/generate_projects.py --check
/tmp/cluster-gitops-config-venv/bin/python -m py_compile scripts/validate_projects.py scripts/generate_projects.py $(find registry/worker/src -name '*.py' | sort)
```

Remove Python cache directories before commits:

```sh
find scripts registry -type d -name __pycache__ -prune -exec rm -rf {} +
```

## Architecture Direction

The preferred direction is a Temporal-backed registry operator:

- Flux bootstraps the minimal registry/operator control plane.
- Flux applies reviewed `DemoProject` custom resources.
- The registry worker watches `DemoProject` resources.
- Each demo maps to one durable entity workflow keyed by project name.
- The workflow reconciles Kubernetes, AWS, Temporal Cloud, image builds,
  secrets, Traefik routing, Worker Controller resources, registry status, and
  ephemeral environments.
- The registry is itself a demo after bootstrap. Bootstrap is the only special
  case.

Do not pivot back to arbitrary project-owned Kubernetes overlays as the default
path. A future BYO-image mode can be discussed, but arbitrary Flux/Kustomize
overlays should not be part of the first testable deployment path.

## Desired End State

The next loop should leave the user able to test full registry-owned rollouts,
not just a manual-image substitute. Required end state:

- Keep the existing bootstrap path working:
  - Flux, External Secrets, the `DemoProject` CRD, generated CRs, and the
    registry operator should remain healthy on `sa-demo`.
- Prove namespace creation from zero:
  - pick a demo or test project whose derived namespace does not already exist;
  - let the workflow create/update that namespace and record it in status.
- Replace manual image overrides with a real build/publish step:
  - create or verify ECR repositories;
  - build from the configured source repo/context/Dockerfile;
  - push immutable tags;
  - write resolved images into candidate rollout status.
- Move external-resource setup into the operator where safe:
  - create or verify AWS Secrets Manager paths;
  - create or verify Temporal Cloud namespace and namespace-scoped API key;
  - sync runtime Kubernetes secrets through External Secrets;
  - do not implement destructive deletion until explicit approval workflow
    exists.
- Implement a real candidate/promotion lifecycle:
  - deploy candidate resources without breaking the previous healthy route;
  - wait for readiness;
  - run owner-defined smoke checks;
  - promote Traefik routing only after success;
  - record failure without deleting the active healthy deployment.
- Make status useful:
  - `DemoProject.status` should show external resource state, build state,
    candidate image tags, smoke-check result, active version, and last error.
- Update the registry/catalog path:
  - choose whether to finish Agent Harness or the registry/catalog UI as the
    second end-to-end project;
  - if Agent Harness is chosen, provision its `app` image and required
    `anthropic-credentials` secret path.
- Keep docs current:
  - `docs/project-plan.md` and `BOOTSTRAP.md` must reflect which steps are
    automated and which remain manual.

## Manual Setup Instructions To Produce

Produce clear instructions for the user. Do not assume they know which manual
steps are still required. Include exact secret names/paths and placeholders.

At minimum, document:

### Temporal Cloud Bootstrap Credential

The registry/operator needs a high-privilege bootstrap Temporal Cloud credential
only for platform reconciliation. This is the "master" setup credential the
registry uses to create/manage demo namespaces and project-scoped API keys. The
user must create it manually.

Instructions must say:

- Create a Temporal Cloud service account or equivalent machine identity for the
  registry/operator.
- Grant it only the permissions required to create/manage demo namespaces,
  namespace-scoped service accounts/API keys, and key rotation.
- Verify current Temporal Cloud API/provider resource names and permission
  names before implementation.
- Create a long-lived bootstrap API key only if that is the current supported
  model. Prefer the narrowest scoped credential that can still create demo
  namespaces and project API keys.
- Store the credential outside git, for example:

```text
AWS Secrets Manager:
tmprl-dem-cld/platform/temporal-cloud-bootstrap
```

Suggested JSON shape:

```json
{
  "TEMPORAL_CLOUD_API_KEY": "...",
  "TEMPORAL_CLOUD_ACCOUNT_ID": "...",
  "TEMPORAL_CLOUD_REGION": "..."
}
```

If the implementation uses a different shape, update the docs and generated
plans to match.

### AWS Bootstrap

Document the AWS region/account assumptions and required setup:

- EKS cluster name and context.
- OIDC provider/IRSA enabled for the cluster.
- IAM role for registry/operator with narrow permissions for:
  - ECR repo create/read/write for `tmprl-dem-cld/*`;
  - Secrets Manager read/write only under `tmprl-dem-cld/*`;
  - reading the platform bootstrap secret path;
  - whatever Kubernetes access is required via service account/RBAC.
- External Secrets Operator installed or bootstrap instructions to install it.
- Traefik installed with CRDs available.
- DNS/cert setup for `tmprl-demo.cloud` and subdomains.

### GitHub / Build Access

Document how builds read source repos:

- Project source repos must be in `temporal-sa`.
- Build system needs read access to source repos.
- Build credentials must only push to the project/build ECR repository.
- Do not give build workers Temporal admin or broad AWS permissions.

### Shared Auth

Document what is required for shared auth, even if implementation remains
placeholder:

- Google OAuth client for `auth.tmprl-demo.cloud`.
- Temporal employee domain restriction.
- Secrets path for auth signing keys/client secrets.
- Traefik middleware/header-stripping requirements.

## Implementation Iterations

Keep iterations small and commit after each coherent slice.

### Iteration 1: Lock The Operator Contract

- Add a CRD definition for `DemoProject`.
- Generate applied or unapplied CRs intentionally. If the CRD/operator is not
  bootstrapped yet, keep generated CRs unapplied and document why.
- Add schema/status fields needed for:
  - desired spec;
  - observed generation;
  - reconciliation phase;
  - last error;
  - active image version;
  - candidate rollout status;
  - smoke check status.

### Iteration 2: Registry Worker Watch/Reconcile Loop

- Keep workflows in Python.
- Add a Kubernetes watch/activity layer that can read `DemoProject` resources.
- Start/signal `DemoProjectWorkflow` by project name.
- Update status back to Kubernetes or a generated/status artifact.
- Keep activities idempotent.
- Do not put network/API calls inside workflow code.

### Iteration 3: Minimal Mutating Reconcile

Implement a narrow first reconciliation path:

- namespace creation/update;
- service accounts/RBAC;
- platform metadata ConfigMap;
- status update.

This should be enough to prove the operator can reconcile a `DemoProject`.

### Iteration 4: External Resources

Implement or scaffold the actual external reconciliation path:

- ECR repositories;
- AWS Secrets Manager secret paths;
- Temporal Cloud namespace;
- project-scoped Temporal API key;
- key rotation status fields.

If provider/API details are uncertain, create verified docs and interfaces
first. Do not guess destructive Temporal Cloud behavior.

### Iteration 5: Build And Candidate Rollout

Implement the first build/deploy path:

- build one demo image from source repo/context/Dockerfile;
- push immutable ECR tag;
- create candidate Deployment/Service resources;
- keep public Traefik route pointed at previous healthy version;
- support separate images and shared-image/multiple-component projects.

Prefer an AWS-side build runner with narrow credentials. If CodeBuild is used,
document project/role creation and required repo access.

### Iteration 6: Smoke-Test-Gated Promotion

Extend project config/schema with a simple owner-defined smoke check. Keep it
small, for example:

```yaml
smokeCheck:
  path: /health
  expectedStatus: 200
```

Workflow behavior:

- wait for candidate readiness;
- run smoke check against candidate route/service;
- promote Traefik route only on success;
- keep previous healthy route active on failure;
- write rollout status.

### Iteration 7: Ephemeral Demo Environments

Do not prioritize this before core deployment testing, but leave clean extension
points for:

- temporary namespace;
- local Temporal dev server pod using Temporal CLI/server-dev mode;
- app components pointed at local dev server;
- protected temporary routes;
- TTL cleanup workflow.

### Iteration 8: Companion SA Onboarding Skill

Build a companion Codex skill for SAs who want to migrate demos onto this
platform. The skill should act like a wizard.

Skill requirements:

- First assess the target demo repo.
- Check whether the registry already contains a matching or similar demo.
- Identify app shape:
  - services/components;
  - workers;
  - Temporal SDK/language;
  - Dockerfiles or missing Dockerfiles;
  - existing Docker Compose/Flux/Kustomize/Kubernetes manifests;
  - ingress/web entrypoint;
  - health endpoints;
  - environment variables;
  - custom secrets;
  - local-only assumptions.
- Estimate migration lift and explain why.
- Discuss required code changes with the SA before editing.
- Produce or update Dockerfiles when missing.
- Update worker/app env handling so Temporal connection values and secrets come
  from environment variables.
- Create the project config file for this registry.
- If custom secrets are needed, generate AWS setup instructions using the
  required path:

```text
tmprl-dem-cld/<project-name>/<secret-name>
```

- Explain the JSON key-to-environment-variable mapping for custom secrets.
- Run validation/generation checks after producing the config.
- Avoid supporting arbitrary BYO Flux overlays as the first path. If a repo
  already has Flux/Kustomize, assess it, explain migration options, and prefer
  platform-managed deployment or future BYO-image mode.

Suggested repo location:

```text
skills/tmprl-demo-cloud-onboarding/SKILL.md
```

Use the local `skill-creator` guidance when implementing the skill. Keep the
skill concise and put long examples or schemas into `references/` only if
needed.

## Safety Rules

- Never commit secrets.
- Never use destructive cloud cleanup until explicit deletion approval workflow
  exists.
- Do not grant project runtime pods broad AWS, Kubernetes, or Temporal admin
  access.
- Do not use GitHub runners that require broad cluster/API credentials.
- Build arbitrary Dockerfiles with narrow credentials.
- Treat Temporal namespace/API key lifecycle as external destructive state.
- Keep generated Deployment templates unapplied until image refs are real.
- Commit generated output when generator behavior changes.

## Expected Final Deliverables

By the end of the goal, leave:

- implementation commits;
- passing local validation/generation checks;
- bootstrap documentation for the user;
- manual setup checklist with exact secret paths and placeholders;
- at least one testable demo deployment path;
- registry/operator workflow status visible somewhere useful;
- companion SA onboarding skill scaffold and docs;
- a short final summary of what is testable and what the user must do next.
