# Architecture

Canonical description of what the `tmprl-demo.cloud` platform is, how it works
today, and the design intent for the parts not built yet. Pair this with
[operations.md](operations.md) (runbook) and [project-plan.md](project-plan.md)
(phase status). The authoring contract lives in
[../projects/README.md](../projects/README.md).

## North star

A Temporal SA drops **one `DemoProject` CR** into `projects/demo/` or one
`InstruqtCourse` CR into `projects/instruqt/`, and the platform does everything
else automatically, with nothing else to commit:

derive all names → provision the Temporal Cloud namespace + scoped API key →
create ECR repos → build images (CodeBuild) → deploy a candidate → wait for
readiness → smoke-test → promote Traefik routing + TLS → publish the catalog →
and, on removal from git, tear everything down via a finalizer.

There is **no generation/render step** and **no `plan` mode** — the operator
derives and provisions for real at reconcile time.

## Two layers

| Layer | Path | Managed by | Changes |
| --- | --- | --- | --- |
| Platform / infra | `clusters/blue/` and `infra/` | **Flux** (GitOps) + **OpenTofu** | rare |
| Demos | `projects/demo/` | the **operator** (reconciles the CRs Flux delivers) | every demo |
| Instruqt courses | `projects/instruqt/` | the **operator** (reconciles the CRs Flux delivers) | every course |

`clusters/blue/` is the active cluster's desired state. It holds the Flux config
(`flux-system/`), registry CRDs (`bootstrap/crds/`), cluster add-ons, and the
bootstrap operator handoff (`bootstrap/registry-operator/`). `infra/` provisions
the blue/green AWS foundation and installs the initial Flux root sync; Flux then
reconciles the cluster-local desired state from `clusters/blue/`.

`projects/demo/` holds thin `DemoProject` CRs. `projects/instruqt/` holds
`InstruqtCourse` CRs. Flux points separate Kustomizations at each directory and
the kustomize-controller auto-includes every CR there, so adding a demo or
course is dropping one file.

## How a project becomes running resources

1. **Watch poller (outside Temporal).** A poll loop in the operator
   (`watch.py`) lists `DemoProject` CRs every ~15s and, for each, issues a
   `SignalWithStart` of the entity workflow (a `reconcile` signal carrying the
   full spec). Running the poller outside Temporal keeps it off the
   billable-actions path; the signal is level-triggered (carries desired state)
   so a missed or duplicate signal converges on the next resync.
2. **Entity workflow.** One `DemoProjectWorkflow` per project, ID
   `demo-project/<name>`. Per reconcile it runs:
   `plan_external_resources` → `reconcile_ecr_repositories` →
   `reconcile_temporal_cloud_resource` → `verify_secrets_manager_resources` →
   `plan_project_images` → `start_project_image_build` →
   `wait_for_project_image_build` → `reconcile_project`
   → (if a smoke check) `wait_for_candidate_readiness` → `run_smoke_check` →
   `promote_candidate`, recording status throughout. A `finalize` signal (sent
   when the CR is being deleted) tears resources down and removes the finalizer.
3. **Derivation — single source of truth.** `derive.py` computes everything from
   `metadata.name`: k8s namespace `tmprl-dem-cld-<name>`, Temporal namespace
   `tmprl-dem-cld-<name>` (sha-256-8 hash-truncated to 39 chars when needed),
   secret prefix `tmprl-dem-cld/<name>`, ECR prefix `tmprl-dem-cld`, host
   `<subdomain>.tmprl-demo.cloud`. The CR carries **no** `derived` block.
4. **Candidate rollout.** Each reconcile renders versioned candidate
   Deployments/Services (`<component>-git-<sha>`); the public Traefik route + TLS
   cert are created only at **promotion**, gated on readiness + smoke success, so
   a failed candidate never takes the live route.
5. **Catalog.** The watch loop (again, outside Temporal) builds a catalog payload
   from all CRs and writes a `registry-catalog` ConfigMap; the registry web app
   mounts it at `/app/data/registry.json` and overlays per-project live status
   from the `registry-catalog-status` ConfigMap at `/app/status`.

## Boundaries

- Flux owns bootstrap/infra desired state; the operator owns project runtime
  resources derived from `DemoProject` specs and Git-backed course reconciliation
  state derived from `InstruqtCourse` specs.
- Flux never needs Temporal Cloud admin credentials. The operator uses a
  high-privilege **bootstrap** Temporal Cloud credential (Secrets Manager:
  `tmprl-dem-cld/platform/temporal-cloud-bootstrap`) only to create per-project
  namespaces + scoped keys; each project runtime gets only its own scoped key.
- Sensitive derived values (Temporal API keys) live in AWS Secrets Manager under
  `tmprl-dem-cld/<name>/...` and reach pods via External Secrets. Per-project IAM
  should scope to `tmprl-dem-cld/<name>/*` as a second enforcement layer.
- Build workers (CodeBuild) push only the project's ECR repos.
- HTTPS source fetches for `temporal-sa` repos use the shared read-only GitHub
  PAT from AWS Secrets Manager path
  `tmprl-dem-cld/platform/github-temporal-sa-readonly`.
- `InstruqtCourse` may resolve two Git sources: `spec.source` for private track
  configuration and optional `spec.content` for separate exercise code. Both
  source revisions participate in the entity workflow reconcile identity.
- `spec.source.testingBranch` enables a separate testing channel. The watcher
  explicitly probes GitHub with the shared read-only PAT before treating a
  branch as deleted; auth or API failures report `BranchUnknown` and retain
  existing testing resources. A confirmed missing testing branch removes the
  testing Flux source-watch resources.
- Managed Instruqt container images are enforced during config rendering:
  `containers[].image` is injected when absent and overwritten when present for
  containers bound by `spec.images`; unmanaged container images are rejected.
  Managed Instruqt images are published to ECR Public so Instruqt can pull them.
- The worker image includes the Instruqt CLI, and the operator syncs
  `tmprl-dem-cld/platform/instruqt-cli` as `INSTRUQT_TOKEN`. The
  `InstruqtCourse` workflow builds managed images with CodeBuild, renders the
  track artifact, validates it, force-pushes it, runs `instruqt track test`, and
  deploys it. A confirmed missing testing branch removes testing source-watch
  resources and deletes the managed testing track.

## What works today (proven end-to-end)

`projects/demo/banking-money-transfer.yaml` (the
[demo-banking-app-money-transfer](https://github.com/temporal-sa/demo-banking-app-money-transfer)
repo) was taken fully automatically from a dropped CR to a live, promoted
deployment: Temporal Cloud namespace + scoped key created, runtime secret synced
via External Secrets, three ECR repos created, three images built on CodeBuild,
candidate deployed, readiness gated, smoke `/api/health` → 200, promoted, Traefik
route + LetsEncrypt cert. Live (behind Temporal-employee Google auth) at
`https://banking-app.tmprl-demo.cloud`.

## Hardening and open work

Implemented hardening is listed first, followed by open work items with chosen
approaches so work can resume directly.

### 1. Activity decomposition and retry tuning
The operator now splits side effects on retry/failure boundaries:
- External resources run as `plan_external_resources` →
  `reconcile_ecr_repositories` → `reconcile_temporal_cloud_resource` →
  `verify_secrets_manager_resources`, with the workflow assembling the aggregate
  status.
- Image builds run as `plan_project_images` → `start_project_image_build` →
  `wait_for_project_image_build`. Build IDs are heartbeated so retries can
  re-attach instead of starting duplicate CodeBuild runs; terminal build
  failures return build error status and are not retried as transient failures.
- Finalization runs namespace deletion, source cleanup, Temporal Cloud teardown,
  ECR deletion, and finalizer removal as separate activities; the finalizer
  remains last.
- Status recording separately patches the `DemoProject` status and catalog
  status ConfigMaps.

Workflow activity calls use explicit retry policies with shared non-retryable
error types for validation/config/auth and terminal build failures. Provider
network/internal failures are classified as retryable.

Remaining build hardening: add **skip-if-image-exists** by resolving branch HEAD,
checking ECR for `git-<sha>`, and skipping CodeBuild when immutable images are
already present.

### 2. Registry web and operator image builds
`projects/demo/registry.yaml` deploys the registry web as an ordinary project and
declares both `web` and `worker` builds from this repository. The `worker` build
publishes the next operator image, and the `registry-operator` component renders
the operator's `WorkerDeployment`.

### 3. Operator self-management (safe upgrades)
The operator is a Temporal worker, so its own rollouts can be made safe — but the
deploy trigger stays in the normal project source/build path rather than a
separate operator-specific image loop. Chosen layering:
- **Pinned Worker Versioning** on the operator: `worker.py` enables SDK Worker
  Versioning when Worker Controller injects `TEMPORAL_DEPLOYMENT_NAME` and
  `TEMPORAL_WORKER_BUILD_ID`.
- **Temporal Worker Controller** owns the operator Temporal worker through the
  `registry-operator` WorkerDeployment in `tmprl-dem-cld-registry-operator`.
  The singleton Kubernetes watch loop is split into the non-versioned
  `registry-operator-watch` Deployment so only one process signals entity
  workflows.
- **A canary `DemoProject`** runs as the WorkerDeployment gate. The target
  version creates a unique canary project, waits for promotion, then deletes it
  and fails closed unless namespace/source/catalog cleanup can be proven.
Note: percentage *ramping* fits poorly here (few long-lived entity workflows, not
high-volume executions); think blue/green + canary, not statistical ramp.

### 4. Flux Kustomizations are self-managed
The Flux `Kustomization` CRs in `clusters/blue/flux-system/kustomizations.yaml`
are reconciled by the root `flux-system` Kustomization. Terraform/OpenTofu or a
human bootstrap still has to install Flux and apply the first root sync object on
a fresh cluster, but subsequent platform wiring changes are GitOps-managed like
the rest of the cluster.

### 5. Internal-repo support (deferred)
Optional `source.internal: true` → require a read-only deploy key in Secrets
Manager (reuse Flux's read-only `identity` key) → operator verifies before build.

### 6. Ephemeral demo environments (deferred)
Registry-owned temporary namespaces with a local Temporal dev server for demos
that need an inspectable UI without Temporal Cloud creds; TTL cleanup workflow.

## Key decisions & lessons (carry forward)

- **Operator derives; no generator.** `spec.derived`, the generated cluster
  manifests, and the `--check` CI gate were removed.
- **Authoring stays thin.** `worker: true` marks a Temporal worker; versioning is
  the object form `worker.versioning` with optional `deploymentName` for the
  Worker Deployment identity. Task queues remain app-owned routing names.
  Defaults: `source.branch`→`main`,
  `ingress.routes`→catch-all to the `servicePort: 80` component, single unnamed
  port→`http`, `smokeCheck` omitted→promote on readiness (no probe), `registry`
  block optional (title falls back to a humanized name). `temporalAccess: true`
  for non-worker components that start/query workflows.
- **Entity-workflow start-signal bug (fixed).** `run()` must **not**
  unconditionally overwrite `self._pending_spec`. With `SignalWithStart` the
  `reconcile` signal is delivered on the first workflow task and its handler sets
  `_pending_spec`; a fresh start passes `carried_pending_spec=None`, so blindly
  assigning it wiped the signal and left `wait_condition` permanently false. Fix:
  only adopt a carried spec from continue-as-new (`if carried_pending_spec is not
  None`). This was *the* reason single-signal fresh runs hung.
- **No workflow patch markers.** The per-sequence `workflow.patched()` guards were
  removed (an anti-pattern — a new patch every reconcile). Use **Worker
  Versioning** for safe upgrades instead. Consequence: old entity workflows stay
  pinned to the worker version they started on while new workflows route to the
  current WorkerDeployment version after the canary gate passes. Terminate an
  entity workflow only when you intentionally want it to restart on current code.
- **Rollout robustness.** Smoke check **polls with backoff** (not single-shot); a
  `tcpSocket` **readiness probe** makes "ready" mean "the port is listening
  (serving)", closing the gap that let smoke fire before an app that builds on
  boot was up. Build activity **retries** (`maximum_attempts=3`) so a worker
  restart mid-build doesn't fail the rollout.
