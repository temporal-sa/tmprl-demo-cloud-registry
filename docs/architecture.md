# Architecture

Canonical description of what the `tmprl-demo.cloud` platform is, how it works
today, and the design intent for the parts not built yet. Pair this with
[operations.md](operations.md) (runbook) and [project-plan.md](project-plan.md)
(phase status). The authoring contract lives in
[../projects/README.md](../projects/README.md).

## North star

A Temporal SA drops **one `DemoProject` CR** into `projects/` and the platform
does everything else, automatically, with nothing else to commit:

derive all names → provision the Temporal Cloud namespace + scoped API key →
create ECR repos → build images (CodeBuild) → deploy a candidate → wait for
readiness → smoke-test → promote Traefik routing + TLS → publish the catalog →
and, on removal from git, tear everything down via a finalizer.

There is **no generation/render step** and **no `plan` mode** — the operator
derives and provisions for real at reconcile time.

## Two layers

| Layer | Path | Managed by | Changes |
| --- | --- | --- | --- |
| Platform / infra | `clusters/sa-demo/` | **Flux** (GitOps) | rare |
| Demos | `projects/` | the **operator** (reconciles the CRs Flux delivers) | every demo |

`clusters/sa-demo/` holds: Flux config (`flux-system/`), the `DemoProject` CRD
(`bootstrap/crds/`), External Secrets (`addons/external-secrets/`), and the
operator Deployment itself (`bootstrap/registry-operator/`). The operator is
**bootstrapped** here because it can't reconcile itself into existence.

`projects/` holds thin `DemoProject` CRs. Flux's `registry-projects`
Kustomization points at `./projects` and the kustomize-controller auto-includes
every CR there, so adding a demo is dropping one file.

## How a project becomes running resources

1. **Watch poller (outside Temporal).** A poll loop in the operator
   (`watch.py`) lists `DemoProject` CRs every ~15s and, for each, issues a
   `SignalWithStart` of the entity workflow (a `reconcile` signal carrying the
   full spec). Running the poller outside Temporal keeps it off the
   billable-actions path; the signal is level-triggered (carries desired state)
   so a missed or duplicate signal converges on the next resync.
2. **Entity workflow.** One `DemoProjectWorkflow` per project, ID
   `demo-project/<name>`. Per reconcile it runs:
   `reconcile_external_resources` → `build_project_images` → `reconcile_project`
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
  resources derived from `DemoProject` specs.
- Flux never needs Temporal Cloud admin credentials. The operator uses a
  high-privilege **bootstrap** Temporal Cloud credential (Secrets Manager:
  `tmprl-dem-cld/platform/temporal-cloud-bootstrap`) only to create per-project
  namespaces + scoped keys; each project runtime gets only its own scoped key.
- Sensitive derived values (Temporal API keys) live in AWS Secrets Manager under
  `tmprl-dem-cld/<name>/...` and reach pods via External Secrets. Per-project IAM
  should scope to `tmprl-dem-cld/<name>/*` as a second enforcement layer.
- Build workers (CodeBuild) push only the project's ECR repos.

## What works today (proven end-to-end)

`projects/banking-money-transfer.yaml` (the
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
`projects/registry.yaml` deploys the registry web as an ordinary project and
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

### 4. Flux Kustomizations are not self-managed
The Flux `Kustomization` CRs in `clusters/sa-demo/flux-system/kustomizations.yaml`
were applied out-of-band at bootstrap; there is **no self-managing `flux-system`
Kustomization**. So edits to that file (e.g. repointing a path) do **not** take
effect until someone `kubectl apply`s it. Add a self-managing Kustomization that
reconciles `clusters/sa-demo/flux-system/` so the platform's own wiring is
GitOps-managed like everything else. (See operations.md for the manual apply in
the meantime.)

### 5. Internal-repo support (deferred)
Optional `source.internal: true` → require a read-only deploy key in Secrets
Manager (reuse Flux's read-only `identity` key) → operator verifies before build.

### 6. Ephemeral demo environments (deferred)
Registry-owned temporary namespaces with a local Temporal dev server for demos
that need an inspectable UI without Temporal Cloud creds; TTL cleanup workflow.

## Key decisions & lessons (carry forward)

- **Operator derives; no generator.** `spec.derived`,
  `scripts/generate_projects.py`, `clusters/sa-demo/generated/`, and the
  `--check` CI gate were removed.
- **Authoring stays thin.** `worker: true` marks a Temporal worker; versioning is
  the object form `worker.versioning` (with `taskQueue`, consumed only there as
  the Worker Deployment Name). Defaults: `source.branch`→`main`,
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
