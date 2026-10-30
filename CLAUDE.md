# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

GitOps config plus the control plane for Temporal demo clusters on EKS (`*.tmprl-demo.cloud`). Project owners commit a single CR; Flux delivers it; the **registry operator** (a Python Temporal worker) reconciles it into a namespace, ECR repos and CodeBuild images, a Temporal Cloud namespace and API key, secrets, and a Traefik route.

- `projects/demo/*.yaml`: `DemoProject` CRs. `projects/instruqt/*.yaml`: `InstruqtCourse` CRs. Adding one is a single file with no render step and no `kustomization.yaml` index, because Flux auto-includes the directory. `projects/demo/banking-money-transfer.yaml` is the reference demo. `projects/operator/registry.yaml` is the operator's own control-plane project, so don't copy it as a template.
- `schemas/`: JSON schemas for both CR kinds (also installed as CRDs under `clusters/*/bootstrap/crds/`).
- `registry/worker/`: the operator (`src/demo_registry_operator/`): workflows, activities, a Kubernetes watch loop, and resource rendering (`resources.py`, `derive.py`).
- `registry/web/`: the catalog/control-panel web server (`server.py`, mostly stdlib plus a Temporal client) and SPA (`static/`, with Tailwind built from `src/styles.css` by the Dockerfile; `static/app.css` is gitignored).
- `registry/onboarding-agent/`, `registry/temporal-cloud-proxy/`: images used by onboarding and by the opt-in `temporalProxy` sidecar.
- `apps/instruqt-litellm/`: the shared LiteLLM service (FastAPI broker, tests, and Flux manifests). Its GitHub Action builds the image and commits the ECR digest via `scripts/promote_image.py`.
- `apps/instruqt-secret-broker/`: the Go CLI. Its GitHub Action runs `go test`/`go vet` and publishes signed binaries.
- `clusters/blue/` (active) and `clusters/green/` (standby): Flux trees. Each is a **self-contained duplicate, not an overlay**. Per-color deltas include the autoscaler cluster name, `DEMO_REGISTRY_CLUSTER`, and Flux paths, and green currently lacks some blue-only addons. When you change shared platform manifests, check whether the other color needs the same change.
- `infra/`: OpenTofu (`tofu`, not `terraform`). The state lives in S3 (`infra/bootstrap/`). Real `*.tfvars` files are gitignored.
- `docs/architecture.md` (design and key decisions), `docs/operations.md` (runbook and gotchas), `RUNBOOK.md` (demo walkthrough), `projects/README.md` (the full CR contract and secrets).

## Commands

The only CI workflows are the two app builds, so run validation and tests locally. `uv` is available. pytest and ruff are not installed globally.

```sh
# Validate all project CRs (schema plus cross-file rules: unique names, subdomains, build/component refs, routes)
uv run --isolated --with jsonschema --with pyyaml python scripts/validate_projects.py

# Operator tests (conftest.py puts src/ on sys.path; tests use asyncio.run, so pytest-asyncio isn't needed)
uv run --isolated --with-requirements registry/worker/requirements.txt --with pytest \
  python -m pytest registry/worker/tests -q
# Single test
uv run --isolated --with-requirements registry/worker/requirements.txt --with pytest \
  python -m pytest registry/worker/tests/test_derive_and_defaults.py::<test_name> -q

# Web server tests
uv run --isolated --with-requirements registry/web/requirements.txt --with pytest \
  python -m pytest registry/web/tests -q

# Go CLI
(cd apps/instruqt-secret-broker && go test ./... && go vet ./...)
```

Some workflow tests start `WorkflowEnvironment.start_local()`, which downloads a Temporal dev server binary on the first run.

You can't run provisioning locally because it needs EKS, AWS, and Temporal Cloud. To work on the web UI only, follow "Run it locally" in `RUNBOOK.md`: run `temporal server start-dev`, then `AUTH_MODE=bootstrap-allow TEMPORAL_ADDRESS=localhost:7233 TEMPORAL_NAMESPACE=default python registry/web/server.py`.

## Operator architecture

- **The operator derives everything at reconcile time from `metadata.name`.** This covers the k8s namespace `tmprl-dem-cld-<name>`, the Temporal namespace (hash-truncated to 39 chars), the ECR prefix `tmprl-dem-cld`, image tags, and env vars. Nothing is baked into the CR, and there is no generator or `spec.derived`. CR defaults are documented in `docs/architecture.md` under "Key decisions".
- **Watch loop → entity workflows.** A singleton watch process (`watch.py`, the non-versioned `registry-operator-watch` Deployment) uses `SignalWithStart` to signal one long-lived entity workflow per CR (`demo-project/<cluster>/<name>`) when the spec hash changes. It keeps an in-memory `seen` cache, so terminating a workflow doesn't trigger a re-signal until the watch pod restarts.
- **Entity workflows** (`workflows/demo_project.py`, `instruqt_course.py`) orchestrate child workflows for artifact builds (CodeBuild, which reattaches to in-flight builds after restarts), the k8s runtime, and serverless Lambda. They also handle external resources and sagas: compensation on a failed first provision, finalizer-held teardown, and API key rotation (30-day TTL, rotated 10 days early). They continue-as-new after 50 reconciles and carry status and pending signals across runs. Don't unconditionally overwrite `_pending_spec` in `run()`, because that bug once dropped the start signal (see architecture.md).
- **Rollout:** a versioned candidate Deployment is rendered, then the operator waits for readiness, runs a smoke check that polls with backoff, and only then promotes the IngressRoute. Status is published to the `registry-catalog` and `registry-catalog-status` ConfigMaps, which the web app mounts.
- **The operator upgrades itself** as a `DemoProject`. Temporal Worker Controller runs it as a `WorkerDeployment` with a canary-workflow gate (`workflows/operator_canary.py`, `canary_templates/`).

## Workflow code changes: Worker Versioning, no patching

The operator worker uses SDK Worker Versioning with `VersioningBehavior.PINNED` (`worker.py`, enabled when Worker Controller injects `TEMPORAL_DEPLOYMENT_NAME` and `TEMPORAL_WORKER_BUILD_ID`). Running workflows finish on their pinned version. Continue-as-new uses `initial_versioning_behavior=AUTO_UPGRADE`, so the next run starts on the current version.

- Change workflow code directly. **Do not add `workflow.patched()` or `deprecate_patch()`.** Patch markers were deliberately removed.
- Don't manually delete old Worker Controller-managed Deployments, because they may still have pinned executions.
- If a change would break a history replayed across continue-as-new or otherwise looks unsafe without patching, stop and raise it instead of patching.

## Operational gotchas

- To restart the watch loop, **delete the pod**. Don't use `kubectl rollout restart`, because Flux reverts the `restartedAt` annotation and causes a second rollout.
- To force Flux: annotate the `cluster-gitops-config` GitRepository and then the Kustomization with `reconcile.fluxcd.io/requestedAt` (exact commands are in `docs/operations.md`).
- The 404 page in `registry/web/static/404.html` is duplicated under `clusters/<color>/addons/traefik/not-found/site/`, so keep them in step.
- `temporal-agent-harness` is pinned identically in `registry/worker/requirements.txt` and `registry/web/requirements.txt`, so keep the pins aligned.
