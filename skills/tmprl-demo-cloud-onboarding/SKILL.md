---
name: tmprl-demo-cloud-onboarding
description: Migrate Temporal SA demo repositories into the tmprl-demo.cloud registry. Use when assessing a demo repo for onboarding, estimating migration lift, creating or updating Dockerfiles/env handling, authoring a DemoProject CR, documenting AWS Secrets Manager setup under the tmprl-dem-cld project secret prefix, or adapting existing Docker Compose/Kubernetes/Flux demos to the platform-managed DemoProject path.
---

# Tmprl Demo Cloud Onboarding

Use this skill as a wizard for moving an SA-owned demo repo into the
`tmprl-demo.cloud` platform registry.

The platform contract is: **the SA drops one `DemoProject` CR under `projects/`
and nothing else.** Flux delivers it and the registry operator does everything —
derives namespaces/host/prefixes, provisions the Temporal Cloud namespace +
scoped API key, builds images, deploys, smoke-tests, and promotes. There is no
generation/render step and no other file to commit. Author a thin config and let
the operator fill in the rest.

## Workflow

1. Identify the registry repo and target demo repo.
   - Registry repo: `temporal-sa/tmprl-demo-cloud-demo-registry`.
   - Project CRs live under `projects/`.
   - Treat `projects/README.md` and `schemas/project.schema.json` as the current
     contract.

2. Check for an existing or similar registry entry.
   - Search `projects/*.yaml` for matching repo URLs, names, app titles, and
     related tags.
   - If a similar entry exists, explain whether to update it or create a new CR.

3. Assess the target repo before editing.
   - Services/components and long-running processes.
   - Temporal workers, SDK languages, task queues, and worker versioning needs.
   - Dockerfiles, missing Dockerfiles, Docker Compose services, Kubernetes,
     Kustomize, Flux, Helm, or other deployment artifacts.
   - Web/API ingress entrypoint, ports, and stable health endpoints. Note which
     component is the public entry (usually the one a browser hits) and whether
     it internally proxies to others (e.g. an nginx frontend proxying `/api` to a
     backend) — that affects routing.
   - Which components actually talk to Temporal: a worker, **and** any API/backend
     that *starts or queries* workflows.
   - Environment variables, local-only values, and config files.
   - Custom secrets and whether each secret can be JSON key-to-env-var mapped.
   - Assumptions such as local Temporal server, host networking, mounted files,
     developer credentials, or broad cloud permissions.

4. Estimate migration lift before changing code.
   - Low: Dockerfiles exist, env-driven config, clear web/worker split, health
     endpoint exists, no custom secrets or only simple JSON secrets.
   - Medium: Dockerfiles or env cleanup needed, Compose-to-components mapping
     needed, health endpoint missing, one or two custom secrets.
   - High: hard-coded local Temporal connection, broad cloud credentials,
     coupled web/worker process, missing containerization, stateful services, or
     existing raw Kubernetes/Flux overlays that need redesign.

5. Discuss code changes with the SA before editing the target repo.
   - Do not silently rewrite app behavior.
   - Prefer small changes that make Temporal connection, ports, secrets, and
     service URLs configurable through environment variables.
   - Produce or update Dockerfiles only after explaining the intended build and
     runtime split.

6. Author the `DemoProject` CR. **Make active suggestions** — do not just accept
   whatever the SA says. The CR is a normal Kubernetes resource:
   `apiVersion: registry.tmprl-demo.cloud/v1alpha1`, `kind: DemoProject`,
   `metadata.name`, and a `spec`. Read `references/project-config-template.md`
   when creating a new config.

   - **Suggest the name, and check its length.** Propose a concise
     `metadata.name` that does NOT include the word "demo" (every platform is a
     demo; it's redundant). The operator derives the Temporal Cloud namespace as
     `tmprl-dem-cld-<name>`, which caps at **39 characters** — the
     `tmprl-dem-cld-` prefix is 14, so keep the name to **≤ 25 characters** or it
     gets hash-truncated (still works, just less readable). Example: suggest
     `banking-money-transfer` (22) over `demo-banking-app-money-transfer` (31,
     truncated).
   - **Suggest a subdomain.** Default it to `metadata.name` (or a shorter, nicer
     label). It must be only the left-hand label — never include
     `tmprl-demo.cloud`. The app will be reachable at
     `<subdomain>.tmprl-demo.cloud`.
   - **Worker versioning — ask explicitly.**
     - If the repo already uses Worker Versioning, suggest the object form and
       help shape it: `worker.versioning` with `enabled: true`, the `taskQueue`
       (this becomes the Worker Deployment Name), and an `env` map naming the
       app's versioning env vars (`enabled`, `buildId`, `deploymentName`).
     - If the repo does **not** use versioning, prompt the SA: *"This worker
       isn't using Temporal Worker Versioning — do you want to enable it?"* If
       no, use the shorthand `worker: true` (no `taskQueue` needed — it's only
       consumed by versioning, and the app keeps its own task-queue default).
   - **Mark Temporal access correctly.** `worker: true` workers get credentials
     automatically. Set `temporalAccess: true` on any **non-worker** component
     that starts or queries workflows (e.g. an API/backend) — without it the
     credentials are not injected and the app breaks.
   - **Lean on defaults to keep the config thin** (suggest omitting these):
     - `source.branch` defaults to `main`.
     - `ingress.routes` can be omitted — the operator exposes a single catch-all
       to the component serving `servicePort: 80`. Declare routes explicitly only
       for multi-route ingress or when no component serves 80.
     - A lone port needs no `name` (becomes `http`); `servicePort` defaults to
       `containerPort`.
     - `smokeCheck` is optional — omitting it promotes once pods are ready (no
       HTTP probe). Suggest adding one with a stable owner-approved path when the
       app has a real health endpoint (e.g. `/health` or `/api/health`); make it
       safe for candidate testing, since public routing moves only after candidate
       readiness and smoke success.
     - `spec.registry` (title/summary/tags) is optional catalog metadata; when
       omitted the catalog title falls back to a humanized name. Suggest filling
       it in for a good landing-page card.
   - Do not set raw Kubernetes namespaces, Temporal namespace names, standard
     Temporal env var names, ECR repos, image tags, or full `tmprl-demo.cloud`
     hostnames — the operator derives all of these.
   - Keep `source.repo` in the `temporal-sa` GitHub organization.
   - Declare every container image under `builds`; the operator derives ECR
     repositories and immutable `git-<source-sha>` tags.

7. Document custom secrets exactly.
   - Required path format: `tmprl-dem-cld/<project-name>/<secret-name>`.
   - Every JSON key in the AWS secret becomes an environment variable for
     components that list the secret.
   - Declare each custom secret under `spec.secrets.<name>` and include
     `requiredProperties` for keys the operator must verify before deployment.
   - Keep secrets narrow and attach them only to components that need them.
   - Project Temporal credentials are platform-managed at
     `tmprl-dem-cld/<project-name>/temporal` and are created by the operator.
   - The Temporal Cloud bootstrap credential is platform-level and pre-provisioned
     at `tmprl-dem-cld/platform/temporal-cloud-bootstrap`.

8. Run registry checks from the registry repo after config changes.

```sh
python scripts/validate_projects.py
python -m py_compile scripts/validate_projects.py $(find registry/worker/src -name '*.py' | sort)
```

   There is no generation step and nothing else to commit — validation is the
   whole gate. If `pyyaml`/`jsonschema` are missing locally, run via
   `uv run --with pyyaml --with jsonschema scripts/validate_projects.py`.

9. Explain what the operator does automatically vs. what stays manual.
   - Automatic on reconcile (default operator config): creates/verifies the ECR
     repositories; builds images via AWS CodeBuild and pushes immutable
     `git-<sha>` tags; creates/verifies the Temporal Cloud namespace, a
     namespace-scoped service account, and a 30-day API key (rotated on a durable
     timer); writes the runtime key to Secrets Manager and syncs it via External
     Secrets; deploys, waits for candidate readiness, smoke-tests, and promotes;
     and on project deletion runs a finalizer-held teardown.
   - Still manual: the AWS Secrets Manager **values** for any custom secrets (the
     operator only verifies they exist with the required JSON keys), and the
     platform Temporal Cloud bootstrap credential. Shared auth prerequisites for
     auth-protected demos.

10. Point the SA at lifecycle status.
    - `DemoProject.status` is the source of truth for external resources, build
      status, candidate rollout, readiness, smoke checks, active version, and
      last error (`kubectl get demoproject <name> -o yaml`).
    - The operator publishes the catalog itself: it lists all `DemoProject` CRs
      and writes a `registry-catalog` ConfigMap (the `registry.json` the web app
      mounts), plus per-project live summaries in `registry-catalog-status`
      (`<project>.json`) that the web component mounts at `/app/status`.
    - `/api/catalog` and the registry landing page show the merged lifecycle
      status.

## Existing Kubernetes Or Flux

If the target repo already has Kubernetes, Kustomize, Helm, or Flux:

- assess what it does and whether it reveals useful ports, probes, env vars, or
  service boundaries;
- explain migration options;
- prefer platform-managed `DemoProject` deployment for the first path;
- mention future BYO-image mode only as future work;
- do not add arbitrary project-owned overlays as the default path.
