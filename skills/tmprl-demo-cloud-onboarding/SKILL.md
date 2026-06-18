---
name: tmprl-demo-cloud-onboarding
description: Assess Temporal SA demo repositories for tmprl-demo.cloud onboarding and author the single DemoProject YAML when the repo can be adapted without code changes. Use when inspecting a target demo repo end to end, estimating migration lift, reporting required app/Docker/env changes, authoring a DemoProject CR, documenting AWS Secrets Manager setup under the tmprl-dem-cld project secret prefix, exposing private/internal repos through source.gitRepository auth, adding path-aware source filters, or adapting existing Docker Compose/Kubernetes/Flux demos to the platform-managed DemoProject path.
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

## Hard Boundaries

- **Do not propose or make demo registry repo changes** to fit a target demo.
  The registry/platform contract is fixed for onboarding. The only permitted
  registry-repo change is creating or updating the single project YAML under
  `projects/` when the user has asked for that output.
- **The goal is zero target-repo code changes.** First determine whether the
  existing repo can be represented as a `DemoProject` as-is.
- **Do a clean end-to-end inspection of the target repo before authoring YAML.**
  Read the app structure, build/deploy artifacts, runtime entrypoints, Temporal
  clients/workers, ports, health checks, env vars, and secrets well enough to
  defend the proposed project config.
- **Do not edit the target repo during onboarding.** If code, Dockerfile, config,
  dependency, health endpoint, secret-shape, or file-creation work is needed,
  produce a report of required changes instead of making them.
- **Only create the project YAML.** Any other file creation or modification,
  even when the skill is being run from the target repo itself, must be discussed
  with the user as a concrete plan and approved before it happens.
- If the repo cannot be onboarded without code changes, stop after the required
  changes report unless the user explicitly approves a follow-up implementation
  plan.

## Workflow

1. Identify the registry repo and target demo repo.
   - Registry repo: `temporal-sa/tmprl-demo-cloud-demo-registry`.
   - Project CRs live under `projects/`.
   - Treat `projects/README.md` and `schemas/project.schema.json` as the current
     contract.
   - Never suggest changing registry/operator code, schema, docs, or platform
     manifests as part of demo onboarding. If the target demo does not fit the
     contract, report the target-repo changes required instead.

2. Check for an existing or similar registry entry.
   - Search `projects/*.yaml` for matching repo URLs, names, app titles, and
     related tags.
   - If a similar entry exists, explain whether to update it or create a new CR.

3. Assess the target repo end to end before authoring YAML.
   - Services/components and long-running processes, including all runtime
     entrypoints and commands.
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
   - Whether Temporal client setup can be satisfied by standard platform env
     vars or by the proxy compatibility path. Local-defaulted code such as
     `Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)` is a good
     proxy candidate if the host/namespace values are env-driven.
   - Custom secrets and whether each secret can be JSON key-to-env-var mapped.
   - Whether the source repo is private/internal and needs an SSH deploy key
     synced from AWS Secrets Manager for Flux and CodeBuild.
   - Whether only specific repo paths should trigger automatic rebuild/rollout.
   - Assumptions such as local Temporal server, host networking, mounted files,
     developer credentials, or broad cloud permissions.

4. Decide whether zero-code-change onboarding is possible.
   - Ready as-is: Dockerfiles exist, config is env-driven, web/API/worker split
     is clear, ports are known, a stable health endpoint exists or readiness-only
     promotion is acceptable, and secrets can be mapped as JSON keys.
   - Needs target-repo changes: Dockerfiles are missing, service URLs are
     hard-coded, Temporal connection cannot be satisfied by standard env vars or
     the platform Temporal proxy sidecar, ports or runtime commands are unclear,
     a required health endpoint is missing, secrets cannot be injected as env
     vars, broad cloud credentials are assumed, stateful services are required,
     or app processes are too coupled to express as components.
   - Existing Kubernetes/Helm/Flux artifacts can inform the project YAML, but
     they are not copied into the registry and do not justify registry changes.

5. If changes are needed, produce a required-changes report instead of editing.
   - Include the exact files likely affected, the behavioral reason each change
     is needed, and the smallest acceptable shape of the change.
   - Separate blockers from nice-to-have improvements.
   - Prefer changes that make Temporal connection, ports, secrets, and service
     URLs configurable through environment variables.
   - Do not create or update Dockerfiles, source files, config files, manifests,
     scripts, docs, or tests until the user approves a concrete plan.
   - If the user approves implementation later, treat that as a separate task
     outside the default onboarding path.

6. Author the `DemoProject` CR. **Make active suggestions** — do not just accept
   whatever the SA says. The CR is a normal Kubernetes resource:
   `apiVersion: registry.tmprl-demo.cloud/v1alpha1`, `kind: DemoProject`,
   `metadata.name`, and a `spec`. Read `references/project-config-template.md`
   when creating a new config.
   Author this only when the target repo can be represented by the current
   platform contract, or when the user explicitly wants a draft YAML plus a
   required-changes report.

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
   - **Handle private/internal repos explicitly.** Keep `source.repo` as the
     canonical HTTPS repo URL. Add `source.gitRepository.url` for the SSH clone
     URL and `source.gitRepository.authSecret` for the project-scoped AWS secret
     containing Flux-compatible `identity` and `known_hosts` keys.
   - **Use source filters when only some paths should roll out.** Add
     `source.gitRepository.sparseCheckout` and/or `source.gitRepository.ignore`
     when only selected paths should count as source changes. The operator uses
     the filtered Flux artifact digest as the rollout identity and renders
     `sparseCheckout` as Flux `ignore` patterns for clusters whose
     source-controller CRD does not support sparse checkout directly. Unrelated
     commits to ignored paths should not trigger image rebuilds or
     WorkerDeployment canaries.
   - **Choose the build strategy explicitly.** Dockerfile builds are the
     default and only need `context` plus `dockerfile`. For repos without a
     Dockerfile, use `strategy: buildpack` with a `builder` image such as
     `paketobuildpacks/builder-jammy-base`; add `builds.<name>.env` only for
     build-time buildpack variables like `BP_PYTHON_VERSION`. If the source
     repo lacks a Procfile but needs multiple runtime entrypoints, add
     `builds.<name>.processes`; the operator writes a generated Procfile before
     `pack build`. Launch those processes with
     `components.<name>.command: ["/cnb/process/<process-name>"]`. Do not infer
     package managers, start commands, ports, or worker processes from the repo;
     those remain explicit config.
   - **Worker versioning — ask explicitly.**
     - If the repo already uses Worker Versioning, suggest the object form and
       help shape it: `worker.versioning` with `enabled: true`, optional
       `deploymentName` (the Worker Deployment identity; defaults to the
       component name), optional `rollout` strategy and gate settings, and
       optional `sunset` delays. Do not map app-specific build/deployment env
       names; Worker Controller injects
       `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_DEPLOYMENT_NAME`,
       and `TEMPORAL_WORKER_BUILD_ID`; the runtime secret supplies
       `TEMPORAL_API_KEY` and `TEMPORAL_TLS`. Task queues are app-owned routing
       names; configure them through generic component `env` only when the app
       needs an env-driven task queue value.
     - If the repo does **not** use versioning, prompt the SA: *"This worker
       isn't using Temporal Worker Versioning — do you want to enable it?"* If
       no, use the shorthand `worker: true`.
   - **Mark Temporal access correctly.** `worker: true` workers get credentials
     automatically. Set `temporalAccess: true` on any **non-worker** component
     that starts or queries workflows (e.g. an API/backend) — without it the
     credentials are not injected and the app breaks.
   - **Use the Temporal proxy only for compatibility.** If a component can only
     be pointed at a local Temporal host and cannot read Cloud auth/TLS/namespace
     settings without code changes, add `temporalProxy.enabled: true` to that
     Temporal-using component. Add `temporalProxy.addressEnv` only when the app
     uses nonstandard host env var names; the operator already sets
     `TEMPORAL_ADDRESS` and `TEMPORAL_HOST` to the local sidecar by default.
     The app container will use `TEMPORAL_NAMESPACE=default`; the sidecar
     rewrites that to the project Temporal Cloud namespace and owns API-key/TLS
     auth. Do not inject `TEMPORAL_API_KEY` directly into the app container for
     this path.
   - **Set Temporal UI URLs literally when needed.** If the app has an env var
     like `TEMPORAL_UI_URL`, set it in component `env` using the known Cloud UI
     URL for the project namespace, e.g.
     `https://cloud.temporal.io/namespaces/<derived-temporal-namespace>.<account-id>`.
     The operator does not currently derive that env var automatically.
   - **Lean on defaults to keep the config thin** (suggest omitting these):
     - `source.branch` defaults to `main`.
     - `ingress.routes` can be omitted — the operator exposes a single catch-all
       to the component serving `servicePort: 80`. Declare routes explicitly only
       for multi-route ingress or when no component serves 80.
     - `ingress.routes[].passHostHeader` defaults to `true`. Set it `false` only
       when an upstream app rejects the public `tmprl-demo.cloud` Host header
       and can safely receive Traefik's upstream host instead.
     - Public demo URLs are HTTPS; HTTP redirects to HTTPS. `ingress.temporalAuthRequired`
       defaults to `true`; set it `false` only for reviewed public demos.
     - A lone port needs no `name` (becomes `http`); `servicePort` defaults to
       `containerPort`. Use `serviceName` only when a component needs stable DNS
       that differs from its component name.
     - `smokeCheck` is optional — omitting it promotes once pods are ready (no
       HTTP probe). Suggest adding one with a stable owner-approved path when the
       app has a real health endpoint (e.g. `/health` or `/api/health`); make it
       safe for candidate testing, since public routing moves only after candidate
       readiness and smoke success.
     - `spec.registry` (title/summary/tags) is optional catalog metadata; when
       omitted the catalog title falls back to a humanized name. Suggest filling
       it in for a good landing-page card.
     - Components get a project-local ServiceAccount with
       `automountServiceAccountToken: false` by default. Set
       `automountServiceAccountToken: true` only when that component must call
       the Kubernetes API.
   - Do not set raw Kubernetes namespaces, Temporal namespace names, standard
     Temporal env var names, ECR repos, image tags, or full hostnames in
     `ingress.subdomain` — the operator derives all of these. Full URLs are OK
     in component env vars or metadata links when the app genuinely needs them.
   - Keep `source.repo` in the `temporal-sa` GitHub organization.
   - Declare every container image under `builds`; the operator derives ECR
     repositories and immutable tags. Dockerfile-only demos use
     `git-<source-sha>`. Buildpack demos include the build definition hash in
     the version so changes to buildpack env/processes produce a new image and
     deployment.

7. Document custom secrets exactly.
   - Required path format: `tmprl-dem-cld/<project-name>/<secret-name>`.
   - Every JSON key in the AWS secret becomes an environment variable for
     components that list the secret.
   - Declare each custom secret under `spec.secrets.<name>`. Use `{}` when the
     operator should inject all JSON keys without fail-fast key validation.
   - `requiredProperties` is optional validation metadata. Include it only when
     the project should fail early if specific JSON keys are missing or malformed.
   - Keep secrets narrow and attach them only to components that need them.
   - Project Temporal credentials are platform-managed at
     `tmprl-dem-cld/<project-name>/temporal` and are created by the operator.
   - The Temporal Cloud bootstrap credential is platform-level and pre-provisioned
     at `tmprl-dem-cld/platform/temporal-cloud-bootstrap`.

8. Run registry checks from the registry repo after config changes.

```sh
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_projects.py
```

   There is no generation step and nothing else to commit — validation is the
   whole gate. If `pyyaml`/`jsonschema` are missing locally, run via
   `uv run --isolated --with jsonschema --with pyyaml python scripts/validate_projects.py`.

9. Explain what the operator does automatically vs. what stays manual.
   - Automatic on reconcile (default operator config): creates/verifies the ECR
     repositories; builds images via AWS CodeBuild and pushes immutable image
     tags; creates/verifies the Temporal Cloud namespace, a
     namespace-scoped service account, and a 30-day API key (rotated on a durable
     timer); writes the runtime key to Secrets Manager and syncs it via External
     Secrets; deploys, waits for candidate readiness, smoke-tests, and promotes;
     and on project deletion runs a finalizer-held teardown.
   - Still manual: the AWS Secrets Manager **values** for any custom secrets (the
     operator verifies required JSON keys only when `requiredProperties` is set),
     private repo source-auth secrets, and the platform Temporal Cloud bootstrap
     credential. Shared auth prerequisites for auth-protected demos.

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
- do not add arbitrary project-owned overlays as the default path;
- do not suggest changing the registry repo to accommodate those artifacts.
