---
name: tmprl-demo-cloud-onboarding
description: Migrate Temporal SA demo repositories into the tmprl-demo.cloud registry. Use when assessing a demo repo for onboarding, estimating migration lift, creating or updating Dockerfiles/env handling, generating registry project YAML configs, documenting AWS Secrets Manager setup under the tmprl-cloud-demo project secret prefix, or adapting existing Docker Compose/Kubernetes/Flux demos to the platform-managed DemoProject path.
---

# Tmprl Demo Cloud Onboarding

Use this skill as a wizard for moving an SA-owned demo repo into the
`tmprl-demo.cloud` platform registry.

## Workflow

1. Identify the registry repo and target demo repo.
   - Registry repo: `temporal-sa/tmprl-demo-cloud-demo-registry`.
   - Project configs live under `projects/`.
   - Treat `projects/README.md`, `schemas/project.schema.json`, and
     `BOOTSTRAP.md` as the current contract.

2. Check for an existing or similar registry entry.
   - Search `projects/*.yaml` for matching repo URLs, names, app titles, and
     related tags.
   - If a similar entry exists, explain whether to update it or create a new
     config.

3. Assess the target repo before editing.
   - Services/components and long-running processes.
   - Temporal workers, SDK languages, task queues, and worker versioning needs.
   - Dockerfiles, missing Dockerfiles, Docker Compose services, Kubernetes,
     Kustomize, Flux, Helm, or other deployment artifacts.
   - Web/API ingress entrypoint, ports, and stable health endpoints.
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

6. Create or update the registry project config.
   - Use the platform-managed contract, not arbitrary project-owned overlays.
   - Do not set raw Kubernetes namespaces, Temporal namespace names, standard
     Temporal env var names, ECR repos, image tags, or full `tmprl-demo.cloud`
     hostnames.
   - Keep `source.repo` in the `temporal-sa` GitHub organization.
   - Declare every container image under `builds`; the operator derives ECR
     repositories and immutable `git-<source-sha>` tags.
   - Workers get Temporal credentials automatically. Set `temporalAccess: true`
     only on non-worker components that need Temporal.
   - Add `smokeCheck` with a stable owner-approved endpoint whenever possible.
   - Make the smoke endpoint safe for candidate testing; public Traefik routing
     moves only after candidate Deployment readiness and smoke success.
   - For a config skeleton, read
     `references/project-config-template.md` only when creating a new config.

7. Document custom secrets exactly.
   - Required path format:
     `tmprl-cloud-demo/<project-name>/<secret-name>`.
   - Every JSON key in the AWS secret becomes an environment variable for
     components that list the secret.
   - Keep secrets narrow and attach them only to components that need them.
   - Project Temporal credentials are platform-managed and use:
     `tmprl-cloud-demo/<project-name>/temporal`.
   - The Temporal Cloud bootstrap credential is platform-level and uses:
     `tmprl-cloud-demo/platform/temporal-cloud-bootstrap`.
   - For Agent Harness, document the required custom secret path:
     `tmprl-cloud-demo/temporal-agent-harness/anthropic-credentials`.

8. Run registry checks from the registry repo after config changes.

```sh
/tmp/cluster-gitops-config-venv/bin/python scripts/validate_projects.py
/tmp/cluster-gitops-config-venv/bin/python scripts/generate_projects.py --check
/tmp/cluster-gitops-config-venv/bin/python -m py_compile scripts/validate_projects.py scripts/generate_projects.py $(find registry/worker/src -name '*.py' | sort)
```

If generation is out of date, run:

```sh
/tmp/cluster-gitops-config-venv/bin/python scripts/generate_projects.py
```

9. Explain what remains manual.
   - In `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=plan`, ECR and secret paths are
     recorded but not mutated.
   - In opt-in `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`, the operator can
     create/verify ECR repositories and verify Secrets Manager paths plus JSON
     properties for platform-managed Temporal secrets.
   - In `DEMO_REGISTRY_BUILD_MODE=plan`, images must come from annotations or
     an external build system.
   - In opt-in `DEMO_REGISTRY_BUILD_MODE=local-docker`, a trusted isolated
     worker can clone, build, push immutable ECR tags, and write build status.
   - In opt-in `DEMO_REGISTRY_BUILD_MODE=aws-codebuild`, the operator starts a
     configured CodeBuild project that performs the clone/build/push work with
     narrow AWS-side credentials.
   - Project Temporal Cloud Namespace/API key creation and rotation remain
     manual until provider automation is reviewed.
   - AWS Secrets Manager values for custom secrets, project Temporal
     credentials, and the Temporal Cloud bootstrap credential remain manual.
   - Shared auth prerequisites for auth-protected demos.

10. Point the SA at lifecycle status.
    - `DemoProject.status` is the source of truth for external resources,
      build status, candidate rollout, readiness, smoke checks, active version,
      and last error.
    - Generated `registry.json` includes `catalogStatus.configMap` for each
      project.
    - The operator writes compact lifecycle summaries to
      `demo-registry-system/demo-registry-catalog-status` under
      `<project>.json`.

## Existing Kubernetes Or Flux

If the target repo already has Kubernetes, Kustomize, Helm, or Flux:

- assess what it does and whether it reveals useful ports, probes, env vars, or
  service boundaries;
- explain migration options;
- prefer platform-managed `DemoProject` deployment for the first path;
- mention future BYO-image mode only as future work;
- do not add arbitrary project-owned overlays as the default path.
