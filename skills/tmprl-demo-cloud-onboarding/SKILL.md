---
name: tmprl-demo-cloud-onboarding
description: Use when assessing a Temporal SA demo repository for tmprl-demo.cloud onboarding and producing a user-facing report, without making code or file changes by default. Covers deciding whether code or Docker adapters are needed, drafting DemoProject YAML in the report, documenting ingress, env vars, AWS Secrets Manager secrets, Temporal API key or mTLS credential wiring, serverless Lambda workers, build options, source auth, catalog metadata, validation, and current platform limitations. Only make repository changes after the user explicitly asks for implementation.
---

# Tmprl Demo Cloud Onboarding

Use this skill to help an agent produce an onboarding report for an existing
Temporal SA demo. The report should explain whether the demo can fit
tmprl-demo.cloud as-is, what adapters are needed, and what `DemoProject` YAML
would be appropriate.

By default, this skill is **read-only**: inspect code, inspect the platform
contract, and report findings. Do not edit source code, Dockerfiles, project
YAML, schemas, platform code, docs, secrets, or cluster resources unless the
user explicitly follows up asking for implementation.

The platform contract is deliberately thin: **one `DemoProject` CR is committed;
Flux applies it; the registry operator derives and reconciles everything else.**
Do not add Kubernetes overlays, Helm charts, generated manifests, ECR tags,
Temporal namespace names, or per-project Flux config unless the platform
contract changes.

## First Steps

1. Inspect the target repo before proposing YAML.
   - Entrypoints, Dockerfiles, compose files, package scripts, ports, health
     endpoints, frontend/backend proxying, workers, task queues, Temporal client
     setup, config files, environment variables, and secret assumptions.
   - Identify which processes are long-running services, which are Temporal
     workers, and which non-worker services start/query workflows.
2. Inspect the registry contract before proposing YAML.
   - Current source of truth: `projects/README.md`, `schemas/project.schema.json`,
     and `scripts/validate_projects.py`.
   - Existing examples live under `projects/demo/`; do not copy
     `projects/demo/registry.yaml` as an ordinary app template because it is the
     control-plane/self-management project.
3. Decide whether the target can fit the platform as-is.
   - Prefer config-only adaptation using env var remaps, service DNS, source
     filters, secret refs, Temporal proxy, mTLS file path envs, or Lambda secret
     materialization.
   - If the app has hard-coded service URLs, hard-coded Temporal hosts,
     non-env-driven secrets, missing build inputs, missing runtime entrypoints,
     incompatible stateful dependencies, or unsupported Lambda requirements,
     report required adapter changes before proposing a “final” YAML.
4. Produce a report, not a patch.
   - Include findings, adapter decisions, required secrets, ingress/env/Temporal
     wiring choices, limitations, and a draft `DemoProject` YAML when useful.
   - Separate “works with config only” from “requires code/Docker changes”.
   - Stop there unless the user explicitly asks you to implement the report.

## Reference Map

Read only the references needed for the current task:

- `references/assessment.md`: repo inspection checklist and how to decide
  whether code/Docker adapters are needed.
- `references/demo-project-contract.md`: all core `DemoProject` fields,
  including source, builds, components, ports, env, ingress, smoke checks,
  worker settings, manual/external runtime limitations, catalog metadata,
  rollouts, and validation.
- `references/temporal-auth.md`: API key, mTLS, Temporal proxy, Worker
  Versioning, mTLS files, runtime secret properties, and env remapping options.
- `references/secrets.md`: user-managed AWS Secrets Manager secrets, source auth
  secrets, component secret injection, targeted `valueFrom` mappings, Lambda
  direct secret reads, and safe AWS CLI examples.
- `references/serverless-lambda.md`: AWS Lambda image worker config, wrapper
  behavior, dynamic vs fixed Worker Deployment build IDs, `/tmp` mTLS
  materialization, secret ARN envs, unsupported fields, IAM implications, and
  when Lambda is a bad fit.
- `references/examples.md`: compact YAML examples for common patterns.

## Operating Rules

- Keep `metadata.name` concise, lowercase, DNS-label-safe, and ideally
  25 characters or less. The operator derives `tmprl-dem-cld-<name>` and
  Temporal Cloud namespace names cap at 39 characters before account suffix.
- Keep `source.repo` in `https://github.com/temporal-sa/<repo>`.
- When reporting a YAML path, use `projects/demo/<metadata.name>.yaml`.
- Add only fields that are needed. Defaults are part of the contract.
- Do not put secret values in YAML. Use AWS Secrets Manager paths under
  `tmprl-dem-cld/<project>/...`.
- Do not assume an app needs code changes until you have tried the platform
  helpers documented in the references.
- Do not make code, Dockerfile, config, YAML, docs, secret, or cluster changes
  during the assessment/report phase.
- If the user explicitly asks for implementation later and you make a YAML
  change in this repo, run:

```sh
python3 scripts/validate_projects.py
```

If Python dependencies are missing, install `requirements-dev.txt` in the
registry repo first.
