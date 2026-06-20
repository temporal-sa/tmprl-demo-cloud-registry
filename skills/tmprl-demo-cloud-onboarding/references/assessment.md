# Assessment And Adapter Decisions

Use this reference before proposing YAML in the onboarding report. The goal is
to decide whether the target repo can run on tmprl-demo.cloud with configuration
only, or whether the repo needs code, Dockerfile, or runtime adapter work.

This reference is for assessment, not implementation. Do not edit the target
repo, registry repo, secrets, or cluster while using it unless the user has
explicitly asked for a follow-up implementation task.

## What To Inspect

Read enough of the repo to answer these questions:

- What processes exist? Identify web UI, API/backend, Temporal workers, one-off
  jobs, migrations, local databases, caches, and supporting services.
- How are those processes started locally? Check Dockerfiles, Compose,
  package scripts, Procfiles, Makefiles, README commands, Kubernetes manifests,
  Helm values, and code entrypoints.
- Which processes are long-running? `DemoProject` components are long-running
  containers or Lambda image workers, not arbitrary local scripts.
- Which process is public? Identify browser entrypoint, API routes, ports, and
  whether a frontend proxy routes paths like `/api` to a backend.
- Which non-worker services talk to Temporal? Any API/backend that starts,
  signals, queries, lists, or describes workflows needs Temporal credentials.
- What does each Temporal client expect? Env var names, namespace defaults,
  TLS/API-key settings, mTLS PEM env vars, mTLS file paths, task queues, worker
  deployment/build IDs, and whether the code assumes `localhost:7233`.
- What secrets are needed? Note every env var or file that carries credentials,
  API tokens, model keys, database URLs, OAuth config, or source auth.
- Is config env-driven? A repo that reads env vars can usually be onboarded
  without code changes. Hard-coded hosts, namespaces, ports, secret file paths,
  and credentials usually require adapter work unless an existing helper fits.
- Does the app require state? External databases, queues, object stores, or
  durable local volumes are not created by ordinary `DemoProject` components.
- Are repo paths clean? In monorepos, identify which paths should trigger
  rebuilds and which docs/examples should be ignored.

## Prefer These Platform Helpers Before Code Changes

- Service DNS: components with `ports` get stable DNS by component name, e.g.
  `http://backend:8000`. Use literal component env vars for internal URLs.
- Ingress routes: expose multiple public paths without changing app code, e.g.
  `/api` to backend and `/` to frontend.
- Env var remapping: map platform values to app-specific names with
  `components.<name>.env[].valueFrom.inputRef`.
- User secrets: put JSON secrets in AWS Secrets Manager under
  `tmprl-dem-cld/<project>/<secret>` and inject them by `components.*.secrets`
  or targeted `secrets.<secret>.<PROPERTY>` refs.
- Temporal proxy: for Kubernetes components that only know how to connect to a
  local plaintext Temporal server, use `temporalProxy.enabled: true` instead of
  changing the app.
- mTLS file path envs: if code expects PEM paths from env vars, use
  `temporalMtls.certFiles.pathEnv`.
- Lambda wrapper: for serverless Lambda image workers, the platform wrapper can
  fetch Secrets Manager values, set env vars, and write mTLS PEM files under
  `/tmp/tmprl-demo-cloud/temporal` before the app starts.
- Source filters: use `source.gitRepository.sparseCheckout` or `ignore` for
  monorepos so unrelated changes do not trigger image rebuilds or worker gates.
- Buildpack builds: if the repo lacks Dockerfiles but is a conventional app,
  use a buildpack build and optional generated `processes`.

## When Adapter Work Is Needed

Report adapter work instead of pretending YAML will fix these:

- No buildable Dockerfile and no buildpack-compatible app shape.
- Runtime command cannot be expressed as Dockerfile entrypoint, buildpack
  process, or component `command`/`args`.
- Required port or health endpoint is unknown and cannot be inferred safely.
- Public app needs a route shape the platform cannot express.
- App hard-codes Temporal host/namespace/auth and cannot use env remaps or
  Temporal proxy.
- App requires mTLS files at fixed absolute paths on Lambda. Lambda only
  materializes files under `/tmp/tmprl-demo-cloud/temporal`; code must read
  path env vars or the platform must be enhanced.
- App expects secret files or non-JSON secret formats that cannot be mapped from
  AWS Secrets Manager JSON properties.
- App requires broad AWS permissions or cross-project secret access.
- App needs Kubernetes API access but does not tolerate the scoped service
  account model.
- App needs local persistent volumes, databases, or brokers that are not already
  externalized.
- App depends on local development URLs like `host.docker.internal` with no env
  override.

## How To Report Required Changes

If adapter work is needed, give a concise implementation report:

- Blocker: exact behavior that prevents deployment.
- Evidence: file/path/function/config that proves it.
- Minimal fix: smallest repo change or Dockerfile change that would make the
  platform config work.
- Config after fix: the `DemoProject` field that will consume the new behavior.

Prefer changes that make runtime values configurable through env vars, because
that keeps future rollouts and auth-mode changes platform-managed.

Do not make those changes in the assessment response. The user can turn the
report into an implementation request after reviewing the tradeoffs.
