# Demo Registry Worker

Python Temporal worker for registry/operator workflows.

The worker hosts Temporal workflows and can also run a Kubernetes watch loop for
`DemoProject` custom resources. In `plan` mode it renders the resources it would
create. In `apply` mode it creates or patches the first operator-managed
Kubernetes slice:

- project namespace;
- component service accounts and tenant-baseline RBAC;
- platform metadata ConfigMap;
- versioned candidate Deployments and Services when every build has an
  immutable image override;
- Traefik middleware and IngressRoutes after a configured smoke check passes,
  or immediately for projects without a smoke check.

External resource reconciliation runs before Kubernetes reconciliation. In the
default `plan` mode it records expected ECR repositories, Secrets Manager
paths, and Temporal Cloud resources in `DemoProject.status.externalResources`
without mutating cloud providers. In `aws` mode it creates or verifies missing
ECR repositories and verifies required Secrets Manager paths plus JSON property
names for platform-managed Temporal credentials. Secret values and Temporal
Cloud namespace/API key lifecycle remain manual until those provider contracts
are implemented.

Candidate resource names use `git-<source-sha>` when the
`registry.tmprl-demo.cloud/source-commit-sha` annotation is present. Public
Traefik routes point at the versioned candidate Services only after the
configured smoke check succeeds, so a failed candidate does not repoint the
existing healthy route.

Image build resolution runs after external resources and before Kubernetes
runtime reconciliation. In the default `plan` mode the worker records build
intent in `DemoProject.status.buildStatus` and keeps waiting for images. If a
project still has immutable image override annotations, those overrides are
treated as already resolved images. In opt-in `local-docker` mode the worker
clones the configured source repo, checks out the requested commit when
provided, builds each configured Dockerfile with `docker build`, pushes
`git-<source-sha>` tags, and passes the resolved image map into candidate
rollout status.

When a project has a smoke check, the workflow waits for every versioned
candidate Deployment to report available replicas before running the smoke
check. If readiness times out, the active route is not promoted and
`DemoProject.status.readinessStatus` records the Deployment states.

Every `record_project_status` activity also writes a compact catalog lifecycle
entry to the configured status ConfigMap. By default each project is written as
`demo-registry-system/demo-registry-catalog-status` key `<project>.json`, with
phase, build, external-resource, candidate, readiness, smoke-check, and active
version fields for the registry/catalog UI.

Environment variables:

- `TEMPORAL_ADDRESS`, default `localhost:7233`
- `TEMPORAL_NAMESPACE`, default `default`
- `TEMPORAL_API_KEY`, optional
- `TEMPORAL_TLS`, default `false`
- `DEMO_REGISTRY_TASK_QUEUE`, default `demo-registry-management`
- `DEMO_REGISTRY_ACTIVITY_WORKERS`, default `20`
- `DEMO_REGISTRY_WATCH_ENABLED`, default `false`
- `DEMO_REGISTRY_WATCH_INTERVAL_SECONDS`, default `15`
- `DEMO_REGISTRY_RECONCILE_MODE`, `plan` or `apply`, default `plan`
- `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE`, `plan` or `aws`, default `plan`
- `DEMO_REGISTRY_BUILD_MODE`, `plan` or `local-docker`, default `plan`
- `DEMO_REGISTRY_BUILD_WORKSPACE`, default `/tmp/demo-registry-builds`
- `DEMO_REGISTRY_READINESS_TIMEOUT_SECONDS`, default `300`
- `DEMO_REGISTRY_READINESS_POLL_SECONDS`, default `5`
- `DEMO_REGISTRY_AWS_REGION`, default from `AWS_REGION`,
  `AWS_DEFAULT_REGION`, then `us-west-1`
- `DEMO_REGISTRY_AWS_ACCOUNT_ID`, optional status metadata
- `DEMO_REGISTRY_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH`, default
  `tmprl-cloud-demo/platform/temporal-cloud-bootstrap`
- `DEMO_REGISTRY_TEMPORAL_CLOUD_ACCOUNT_ID`, optional status metadata; falls
  back to `TEMPORAL_CLOUD_ACCOUNT_ID`
- `DEMO_REGISTRY_TEMPORAL_CLOUD_REGION`, optional status metadata; falls back
  to `TEMPORAL_CLOUD_REGION`
- `DEMO_REGISTRY_CATALOG_STATUS_NAMESPACE`, default `demo-registry-system`
- `DEMO_REGISTRY_CATALOG_STATUS_CONFIGMAP`, default
  `demo-registry-catalog-status`

`local-docker` build mode requires `git`, `docker`, network read access to the
project source repo, Docker daemon access, and ECR push credentials scoped to
the project/build repositories. Do not enable it on the long-running registry
operator pod unless those permissions are intentionally isolated.

As a manual fallback, annotate a `DemoProject` with immutable images for every
declared build:

```sh
kubectl annotate demoproject demo-registry \
  'registry.tmprl-demo.cloud/image-overrides={"web":"<account>.dkr.ecr.<region>.amazonaws.com/tmprl-cloud-demo/demo-registry/web:<sha>","worker":"<account>.dkr.ecr.<region>.amazonaws.com/tmprl-cloud-demo/demo-registry/worker:<sha>"}' \
  --overwrite
```

Optionally record the source revision that produced those images:

```sh
kubectl annotate demoproject demo-registry \
  registry.tmprl-demo.cloud/source-commit-sha=<git-sha> \
  --overwrite
```
