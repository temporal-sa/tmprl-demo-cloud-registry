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

Each `DemoProject` entity workflow continues as new after 50 completed
reconciles, or sooner when Temporal suggests history rollover. The workflow
carries its latest status and any queued reconcile signal into the next run.

External resource reconciliation runs before Kubernetes reconciliation. In the
default `plan` mode it records expected ECR repositories, Secrets Manager
paths, and Temporal Cloud resources in `DemoProject.status.externalResources`
without mutating cloud providers. In `aws` mode it creates or verifies missing
ECR repositories and verifies required Secrets Manager paths plus JSON property
names for platform-managed Temporal credentials. Secret values and Temporal
Cloud namespace/API key lifecycle remain manual until those provider contracts
are implemented.
`DemoProject.status.externalResources.temporalCloud.tcldCommandPlan` records
the verified `tcld` namespace, scoped service-account, and API-key command
sequence needed for the manual step.

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
rollout status. In opt-in `aws-codebuild` mode the worker starts a configured
AWS CodeBuild project with source, buildspec, and ECR image overrides, waits
for completion, and records the immutable images pushed by the build. CodeBuild
execution IDs and CloudWatch log references are recorded under
`DemoProject.status.buildStatus.codebuild` and mirrored into catalog status.
The same resolved image map drives candidate resources and promoted Traefik
routes, so `aws-codebuild` does not require image override annotations.

When a project has a smoke check, the workflow waits for every versioned
candidate Deployment to report available replicas before running the smoke
check. If readiness times out, the active route is not promoted and
`DemoProject.status.readinessStatus` records the Deployment states.
Runtime ExternalSecret sync is also recorded under
`DemoProject.status.externalSecretStatus`, including each ExternalSecret's
Ready condition and controller message.
If AWS Secrets Manager verification reports required values missing or
malformed, the reconcile phase becomes `WaitingForExternalResources`, records
`DemoProject.status.externalResourceBlock`, and holds back runtime
Deployments/Services/routes while still creating ExternalSecrets for sync
visibility.
ExternalSecrets are reconciled before workload resources, so their sync status
is visible even while images are missing or a build has failed.

Every `record_project_status` activity also writes a compact catalog lifecycle
entry to the configured status ConfigMap. By default each project is written as
`demo-registry-system/demo-registry-catalog-status` key `<project>.json`, with
phase, namespace, external-resource blockers, external-secret sync, build,
external-resource, candidate, readiness, smoke-check, and active version fields
for the registry/catalog UI.

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
- `DEMO_REGISTRY_BUILD_MODE`, `plan`, `local-docker`, or `aws-codebuild`,
  default `plan`
- `DEMO_REGISTRY_BUILD_WORKSPACE`, default `/tmp/demo-registry-builds`
- `DEMO_REGISTRY_CODEBUILD_PROJECT`, required for `aws-codebuild`
- `DEMO_REGISTRY_CODEBUILD_POLL_SECONDS`, default `15`
- `DEMO_REGISTRY_CODEBUILD_TIMEOUT_MINUTES`, default `60`
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

`aws-codebuild` mode requires a pre-created CodeBuild project whose service
role has read access to the `temporal-sa` source repositories and push access
only to `tmprl-cloud-demo/<project>/<build>` ECR repositories. The registry
operator role only needs `codebuild:StartBuild` and `codebuild:BatchGetBuilds`
for that project plus read/create/verify permissions for the ECR repositories
it passes to the build.

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
