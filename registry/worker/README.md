# Demo Registry Worker

Python Temporal worker for registry/operator workflows.

The process can host Temporal workflows, run the Kubernetes watch loop for
`DemoProject` custom resources, or both. The bootstrapped operator uses this to
split the singleton watcher from the versioned Temporal worker: the watcher
Deployment has `DEMO_REGISTRY_WORKER_ENABLED=false`, while the Worker Controller
`WorkerDeployment` can use `DEMO_REGISTRY_WATCH_ENABLED=auto` so only the
current ready WorkerDeployment build runs the watch loop.

During reconcile it creates or patches:

- project namespace;
- component service accounts and tenant-baseline RBAC;
- platform metadata ConfigMap;
- versioned candidate Deployments and Services when every build has an
  immutable image override;
- Temporal Worker Controller `Connection` and `WorkerDeployment` resources for
  Kubernetes components that opt into `worker.versioning.enabled: true`;
- Traefik middleware and IngressRoutes after a configured smoke check passes,
  or immediately for projects without a smoke check.

Each `DemoProject` entity workflow continues as new after 50 completed
reconciles, or sooner when Temporal suggests history rollover. The workflow
carries its latest status and any queued reconcile signal into the next run.

External resource reconciliation runs before Kubernetes reconciliation. The
operator creates or verifies ECR repositories, verifies required Secrets Manager
paths and JSON property names, and manages Temporal Cloud namespace/service
account/API-key lifecycle through the Cloud Operations API. Project-owned secret
values still have to be created outside git before External Secrets can sync
them.

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
Build failures are reported on `DemoProject.status.buildStatus`, set the
top-level phase to `Error`, and set
`DemoProject.status.candidateRolloutStatus.phase=BuildFailed`.

When a project has a smoke check, the workflow waits for every versioned
candidate Deployment to report available replicas before running the smoke
check. If readiness times out, the active route is not promoted and
`DemoProject.status.readinessStatus` records the Deployment states.
When a worker component opts into Worker Controller versioning, reconcile
renders a project-local `Connection` and `WorkerDeployment`, mirrors
`WorkerDeployment.status` into `DemoProject.status.workerDeploymentStatus`, and
waits for the controller rollout to become ready before public route promotion.
Before a failed candidate is deleted, the Kubernetes runtime workflow captures
bounded pod status, namespace Events, and current/previous container logs as a
gzip-compressed JSON object in the configured S3 diagnostics bucket. The durable
object reference is recorded under `DemoProject.status.diagnosticsStatus.s3Uri`
and retained across later successful reconciles as `lastFailedDiagnostics`.
If the S3 upload fails, route rollback still runs but candidate deletion is
blocked so the Kubernetes evidence is not silently lost.
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
`tmprl-dem-cld-registry-operator/registry-catalog-status` key `<project>.json`, with
phase, namespace, external-resource blockers, external-secret sync, build,
external-resource, candidate, readiness, smoke-check, and active version fields
for the registry/catalog UI. The worker also mirrors the same keys into
`DEMO_REGISTRY_CATALOG_STATUS_MIRROR_NAMESPACE` so the self-managed registry web
Deployment can mount the ConfigMap in its own namespace.

Environment variables:

- `TEMPORAL_ADDRESS`, default `localhost:7233`
- `TEMPORAL_NAMESPACE`, default `default`
- `TEMPORAL_API_KEY`, optional
- `TEMPORAL_TLS`, default `false`
- `DEMO_REGISTRY_TASK_QUEUE`, default `registry-management`
- `DEMO_REGISTRY_ACTIVITY_WORKERS`, default `20`
- `DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET`, optional; private S3 bucket for
  failed Kubernetes rollout evidence. Blue/green configure the Tofu-managed
  platform diagnostics bucket, whose objects expire after 30 days.
- `DEMO_REGISTRY_WORKER_ENABLED`, default `true`
- `DEMO_REGISTRY_WATCH_ENABLED`, default `false`; accepts `true` for the legacy
  always-on watcher, `auto` to run only from the current ready WorkerDeployment
  build, or `bootstrap` to run only until the handoff WorkerDeployment has a
  current ready build.
- `DEMO_REGISTRY_WATCH_MODE`, optional; when set, it overrides the mode parsed
  from `DEMO_REGISTRY_WATCH_ENABLED`. This lets bootstrap manifests keep
  `DEMO_REGISTRY_WATCH_ENABLED=true` for old images while new images run in
  `bootstrap` handoff mode.
- `DEMO_REGISTRY_WATCH_HANDOFF_DEPLOYMENT`, default
  `<DEMO_REGISTRY_SOURCE_WATCH_NAMESPACE>/registry-operator`; used by
  `bootstrap` mode.
- `DEMO_REGISTRY_WATCH_INTERVAL_SECONDS`, default `15`
- `DEMO_REGISTRY_RECONCILE_MODE`, `apply`
- `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE`, `aws`
- `DEMO_REGISTRY_BUILD_MODE`, `aws-codebuild`
- `DEMO_REGISTRY_CODEBUILD_PROJECT`, required for `aws-codebuild`
- `DEMO_REGISTRY_CODEBUILD_POLL_SECONDS`, default `30`
- `DEMO_REGISTRY_CODEBUILD_TIMEOUT_MINUTES`, default `30`
- `DEMO_REGISTRY_READINESS_TIMEOUT_SECONDS`, default `300`
- `DEMO_REGISTRY_READINESS_POLL_SECONDS`, default `5`
- `DEMO_REGISTRY_AWS_REGION`, default from `AWS_REGION`,
  `AWS_DEFAULT_REGION`, then `us-west-1`
- `DEMO_REGISTRY_PUBLIC_ECR_REGION`, default `us-east-1`; ECR Public control
  plane region for managed Instruqt course images
- `DEMO_REGISTRY_AWS_ACCOUNT_ID`, optional status metadata
- `DEMO_REGISTRY_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH`, default
  `tmprl-dem-cld/platform/temporal-cloud-bootstrap`
- `DEMO_REGISTRY_TEMPORAL_CLOUD_ACCOUNT_ID`, optional status metadata; falls
  back to `TEMPORAL_CLOUD_ACCOUNT_ID`
- `DEMO_REGISTRY_TEMPORAL_CLOUD_REGION`, operator-wide default namespace region
  (falls back to `TEMPORAL_CLOUD_REGION`, then `gcp-us-central1`); overridden
  per project by `spec.temporal.region`
- `DEMO_REGISTRY_TEMPORAL_CLOUD_SERVERLESS_REGION`, default `aws-ca-central-1`;
  the default namespace region for projects with serverless (AWS Lambda) workers
  when `spec.temporal.region` is omitted, so Temporal's Lambda compute provider
  can reach the worker
- `DEMO_REGISTRY_TEMPORAL_CLOUD_MODE`, `cloud-api`
- `DEMO_REGISTRY_TEMPORAL_CLOUD_API_VERSION`, default `v0.17.0` (sent as the
  `temporal-cloud-api-version` header to the Cloud Operations API)
- `DEMO_REGISTRY_TEMPORAL_CLOUD_OPS_HOST`, default `saas-api.tmprl.cloud:443`
- `DEMO_REGISTRY_NAMESPACE_READERS_GROUP`, default `tmprl-demo-cld-read`;
  the Temporal Cloud user group granted exactly Read on every managed demo
  namespace (see "Namespace readers group" below). `none`/`off` disables it.
- `DEMO_REGISTRY_CATALOG_STATUS_NAMESPACE`, default `tmprl-dem-cld-registry-operator`
- `DEMO_REGISTRY_CATALOG_STATUS_CONFIGMAP`, default
  `registry-catalog-status`
- `DEMO_REGISTRY_CATALOG_STATUS_MIRROR_NAMESPACE`, default
  `tmprl-dem-cld-registry`
- `DEMO_REGISTRY_OPERATOR_IMAGE`, optional; when set by the operator
  WorkerDeployment, `RegistryOperatorCanaryWorkflow` verifies it against
  `expectedOperatorImage` gate input.
- `DEMO_REGISTRY_OPERATOR_SOURCE_SHA`, optional; when set by the operator
  WorkerDeployment, `RegistryOperatorCanaryWorkflow` verifies it against
  `expectedOperatorSourceSha` gate input.
- `INSTRUQT_TOKEN`, synced from `tmprl-dem-cld/platform/instruqt-cli`; required
  for managed `InstruqtCourse` force-push/delete activities. Legacy
  validate/test/deploy activities remain registered so pinned workflows can drain.
  CLI/token readiness is recorded under `status.cli`.

`aws-codebuild` mode requires a pre-created CodeBuild project whose service
role has read access to the `temporal-sa` source repositories and push access
only to `tmprl-dem-cld/<project>/<build>` ECR repositories. Managed
`InstruqtCourse` images are published to ECR Public repositories with the same
repository path shape: `tmprl-dem-cld/<course>/<image>`. The registry operator
role only needs `codebuild:StartBuild` and `codebuild:BatchGetBuilds` for that
project plus read/create/verify permissions for the ECR repositories it passes
to the build.

### Namespace readers group

One Temporal Cloud user group (`DEMO_REGISTRY_NAMESPACE_READERS_GROUP`, default
`tmprl-demo-cld-read`) holds exactly Read on every demo namespace the operator
manages, so anyone in it can read them all. The group has no account role, and
the operator never changes anyone's account role or personal namespace
permissions; it only keeps the group's grants in sync and adds members.

- An account admin creates the group once (a Cloud group, no account role, no
  namespace access). The operator never creates groups.
- `SyncNamespaceReadersWorkflow` (fixed id `namespace-readers/sync`, shared by
  blue and green) runs once whenever an operator's watch activates and grants
  the group exactly Read on every namespace tagged `owner=tmprl-demo.cloud`.
- Each `DemoProjectWorkflow` reconcile does the same for its own namespace right
  after tagging it (best-effort; it never fails or rolls back the demo). A
  higher permission found on a managed namespace is set back to Read.
- Deleting a namespace first removes the group's access to it (best-effort).
- `GrantNamespaceReadAccessWorkflow` backs the catalog's "Request Namespace
  Access" button: the web server starts it with the signed-in user's email, and
  it adds that Temporal Cloud user to the group. A missing Cloud user is
  reported back ("user-not-found"), never invited.

Granting the group access on a namespace needs Namespace Admin on it (a
Developer has that on every namespace it creates). Adding group members, and
setting or editing namespace tags (even at creation), are account-level changes
that Temporal Cloud only allows Account Owner and Global Admin identities to
make, so the operator's control-plane key stays Global Admin for now. A custom
role limited to managing this group's membership would let it drop that.

On the active blue cluster, non-secret runtime settings for the bootstrap
handoff live in
`clusters/blue/bootstrap/registry-operator/kustomization.yaml` as a Kustomize
`configMapGenerator`. Changing that generator rolls the operator through Flux
because the generated ConfigMap name changes.

`cloud-api` mode uses the Temporal Cloud Operations API through the bundled
`temporalio` SDK. It runs with `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`
because it needs Secrets Manager to read the bootstrap credential and write the
runtime key. It will create-or-verify the project namespace (with delete
protection), a namespace-scoped service account, and a runtime API key, then
write `address`/`namespace`/`apiKey`/`tls` to the project's Temporal secret path
so External Secrets can sync it. Notes:

- The namespace region is resolved by precedence: `spec.temporal.region` (if
  set) > the serverless AWS default (`DEMO_REGISTRY_TEMPORAL_CLOUD_SERVERLESS_REGION`,
  `aws-ca-central-1`) for projects with AWS Lambda workers > the operator-wide
  default (`TEMPORAL_CLOUD_REGION`, default `gcp-us-central1`). The operator
  queries `GetRegions` and fails fast if the resolved region is not available to
  the account instead of leaving a namespace stuck in `ACTIVATION_FAILED`.
- A Temporal Cloud namespace's region is immutable, so when the resolved region
  of an existing project changes the operator tears the namespace down (deleting
  its data) and recreates it in the new region, then re-mints runtime
  credentials. The migration is reported under
  `status.externalResources.temporalCloud.regionMigration`.
- The runtime address is the per-namespace endpoint
  `<namespace>.tmprl.cloud:7233`.
- A newly minted API key takes a few seconds to propagate to the data plane.
  Because namespace/key creation runs before the image build and workload
  deploy, the key is live by the time the worker pod connects; workers also log
  one benign `describe_namespace` `PermissionDenied` WARN at startup (a
  capability probe — namespace-scoped service-account keys are worker-scoped by
  design).
- Keys are minted with a 30-day TTL; the entity workflow stores `expiryTime`
  and sleeps durably until 10 days before expiry. Rotation mints a replacement,
  writes AWS Secrets Manager, patches the existing Kubernetes runtime Secret
  when the namespace already exists, and rolls dependent Pods by rendering a new
  candidate version from the new `keyId`. The old key is left in place to expire
  naturally, giving the readiness/smoke path an overlap window.

As a manual fallback, annotate a `DemoProject` with immutable images for every
declared build:

```sh
kubectl annotate demoproject registry \
  'registry.tmprl-demo.cloud/image-overrides={"web":"<account>.dkr.ecr.<region>.amazonaws.com/tmprl-dem-cld/registry/web:<sha>","worker":"<account>.dkr.ecr.<region>.amazonaws.com/tmprl-dem-cld/registry/worker:<sha>"}' \
  --overwrite
```

Optionally record the source revision that produced those images:

```sh
kubectl annotate demoproject registry \
  registry.tmprl-demo.cloud/source-commit-sha=<git-sha> \
  --overwrite
```
