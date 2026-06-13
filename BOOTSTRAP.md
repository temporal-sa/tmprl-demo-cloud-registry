# Bootstrap And First Deployment Test

> **Architecture update:** The platform moved to an operator-derives model with
> no generation step. `DemoProject` CRs are authored directly under `projects/`
> and Flux's `registry-projects` Kustomization applies them from there;
> `clusters/sa-demo/generated/` and `scripts/generate_projects.py` were removed,
> and `plan` modes were dropped (the operator provisions and builds for real).
> The `dejavu-tacos` first-test walkthrough below (manual image overrides, manual
> Temporal/secret setup, `BUILD_MODE=plan`) is retained as a historical bootstrap
> record. For the current authoring contract see `projects/README.md` and
> `schemas/project.schema.json`; for cluster layout see
> `clusters/sa-demo/README.md`.

This repo has two layers:

- `clusters/sa-demo/bootstrap/` installs the minimum registry/operator control
  plane: `DemoProject` CRD, registry operator Deployment, RBAC, and platform
  ExternalSecrets.
- `projects/` holds the `DemoProject` CRs that Flux applies and the operator
  reconciles. Adding a demo is dropping one CR file there — nothing is generated
  or otherwise committed.

Use these instructions for the first `sa-demo` test. Replace every placeholder
before applying to a real cluster.

## Prerequisites

- AWS account: `<aws-account-id>`
- AWS region: `<aws-region>`, for example `us-east-1`
- EKS cluster: `<eks-cluster-name>`
- Kubernetes context: `<kube-context>`
- DNS zone: `tmprl-demo.cloud`
- External Secrets Operator installed with a `ClusterSecretStore` named
  `platform-secrets`
- Traefik installed with `traefik.io/v1alpha1` `IngressRoute` and `Middleware`
  CRDs
- Flux installed or a human applying the same kustomizations with `kubectl`
- IRSA/OIDC enabled for the EKS cluster

## Temporal Cloud Bootstrap Credential

The registry/operator needs a high-privilege Temporal Cloud machine identity
only for platform reconciliation. Create a Temporal Cloud Service Account or the
current equivalent machine identity for the operator.

As of 2026-06-12, Temporal Cloud documents Service Accounts as machine
identities that use API keys, namespace-scoped Service Accounts for single
Namespace access, API keys for Cloud Ops API/Terraform/tcld automation, and
Cloud Ops permissions such as `CreateNamespace`, `CreateApiKey`, and
`CreateServiceAccount`. Verify the current names and permission model again
before enabling destructive provider automation.

Grant the bootstrap identity only the permissions required to:

- create/read/update demo Namespaces;
- create/read/update namespace-scoped Service Accounts or API keys for demo
  Namespaces;
- rotate project-scoped API keys;
- read the account/regions needed for generated Namespace endpoints.

Prefer the narrowest supported credential. Create a long-lived bootstrap API key
only if Temporal Cloud still requires that model for this automation.

Store the credential outside git:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-dem-cld/platform/temporal-cloud-bootstrap \
  --secret-string '{
    "TEMPORAL_CLOUD_API_KEY": "<bootstrap-cloud-ops-api-key>",
    "TEMPORAL_CLOUD_ACCOUNT_ID": "<temporal-account-id>",
    "TEMPORAL_CLOUD_REGION": "<temporal-region>"
  }'
```

## Registry Worker Temporal Credential

The registry worker itself runs Temporal workflows. Create a dedicated Temporal
Cloud Namespace and API key for the registry worker, then store the SDK
connection values:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-dem-cld/platform/registry-worker-temporal \
  --secret-string '{
    "TEMPORAL_ADDRESS": "tmprl-dem-cld-registry-operator.<temporal-account-id>.tmprl.cloud:7233",
    "TEMPORAL_NAMESPACE": "tmprl-dem-cld-registry-operator.<temporal-account-id>",
    "TEMPORAL_API_KEY": "<registry-worker-namespace-api-key>",
    "TEMPORAL_TLS": "true"
  }'
```

## Project Temporal Cloud Resources

The generated external resource plan and
`DemoProject.status.externalResources.temporalCloud.tcldCommandPlan` now record
the current `tcld` command contract for each project. Verify the target account
and region, then run equivalent commands with the platform bootstrap API key
outside git:

```sh
export TEMPORAL_CLOUD_API_KEY=<bootstrap-cloud-ops-api-key>

tcld namespace get \
  --namespace <project-name>

tcld namespace create \
  --namespace <project-name> \
  --region <temporal-region> \
  --auth-method api_key \
  --enable-delete-protection

tcld service-account create-scoped \
  --name <project-name>-runtime \
  --namespace-permission <project-name>=Write

tcld apikey create \
  --name <project-name>-runtime \
  --description "tmprl-demo.cloud runtime key for <project-name>" \
  --duration 365d \
  --service-account-id <service-account-id>
```

Copy the generated API key secret immediately; Temporal Cloud does not let you
retrieve it later. Store the SDK connection values in:

```text
tmprl-dem-cld/<project-name>/temporal
```

using the JSON shape documented below. Full mutation from the operator remains
manual until the provider flow can safely create the API key and write the
one-time secret to AWS Secrets Manager without exposing it in logs/status.

## AWS Bootstrap

Create the ECR repository needed for the bootstrap worker image before the
operator exists:

```sh
aws ecr create-repository --region <aws-region> --repository-name tmprl-dem-cld/registry/worker
```

After the operator is running, project ECR repositories can be created or
verified by setting:

```yaml
DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws
DEMO_REGISTRY_AWS_REGION=<aws-region>
DEMO_REGISTRY_AWS_ACCOUNT_ID=<aws-account-id>
```

In the default `plan` mode, the operator records the expected ECR repositories
and secret paths on `DemoProject.status.externalResources` without mutating
AWS. In `aws` mode it creates or verifies ECR repositories and verifies
Secrets Manager paths. For platform-managed Temporal secrets and project-owned
custom secrets that declare `requiredProperties`, it also verifies JSON
property names without returning secret values in status. It does not create
placeholder secret values.

Create an IAM role for the registry/operator service account. The bootstrap
manifests expect this annotation until you replace it:

```text
arn:aws:iam::<aws-account-id>:role/tmprl-registry-operator
```

Scope the role to:

- create/read/write ECR repositories under `tmprl-dem-cld/*`;
- read/write Secrets Manager values under `tmprl-dem-cld/*`;
- read `tmprl-dem-cld/platform/registry-worker-temporal`;
- read `tmprl-dem-cld/platform/temporal-cloud-bootstrap`;
- use the EKS service account through IRSA.

Do not grant project runtime pods broad AWS access. Project runtime secret
access should stay under the project prefix, for example:

```text
tmprl-dem-cld/dejavu-tacos/*
```

Before deploying a demo that needs Temporal credentials or custom secrets,
create the required secret values. The operator verifies these paths in
`DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`:

```text
tmprl-dem-cld/platform/temporal-cloud-bootstrap
tmprl-dem-cld/<project-name>/temporal
tmprl-dem-cld/<project-name>/<custom-secret-name>
```

The platform bootstrap secret must contain:

```json
{
  "TEMPORAL_CLOUD_API_KEY": "<bootstrap-cloud-ops-api-key>",
  "TEMPORAL_CLOUD_ACCOUNT_ID": "<temporal-account-id>",
  "TEMPORAL_CLOUD_REGION": "<temporal-region>"
}
```

Each project Temporal credential secret must contain:

```json
{
  "address": "<region>.<cloud-provider>.api.temporal.io:7233",
  "namespace": "<project-temporal-namespace>.<temporal-account-id>",
  "apiKey": "<namespace-scoped-api-key>",
  "tls": "true"
}
```

Missing keys are reported under:

```text
DemoProject.status.externalResources.secretsManager
```

After Kubernetes reconcile, External Secrets Operator sync state is reported
separately under:

```text
DemoProject.status.externalSecretStatus
```

Use it to distinguish a missing AWS Secrets Manager value from an
ExternalSecret controller sync delay or error.
When required secret values are missing or malformed, the operator reports:

```text
DemoProject.status.phase=WaitingForExternalResources
DemoProject.status.externalResourceBlock
```

In that state the operator creates/patches the project namespace, RBAC,
metadata, and ExternalSecrets, but it does not deploy runtime
Deployments/Services/routes until the required AWS secret values verify.

For Agent Harness, the custom secret path is:

```text
tmprl-dem-cld/temporal-agent-harness/anthropic-credentials
```

The project config declares `ANTHROPIC_API_KEY` as a required JSON property for
that secret. In `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`, a missing,
malformed, or blank value keeps Agent Harness in `WaitingForExternalResources`.

## Shared Auth Setup

Shared auth is still a bootstrap dependency. Create these values before routing
auth-protected demos:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-dem-cld/platform/shared-auth \
  --secret-string '{
    "GOOGLE_CLIENT_ID": "<google-oauth-client-id>",
    "GOOGLE_CLIENT_SECRET": "<google-oauth-client-secret>",
    "AUTH_COOKIE_SIGNING_KEY": "<32-byte-random-secret>",
    "AUTH_ALLOWED_DOMAIN": "temporal.io"
  }'
```

The auth service must:

- serve `https://auth.tmprl-demo.cloud/traefik/verify`;
- allow only Temporal employee Google identities;
- strip untrusted inbound auth headers before setting
  `X-Temporal-Auth-JWT`, `X-Temporal-Auth-Subject`, and
  `X-Temporal-Auth-Email`;
- be reachable from Traefik.

## Build And Push Bootstrap Worker

From this repo:

```sh
export AWS_ACCOUNT_ID=<aws-account-id>
export AWS_REGION=<aws-region>
export GIT_SHA=$(git rev-parse --short=12 HEAD)
export ECR_BASE="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_BASE"

docker build \
  -f registry/worker/Dockerfile \
  -t "$ECR_BASE/tmprl-dem-cld/registry/worker:$GIT_SHA" \
  .

docker push "$ECR_BASE/tmprl-dem-cld/registry/worker:$GIT_SHA"
```

Edit `clusters/sa-demo/bootstrap/registry-operator/kustomization.yaml` and
replace:

```text
000000000000.dkr.ecr.us-east-1.amazonaws.com
replace-with-bootstrap-image
```

with your ECR registry and `$GIT_SHA`.

Also replace the IRSA role placeholder in
`clusters/sa-demo/bootstrap/registry-operator/rbac.yaml`.

## Apply Bootstrap

```sh
kubectl config use-context <kube-context>
kubectl apply -k clusters/sa-demo/bootstrap
kubectl -n tmprl-dem-cld-registry-operator rollout status deploy/registry-operator
kubectl get demoprojects
```

Expected early state:

- `DemoProject` CRs exist for `dejavu-tacos`, `demo-registry`, and
  `temporal-agent-harness`.
- The operator starts one workflow per project.
- CR status reaches `WaitingForImages` after namespace/RBAC/metadata reconcile.

## First Test Path: `dejavu-tacos`

The operator now has three build paths:

- `DEMO_REGISTRY_BUILD_MODE=plan`: record build intent and wait for immutable
  images from annotations or an external build system.
- `DEMO_REGISTRY_BUILD_MODE=local-docker`: clone the configured source repo,
  build each Dockerfile locally with Docker, push `git-<source-sha>` tags to
  ECR, and write the resolved image map into
  `DemoProject.status.buildStatus` and
  `DemoProject.status.candidateRolloutStatus`.
- `DEMO_REGISTRY_BUILD_MODE=aws-codebuild`: start a pre-created AWS CodeBuild
  project, override the source repo/commit/buildspec for the `DemoProject`,
  wait for the build, and write the pushed immutable images into status.

Prefer `aws-codebuild` for full rollout testing because the Docker daemon and
source checkout run in a short-lived AWS build environment instead of the
long-running registry operator pod. Use
`clusters/sa-demo/generated/codebuild-setup-plan.json` as the reviewed setup
source for the shared CodeBuild project shape, source repository access,
operator IAM policy, CodeBuild service-role policy, and ECR repository ARN
templates.

The operator role needs only:

```text
codebuild:StartBuild
codebuild:BatchGetBuilds
```

on the configured project. The CodeBuild service role needs:

```text
ecr:GetAuthorizationToken
ecr:BatchCheckLayerAvailability
ecr:InitiateLayerUpload
ecr:UploadLayerPart
ecr:CompleteLayerUpload
ecr:PutImage
ecr:DescribeRepositories
```

scoped to the project/build repositories under `tmprl-dem-cld/*`, plus read
access to the `temporal-sa` source repositories through the configured
CodeBuild source connection.

Use `local-docker` only from an isolated build worker or trusted test
environment with:

- `git` and `docker` installed;
- Docker daemon access;
- read access to the project source repository;
- ECR push access only to the project/build repositories;
- no Temporal admin, Kubernetes admin, or broad AWS permissions.

For `dejavu-tacos`, the build worker needs read access to
`temporal-sa/demo-dejavu-tacos` and push access only to:

```text
tmprl-dem-cld/dejavu-tacos/frontend
tmprl-dem-cld/dejavu-tacos/backend
tmprl-dem-cld/dejavu-tacos/worker-python
```

Set these environment variables on the build-capable worker:

```yaml
DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws
DEMO_REGISTRY_BUILD_MODE=aws-codebuild
DEMO_REGISTRY_CODEBUILD_PROJECT=<codebuild-project-name>
DEMO_REGISTRY_AWS_REGION=<aws-region>
DEMO_REGISTRY_AWS_ACCOUNT_ID=<aws-account-id>
```

The GitOps-managed operator reads non-secret runtime settings from the
`configMapGenerator` in
`clusters/sa-demo/bootstrap/registry-operator/kustomization.yaml`. Keep the
committed defaults at `plan` until the CodeBuild project, operator IAM policy,
AWS account/region, and required secret values are ready. To run a reviewed
full rollout test, update that generator with:

```yaml
DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws
DEMO_REGISTRY_TEMPORAL_CLOUD_MODE=manual
DEMO_REGISTRY_BUILD_MODE=aws-codebuild
DEMO_REGISTRY_CODEBUILD_PROJECT=<codebuild-project-name>
DEMO_REGISTRY_AWS_REGION=<aws-region>
DEMO_REGISTRY_AWS_ACCOUNT_ID=<aws-account-id>
```

Kustomize gives the generated ConfigMap a content hash and rewrites the
Deployment reference, so a committed config change rolls the operator pod
through Flux.
Keep `DEMO_REGISTRY_TEMPORAL_CLOUD_MODE=manual` until the operator image
includes the `tcld` binary. After that, `tcld-verify` can verify namespace
existence read-only; scoped service-account/API-key creation still remains
manual.

For a temporary live test, patch the operator Deployment and let the restarted
watch loop signal every `DemoProject` again:

```sh
kubectl -n tmprl-dem-cld-registry-operator set env deploy/registry-operator \
  DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws \
  DEMO_REGISTRY_BUILD_MODE=aws-codebuild \
  DEMO_REGISTRY_CODEBUILD_PROJECT=<codebuild-project-name> \
  DEMO_REGISTRY_AWS_REGION=<aws-region> \
  DEMO_REGISTRY_AWS_ACCOUNT_ID=<aws-account-id>

kubectl -n tmprl-dem-cld-registry-operator rollout status deploy/registry-operator
```

For the GitOps-managed path, make the same environment change in
`clusters/sa-demo/bootstrap/registry-operator/kustomization.yaml`, commit it,
and let Flux reconcile the Deployment. In `aws-codebuild` mode, do not add
`registry.tmprl-demo.cloud/image-overrides`; the workflow starts CodeBuild,
records `DemoProject.status.buildStatus.codebuild` with the build ID/logs, and
uses `DemoProject.status.buildStatus.images` for the candidate rollout and
promoted Traefik routes. To pin an exact source revision instead of building
branch HEAD, annotate only the source commit:

```sh
kubectl annotate demoproject dejavu-tacos \
  registry.tmprl-demo.cloud/source-commit-sha=<git-sha> \
  --overwrite
```

For trusted local testing only, set `DEMO_REGISTRY_BUILD_MODE=local-docker`
instead and run the worker where `git`, `docker`, Docker daemon access, and
project-scoped ECR push credentials are intentionally available.

If you keep `DEMO_REGISTRY_BUILD_MODE=plan`, the build activity will not
produce image refs. In that manual fallback only, build images from the reviewed
source repo using narrow credentials:

```sh
git clone https://github.com/temporal-sa/demo-dejavu-tacos /tmp/demo-dejavu-tacos
cd /tmp/demo-dejavu-tacos
export SOURCE_SHA=$(git rev-parse --short=12 HEAD)
export ECR_BASE="<aws-account-id>.dkr.ecr.<aws-region>.amazonaws.com"

docker build -f docker/frontend.Dockerfile -t "$ECR_BASE/tmprl-dem-cld/dejavu-tacos/frontend:git-$SOURCE_SHA" .
docker build -f docker/backend.Dockerfile -t "$ECR_BASE/tmprl-dem-cld/dejavu-tacos/backend:git-$SOURCE_SHA" .
docker build -f docker/worker-python.Dockerfile -t "$ECR_BASE/tmprl-dem-cld/dejavu-tacos/worker-python:git-$SOURCE_SHA" .

docker push "$ECR_BASE/tmprl-dem-cld/dejavu-tacos/frontend:git-$SOURCE_SHA"
docker push "$ECR_BASE/tmprl-dem-cld/dejavu-tacos/backend:git-$SOURCE_SHA"
docker push "$ECR_BASE/tmprl-dem-cld/dejavu-tacos/worker-python:git-$SOURCE_SHA"
```

Manually create the project Temporal credential until provider automation is
implemented:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-dem-cld/dejavu-tacos/temporal \
  --secret-string '{
    "address": "<region>.<cloud-provider>.api.temporal.io:7233",
    "namespace": "dejavu-tacos.<temporal-account-id>",
    "apiKey": "<dejavu-tacos-namespace-api-key>",
    "tls": "true"
  }'
```

For the manual fallback only, annotate the `DemoProject` with the immutable
image set:

```sh
kubectl annotate demoproject dejavu-tacos \
  registry.tmprl-demo.cloud/source-commit-sha="$SOURCE_SHA" \
  --overwrite

kubectl annotate demoproject dejavu-tacos \
  "registry.tmprl-demo.cloud/image-overrides={\"frontend\":\"$ECR_BASE/tmprl-dem-cld/dejavu-tacos/frontend:git-$SOURCE_SHA\",\"backend\":\"$ECR_BASE/tmprl-dem-cld/dejavu-tacos/backend:git-$SOURCE_SHA\",\"worker-python\":\"$ECR_BASE/tmprl-dem-cld/dejavu-tacos/worker-python:git-$SOURCE_SHA\"}" \
  --overwrite
```

Watch the operator:

```sh
kubectl get demoproject dejavu-tacos -o yaml
kubectl -n tmprl-dem-cld-dejavu-tacos get deploy,svc,externalsecret,ingressroute
kubectl -n tmprl-dem-cld-registry-operator get configmap registry-catalog-status \
  -o jsonpath='{.data.dejavu-tacos\.json}'
```

Before running the smoke check, the workflow waits for all candidate
Deployments listed in `DemoProject.status.candidateRolloutStatus.deployments`
to report available replicas. Readiness progress and failures are recorded in:

```text
DemoProject.status.readinessStatus
```

The generated config defines this owner smoke check:

```yaml
smokeCheck:
  path: /api/health
  expectedStatus: 200
```

The workflow runs the smoke check from the operator against the in-cluster
candidate service URL. With the `SOURCE_SHA=$(git rev-parse --short=12 HEAD)`
value above, the backend candidate Service is named
`backend-git-$SOURCE_SHA`:

```text
http://backend-git-$SOURCE_SHA.tmprl-dem-cld-dejavu-tacos.svc.cluster.local:8000/api/health
```

Only after that returns HTTP 200 does the promotion activity apply the Traefik
route for:

```text
https://dejavu-tacos.tmprl-demo.cloud
```

The promoted Traefik route points at the versioned candidate Services recorded
in `DemoProject.status.activeRouteServices`. The operator may also patch the
stable same-namespace Services, such as `backend`, to the latest candidate for
candidate-internal traffic, but the public route is not repointed until smoke
success. Promotion is recorded in `DemoProject.status.phase=Promoted` and
`DemoProject.status.activeImageVersion=git-$SOURCE_SHA`. If the endpoint
differs in the source app, update `projects/dejavu-tacos.yaml`, regenerate, and
commit the reviewed config before testing again.

The registry/catalog status ConfigMap entry mirrors the lifecycle fields the UI
should show: phase, message, external-resource phases, build output, candidate
deployment/services, readiness, smoke check, active version, and last error.

## Second Test Target: `temporal-agent-harness`

Use Agent Harness to prove the single-image/multiple-component path and, if the
namespace does not exist yet, namespace creation from zero:

```sh
kubectl get namespace tmprl-dem-cld-temporal-agent-harness
```

If that returns `NotFound`, do not create it manually; let the operator create
it from `DemoProject/temporal-agent-harness`.

The project has one build consumed by web, API, and worker components:

```text
source repo: https://github.com/temporal-sa/temporal-agent-harness-example
dockerfile: simple_chat_agent/Dockerfile
ECR repository: tmprl-dem-cld/temporal-agent-harness/app
```

Create the project Temporal credential:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-dem-cld/temporal-agent-harness/temporal \
  --secret-string '{
    "address": "<region>.<cloud-provider>.api.temporal.io:7233",
    "namespace": "temporal-agent-harness.<temporal-account-id>",
    "apiKey": "<temporal-agent-harness-namespace-api-key>",
    "tls": "true"
  }'
```

Create the Agent Harness custom secret:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-dem-cld/temporal-agent-harness/anthropic-credentials \
  --secret-string '{
    "ANTHROPIC_API_KEY": "<anthropic-api-key>"
  }'
```

`ANTHROPIC_API_KEY` is declared in
`projects/temporal-agent-harness.yaml` under
`secrets.anthropic-credentials.requiredProperties`; AWS external-resource mode
verifies that key before deploying the API and worker workloads.

Then use `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws` and
`DEMO_REGISTRY_BUILD_MODE=aws-codebuild` with the configured CodeBuild project,
or manually build and annotate the immutable app image. In `aws-codebuild`
mode, confirm the build reached `Pushed` and find the CodeBuild build/logs in:

```text
DemoProject.status.buildStatus.codebuild
DemoProject.status.buildStatus.images.app
```

The generated smoke check is:

```yaml
smokeCheck:
  path: /api/auth/google/configured
  expectedStatus: 200
```

Watch both status surfaces:

```sh
kubectl get demoproject temporal-agent-harness -o yaml
kubectl -n tmprl-dem-cld-temporal-agent-harness get deploy,svc,externalsecret,ingressroute
kubectl -n tmprl-dem-cld-registry-operator get configmap registry-catalog-status \
  -o jsonpath='{.data.temporal-agent-harness\.json}'
```

## Current Manual Substitutes

These are intentional gaps for the first deployment test:

- ECR repositories can be created/verified by opt-in
  `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`, enabled through the bootstrap
  operator config generator after IAM is ready, or created manually while
  testing in `plan` mode.
- Image builds can be run by opt-in `aws-codebuild`, by trusted
  `local-docker`, or manually via immutable image annotations.
- Temporal Cloud Namespace/API key lifecycle is manual. Do not automate delete
  or rotation until the provider contract is reviewed.
- Project Temporal credentials are written manually to
  `tmprl-dem-cld/<project>/temporal`.
- Shared auth service deployment is not implemented here; the Traefik
  middleware path is fixed so the service can be added later.
- `demo-registry` web image is buildable from `registry/web/Dockerfile`.
  The updated worker mirrors catalog status into
  `tmprl-dem-cld-registry/registry-catalog-status`, and the
  generated web Deployment mounts it at `/app/status`. Until those updated
  images are rolled out, inspect live lifecycle state with the `kubectl`
  commands above.
