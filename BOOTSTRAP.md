# Bootstrap And First Deployment Test

This repo now has two layers:

- `clusters/sa-demo/bootstrap/` installs the minimum registry/operator control
  plane: `DemoProject` CRD, registry operator Deployment, RBAC, and platform
  ExternalSecrets.
- `clusters/sa-demo/generated/` contains reviewed `DemoProject` CRs generated
  from `projects/*.yaml`. The older per-project Kubernetes manifests remain
  review artifacts; the applied path is the `DemoProject` CR set.

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
  --name tmprl-cloud-demo/platform/temporal-cloud-bootstrap \
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
  --name tmprl-cloud-demo/platform/registry-worker-temporal \
  --secret-string '{
    "TEMPORAL_ADDRESS": "<region>.<cloud-provider>.api.temporal.io:7233",
    "TEMPORAL_NAMESPACE": "demo-registry.<temporal-account-id>",
    "TEMPORAL_API_KEY": "<registry-worker-namespace-api-key>",
    "TEMPORAL_TLS": "true"
  }'
```

## AWS Bootstrap

Create the ECR repository needed for the bootstrap worker image before the
operator exists:

```sh
aws ecr create-repository --region <aws-region> --repository-name tmprl-cloud-demo/demo-registry/worker
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
Secrets Manager paths. For platform-managed Temporal secrets it also verifies
the JSON property names without returning secret values in status. It does not
create placeholder secret values.

Create an IAM role for the registry/operator service account. The bootstrap
manifests expect this annotation until you replace it:

```text
arn:aws:iam::<aws-account-id>:role/tmprl-demo-registry-operator
```

Scope the role to:

- create/read/write ECR repositories under `tmprl-cloud-demo/*`;
- read/write Secrets Manager values under `tmprl-cloud-demo/*`;
- read `tmprl-cloud-demo/platform/registry-worker-temporal`;
- read `tmprl-cloud-demo/platform/temporal-cloud-bootstrap`;
- use the EKS service account through IRSA.

Do not grant project runtime pods broad AWS access. Project runtime secret
access should stay under the project prefix, for example:

```text
tmprl-cloud-demo/dejavu-tacos/*
```

Before deploying a demo that needs Temporal credentials or custom secrets,
create the required secret values. The operator verifies these paths in
`DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`:

```text
tmprl-cloud-demo/platform/temporal-cloud-bootstrap
tmprl-cloud-demo/<project-name>/temporal
tmprl-cloud-demo/<project-name>/<custom-secret-name>
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

For Agent Harness, the custom secret path is:

```text
tmprl-cloud-demo/temporal-agent-harness/anthropic-credentials
```

## Shared Auth Setup

Shared auth is still a bootstrap dependency. Create these values before routing
auth-protected demos:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-cloud-demo/platform/shared-auth \
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
  -t "$ECR_BASE/tmprl-cloud-demo/demo-registry/worker:$GIT_SHA" \
  .

docker push "$ECR_BASE/tmprl-cloud-demo/demo-registry/worker:$GIT_SHA"
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
kubectl -n demo-registry-system rollout status deploy/demo-registry-operator
kubectl get demoprojects
```

Expected early state:

- `DemoProject` CRs exist for `dejavu-tacos`, `demo-registry`, and
  `temporal-agent-harness`.
- The operator starts one workflow per project.
- CR status reaches `WaitingForImages` after namespace/RBAC/metadata reconcile.

## First Test Path: `dejavu-tacos`

The operator now has two build paths:

- `DEMO_REGISTRY_BUILD_MODE=plan`: record build intent and wait for immutable
  images from annotations or an external build system.
- `DEMO_REGISTRY_BUILD_MODE=local-docker`: clone the configured source repo,
  build each Dockerfile locally with Docker, push `git-<source-sha>` tags to
  ECR, and write the resolved image map into
  `DemoProject.status.buildStatus` and
  `DemoProject.status.candidateRolloutStatus`.

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
tmprl-cloud-demo/dejavu-tacos/frontend
tmprl-cloud-demo/dejavu-tacos/backend
tmprl-cloud-demo/dejavu-tacos/worker-python
```

Set these environment variables on the build-capable worker:

```yaml
DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws
DEMO_REGISTRY_BUILD_MODE=local-docker
DEMO_REGISTRY_AWS_REGION=<aws-region>
DEMO_REGISTRY_AWS_ACCOUNT_ID=<aws-account-id>
```

If you keep `DEMO_REGISTRY_BUILD_MODE=plan`, build images manually from the
reviewed source repo using narrow credentials:

```sh
git clone https://github.com/temporal-sa/demo-dejavu-tacos /tmp/demo-dejavu-tacos
cd /tmp/demo-dejavu-tacos
export SOURCE_SHA=$(git rev-parse --short=12 HEAD)
export ECR_BASE="<aws-account-id>.dkr.ecr.<aws-region>.amazonaws.com"

docker build -f docker/frontend.Dockerfile -t "$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/frontend:git-$SOURCE_SHA" .
docker build -f docker/backend.Dockerfile -t "$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/backend:git-$SOURCE_SHA" .
docker build -f docker/worker-python.Dockerfile -t "$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/worker-python:git-$SOURCE_SHA" .

docker push "$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/frontend:git-$SOURCE_SHA"
docker push "$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/backend:git-$SOURCE_SHA"
docker push "$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/worker-python:git-$SOURCE_SHA"
```

Manually create the project Temporal credential until provider automation is
implemented:

```sh
aws secretsmanager create-secret \
  --region <aws-region> \
  --name tmprl-cloud-demo/dejavu-tacos/temporal \
  --secret-string '{
    "address": "<region>.<cloud-provider>.api.temporal.io:7233",
    "namespace": "dejavu-tacos.<temporal-account-id>",
    "apiKey": "<dejavu-tacos-namespace-api-key>",
    "tls": "true"
  }'
```

Annotate the `DemoProject` with the immutable image set:

```sh
kubectl annotate demoproject dejavu-tacos \
  registry.tmprl-demo.cloud/source-commit-sha="$SOURCE_SHA" \
  --overwrite

kubectl annotate demoproject dejavu-tacos \
  "registry.tmprl-demo.cloud/image-overrides={\"frontend\":\"$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/frontend:git-$SOURCE_SHA\",\"backend\":\"$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/backend:git-$SOURCE_SHA\",\"worker-python\":\"$ECR_BASE/tmprl-cloud-demo/dejavu-tacos/worker-python:git-$SOURCE_SHA\"}" \
  --overwrite
```

Watch the operator:

```sh
kubectl get demoproject dejavu-tacos -o yaml
kubectl -n tmprl-cloud-demo-dejavu-tacos get deploy,svc,externalsecret,ingressroute
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
http://backend-git-$SOURCE_SHA.tmprl-cloud-demo-dejavu-tacos.svc.cluster.local:8000/api/health
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

## Current Manual Substitutes

These are intentional gaps for the first deployment test:

- ECR repositories are created manually.
- Image builds are run manually or by a narrow AWS-side builder.
- Temporal Cloud Namespace/API key lifecycle is manual. Do not automate delete
  or rotation until the provider contract is reviewed.
- Project Temporal credentials are written manually to
  `tmprl-cloud-demo/<project>/temporal`.
- Shared auth service deployment is not implemented here; the Traefik
  middleware path is fixed so the service can be added later.
- `demo-registry` web deployment is not the first test target because
  `registry/web/Dockerfile` is not present yet.
