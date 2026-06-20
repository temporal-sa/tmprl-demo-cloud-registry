# Serverless AWS Lambda Workers

Use this reference when a Temporal worker should run as an AWS Lambda image
worker instead of a Kubernetes pod.

## When Lambda Fits

Lambda is a good fit when:

- The component is a Temporal worker, not a web server.
- The worker has a Dockerfile image entrypoint that can run inside Lambda's
  container environment.
- The app uses Temporal SDK Worker Versioning. The platform creates the
  Temporal Worker Deployment and version through the Lambda deployment flow;
  it does not use the Kubernetes Worker Controller for Lambda.
- Runtime config can be passed through env vars, Secrets Manager refs, or mTLS
  file paths under `/tmp/tmprl-demo-cloud/temporal`.
- The worker does not need stable Kubernetes DNS, Kubernetes services, inbound
  ports, sidecars, or mounted Kubernetes volumes.

Lambda is a bad fit when:

- The process exposes HTTP ports or needs ingress.
- The process needs Kubernetes service discovery.
- The repo only has a buildpack path and no Dockerfile image build.
- The app requires fixed mTLS file paths outside `/tmp`.
- The app needs `command`/`args` overrides at the component level.
- The app depends on `temporalProxy`.
- The app expects bulk `components.<name>.secrets` injection rather than
  targeted env mappings or direct Secrets Manager reads.

## YAML Shape

```yaml
spec:
  builds:
    worker:
      context: .
      dockerfile: docker/worker-lambda.Dockerfile
  components:
    worker:
      build: worker
      env:
        - name: BANKING_BACKEND_URL
          value: https://banking-app.tmprl-demo.cloud
      serverless:
        provider: aws-lambda
        packageType: image
        architecture: x86_64
        memoryMb: 512
        timeoutSeconds: 600
        workerDeployment:
          deploymentName: money-transfer
```

Fields:

- `serverless.provider`: must be `aws-lambda`.
- `packageType`: image only; default is `image`.
- `architecture`: `x86_64` or `arm64`; default `x86_64`.
- `memoryMb`: 128 to 10240; default 512.
- `timeoutSeconds`: 1 to 900; default 600.
- `workerDeployment.deploymentName`: optional Temporal Worker Deployment name.
  Defaults to the component name. Use this when app code expects a fixed SDK
  deployment name such as `money-transfer`.
- `workerDeployment.buildId`: optional fixed Temporal Worker Deployment build
  ID. Omit it for normal rollouts so the platform uses the generated rollout
  build ID.
- `workerDeployment.env.deploymentName`: optional list of extra env var names
  that should receive the resolved deployment name.
- `workerDeployment.env.buildId`: optional list of extra env var names that
  should receive the resolved build ID.
- `build`: must reference a Dockerfile build.

Do not set `worker`, `worker.versioning`, `temporalAccess`, or
`temporalProxy` on a Lambda component. Those are Kubernetes runtime controls.

## Unsupported Component Fields

Serverless Lambda components reject:

- `ports`
- `serviceName`
- `replicas`
- `automountServiceAccountToken`
- `command`
- `args`
- `temporalProxy`
- `temporalAccess`
- `secrets`
- `worker`

Use a separate Kubernetes component for public HTTP/API service needs. A Lambda
worker can call that service through its public ingress URL or another external
URL the app can reach.

## Image Wrapper Behavior

For Lambda image builds, CodeBuild creates the app image first, then wraps it
with a platform binary. The demo Dockerfile is not edited.

At Lambda cold start the wrapper:

1. Reads platform manifests from environment variables.
2. Calls AWS Secrets Manager with the Lambda execution role.
3. Sets requested env vars from JSON secret properties.
4. Writes requested mTLS PEM files under `/tmp/tmprl-demo-cloud/temporal`.
5. Executes the original image entrypoint/CMD.

The build definition hash includes the wrapper implementation version so wrapper
changes produce a new image version.

## Temporal API Key On Lambda

In API-key mode, the wrapper materializes the standard Temporal env vars:

- `TEMPORAL_ADDRESS`
- `TEMPORAL_NAMESPACE`
- `TEMPORAL_TLS`
- `TEMPORAL_API_KEY`

The platform also sets Worker Versioning defaults:

- `TEMPORAL_DEPLOYMENT_NAME`
- `TEMPORAL_WORKER_DEPLOYMENT_NAME`
- `TEMPORAL_WORKER_BUILD_ID`

Many Python demos use a helper like:

```python
WorkerDeploymentVersion(
    deployment_name=os.getenv("TEMPORAL_WORKER_DEPLOYMENT_NAME", "default"),
    build_id=(
        os.getenv("TEMPORAL_WORKER_BUILD_ID")
        or os.getenv("AWS_LAMBDA_FUNCTION_VERSION")
        or "default"
    ),
)
```

For that shape, set `serverless.workerDeployment.deploymentName` if the app
expects a fixed deployment name and leave `buildId` omitted for generated
rollout IDs.

If the app needs different names, use `env[].valueFrom.inputRef`:

```yaml
components:
  worker:
    build: worker
    env:
      - name: TEMPORAL_HOST
        valueFrom:
          inputRef: temporal.address
      - name: TEMPORAL_TOKEN
        valueFrom:
          inputRef: temporal.apiKey
    serverless:
      provider: aws-lambda
```

The wrapper sets both the standard names and requested remaps.

If the app needs custom names for Worker Deployment metadata, map them under
`serverless.workerDeployment.env` instead of authoring literal values:

```yaml
serverless:
  provider: aws-lambda
  workerDeployment:
    deploymentName: money-transfer
    env:
      deploymentName:
        - APP_WORKER_DEPLOYMENT
      buildId:
        - APP_WORKER_BUILD_ID
```

Only set `workerDeployment.buildId` when the app really needs a fixed build ID:

```yaml
serverless:
  provider: aws-lambda
  workerDeployment:
    deploymentName: money-transfer
    buildId: stable-money-transfer-worker
```

Fixed build IDs trade rollout identity for app compatibility. Reusing the same
build ID across image changes can make Temporal see the update as the same
Worker Deployment version with updated compute config. Prefer the generated
build ID unless the demo code cannot tolerate it.

## Temporal mTLS On Lambda

Use project mTLS mode:

```yaml
spec:
  temporal:
    auth:
      mode: mtls
```

The Lambda wrapper writes:

- `/tmp/tmprl-demo-cloud/temporal/ca.crt`
- `/tmp/tmprl-demo-cloud/temporal/tls.crt`
- `/tmp/tmprl-demo-cloud/temporal/tls.key`

Default path envs:

- `TEMPORAL_MTLS_CERT_DIR`
- `TEMPORAL_MTLS_CA_CERT_PATH`
- `TEMPORAL_MTLS_CLIENT_CERT_PATH`
- `TEMPORAL_MTLS_CLIENT_KEY_PATH`

Add app-specific path env var names with `temporalMtls.certFiles.pathEnv`:

```yaml
components:
  worker:
    build: worker
    temporalMtls:
      certFiles:
        pathEnv:
          clientCert: TEMPORAL_CERT_PATH
          clientKey: TEMPORAL_KEY_PATH
    serverless:
      provider: aws-lambda
```

Lambda does not support `temporalMtls.certFiles.mountPath` or
`temporalMtls.certFiles.paths`; files are always under `/tmp`.

If the app expects raw PEM env vars instead of files, map them:

```yaml
env:
  - name: TEMPORAL_CERT_PEM
    valueFrom:
      inputRef: temporal.clientCert
  - name: TEMPORAL_KEY_PEM
    valueFrom:
      inputRef: temporal.clientKey
```

## Reading The Runtime Secret Directly

The Lambda function environment includes:

- `TEMPORAL_RUNTIME_SECRET_ARN`
- `TEMPORAL_RUNTIME_SECRET_ID`

If app code already uses AWS Secrets Manager, it can read
`TEMPORAL_RUNTIME_SECRET_ARN` and parse the JSON itself. This is useful for code
that wants to construct Temporal client config manually.

For ordinary onboarding, prefer wrapper materialization through standard env
vars, `temporal.*` refs, and mTLS file path envs.

## User Secrets On Lambda

Lambda components cannot use `components.<name>.secrets`. Use targeted refs:

```yaml
spec:
  secrets:
    llm:
      requiredProperties:
        - ANTHROPIC_API_KEY
  components:
    worker:
      build: worker
      env:
        - name: MODEL_API_KEY
          valueFrom:
            inputRef: secrets.llm.ANTHROPIC_API_KEY
      serverless:
        provider: aws-lambda
```

The wrapper fetches the secret and sets `MODEL_API_KEY` before starting the
original worker process.

If the app intentionally reads Secrets Manager itself, pass only a secret name
or ARN as a literal env var and keep the secret under
`tmprl-dem-cld/<project>/...`.

## IAM Model

The operator creates dynamic IAM roles for Lambda:

- Lambda execution role: lets the function run, write logs, pull the image, and
  read project-scoped Secrets Manager values.
- Temporal invoke role: allows Temporal Cloud to invoke the versioned Lambda.

The Lambda execution role is scoped to project secrets under
`tmprl-dem-cld/<project>/*` and the runtime secret paths the operator knows
about. Do not design demos that need cross-project secrets or broad AWS access.

## Backend/API Access From Lambda

Lambda workers do not get Kubernetes service DNS. If a worker needs to call a
backend component, give it a reachable URL:

```yaml
env:
  - name: BANKING_BACKEND_URL
    value: https://banking-app.tmprl-demo.cloud
```

If the backend needs public ingress, define ingress on the Kubernetes
components. Lambda itself does not receive ingress.

## Build Sharing And Architecture

If multiple Lambda components share one build, they must use the same Lambda
architecture because the wrapper binary is architecture-specific. Prefer one
build per architecture.
