# Temporal Auth, Workers, And Credential Wiring

Use this reference when a component talks to Temporal, when a repo uses Worker
Versioning, when an app expects custom Temporal env var names, or when deciding
between API key, mTLS, proxy, and serverless credential paths.

## Auth Modes

API key is the default:

```yaml
spec:
  temporal:
    auth:
      mode: apiKey
```

Omitting `spec.temporal.auth.mode` is equivalent to `apiKey`.

mTLS:

```yaml
spec:
  temporal:
    auth:
      mode: mtls
```

Switching between `apiKey` and `mtls` is destructive for the Temporal Cloud
namespace because the namespace cannot run both auth modes at once. Finalize and
recreate the project when changing modes.

## Namespace Region

Pin the Temporal Cloud namespace region with `spec.temporal.region` in Temporal's
provider notation (`aws-*`, `gcp-*`, `azure-*`):

```yaml
spec:
  temporal:
    region: aws-ca-central-1
```

When omitted, the namespace uses the operator-wide default region, except that
projects with serverless (AWS Lambda) workers default to an AWS region
(`aws-ca-central-1`) so Temporal's Lambda compute provider can reach the worker.
Like an auth-mode switch, changing the region of an existing namespace is
destructive: a Temporal Cloud namespace's region is immutable, so the operator
tears the namespace down (deleting its data) and recreates it in the new region.

The operator creates the project runtime secret at:

```text
tmprl-dem-cld/<project>/temporal
```

API-key runtime secret properties:

- `address`
- `namespace`
- `apiKey`
- `tls`

mTLS runtime secret properties:

- `address`
- `namespace`
- `tls`
- `caCert`
- `clientCert`
- `clientKey`

## Which Components Need Temporal Credentials

- `worker: true` marks an ordinary Kubernetes Temporal worker.
- `worker.versioning.enabled: true` marks a Kubernetes worker managed by the
  Temporal Worker Controller.
- `serverless.provider: aws-lambda` marks a Lambda Temporal worker. Configure
  its Worker Deployment under `serverless.workerDeployment`, not `worker`.
- Set `temporalAccess: true` on non-worker components that start, signal, query,
  describe, list, or otherwise call workflows.
- Do not set `temporalAccess` merely because a component is public or talks to a
  backend that talks to Temporal.
- Serverless Lambda worker components must not set `temporalAccess`; serverless
  wiring is handled by the Lambda workflow.

## Default Kubernetes Env Injection

For non-versioned Kubernetes workers and `temporalAccess: true` components in
API-key mode, the platform injects:

- `TEMPORAL_ADDRESS`
- `TEMPORAL_NAMESPACE`
- `TEMPORAL_API_KEY`
- `TEMPORAL_TLS`

For mTLS mode, the platform injects:

- `TEMPORAL_ADDRESS`
- `TEMPORAL_NAMESPACE`
- `TEMPORAL_TLS`
- `TEMPORAL_CA_CERT`
- `TEMPORAL_CLIENT_CERT`
- `TEMPORAL_CLIENT_KEY`

mTLS mode also mounts PEM files for Kubernetes components that receive Temporal
credentials. Defaults:

- directory: `/var/run/secrets/tmprl-demo.cloud/temporal`
- CA file: `ca.crt`
- client cert: `tls.crt`
- client key: `tls.key`
- path envs:
  - `TEMPORAL_MTLS_CERT_DIR`
  - `TEMPORAL_MTLS_CA_CERT_PATH`
  - `TEMPORAL_MTLS_CLIENT_CERT_PATH`
  - `TEMPORAL_MTLS_CLIENT_KEY_PATH`

## Env Var Remapping

Use `env[].valueFrom.inputRef` when app code expects different env var names.

API-key example:

```yaml
components:
  api:
    build: api
    temporalAccess: true
    env:
      - name: TEMPORAL_HOST
        valueFrom:
          inputRef: temporal.address
      - name: TEMPORAL_TOKEN
        valueFrom:
          inputRef: temporal.apiKey
```

mTLS raw PEM example:

```yaml
temporal:
  auth:
    mode: mtls
components:
  worker:
    build: worker
    worker: true
    env:
      - name: TEMPORAL_CERT_PEM
        valueFrom:
          inputRef: temporal.clientCert
      - name: TEMPORAL_KEY_PEM
        valueFrom:
          inputRef: temporal.clientKey
```

Rules:

- API-key refs are valid only in API-key mode:
  `temporal.address`, `temporal.namespace`, `temporal.apiKey`, `temporal.tls`.
- mTLS cert refs are valid only in mTLS mode:
  `temporal.caCert`, `temporal.clientCert`, `temporal.clientKey`.
- `temporal.*` refs cannot be used with `temporalProxy`.

## mTLS Files For Kubernetes Components

If code wants file paths, configure `temporalMtls.certFiles`.

```yaml
temporal:
  auth:
    mode: mtls
components:
  worker:
    build: worker
    worker: true
    temporalMtls:
      certFiles:
        mountPath: /etc/temporal/certs
        paths:
          caCert: /etc/temporal/certs/root-ca.pem
          clientCert: /etc/temporal/certs/client.pem
          clientKey: /etc/temporal/certs/client-key.pem
        pathEnv:
          clientCert: TEMPORAL_CERT_PATH
          clientKey: TEMPORAL_KEY_PATH
```

Rules:

- `temporalMtls.certFiles` requires `spec.temporal.auth.mode: mtls`.
- `paths` is optional. If omitted, files are `ca.crt`, `tls.crt`, and `tls.key`
  under `mountPath`.
- Custom file paths must share a non-root parent directory.
- `enabled: false` is only for components that need PEM env vars but should not
  receive mounted cert files.
- For Lambda, only `pathEnv` is supported; `mountPath` and `paths` are rejected.

## Temporal Proxy Compatibility Path

Use `temporalProxy.enabled: true` only for Kubernetes components that need
Temporal but can only connect to a local plaintext Temporal server without code
changes.

```yaml
components:
  api:
    build: api
    temporalAccess: true
    temporalProxy:
      enabled: true
```

The app container receives local defaults:

- `TEMPORAL_ADDRESS=127.0.0.1:7233`
- `TEMPORAL_HOST=127.0.0.1:7233`
- `TEMPORAL_NAMESPACE=default`
- `TEMPORAL_TLS=false`

The sidecar owns the real Temporal Cloud address, namespace, TLS, and API key.
It rewrites local `default` namespace requests to the project namespace.

Use `addressEnv` if the app expects other host env names:

```yaml
temporalProxy:
  enabled: true
  addressEnv:
    - TEMPORAL_TARGET
    - TEMPORAL_SERVER
```

Limitations:

- Not supported for serverless Lambda.
- Not compatible with mTLS mode.
- Do not set `temporal.*` input refs on a component using the proxy.
- Do not inject `TEMPORAL_API_KEY` into the app container on the proxy path.

## Kubernetes Worker Versioning

Ordinary worker:

```yaml
worker: true
```

Worker Versioning object:

```yaml
worker:
  versioning:
    enabled: true
    deploymentName: money-transfer
    rollout:
      strategy: AllAtOnce
    sunset:
      scaledownDelay: 1h
      deleteDelay: 24h
```

Use the object form only when the code is compatible with Temporal SDK Worker
Versioning and the worker runs as a Kubernetes component.

Worker Controller injects versioning metadata and owns these values:

- `TEMPORAL_ADDRESS`
- `TEMPORAL_NAMESPACE`
- `TEMPORAL_DEPLOYMENT_NAME`
- `TEMPORAL_WORKER_DEPLOYMENT_NAME`
- `TEMPORAL_WORKER_BUILD_ID`

Do not author those env vars manually for versioned workers. Task queues remain
app-owned routing names; set task queue env vars only when the app needs them.

Compatibility:

- Kubernetes Worker Versioning with mTLS is currently blocked until Connection
  CRD support exists for client certificate refs.
- Serverless Lambda workers also use Temporal SDK Worker Versioning, but they do
  not use `worker.versioning` or the Kubernetes Worker Controller. See
  `serverless-lambda.md`.

## Lambda Worker Deployment Metadata

Lambda worker deployment settings live under `serverless.workerDeployment`:

```yaml
serverless:
  provider: aws-lambda
  workerDeployment:
    deploymentName: money-transfer
```

The platform sets:

- `TEMPORAL_DEPLOYMENT_NAME`
- `TEMPORAL_WORKER_DEPLOYMENT_NAME`
- `TEMPORAL_WORKER_BUILD_ID`

Use `workerDeployment.deploymentName` for a fixed SDK deployment name. Omit
`workerDeployment.buildId` for normal dynamic rollouts, or set it only when app
code requires a fixed build ID. Use `workerDeployment.env.deploymentName` and
`workerDeployment.env.buildId` to copy resolved values into app-specific env
var names.

Lambda workers can use either API-key or mTLS project auth.

## Credentials For Additional Namespaces

A component bound with `temporalNamespace` gets the same env vars, sourced from
that namespace's own credentials instead of the project's. Nothing in the app
changes: it still reads `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and the rest.

What differs is where they come from. The bound namespace gets its own
credential in Secrets Manager at `tmprl-dem-cld/<project>/temporal-<label>`, and
its own Kubernetes Secret, named by appending a short digest of the label to the
usual name (`temporal-credentials-<digest>`, or
`temporal-worker-runtime-<digest>` for a versioned worker). The digest keeps the
name within the 63-character limit whatever the label is.

Components with no `temporalNamespace` are untouched: they keep reading
`temporal-credentials` and `temporal-worker-runtime` exactly as before.

Two carve-outs, both rejected by validation rather than silently ignored:
`temporalNamespace` does not work on `serverless.provider: aws-lambda`
components, whose Temporal wiring is a separate path, or alongside
`temporalProxy`, which already rewrites namespaces its own way.

Credential rotation runs per namespace, so an mTLS project pays the CA staging
and weekly client-cert rotation cost once per declared namespace.

See `demo-project-contract.md` for the schema.

## Temporal Search Attributes

Declare namespace search attributes when the demo requires them:

```yaml
temporal:
  searchAttributes:
    CustomerId: {}
    CartTotal:
      type: double
    FulfillmentTags:
      type: keywordList
```

Supported types: `text`, `keyword`, `int`, `double`, `bool`, `datetime`,
`keywordList`. Omitted type defaults to `keyword`. The operator only deletes
attributes it previously created and recorded as managed.

## Temporal UI URLs

The operator does not automatically inject arbitrary app-specific Temporal UI
URL env vars. If the app expects one, set a literal value:

```yaml
env:
  - name: TEMPORAL_UI_URL
    value: https://cloud.temporal.io/namespaces/<qualified-namespace>
```

Only do this when the app really reads such an env var.
