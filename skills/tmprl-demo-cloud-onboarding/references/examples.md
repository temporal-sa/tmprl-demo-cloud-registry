# Common YAML Examples

Use these examples as report snippets and starting points for proposed YAML.
Keep final project YAML thinner when defaults are enough.

Do not create or edit files from these examples unless the user explicitly asks
for implementation after reviewing the onboarding report.

## Minimal Web/API/Worker

```yaml
apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: banking-money-transfer
spec:
  source:
    repo: https://github.com/temporal-sa/demo-banking-app-money-transfer
  builds:
    frontend: { context: ., dockerfile: docker/frontend.Dockerfile }
    backend: { context: ., dockerfile: docker/backend.Dockerfile }
    worker: { context: ., dockerfile: docker/worker-python.Dockerfile }
  components:
    frontend:
      build: frontend
      ports:
        - containerPort: 5173
          servicePort: 80
    backend:
      build: backend
      temporalAccess: true
      ports:
        - containerPort: 8000
          servicePort: 8000
    worker:
      build: worker
      worker: true
  ingress:
    subdomain: banking-app
```

## Multi-Route Ingress

```yaml
ingress:
  subdomain: banking-app
  temporalAuthRequired: false
  routes:
    - pathPrefix: /api
      component: backend
      port: http
    - pathPrefix: /
      component: frontend
      port: http
```

Use this when the public hostname needs to route different paths to different
components. If the frontend proxies `/api` internally and serves on
`servicePort: 80`, omit routes and let the catch-all default work.

## Internal Service URL

```yaml
components:
  backend:
    build: backend
    ports:
      - containerPort: 8000
        servicePort: 8000
  worker:
    build: worker
    worker: true
    env:
      - name: BACKEND_URL
        value: http://backend:8000
```

For Lambda workers, use a public/external URL instead of Kubernetes service DNS.

## Buildpack With Generated Processes

```yaml
builds:
  app:
    context: .
    strategy: buildpack
    builder: paketobuildpacks/builder-jammy-base
    env:
      BP_PYTHON_VERSION: "3.12.*"
    processes:
      web: python -m demo.web
      worker: python -m demo.worker
components:
  web:
    build: app
    command: ["/cnb/process/web"]
    ports:
      - containerPort: 8080
        servicePort: 80
  worker:
    build: app
    command: ["/cnb/process/worker"]
    worker: true
```

## API Key Env Remap

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

## Temporal Proxy Compatibility

```yaml
components:
  api:
    build: api
    temporalAccess: true
    temporalProxy:
      enabled: true
      addressEnv:
        - TEMPORAL_TARGET
```

Use for Kubernetes components whose code can connect to a local plaintext
Temporal endpoint but cannot consume Cloud auth/TLS env vars without code
changes.

## Kubernetes mTLS Files With Custom Env Names

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
          caCert: /etc/temporal/certs/ca.pem
          clientCert: /etc/temporal/certs/client.pem
          clientKey: /etc/temporal/certs/client-key.pem
        pathEnv:
          clientCert: TEMPORAL_CERT_PATH
          clientKey: TEMPORAL_KEY_PATH
```

## Lambda Worker With mTLS Path Env Remap

```yaml
temporal:
  auth:
    mode: mtls
components:
  worker:
    build: worker
    env:
      - name: BANKING_BACKEND_URL
        value: https://banking-app.tmprl-demo.cloud
    temporalMtls:
      certFiles:
        pathEnv:
          clientCert: TEMPORAL_CERT_PATH
          clientKey: TEMPORAL_KEY_PATH
    serverless:
      provider: aws-lambda
      packageType: image
      architecture: x86_64
      memoryMb: 512
      timeoutSeconds: 600
    worker:
      versioning:
        enabled: true
        deploymentName: money-transfer
```

## User Secret Targeted Env

```yaml
secrets:
  llm:
    requiredProperties:
      - ANTHROPIC_API_KEY
components:
  api:
    build: api
    env:
      - name: MODEL_API_KEY
        valueFrom:
          inputRef: secrets.llm.ANTHROPIC_API_KEY
```

Create:

```text
tmprl-dem-cld/<project>/llm
```

with:

```json
{
  "ANTHROPIC_API_KEY": "..."
}
```

## Private Repo Source Auth

```yaml
source:
  repo: https://github.com/temporal-sa/internal-demo
  gitRepository:
    url: ssh://git@github.com/temporal-sa/internal-demo.git
    authSecret: source-auth
```

Create:

```text
tmprl-dem-cld/<project>/source-auth
```

with JSON keys `identity` and `known_hosts`.

## Catalog Metadata

```yaml
registry:
  title: Banking Money Transfer
  summary: Demonstrates Temporal workflow orchestration for bank transfers.
  tags: [Banking, Workflows]
  temporalSdks: [Python]
  readiness:
    score: 4
    maxScore: 5
    note: Ready for technical demos.
```
