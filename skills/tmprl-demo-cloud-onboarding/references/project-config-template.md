# Project Config Template

Use this only when creating a new registry config. A `DemoProject` is a normal
Kubernetes CR. Author it **thin** — the operator derives namespaces, host, and
prefixes from `metadata.name`, and sensible defaults cover routes, smoke checks,
port names, and the branch. Only add fields you actually need.

## Minimal thin config (the common case)

```yaml
apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  # concise, no "demo", ideally <= 25 chars (prefix + name must fit Temporal's 39)
  name: banking-money-transfer
spec:
  source:
    repo: https://github.com/temporal-sa/demo-banking-app-money-transfer
    # branch defaults to main
  builds:
    frontend: { context: ., dockerfile: docker/frontend.Dockerfile }
    backend:  { context: ., dockerfile: docker/backend.Dockerfile }
    worker:   { context: ., dockerfile: docker/worker-python.Dockerfile }
  components:
    frontend:
      build: frontend
      # single unnamed port -> name "http"; servicePort 80 => public catch-all
      ports: [{ containerPort: 5173, servicePort: 80 }]
    backend:
      build: backend
      temporalAccess: true        # starts/queries workflows -> needs Temporal creds
      ports: [{ containerPort: 8000, servicePort: 8000 }]
    worker:
      build: worker
      worker: true                # Temporal worker (no versioning)
  ingress:
    subdomain: banking-app
    # routes omitted -> catch-all to the servicePort-80 component (frontend)
  # smokeCheck omitted -> promote once pods are ready. Add one to gate on a path:
  # smokeCheck: { path: /api/health, expectedStatus: 200 }
```

## Optional catalog metadata

Add `spec.registry` for a richer landing-page card (otherwise the title defaults
to a humanized name):

```yaml
spec:
  registry:
    title: Banking Money Transfer
    summary: One concise sentence for the registry landing page.
    tags: [Banking, Workflows]
    temporalSdks: [Python]
    # links/assets: runbooks, decks, recordings, canonical docs
```

## Worker Versioning (only when the app uses it)

Use the object form. `taskQueue` lives here because it is consumed only by
versioning (it becomes the Worker Deployment Name); the `env` map names the
app's own versioning env vars.

```yaml
spec:
  components:
    worker:
      build: worker
      worker:
        versioning:
          enabled: true
          taskQueue: money-transfer
          env:
            enabled: TEMPORAL_VERSIONING_ENABLED
            buildId: TEMPORAL_BUILD_ID
            deploymentName: TEMPORAL_DEPLOYMENT_NAME
```

## Explicit ingress routes (only for multi-route ingress)

```yaml
spec:
  ingress:
    subdomain: my-app
    routes:
      - { component: api, pathPrefix: /api, port: http }
      - { component: web, port: http }   # catch-all (no pathPrefix)
```

## Custom secrets

```yaml
spec:
  secrets:
    llm:
      requiredProperties:
        - ANTHROPIC_API_KEY
  components:
    api:
      build: api
      temporalAccess: true
      secrets:
        - llm
```

Create this AWS Secrets Manager value before deployment:

```text
tmprl-dem-cld/<project-name>/llm
```

with JSON keys matching the environment variables the component expects:

```json
{
  "ANTHROPIC_API_KEY": "..."
}
```

The platform-managed Temporal credential is created by the operator at
`tmprl-dem-cld/<project-name>/temporal` (keys: `address`, `namespace`, `apiKey`,
`tls`) and injected as `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`,
`TEMPORAL_API_KEY`, `TEMPORAL_TLS`. The platform bootstrap credential remains
outside project configs at `tmprl-dem-cld/platform/temporal-cloud-bootstrap`.
