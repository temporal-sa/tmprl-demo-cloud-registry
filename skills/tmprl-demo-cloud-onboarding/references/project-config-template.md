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
    # Repos without Dockerfiles can opt into a buildpack build instead:
    # app:
    #   context: .
    #   strategy: buildpack
    #   builder: paketobuildpacks/builder-jammy-base
    #   env:
    #     BP_PYTHON_VERSION: "3.12.*"
    #   processes:
    #     web: python -m demo.web
    #     worker: python -m demo.worker
  components:
    frontend:
      build: frontend
      # For a buildpack process, use command: ["/cnb/process/web"]
      # single unnamed port -> name "http"; servicePort 80 => public catch-all
      ports: [{ containerPort: 5173, servicePort: 80 }]
    backend:
      build: backend
      temporalAccess: true        # starts/queries workflows -> needs Temporal creds
      # temporalProxy:
      #   enabled: true            # optional local-host compatibility sidecar
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

## Private/internal repos and source filters

Keep `source.repo` as the canonical HTTPS repo URL. For private repos, add the
SSH clone URL and the project-scoped AWS secret name containing Flux-compatible
`identity` and `known_hosts` keys. Use sparse checkout or ignore rules when only
some paths should trigger automatic image rebuilds and rollouts.

```yaml
spec:
  source:
    repo: https://github.com/temporal-sa/internal-demo
    gitRepository:
      url: ssh://git@github.com/temporal-sa/internal-demo.git
      authSecret: source-auth
      # Rendered by the operator as Flux ignore whitelist patterns.
      sparseCheckout:
        - app
        - workers
      ignore: |
        /docs
        /examples
```

Create the source-auth AWS secret before deployment:

```text
tmprl-dem-cld/<project-name>/source-auth
```

## Worker Versioning (only when the app uses it)

Use the object form. `taskQueue` lives here because it is consumed only by
versioning (it becomes the Worker Deployment Name). Worker Controller injects
`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_DEPLOYMENT_NAME`, and
`TEMPORAL_WORKER_BUILD_ID`; do not author those values in `env`.

```yaml
spec:
  components:
    worker:
      build: worker
      worker:
        versioning:
          enabled: true
          taskQueue: money-transfer
          rollout:
            strategy: AllAtOnce
            gate:
              workflowType: WorkerCanary
              input:
                project: my-project
          sunset:
            scaledownDelay: 1h
            deleteDelay: 24h
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

Use `{}` when every JSON key should be injected and no fail-fast key validation
is needed. Add `requiredProperties` only when missing keys should block deploy.

```yaml
spec:
  secrets:
    analytics: {}
    llm:
      requiredProperties:
        - ANTHROPIC_API_KEY
  components:
    api:
      build: api
      temporalAccess: true
      secrets:
        - analytics
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
`tls`). Non-versioned workers and API components receive those as
`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and
`TEMPORAL_TLS`. Versioned workers receive `TEMPORAL_ADDRESS` and
`TEMPORAL_NAMESPACE` from Worker Controller, while the project runtime secret
supplies only `TEMPORAL_API_KEY` and `TEMPORAL_TLS`. The platform bootstrap
credential remains outside project configs at
`tmprl-dem-cld/platform/temporal-cloud-bootstrap`.

For components that cannot consume those Cloud settings without target-repo code
changes, set `temporalProxy.enabled: true` on that Temporal-using component. The
app container connects to a local plaintext proxy at `127.0.0.1:7233`; the
sidecar owns the Cloud endpoint, namespace, TLS, and API-key auth. Use
`temporalProxy.addressEnv` only when the app expects host env vars beyond the
default `TEMPORAL_ADDRESS` and `TEMPORAL_HOST`.

## Kubernetes API access

Components get a project-local ServiceAccount with
`automountServiceAccountToken: false` by default. Enable token mounting only for
components that must call the Kubernetes API:

```yaml
spec:
  components:
    controller:
      build: controller
      automountServiceAccountToken: true
```
