# Project Config Template

Use this only when creating a new registry config.

```yaml
metadata:
  name: <dns-label-project-name>

registry:
  title: <Human Demo Title>
  summary: <One concise sentence for the registry landing page.>
  tags:
    - <Tag>
  temporalSdks:
    - <Python|TypeScript|Go|Java|.NET|PHP>

source:
  repo: https://github.com/temporal-sa/<repo>
  branch: main

builds:
  api:
    context: .
    dockerfile: <path/to/api.Dockerfile>
  web:
    context: .
    dockerfile: <path/to/web.Dockerfile>
  worker:
    context: .
    dockerfile: <path/to/worker.Dockerfile>

components:
  api:
    build: api
    temporalAccess: true
    ports:
      - name: http
        containerPort: 8000
        servicePort: 80

  web:
    build: web
    ports:
      - name: http
        containerPort: 8080
        servicePort: 80

  worker:
    build: worker
    worker:
      taskQueue: <task-queue>
    env:
      - name: <APP_SETTING>
        value: <value>

ingress:
  subdomain: <left-hand-subdomain-only>
  routes:
    - component: api
      pathPrefix: /api
      port: http
    - component: web
      port: http

smokeCheck:
  path: /api/health
  expectedStatus: 200
```

For custom secrets:

```yaml
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

For components with Temporal access, the platform syncs this managed secret:

```text
tmprl-dem-cld/<project-name>/temporal
```

with JSON keys:

```json
{
  "address": "<region>.<cloud-provider>.api.temporal.io:7233",
  "namespace": "<project-temporal-namespace>.<temporal-account-id>",
  "apiKey": "<namespace-scoped-api-key>",
  "tls": "true"
}
```

The platform bootstrap credential remains outside project configs:

```text
tmprl-dem-cld/platform/temporal-cloud-bootstrap
```

with JSON keys:

```json
{
  "TEMPORAL_CLOUD_API_KEY": "<bootstrap-cloud-ops-api-key>",
  "TEMPORAL_CLOUD_ACCOUNT_ID": "<temporal-account-id>",
  "TEMPORAL_CLOUD_REGION": "<temporal-region>"
}
```
