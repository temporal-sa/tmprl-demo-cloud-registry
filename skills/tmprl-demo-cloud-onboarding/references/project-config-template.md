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
  web:
    context: .
    dockerfile: <path/to/web.Dockerfile>
  worker:
    context: .
    dockerfile: <path/to/worker.Dockerfile>

components:
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
    - component: web
      port: http

smokeCheck:
  path: /health
  expectedStatus: 200
```

For custom secrets:

```yaml
components:
  api:
    build: api
    temporalAccess: true
    secrets:
      - llm
```

Create this AWS Secrets Manager value before deployment:

```text
tmprl-cloud-demo/<project-name>/llm
```

with JSON keys matching the environment variables the component expects:

```json
{
  "ANTHROPIC_API_KEY": "..."
}
```
