# Projects

Add **one file per managed project** — that file is the only thing you drop into
the repo. Each file is a `DemoProject` custom resource. Flux delivers it to the
cluster and the registry operator does the rest: it derives the namespaces, host,
and prefixes; provisions the Temporal Cloud namespace + scoped API key; builds
images; deploys; smoke-tests; and promotes. There is no generation/render step
and nothing else to commit.

Files are validated in CI against `schemas/project.schema.json`.

A `DemoProject` is a normal Kubernetes CR — `apiVersion`, `kind`, `metadata.name`,
and a `spec`:

```yaml
apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
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
      ports: [{ containerPort: 5173, servicePort: 80 }]   # servicePort 80 => public
    backend:
      build: backend
      temporalAccess: true                                  # starts workflows
      ports: [{ containerPort: 8000, servicePort: 8000 }]
    worker:
      build: worker
      worker: true                                          # Temporal worker
  ingress:
    subdomain: banking-app
    # routes default to a catch-all at the servicePort-80 component
```

Rules:

- `source.repo` must be in the `temporal-sa` GitHub organization. `source.branch`
  defaults to `main`.
- `metadata.name` is the project identity. The operator derives everything from
  it: Kubernetes namespace `tmprl-dem-cld-<name>`, Temporal namespace
  `tmprl-dem-cld-<name>` (hash-truncated to 39 chars when needed), secret prefix
  `tmprl-dem-cld/<name>`, and ECR prefix `tmprl-dem-cld`. Do not hand-write these.
- `spec.registry` is **optional** catalog metadata (title/summary/tags/links/
  assets). When omitted, the catalog title falls back to a humanized name. Use
  it for runbooks, decks, recordings, and canonical docs.
- `builds` declare source build inputs. The platform derives ECR repositories
  and immutable image tags.
- `components` declare runtime pods and point at one declared build.
- `components.<name>.command` accepts a list of strings (inline YAML arrays are
  fine).
- A component gets stable Kubernetes service DNS when it declares `ports`. A lone
  port needs no `name` (it becomes `http`) and `servicePort` defaults to the
  container port. Default Service name is the component name; override with
  `serviceName` for cases like `http://backend:8000`.
- `ingress.subdomain` is only the left-hand label; never include
  `tmprl-demo.cloud`.
- `ingress.routes` is **optional**. When omitted, the operator exposes a single
  catch-all to the component serving `servicePort: 80`. Declare routes explicitly
  only for multi-route ingress or when no component serves 80.
- `ingress.temporalAuthRequired` defaults to `true`. Set it `false` only for
  reviewed public demos.
- `smokeCheck` is **optional**. When omitted, the candidate is promoted once its
  pods are ready (no HTTP probe). Add it to gate promotion on an owner-approved
  path, e.g. `path: /health`, `expectedStatus: 200`.
- Mark a worker component with `worker: true`. Worker components automatically
  receive Temporal connection env vars and a namespace-scoped API key. Set
  `temporalAccess: true` only on non-worker components, such as an API server,
  that need to start or query workflows.
- Worker versioning is opt-in via the object form: `components.<name>.worker.versioning`
  with `enabled`, `taskQueue`, and an `env` map of app-specific env var names.
  `taskQueue` is consumed only here (as the Worker Deployment Name), so it lives
  under `versioning`. Do not hard-code worker build/version env values in
  `components.<name>.env`.
- The platform injects standard Temporal env vars:
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and
  `TEMPORAL_TLS`.
- User-managed secrets are declared under `spec.secrets` and then referenced by
  components. The platform derives the AWS Secrets Manager path as
  `tmprl-dem-cld/<metadata.name>/<secret-name>`.
- Components receive user-managed secrets only when they list the secret name in
  `components.<name>.secrets`. Every JSON key in the AWS secret is injected as
  an environment variable, so keep each secret narrow and use env-var-shaped
  JSON keys.
- `secrets.<name>.requiredProperties` declares required JSON keys that the
  operator verifies in opt-in AWS external-resource mode before runtime
  workloads are deployed.

Example user-managed secret:

```yaml
spec:
  secrets:
    anthropic-credentials:
      requiredProperties:
        - ANTHROPIC_API_KEY
  components:
    worker:
      build: app
      worker: true
      secrets:
        - anthropic-credentials
```

Before deployment, create this AWS Secrets Manager secret:

```text
tmprl-dem-cld/<metadata.name>/anthropic-credentials
```

with JSON keys that should become environment variables:

```json
{
  "ANTHROPIC_API_KEY": "..."
}
```

In `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE=aws`, the operator reports missing or
malformed required keys under
`DemoProject.status.externalResources.secretsManager.userManaged`.

The platform IAM role for the project should be scoped to only:

```text
tmprl-dem-cld/<metadata.name>/*
```

so it cannot read or mutate secrets for other demos.

## Local to Cloud Shape

For locally runnable projects, keep `docker-compose.yml` focused on local
development. The project config maps the same service boundaries into cloud
deployment intent:

- each Compose `build` becomes a project `build`;
- each long-running Compose service becomes a `component`;
- Compose service names used for local DNS usually become component names or
  `serviceName` values;
- local-only environment values, such as `host.docker.internal`, are replaced
  with literal Kubernetes service DNS URLs like `http://backend:8000`.
  Temporal connection values come from platform injection.

Run validation before opening a PR:

```sh
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_projects.py
```
