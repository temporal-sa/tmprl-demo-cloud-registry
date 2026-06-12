# Projects

Add **one file per managed project** — that file is the only thing you drop into
the repo. Each file is a `DemoProject` custom resource. Flux delivers it to the
cluster and the registry operator does the rest: it derives the namespaces, host,
and prefixes; provisions the Temporal Cloud namespace + scoped API key; builds
images; deploys; smoke-tests; and promotes. There is no generation/render step
and nothing else to commit.

Files are validated in CI against `schemas/project.schema.json`.

## Supported Path Today

The supported path for newly added demos is the thin `DemoProject` contract
proven by `banking-money-transfer`:

- The source repo is in the `temporal-sa` GitHub organization and can be built by
  CodeBuild from one or more Dockerfiles declared under `builds`.
- Runtime shape is stateless, long-running components. Components that need
  stable Kubernetes DNS declare `ports`; public ingress points at either the lone
  component serving `servicePort: 80` or explicit `ingress.routes`.
- Temporal access is platform-managed. Use `worker: true` for normal
  non-versioned workers, and set `temporalAccess: true` only on non-worker
  components that start or query workflows.
- Components that can only be pointed at a local Temporal host can opt into the
  platform Temporal Cloud proxy sidecar with `temporalProxy.enabled: true`.
- Private/internal repos and monorepo path filters are supported through
  `source.gitRepository.url`, `source.gitRepository.authSecret`,
  `source.gitRepository.sparseCheckout`, and `source.gitRepository.ignore`. The
  referenced source-auth AWS secret value must exist before deployment.
- Custom component secrets are supported as AWS Secrets Manager JSON objects
  under `tmprl-dem-cld/<metadata.name>/<secret-name>`. Secret values are still
  created manually before deployment; the operator verifies declared
  `requiredProperties` when present.
- Worker Versioning is advanced and opt-in. Use `worker.versioning` only for
  workers that already run with SDK Worker Versioning and can satisfy Worker
  Controller rollout expectations. Otherwise use the shorthand `worker: true`.
- Project-owned Kubernetes, Helm, Flux overlays, and BYO image rollouts are not
  the current onboarding path. Adapt those demos into container builds and
  `components` first.
- `projects/registry.yaml` is a control-plane/self-management project and should
  not be copied as the ordinary demo template.

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
- For private/internal repositories, set `source.gitRepository.authSecret` to an
  AWS Secrets Manager secret name under `tmprl-dem-cld/<metadata.name>/`. The
  operator syncs it into the source-watch namespace and points the generated
  Flux `GitRepository` at it. CodeBuild also uses the same secret to clone the
  repo when building images. For SSH deploy keys, the AWS secret should contain
  the Flux-compatible JSON keys `identity` and `known_hosts`. Use
  `source.gitRepository.url` for the SSH clone URL, such as
  `ssh://git@github.com/temporal-sa/private-demo.git`, while `source.repo`
  remains the canonical project source URL.
- `source.gitRepository.sparseCheckout` is an authoring shortcut for path
  whitelists; the operator renders it as Flux `GitRepository.spec.ignore`
  patterns because the installed source-controller CRD does not support a
  native sparse checkout field. Use `source.gitRepository.ignore` directly for
  additional Flux ignore patterns when only specific paths should count as
  source changes for automatic image rollouts.
- `metadata.name` is the project identity. The operator derives everything from
  it: Kubernetes namespace `tmprl-dem-cld-<name>`, Temporal namespace
  `tmprl-dem-cld-<name>` (hash-truncated to 39 chars when needed), secret prefix
  `tmprl-dem-cld/<name>`, and ECR prefix `tmprl-dem-cld`. Do not hand-write these.
- `spec.registry` is **optional** catalog metadata (title/summary/tags/links/
  assets). When omitted, the catalog title falls back to a humanized name. Use
  it for runbooks, decks, recordings, and canonical docs.
- `builds` declare source build inputs. The platform derives ECR repositories
  and immutable image tags. Dockerfile builds are the default:
  `{ context: ., dockerfile: Dockerfile }`. For repos without Dockerfiles, use
  an explicit buildpack build:

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
  ```

  Buildpack `env` is build-time env passed to `pack build`; runtime env still
  belongs under `components.<name>.env`. Buildpack `processes` is optional; when
  present, the operator appends a generated process buildpack that contributes
  those entries to the CNB launch metadata. Start one of those generated process
  types with `components.<name>.command: ["<process-name>"]`.
- `components` declare runtime pods and point at one declared build.
- `components.<name>.command` accepts a list of strings (inline YAML arrays are
  fine).
- Components get a project-local ServiceAccount with
  `automountServiceAccountToken: false` by default. Set
  `components.<name>.automountServiceAccountToken: true` only when that
  component must call the Kubernetes API.
- A component gets stable Kubernetes service DNS when it declares `ports`. A lone
  port needs no `name` (it becomes `http`) and `servicePort` defaults to the
  container port. Default Service name is the component name; override with
  `serviceName` for cases like `http://backend:8000`.
- `ingress.subdomain` is only the left-hand label; never include
  `tmprl-demo.cloud`.
- Public demo URLs are always HTTPS. The operator creates a TLS route and a
  matching HTTP route that permanently redirects to HTTPS.
- `ingress.routes` is **optional**. When omitted, the operator exposes a single
  catch-all to the component serving `servicePort: 80`. Declare routes explicitly
  only for multi-route ingress or when no component serves 80.
- `ingress.routes[].passHostHeader` defaults to `true`. Set it `false` only
  when the upstream app rejects the public `tmprl-demo.cloud` Host header and
  can safely receive Traefik's upstream host instead.
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
  with `enabled`, `taskQueue`, and optional `rollout`/`sunset` settings.
  `taskQueue` is consumed only here (as the Worker Deployment Name), so it lives
  under `versioning`. Do not hard-code worker build/version env values in
  `components.<name>.env`; Worker Controller injects `TEMPORAL_DEPLOYMENT_NAME`
  and `TEMPORAL_WORKER_BUILD_ID`. For versioned workers, Worker Controller also
  owns `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`; the runtime secret only
  supplies `TEMPORAL_API_KEY` and `TEMPORAL_TLS`.
- For non-versioned workers and API components, the platform injects standard
  Temporal env vars:
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and
  `TEMPORAL_TLS`.
- `components.<name>.temporalProxy.enabled: true` replaces direct Temporal
  credential injection for that app container with a local plaintext proxy at
  `127.0.0.1:7233`. The sidecar receives the platform-managed Cloud credential,
  forwards to Temporal Cloud over TLS, and rewrites local `default` namespace
  requests to the project namespace. The app container receives
  `TEMPORAL_ADDRESS=127.0.0.1:7233`, `TEMPORAL_HOST=127.0.0.1:7233`,
  `TEMPORAL_NAMESPACE=default`, and `TEMPORAL_TLS=false` by default. Use
  `temporalProxy.addressEnv` when the app uses different host env var names.
- The proxy path supports demos with SDK setup like
  `Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)`, where the app
  defaults to `localhost:7233` and namespace `default` for local development.
  Do not set `TEMPORAL_API_KEY` for the app container; the sidecar receives the
  real Cloud credential. If the app needs a Temporal UI link, set
  `TEMPORAL_UI_URL` as a literal env var using the derived Cloud namespace URL,
  for example
  `https://cloud.temporal.io/namespaces/tmprl-dem-cld-flourish-and-blotts.a2dd6`.
- Components receive user-managed secrets when they list the secret name in
  `components.<name>.secrets`. The platform derives the AWS Secrets Manager path
  as `tmprl-dem-cld/<metadata.name>/<secret-name>` and injects every JSON key in
  that AWS secret as an environment variable. Keep each secret narrow and use
  env-var-shaped JSON keys.
- `spec.secrets.<name>.requiredProperties` is optional validation metadata. Use
  it only when the project should fail early if specific JSON keys are missing
  from the AWS secret.

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
      temporalProxy:
        enabled: true
      env:
        - name: TEMPORAL_UI_URL
          value: https://cloud.temporal.io/namespaces/tmprl-dem-cld-flourish-and-blotts.a2dd6
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
