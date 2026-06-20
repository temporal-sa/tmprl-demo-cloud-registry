# DemoProject Contract

This reference documents the fields agents commonly need when proposing a
`DemoProject` YAML in an onboarding report. The live contract is still
`schemas/project.schema.json`, `projects/README.md`, and
`scripts/validate_projects.py`.

Do not write or modify project YAML while using this reference unless the user
has explicitly asked for implementation. In the default assessment path, include
YAML as a draft block in the report.

## Resource Shape

```yaml
apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: my-project
spec:
  source:
    repo: https://github.com/temporal-sa/my-project
  builds: {}
  components: {}
```

The live schema currently requires `spec.source`, `spec.builds`, and
`spec.components`; `builds` and `components` must each contain at least one
entry. A pure “manage only the Temporal namespace and credentials while I deploy
everything myself” project is not first-class today.

`metadata.name` is the project identity. The operator derives:

- Kubernetes namespace: `tmprl-dem-cld-<metadata.name>`
- Temporal Cloud namespace: `tmprl-dem-cld-<metadata.name>` with hash
  truncation when it would exceed the 39-character Temporal Cloud limit
- AWS Secrets Manager prefix: `tmprl-dem-cld/<metadata.name>`
- ECR repository prefix: `tmprl-dem-cld`

Keep the name lowercase, DNS-label-safe, and ideally 25 characters or less.
Do not include the word `demo`; every project is a demo.

## Source

`spec.source.repo` must be an HTTPS repo in the `temporal-sa` GitHub org.
`source.branch` defaults to `main`.

```yaml
spec:
  source:
    repo: https://github.com/temporal-sa/banking-money-transfer
    branch: main
```

For private/internal repos, keep `source.repo` as canonical HTTPS and add
Flux/CodeBuild auth under `source.gitRepository`:

```yaml
spec:
  source:
    repo: https://github.com/temporal-sa/internal-demo
    gitRepository:
      url: ssh://git@github.com/temporal-sa/internal-demo.git
      authSecret: source-auth
```

Create the source auth secret at
`tmprl-dem-cld/<project>/source-auth` with Flux-compatible JSON keys
`identity` and `known_hosts`. See `secrets.md`.

For monorepos, add filters:

```yaml
spec:
  source:
    repo: https://github.com/temporal-sa/platform-demos
    gitRepository:
      sparseCheckout:
        - demos/banking
        - packages/shared
      ignore: |
        /docs
        /examples
```

The operator renders `sparseCheckout` as Flux ignore whitelist patterns when the
cluster CRD lacks native sparse checkout. Use filters to prevent unrelated
commits from rebuilding images or triggering WorkerDeployment gates.

## Builds

Every component references a build by name. Build names are DNS labels.

Dockerfile build, the default:

```yaml
spec:
  builds:
    api:
      context: .
      dockerfile: docker/api.Dockerfile
```

Buildpack build:

```yaml
spec:
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

Use `builds.<name>.env` only for build-time buildpack settings. Runtime env
belongs under components. If `processes` are supplied, launch one with
`components.<name>.command`, usually `["/cnb/process/<process-name>"]`.

Serverless Lambda workers must reference a Dockerfile build; buildpacks are not
supported for Lambda image workers.

## Components

Components are runtime units. A Kubernetes component is a pod/deployment/service
candidate. A serverless component is an AWS Lambda image worker and has stricter
rules; see `serverless-lambda.md`.

Basic Kubernetes components:

```yaml
spec:
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
        - name: http
          containerPort: 8000
          servicePort: 8000
    worker:
      build: worker
      worker: true
```

Fields:

- `build`: required; references `spec.builds`.
- `command` and `args`: string arrays overriding image defaults.
- `ports`: creates stable service DNS and enables ingress targets.
- `serviceName`: override only when app code expects a specific DNS name.
- `replicas`: optional; omit for platform defaults.
- `env`: literal env vars or platform input refs.
- `secrets`: bulk inject user-managed AWS secret JSON keys as env vars.
- `temporalAccess`: true for non-worker services that call Temporal.
- `worker`: `true` for ordinary workers, or object form for Worker Versioning.
- `temporalProxy`: compatibility sidecar for Kubernetes API-key projects.
- `temporalMtls`: mTLS file/env configuration.
- `automountServiceAccountToken`: defaults false; true only when the component
  must call the Kubernetes API.

## Ports And Service DNS

A component gets a Kubernetes Service only when it declares `ports`.

```yaml
ports:
  - containerPort: 8000
    servicePort: 8000
```

Rules:

- A lone unnamed port becomes `name: http`.
- `servicePort` defaults to `containerPort`.
- Internal DNS is normally `http://<component>:<servicePort>`.
- Use `serviceName` only when app code or config needs a name different from the
  component name.

## Environment Variables

Literal env:

```yaml
env:
  - name: BANKING_BACKEND_URL
    value: http://backend:8000
```

Platform input refs:

```yaml
env:
  - name: TEMPORAL_HOST
    valueFrom:
      inputRef: temporal.address
  - name: MODEL_API_KEY
    valueFrom:
      inputRef: secrets.llm.ANTHROPIC_API_KEY
```

Supported `temporal.*` refs:

- API-key mode: `temporal.address`, `temporal.namespace`,
  `temporal.apiKey`, `temporal.tls`
- mTLS mode: `temporal.caCert`, `temporal.clientCert`, `temporal.clientKey`

`secrets.<secret>.<PROPERTY>` refs require `spec.secrets.<secret>` and the
property must be env-var-shaped, e.g. `ANTHROPIC_API_KEY`.

Do not author standard Worker Versioning envs such as
`TEMPORAL_DEPLOYMENT_NAME` or `TEMPORAL_WORKER_BUILD_ID`; Worker Controller
owns those.

For Lambda workers, standard Temporal and Worker Deployment env vars are also
set by the Lambda wrapper. Custom Worker Deployment env names are configured
under `serverless.workerDeployment.env`, not as literal `env` values.

## Ingress

`ingress.subdomain` is the left-hand label only. Do not include
`tmprl-demo.cloud`.

```yaml
spec:
  ingress:
    subdomain: banking-app
```

When routes are omitted, the operator exposes one catch-all route to the
component serving `servicePort: 80`.

Explicit single target:

```yaml
ingress:
  subdomain: api-demo
  component: api
  port: http
```

Explicit routes:

```yaml
ingress:
  subdomain: banking-app
  routes:
    - pathPrefix: /api
      component: backend
      port: http
    - pathPrefix: /
      component: frontend
      port: http
```

Rules:

- Public URLs are HTTPS; HTTP redirects to HTTPS.
- `ingress.routes[].passHostHeader` defaults true. Set false only if the
  upstream rejects the public host and can safely receive Traefik's upstream
  host.
- `ingress.temporalAuthRequired` defaults true. Set false only for reviewed
  public demos.
- For multi-route apps, include a catch-all `/` route if the frontend should
  own everything not matched earlier.

## Smoke Check

`smokeCheck` is optional. Without it, candidate promotion waits for pod
readiness. Add a stable owner-approved endpoint when available:

```yaml
spec:
  smokeCheck:
    path: /api/health
    expectedStatus: 200
    timeoutSeconds: 10
```

The smoke check runs against the candidate before public routing is promoted, so
the path must work in the candidate namespace.

## Worker Settings

Ordinary worker:

```yaml
worker: true
```

Worker Versioning:

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

Use Kubernetes Worker Versioning only when the app uses SDK Worker Versioning
and can satisfy Worker Controller rollout expectations. Serverless Lambda
workers use SDK Worker Versioning too, but configure their Worker Deployment
under `serverless.workerDeployment`; do not add `worker.versioning` to Lambda
components.

`worker.livenessProbe` may be enabled in the object form when the worker process
supports the platform heartbeat file contract. Do not enable it unless the app
or worker wrapper actually refreshes the heartbeat after a successful Temporal
round trip.

## Manual Or External Runtime Mode

Some users may want the operator to manage only Temporal Cloud namespace/auth
material while they deploy their own Kubernetes resources manually. That is not
currently a clean `DemoProject` mode.

Current limitations:

- `source`, `builds`, and `components` are required by schema.
- Temporal runtime credentials are created and rotated only when at least one
  component needs Temporal (`worker`, `temporalAccess`, `temporal.*` refs,
  `temporalProxy`, mTLS files, or Lambda serverless worker).
- Search attributes can make a Temporal namespace required, but they do not by
  themselves require API key or mTLS runtime credentials.
- Kubernetes `ExternalSecret` resources for `temporal-credentials` or
  `temporal-worker-runtime` are rendered only for components that need those
  credentials.

There is an internal retain annotation:

```yaml
metadata:
  annotations:
    registry.tmprl-demo.cloud/finalization-policy: retain
```

Do not recommend it as a general user-facing fix. Today it is overloaded: it
skips normal finalizer cleanup and also disables revision runtime namespaces,
leaving resources in the stable project namespace. It is used for the
control-plane project, but ordinary demos need a first-class external/manual
runtime mode before this pattern is safe to expose.

## Registry Catalog Metadata

`spec.registry` is optional but useful for catalog cards:

```yaml
spec:
  registry:
    title: Banking Money Transfer
    summary: Demonstrates money-transfer workflow orchestration.
    tags: [Banking, Workflows]
    temporalSdks: [Python]
    readiness:
      score: 4
      maxScore: 5
      note: Ready for technical demos.
    links:
      speakerNotes:
        label: Speaker notes
        url: https://example.com/notes
```

When omitted, the catalog falls back to a humanized project name.

## Rollout Overrides

`spec.rollout` is advanced and usually omitted. It can pin image overrides or a
source commit SHA for controlled rollout/testing:

```yaml
spec:
  rollout:
    sourceCommitSha: abc1234
    imageOverrides:
      worker: 123456789012.dkr.ecr.us-west-1.amazonaws.com/...
```

Do not use rollout overrides for normal onboarding unless the user explicitly
wants a pinned build.

## Validation

From the registry repo:

```sh
python3 scripts/validate_projects.py
```

Validation catches schema errors, duplicate project names/subdomains, invalid
component refs, bad ingress routes, invalid secret refs, incompatible Temporal
auth, serverless Lambda unsupported fields, and catalog list issues.
