# Projects

Add **one file per managed project or course** — that file is the only thing you
drop into the repo. Demo apps go under `projects/demo/` as `DemoProject`
resources. Instruqt courses go under `projects/instruqt/` as `InstruqtCourse`
resources. Flux delivers the CRs to the cluster and the registry operator does
the rest. There is no generation/render step and nothing else to commit.
Do not maintain per-directory `kustomization.yaml` indexes here; the Flux
Kustomizations point directly at these folders and auto-include the plain YAML
manifests under each path.

Files are validated in CI against the matching schema for their `kind`.

## Supported Path Today

The supported path for newly added demos is the thin `DemoProject` contract
proven by `banking-money-transfer`:

- The source repo is in the `temporal-sa`, `temporalio`, or `temporal-community`
  GitHub organization and can be built by CodeBuild from one or more
  Dockerfiles declared under `builds`.
- Runtime shape is stateless, long-running components. Components that need
  stable Kubernetes DNS declare `ports`; public ingress points at either the lone
  component serving `servicePort: 80` or explicit `ingress.routes`.
- Temporal access is platform-managed. Use `worker: true` for normal
  non-versioned workers, and set `temporalAccess: true` only on non-worker
  components that start or query workflows.
- Components that can only be pointed at a local Temporal host can opt into the
  platform Temporal Cloud proxy sidecar with `temporalProxy.enabled: true`.
- Private/internal repos in `temporal-sa` use the shared platform GitHub PAT
  from AWS Secrets Manager path
  `tmprl-dem-cld/platform/github-temporal-sa-readonly`. Monorepo path filters
  are supported through `source.gitRepository.sparseCheckout` and
  `source.gitRepository.ignore`. Per-project `source.gitRepository.authSecret`
  is still available for explicit source-auth overrides such as SSH deploy keys.
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
- `projects/demo/registry.yaml` is a control-plane/self-management project and
  should not be copied as the ordinary demo template.

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

An `InstruqtCourse` is also a normal Kubernetes CR. The CR means the operator
owns reconciliation from Git for that course. Local `instruqt track push` is fine
while authoring, but committed Git state is authoritative when the operator
reconciles.

```yaml
apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: InstruqtCourse
metadata:
  name: durable-ad-campaign-python-workshop
spec:
  source:
    repo: https://github.com/temporal-sa/instruqt-example-track
    branch: main
    testingBranch: live-test
    path: instruqt
  content:
    repo: https://github.com/temporal-sa/public-workshop-code
    branch: main
    # Optional. If omitted, the testing track uses content.branch.
    testingBranch: live-test
    path: .
  images:
    workstation:
      container: workstation
      context:
        source: content
        path: .
      dockerfile:
        source: source
        path: Dockerfile
      platform: linux/amd64
      buildArgs:
        PYTHON_VERSION: "3.12"
  track:
    organization: temporal
    slug: durable-ad-campaign-python-workshop
  testingTrack:
    slug: testing-durable-ad-campaign-python-workshop
    titlePrefix: "[testing] "
  catalog:
    title: Hands on Temporal Introduction
    summary: Build a durable advertising campaign application with Temporal and Python.
    tags: [Durable execution, Workshop]
    temporalSdks: [Python]
    resources:
      - label: Facilitator guide
        url: https://docs.google.com/document/d/example
        kind: guide
```

`source` is the Instruqt track config repo and path. `content` is optional and
points at a second repo for exercise code or other build inputs. `images` binds
track containers to operator-built images; the operator will replace matching
`config.yml` image refs with ECR images instead of trusting user-authored image
strings. If the matching `containers[].image` key is missing, the operator
injects it. If it is present, the operator overwrites it. Track configs that use
VMs or lifecycle scripts without managed containers are left alone. The workflow
reconciles ECR repositories, builds managed images with AWS CodeBuild, renders a
local track artifact, then runs only `instruqt track push --force`. It does not
separately run `track validate`, `track test`, or `track deploy`.

`catalog` controls how the course appears in the registry's Courses section.
`title` and `summary` are required when the block is present. Add searchable
`tags` and `temporalSdks`, plus any related HTTPS resources such as facilitator
guides, slide decks, recordings, or source repositories. Courses that are
language versions of the same lab set the same `parent` and a distinct
`variant`, such as `Java` or `Python`. The registry shows that parent once and
uses each variant to open its track. Every course in a parent group must use
the same title and summary.

Testing tracks are branch-driven. `source.branch` is the production source of
truth and defaults to `main`. `source.testingBranch` enables a separate managed
testing track. If that branch exists, the operator resolves it through a
separate Flux `GitRepository` named `<metadata.name>-testing-source` and records
testing status under `status.testing`. If GitHub confirms the branch is absent,
the operator deletes the testing source-watch resources and reports the testing
channel as `BranchMissing`. `content.testingBranch` is optional; when omitted,
the testing track uses `content.branch`. Removing `source.testingBranch` from
the CR disables the testing channel. When GitHub confirms the configured testing
branch is absent, the workflow deletes the managed testing track.

The operator expects the platform Instruqt CLI token at
`tmprl-dem-cld/platform/instruqt-cli` with JSON shape
`{ "INSTRUQT_TOKEN": "...", "organization": "temporal" }`. The worker image
contains the Instruqt CLI and records CLI/token readiness under
`status.cli`.

Rules:

- `source.repo` and, when configured, `content.repo` must be in the
  `temporal-sa`, `temporal-community`, or `temporalio` GitHub organization.
  `source.branch` defaults to `main`.
- `track.organization` and `track.slug` define the production Instruqt target.
  The operator rewrites rendered `track.yml` to that owner/slug before pushing.
- `source.testingBranch` is the testing-track lifecycle switch. The operator
  probes GitHub with the shared read-only PAT; auth/API failures report
  `BranchUnknown` and do not delete testing resources.
- `testingTrack.slug` defaults to `testing-<track.slug>` when omitted.
  `testingTrack.titlePrefix` defaults to `[testing] `.
- `InstruqtCourse.spec.content` is optional. Use it when the track config repo is
  private/internal but the exercise code should live in a separate public repo.
  The operator resolves both Git sources before reconciling the course.
- `content.testingBranch` requires `source.testingBranch`; it only selects
  alternate exercise code for the testing track.
- `InstruqtCourse.spec.images.*.context.source` and `.dockerfile.source` may be
  `source` or `content`. A reference to `content` requires `spec.content`.
- `InstruqtCourse.spec.images.*.container` must match a `containers[].name` entry
  in the rendered Instruqt `config.yml`. The operator owns that container's
  `image` value and will inject or overwrite it with the resolved ECR image.
  Container images without a matching `spec.images` binding are rejected for
  managed courses.
- `InstruqtCourse.spec.images.*.platform`, `.target`, and string-valued
  `.buildArgs` are optional Docker build parameters passed to CodeBuild.
  Managed Instruqt images are published to ECR Public because Instruqt pulls
  them from outside the cluster.
- HTTPS source watches and CodeBuild checkouts for temporal-sa repos use the
  shared platform AWS Secrets Manager value
  `tmprl-dem-cld/platform/github-temporal-sa-readonly`. Its JSON shape is
  `{ "username": "<github-user>", "password": "github_pat_...", "expiresAt": "YYYY-MM-DD" }`.
- Instruqt CLI operations use the shared platform AWS Secrets Manager value
  `tmprl-dem-cld/platform/instruqt-cli`. Its JSON shape is
  `{ "INSTRUQT_TOKEN": "...", "organization": "temporal" }`.
- For explicit per-project source auth, set `source.gitRepository.authSecret` to
  an AWS Secrets Manager secret name under `tmprl-dem-cld/<metadata.name>/`.
  The operator syncs it into the source-watch namespace and points the generated
  Flux `GitRepository` at it. CodeBuild also uses the same secret to clone the
  repo when building images. For SSH deploy keys, the AWS secret should contain
  the Flux-compatible JSON keys `identity` and `known_hosts`. Use
  `source.gitRepository.url` for the SSH clone URL, such as
  `ssh://git@github.com/temporal-sa/private-demo.git`, while `source.repo`
  remains the canonical project source URL. An override URL must also target
  `temporal-sa`, `temporal-community`, or `temporalio`.
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
  A project that declares extra namespaces derives each as
  `tmprl-dem-cld-<name>-<label>` under the same truncation rule — see
  "Additional Temporal namespaces" below.
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
- Kubernetes components can set `awsIamRoleArn` to an existing IAM role ARN.
  The operator annotates that component's ServiceAccount with
  `eks.amazonaws.com/role-arn`, allowing the EKS identity webhook and AWS SDK
  credential chain to use temporary workload credentials. This does not enable
  Kubernetes API token automount. Provision the role and its resource policy
  separately, and scope its trust to the cluster OIDC provider and component
  service account in all applicable project revision namespaces. Lambda
  components use their managed execution role and reject this field.
  When this field is omitted, existing role annotations are preserved, including
  those managed by platform bootstrap manifests. Removing an existing role
  requires an explicit change by its owner; omission does not revoke access.
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
- Temporal runtime authentication defaults to API keys. Projects may declare
  `spec.temporal.auth.mode: apiKey` explicitly, but omitting it is equivalent.
  `spec.temporal.auth.mode: mtls` creates a Temporal Cloud namespace using mTLS
  instead. The mTLS contract is a platform-scoped CA with namespace-scoped
  client certificates filtered by common name. Client certificates rotate
  weekly, and the platform CA rotates quarterly on a separate schedule. New
  API keys and client certificates are canaried by running a worker workflow in
  the namespace before the runtime secret is promoted. The runtime secret exposes `address`,
  `namespace`, `tls`, `caCert`, `clientCert`, and `clientKey`.
- Switching between `apiKey` and `mtls` is destructive because Temporal Cloud
  namespaces cannot enable both at once. Finalize the project first so the
  existing namespace is deleted, then deploy it again with the new auth mode.
- Projects may pin the Temporal Cloud namespace region with `spec.temporal.region`
  in Temporal's provider notation (`aws-*`, `gcp-*`, `azure-*`, e.g.
  `aws-ca-central-1`, `gcp-us-central1`, `azure-centralus`). When omitted, the
  namespace uses the operator-wide default region, except that projects with
  serverless (AWS Lambda) workers default to an AWS region so Temporal's Lambda
  compute provider can reach the worker. Changing the region of an existing
  namespace is destructive: because a Temporal Cloud namespace's region is
  immutable, the operator tears the namespace down (deleting its data) and
  recreates it in the new region.
- Projects can declare Temporal Cloud namespace search attributes with
  `spec.temporal.searchAttributes`. Each attribute defaults to `keyword` when
  `type` is omitted; supported types are `text`, `keyword`, `int`, `double`,
  `bool`, `datetime`, and `keywordList`.

  ```yaml
  temporal:
    searchAttributes:
      CustomerId: {}
      CartTotal:
        type: double
      FulfillmentTags:
        type: keywordList
  ```

  The operator creates or verifies declared attributes after namespace
  reconciliation. It only deletes removed attributes that it previously created
  and recorded as managed; unrelated namespace attributes are left intact.
  Search attributes apply to the project's own namespace only. Declaring
  `searchAttributes` under a `spec.temporal.namespaces` entry is a validation
  error, not a silent no-op: the reconcile step that manages them computes its
  delete set against one project-scoped status blob, so running it per namespace
  would let one namespace delete another's attributes.
- Projects can declare **additional Temporal Cloud namespaces** with
  `spec.temporal.namespaces`, keyed by a label. Each label becomes a real
  namespace `tmprl-dem-cld-<name>-<label>` with its own service account, API
  key, Secrets Manager path, and Kubernetes Secret. Omitting the block is the
  default and leaves a project with the single namespace it has always had.

  ```yaml
  temporal:
    namespaces:
      alpha: {}
      beta:
        region: aws-us-west-2     # optional; defaults like the project's own
        retentionDays: 14         # optional; defaults to 7
  ```

  Labels are DNS labels. `candidate` and `ca-candidate` are reserved — they
  collide with the credential-rotation staging paths. Additional namespaces
  inherit `spec.temporal.auth.mode`; auth is not configured per namespace.
  Every declared label must be referenced by a component or a Nexus endpoint.

  Finalizing the project tears down every declared namespace along with the
  project's own — Nexus Endpoints first, then each declared namespace newest
  first, then the project's own last. Deleting a single entry from a live
  project is *not* the same thing: it stops reconciling that namespace but
  leaves it running. The operator reports those under `orphanedNamespaces` in
  `status.externalResources.temporalCloud`, with the namespace and secret paths
  to clean up by hand; it never deletes them itself. See `docs/operations.md`.
- Bind a component to a declared namespace with `temporalNamespace`. Absent, a
  component uses the project's own namespace, which is what every component
  does today.

  ```yaml
  components:
    alpha-worker:
      build: app
      worker: true
      temporalNamespace: alpha
  ```

  `temporalNamespace` cannot be combined with `worker.versioning`,
  `temporalProxy`, or `serverless.provider: aws-lambda`. The Worker Controller's
  `Connection` and `WorkerDeployment` resolve the project's own namespace and
  credentials and are not namespace-bindable, so a bound versioned worker would
  be registered in one namespace while its pod authenticated to another. A
  project may still have versioned workers *and* additional namespaces — what is
  refused is one component being both versioned and bound.
- Projects can declare **Nexus Endpoints** with `spec.temporal.nexusEndpoints`.
  `allowedCallerNamespaces` names namespaces inside this demo. `allowedCallerProjects`
  names other demos by `DemoProject` name. The operator derives each caller's
  primary Temporal namespace. Write the project name, not the resolved namespace.
  At least one of the two lists is required, because Temporal admits no callers
  by default. A caller whose project CR is gone is dropped on the next reconcile.
  A caller whose namespace is not active yet is retried, and that retry does not
  wait on the other demo's endpoints.

  ```yaml
  temporal:
    nexusEndpoints:
      my-project-orders:
        targetNamespace: beta            # "primary" or a declared label
        targetTaskQueue: orders-tq
        allowedCallerNamespaces:         # this demo's own namespaces
          - primary
          - alpha
        allowedCallerProjects:           # other DemoProject names
          - other-demo
  ```

  Endpoint names are unique across the whole Temporal Cloud account, not just
  your project, so prefix them with the project name. They must start with a
  letter and use only letters, digits, and hyphens. `targetTaskQueue` is not
  cross-checked against anything — the platform does not track task queues, so
  keeping it aligned with whatever your worker polls is on you, exactly like the
  `TASK_QUEUE` env vars already are. Point it at a task queue served by an
  **unversioned** worker: recent Temporal Python SDK versions no longer create a
  versioned Nexus task queue automatically. Endpoints reconcile only after every
  namespace reports Ready, and are deleted before any namespace during finalize.
  The Nexus contract itself — service definitions, handlers, SDK wiring — lives
  in your app's own code.
- Worker versioning is opt-in via the object form:
  `components.<name>.worker.versioning` with `enabled`, optional
  `deploymentName`, and optional `rollout`/`sunset` settings. `deploymentName`
  is the Worker Deployment identity managed by the operator; when omitted it
  defaults to the component name. Do not hard-code worker build/version env
  values in `components.<name>.env`; Worker Controller injects
  `TEMPORAL_DEPLOYMENT_NAME` and `TEMPORAL_WORKER_BUILD_ID`. For versioned
  workers, Worker Controller also owns `TEMPORAL_ADDRESS` and
  `TEMPORAL_NAMESPACE`; the runtime secret only supplies `TEMPORAL_API_KEY` and
  `TEMPORAL_TLS`. Task queues are app-owned routing names; configure them with
  generic component env when the app needs an env-driven value. mTLS is
  currently blocked with Worker Controller versioning until the Connection CRD
  supports client certificate refs.
- For non-versioned workers and API components, the platform injects standard
  Temporal env vars. API-key projects receive `TEMPORAL_ADDRESS`,
  `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and `TEMPORAL_TLS`. mTLS projects
  receive `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TLS`,
  `TEMPORAL_CA_CERT`, `TEMPORAL_CLIENT_CERT`, and `TEMPORAL_CLIENT_KEY`.
  mTLS projects also mount the same certificate material as read-only files for
  Kubernetes components that receive Temporal credentials. The default mount is
  `/var/run/secrets/tmprl-demo.cloud/temporal` with `ca.crt`, `tls.crt`, and
  `tls.key`. The platform injects path env vars
  `TEMPORAL_MTLS_CERT_DIR`, `TEMPORAL_MTLS_CA_CERT_PATH`,
  `TEMPORAL_MTLS_CLIENT_CERT_PATH`, and `TEMPORAL_MTLS_CLIENT_KEY_PATH`.
  Components can customize the mount and app-specific path env var names with
  `components.<name>.temporalMtls.certFiles`:

  ```yaml
  temporalMtls:
    certFiles:
      mountPath: /etc/temporal/certs
      paths:
        caCert: /etc/temporal/certs/root-ca.pem
        clientCert: /etc/temporal/certs/client.pem
        clientKey: /etc/temporal/certs/client-key.pem
      pathEnv:
        clientCert: TEMPORAL_TLS_CERT
        clientKey: TEMPORAL_TLS_KEY
  ```

  `paths` is optional; when omitted, the platform uses `ca.crt`, `tls.crt`,
  and `tls.key` under `mountPath`. Custom file paths must share a non-root
  parent directory so they can be mounted from the same Secret volume.
  Use `enabled: false` under `certFiles` only when the component needs PEM env
  values but should not receive mounted cert files.
- Serverless AWS Lambda image workers use the same Temporal runtime secret, but
  the platform wraps the built image instead of editing the demo Dockerfile.
  At Lambda cold start, the wrapper fetches the referenced AWS Secrets Manager
  values, writes mTLS cert files under `/tmp/tmprl-demo-cloud/temporal`, sets
  the requested environment variables, and then execs the original image
  entrypoint. Lambda components can use `temporalMtls.certFiles.pathEnv` to
  map those `/tmp` paths into app-specific env names. `mountPath` and `paths`
  are Kubernetes-only and are rejected for serverless Lambda components.
- Serverless Lambda Worker Deployment settings live under
  `components.<name>.serverless.workerDeployment`, not `worker.versioning`.
  `deploymentName` and `buildId` can be hard-coded there; otherwise the
  platform derives them from the component name and rollout version. Use
  `env.deploymentName` and `env.buildId` arrays when the app needs those values
  in app-specific environment variable names. The platform still sets
  `TEMPORAL_DEPLOYMENT_NAME`, `TEMPORAL_WORKER_DEPLOYMENT_NAME`, and
  `TEMPORAL_WORKER_BUILD_ID` for SDK defaults.
- Components can remap platform inputs with
  `components.<name>.env[].valueFrom.inputRef`. API-key Temporal refs are
  `temporal.address`, `temporal.namespace`, `temporal.apiKey`, and
  `temporal.tls`. mTLS cert refs are `temporal.caCert`,
  `temporal.clientCert`, and `temporal.clientKey`, and require
  `spec.temporal.auth.mode: mtls`. These render as targeted `secretKeyRef` env
  vars for Kubernetes components. For serverless Lambda image workers, the
  wrapper materializes the same refs from Secrets Manager before process start,
  so the app can consume names like `TEMPORAL_HOST`, `TEMPORAL_TOKEN`, or
  `TEMPORAL_CLIENT_CERT_PEM` without changing code.
- `platform.rolloutVersion` maps the generated rollout version into a
  Kubernetes component environment variable. Use it when an ordinary pod must
  join a Worker Deployment Version managed by another component.
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
  Do not use `temporal.*` input refs in app containers with
  `temporalProxy.enabled`; use `temporalProxy.addressEnv` for alternate host env
  names.
- Components receive user-managed secrets when they list the secret name in
  `components.<name>.secrets`. The platform derives the AWS Secrets Manager path
  as `tmprl-dem-cld/<metadata.name>/<secret-name>` and injects every JSON key in
  that AWS secret as an environment variable. Keep each secret narrow and use
  env-var-shaped JSON keys.
- Components can also map one user-managed secret property to a custom env var
  with `valueFrom.inputRef: secrets.<secret-name>.<property>`. The secret must
  still be declared under `spec.secrets`, and mapped properties are treated as
  required properties in status and external-resource validation.
- `spec.secrets.<name>.requiredProperties` is optional validation metadata. Use
  it only when the project should fail early if specific JSON keys are missing
  from the AWS secret.
- `spec.secrets.<name>.shared` selects a platform-managed secret under
  `tmprl-dem-cld/platform/shared/` instead of the project-owned path. Any secret
  under that prefix can be requested by any demo. Use a relative name such as
  `openai` or a nested path such as `team/service`; absolute paths, wildcards,
  and `.` or `..` segments are rejected. Components opt in using the same
  `secrets` list or `secrets.<name>.<property>` input refs. Kubernetes and
  serverless Lambda workers both support shared secrets. Shared AWS secrets
  remain platform-owned when a demo is deleted.

Example shared credential:

```yaml
spec:
  secrets:
    openai-credentials:
      shared: openai
      requiredProperties: [OPENAI_API_KEY]
  components:
    worker:
      build: app
      worker: true
      env:
        - name: OPENAI_API_KEY
          valueFrom:
            inputRef: secrets.openai-credentials.OPENAI_API_KEY
```

This reads `tmprl-dem-cld/platform/shared/openai`. Shared `anthropic` provides
`ANTHROPIC_API_KEY`, and shared `gemini` provides both `GEMINI_API_KEY` and
`GOOGLE_API_KEY`. Platform operators provision the JSON values in Secrets
Manager outside git and OpenTofu state. Missing shared values block deployment
and report `managedBy: platform` in secret status.

Example targeted env remapping:

```yaml
spec:
  secrets:
    anthropic-credentials:
      requiredProperties:
        - ANTHROPIC_API_KEY
  components:
    api:
      build: app
      temporalAccess: true
      env:
        - name: TEMPORAL_HOST
          valueFrom:
            inputRef: temporal.address
        - name: TEMPORAL_TOKEN
          valueFrom:
            inputRef: temporal.apiKey
        - name: MODEL_API_KEY
          valueFrom:
            inputRef: secrets.anthropic-credentials.ANTHROPIC_API_KEY
```

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

## Isolated shell executor components

`components.<name>.isolatedExecutor: true` selects the credential-free
Bubblewrap profile: UID/GID 65532, read-only root filesystem, no capabilities
or service-account token, writable `/workspaces` and `/tmp`, and a root init
container that installs a pod-local firewall before starting the executor.
Only public HTTP/HTTPS and DNS are allowed outbound; private, link-local,
metadata, and IPv6 destinations are blocked. Bubblewrap needs an Unconfined
seccomp profile for its unprivileged namespaces. The application must clear
the command environment and mask `/proc` and other workspaces.

The image must include bash, iptables/ip6tables, and Bubblewrap, and serve
`/healthz` on port 8082. Use a targeted secret input ref for the executor
bearer token. The profile rejects worker/Temporal access, AWS IAM roles,
provider API keys, and bulk secret injection. The operator renders the
profile for normal runtimes and crashable clones. Shell files live for the
lifetime of the executor pod; crash controls target workers only.

`components.<name>.terminationGracePeriodSeconds` overrides Kubernetes'
shutdown grace period for long-running Activities. It must exceed the
worker's SDK graceful shutdown timeout.
