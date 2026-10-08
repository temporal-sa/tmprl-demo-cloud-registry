# Secrets And AWS Secrets Manager

Use this reference when an app needs API keys, OAuth secrets, source auth,
custom model credentials, or wants to fetch Secrets Manager values itself.

## Rules

- Never put secret values in `DemoProject` YAML.
- Project-owned secrets live under:

```text
tmprl-dem-cld/<project>/<secret-name>
```

- Secret names in YAML are DNS labels, e.g. `llm`, `analytics`,
  `source-auth`.
- Secrets should be JSON objects. Keys should usually be env-var-shaped, e.g.
  `ANTHROPIC_API_KEY`.
- Keep each secret narrow and attach it only to components that need it.
- Prefer platform-managed shared provider credentials for hosted demos:
  `spec.secrets.<name>.shared: anthropic`, `openai`, or `gemini` resolves to
  `tmprl-dem-cld/platform/shared/<value>`. Any relative path under `shared/`
  can be requested, including nested names. Absolute paths, wildcards, and dot
  segments are rejected. Shared Gemini has both `GEMINI_API_KEY` and
  `GOOGLE_API_KEY`. Keep `requiredProperties` and component references as usual;
  omitting `shared` continues to select the project-owned path.
- The operator creates the platform-managed Temporal runtime secret at
  `tmprl-dem-cld/<project>/temporal`; do not create it manually for normal
  projects.
- That runtime secret is managed only when the project has at least one
  Temporal-consuming component. A `spec.temporal` block alone is not enough.

## Declare User Secrets

Declare each project-owned or shared secret under `spec.secrets`.

```yaml
spec:
  secrets:
    llm:
      shared: anthropic
      requiredProperties:
        - ANTHROPIC_API_KEY
    analytics: {}
```

`requiredProperties` is fail-fast validation metadata. Use `{}` when the secret
should be injected without asserting exact keys.

Validation requires each declared secret to be referenced by at least one
component, either through `components.<name>.secrets` or a targeted
`secrets.<name>.<PROPERTY>` input ref.

## Bulk Inject Secret JSON As Env Vars

For Kubernetes components, list the secret under `components.<name>.secrets`.
Every JSON key becomes an env var.

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

If the AWS secret is:

```json
{
  "ANTHROPIC_API_KEY": "...",
  "MODEL_NAME": "claude-sonnet-4"
}
```

the component receives `ANTHROPIC_API_KEY` and `MODEL_NAME`.

Serverless Lambda components do not support `components.<name>.secrets`.
Use targeted `valueFrom` mappings for Lambda.

## Target One Secret Property To One Env Var

Use `env[].valueFrom.inputRef` to map one property to one env var. This works
for Kubernetes components and serverless Lambda image workers.

```yaml
spec:
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

The ref format is:

```text
secrets.<secret-name>.<PROPERTY>
```

The property must match env var naming rules: uppercase letters, numbers, and
underscores, starting with a letter or underscore.

## Create Or Update AWS Secrets

Use the configured AWS account/profile and region for the platform. Examples
use `us-west-1`.

Create:

```sh
aws secretsmanager create-secret \
  --region us-west-1 \
  --name 'tmprl-dem-cld/<project>/llm' \
  --secret-string '{"ANTHROPIC_API_KEY":"..."}'
```

Update:

```sh
aws secretsmanager put-secret-value \
  --region us-west-1 \
  --secret-id 'tmprl-dem-cld/<project>/llm' \
  --secret-string '{"ANTHROPIC_API_KEY":"..."}'
```

Inspect metadata without printing the secret value:

```sh
aws secretsmanager describe-secret \
  --region us-west-1 \
  --secret-id 'tmprl-dem-cld/<project>/llm'
```

Avoid `get-secret-value` in chat transcripts unless the user explicitly asks
and it is safe to reveal the value.

## Source Auth Secrets

For private/internal source repos, the project can reference a source auth
secret:

```yaml
spec:
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

For SSH deploy keys, the JSON should contain Flux-compatible keys:

```json
{
  "identity": "-----BEGIN OPENSSH PRIVATE KEY-----\n...\n-----END OPENSSH PRIVATE KEY-----\n",
  "known_hosts": "github.com ssh-ed25519 ..."
}
```

The operator syncs it for Flux source watches and CodeBuild checkouts.

For normal `temporal-sa` HTTPS repos, the platform shared GitHub PAT is used
from:

```text
tmprl-dem-cld/platform/github-temporal-sa-readonly
```

Do not reference the platform PAT from project YAML.

## Temporal Runtime Secret

The platform-managed runtime credential is:

```text
tmprl-dem-cld/<project>/temporal
```

API-key mode properties:

```json
{
  "authMode": "apiKey",
  "address": "...",
  "namespace": "...",
  "apiKey": "...",
  "tls": "true"
}
```

mTLS mode properties:

```json
{
  "authMode": "mtls",
  "address": "...",
  "namespace": "...",
  "tls": "true",
  "caCert": "-----BEGIN CERTIFICATE-----...",
  "clientCert": "-----BEGIN CERTIFICATE-----...",
  "clientKey": "-----BEGIN PRIVATE KEY-----..."
}
```

Kubernetes components normally consume these through platform-injected env vars,
secretKeyRefs, mTLS files, or the Temporal proxy. Serverless Lambda components
can consume them through the Lambda wrapper or by reading the injected ARN.

The project namespace gets Kubernetes `ExternalSecret` mirrors only when the
rendered components need them:

- `temporal-credentials`: ordinary workers, `temporalAccess`, targeted
  `temporal.*` refs, mTLS file mounts, or Temporal proxy.
- `temporal-worker-runtime`: Kubernetes Worker Controller workers or targeted
  worker runtime refs.

There is no first-class “create only Temporal secrets for externally managed
resources” mode today. Do not tell users they can omit `builds` and
`components`; the schema rejects that shape.

## Direct Secrets Manager Reads From Lambda

Serverless Lambda image workers get these env vars:

- `TEMPORAL_RUNTIME_SECRET_ARN`
- `TEMPORAL_RUNTIME_SECRET_ID`

If the app code already knows how to call AWS Secrets Manager, it can read the
Temporal runtime secret itself using `TEMPORAL_RUNTIME_SECRET_ARN`. This is a
valid approach for code that wants to construct Temporal client config manually.
The Lambda execution role is granted project-scoped Secrets Manager read access
for the runtime secret and declared project secrets.

For user-managed secrets, prefer targeted `valueFrom` mappings so the platform
wrapper materializes env vars. If the app intentionally reads Secrets Manager
itself, keep those secrets under `tmprl-dem-cld/<project>/...` and pass only the
secret name or ARN as a literal env var:

```yaml
env:
  - name: APP_CONFIG_SECRET_ID
    value: tmprl-dem-cld/my-project/app-config
```

The dynamic Lambda execution role is scoped to the project secret prefix and
declared paths. Do not rely on cross-project secret access.

## Secret Mapping Decision Table

- App expects one env var and secret value is a JSON property:
  use `env.valueFrom.inputRef: secrets.<secret>.<PROPERTY>`.
- Kubernetes app expects all JSON keys as env vars:
  use `components.<name>.secrets`.
- App expects a Temporal API key under a custom env name:
  use `temporal.apiKey` valueFrom in API-key mode.
- App expects mTLS PEM content in env vars:
  use `temporal.clientCert` and `temporal.clientKey` valueFrom in mTLS mode.
- Kubernetes app expects mTLS PEM file paths:
  use `temporalMtls.certFiles`.
- Lambda app expects mTLS PEM file paths:
  use `temporalMtls.certFiles.pathEnv`; files are under
  `/tmp/tmprl-demo-cloud/temporal`.
- Lambda app wants to fetch the runtime credential itself:
  read `TEMPORAL_RUNTIME_SECRET_ARN`.
