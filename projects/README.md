# Projects

Add one YAML file per managed project.

Project files are intent, not raw Kubernetes manifests. They are validated
against `schemas/project.schema.json`, then rendered by platform automation into
cluster and external-provider resources.

Rules:

- `source.repo` must be in the `temporal-sa` GitHub organization.
- `metadata.name` is the project identity. The platform derives the Kubernetes
  namespace as `tmprl-dem-cld-<metadata.name>`.
- `registry` controls how the project appears on the demo landing page.
  App UI, Temporal Cloud namespace, and source repository links should be
  platform-derived where possible; use registry assets for runbooks, speaker
  notes, decks, recordings, and canonical docs.
- `builds` declare source build inputs. The platform derives ECR repositories
  and immutable image tags.
- `components` declare runtime pods and point at one declared build.
- `components.<name>.command` accepts a list of strings. Inline YAML arrays are
  valid and usually easier to read for process commands.
- A component gets stable Kubernetes service DNS when it declares `ports`.
  The default Service name is the component name, with optional `serviceName`
  override for cases like `http://backend:8000`.
- `ingress.subdomain` must be only the left-hand subdomain label/path segment;
  do not include `tmprl-demo.cloud`.
- `ingress.routes` must explicitly choose which component and named port
  receive traffic for each path.
- `ingress.temporalAuthRequired` defaults to `true` when omitted. Set it to
  `false` only for reviewed public demos.
- `smokeCheck` defines the owner-approved HTTP path and expected status used to
  promote a candidate route after deployment. Keep it narrow and stable, for
  example `path: /health` with `expectedStatus: 200`.
- Worker components automatically receive Temporal connection env vars and a
  namespace-scoped API key. Set `temporalAccess: true` only on non-worker
  components, such as an API server, that need to start or query workflows.
- Worker versioning belongs under `components.<name>.worker.versioning`. The
  project may map app-specific env var names there, but should not hard-code
  worker build/version env values in `components.<name>.env`.
- The platform injects standard Temporal env vars:
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and
  `TEMPORAL_TLS`.
- User-managed secrets must be declared under top-level `secrets` and then
  referenced by components. The platform derives the AWS Secrets Manager path
  as `tmprl-dem-cld/<metadata.name>/<secret-name>`.
- Components receive user-managed secrets only when they list the secret name in
  `components.<name>.secrets`. Every JSON key in the AWS secret is injected as
  an environment variable, so keep each secret narrow and use env-var-shaped
  JSON keys.
- `secrets.<name>.requiredProperties` declares required JSON keys that the
  operator verifies in opt-in AWS external-resource mode before runtime
  workloads are deployed.

Example user-managed secret:

```yaml
secrets:
  anthropic-credentials:
    requiredProperties:
      - ANTHROPIC_API_KEY

components:
  worker:
    build: app
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
