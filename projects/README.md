# Projects

Add one YAML file per managed project.

Project files are intent, not raw Kubernetes manifests. They are validated
against `schemas/project.schema.json`, then rendered by platform automation into
cluster and external-provider resources.

Rules:

- `source.repo` must be in the `temporal-sa` GitHub organization.
- `metadata.name` is the project identity. The platform derives the Kubernetes
  namespace as `tmprl-cloud-demo-<metadata.name>`.
- `registry` controls how the project appears on the demo landing page.
  App UI, Temporal Cloud namespace, and source repository links should be
  platform-derived where possible; use registry assets for runbooks, speaker
  notes, decks, recordings, and canonical docs.
- `builds` declare source build inputs. The platform derives ECR repositories
  and immutable image tags.
- `components` declare runtime pods and point at one declared build.
- A component gets stable Kubernetes service DNS when it declares `ports`.
  The default Service name is the component name, with optional `serviceName`
  override for cases like `http://backend:8000`.
- `ingress.subdomain` must be only the left-hand subdomain label/path segment;
  do not include `tmprl-demo.cloud`.
- `ingress.routes` must explicitly choose which component and named port
  receive traffic for each path.
- `ingress.temporalAuthRequired` defaults to `true` when omitted. Set it to
  `false` only for reviewed public demos.
- Worker components automatically receive Temporal connection env vars and a
  namespace-scoped API key. Set `temporalAccess: true` only on non-worker
  components, such as an API server, that need to start or query workflows.
- The platform injects standard Temporal env vars:
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, and
  `TEMPORAL_TLS`.

## Local to Cloud Shape

For locally runnable projects, keep `docker-compose.yml` focused on local
development. The project config maps the same service boundaries into cloud
deployment intent:

- each Compose `build` becomes a project `build`;
- each long-running Compose service becomes a `component`;
- Compose service names used for local DNS usually become component names or
  `serviceName` values;
- local-only environment values, such as `host.docker.internal`, are replaced
  with generated service URLs. Temporal connection values come from platform
  injection.

Run validation before opening a PR:

```sh
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_projects.py
```
