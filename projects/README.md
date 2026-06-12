# Projects

Add one YAML file per managed project.

Project files are intent, not raw Kubernetes manifests. They are validated
against `schemas/project.schema.json`, then rendered by platform automation into
cluster and external-provider resources.

Rules:

- `source.repo` must be in the `temporal-sa` GitHub organization.
- `ingress.subdomain` must be only the left-hand subdomain label/path segment;
  do not include `tmprl-demo.cloud`.
- `ingress.target` is required so multi-pod projects explicitly choose which
  service receives external traffic.
- Temporal connection values should be injected through the configured
  environment variable names unless the project has a documented reason to
  manage them directly.
