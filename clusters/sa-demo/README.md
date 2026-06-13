# sa-demo

Flux bootstrap configuration for the `sa-demo` EKS cluster — the platform/infra
layer Flux applies directly. (Demo `DemoProject` CRs live in the top-level
`projects/` directory, not here.)

- `flux-system/` — the Flux `GitRepository` and `Kustomization`s. The
  `registry-projects` Kustomization points at `./projects`, so the operator
  reconciles every CR dropped there.
- `bootstrap/crds/` — the `DemoProject` CRD.
- `bootstrap/registry-operator/` — the operator Deployment, RBAC, namespace, and
  its non-secret runtime settings (a `configMapGenerator`). Defaults run external
  resources and builds in `aws` / `aws-codebuild` mode.
- `addons/external-secrets/` — External Secrets Operator install and the
  ClusterSecretStore.

There is no committed generated output and no generation step: the operator
derives namespaces/host/prefixes from each `DemoProject` at reconcile time and
publishes the catalog ConfigMap itself.

Validate project configs from the repository root:

```sh
python3 scripts/validate_projects.py
```
