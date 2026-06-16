# sa-demo

Flux bootstrap configuration for the `sa-demo` EKS cluster — the platform/infra
layer Flux applies directly. Demo `DemoProject` CRs live under
`projects/demo/`; managed `InstruqtCourse` CRs live under `projects/instruqt/`.

- `flux-system/` — the Flux `GitRepository` and `Kustomization`s. The
  `registry-projects` Kustomization points at `./projects/demo`, and
  `registry-instruqt-courses` points at `./projects/instruqt`.
- `bootstrap/crds/` — the `DemoProject` CRD.
- `bootstrap/registry-operator/` — the operator WorkerDeployment, singleton
  watch Deployment, RBAC, namespace, ExternalSecrets, and non-secret runtime
  settings. Defaults run external resources and builds in `aws` /
  `aws-codebuild` mode.
- `addons/temporal-worker-controller/` — Temporal Worker Controller CRDs and
  controller Helm releases.
- `addons/metrics-server/` — Kubernetes metrics API used by HPAs.
- `addons/cluster-autoscaler/` — EKS managed node group autoscaling for tagged
  Auto Scaling Groups.
- `addons/instruqt-litellm/` — Shared LiteLLM gateway/broker infrastructure for
  Instruqt tracks. This currently owns secret sync through External Secrets.
- `addons/external-secrets/` — External Secrets Operator install and the
  ClusterSecretStore.

There is no committed generated output and no generation step: the operator
derives namespaces/host/prefixes from each `DemoProject` at reconcile time and
publishes the catalog ConfigMap itself.

Validate project configs from the repository root:

```sh
python3 scripts/validate_projects.py
```
