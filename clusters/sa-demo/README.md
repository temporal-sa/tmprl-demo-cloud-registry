# sa-demo

Cluster-specific generated output and Flux bootstrap configuration for the
`sa-demo` EKS cluster live here.

Generated files should be deterministic and reviewable.

- `bootstrap/` installs the `DemoProject` CRD, registry/operator Deployment,
  RBAC, platform ExternalSecrets, and the generated `DemoProject` CRs.
- `bootstrap/registry-operator/kustomization.yaml` owns non-secret operator
  runtime settings through a `configMapGenerator`. The committed defaults keep
  external resources and builds in `plan` mode; switch to `aws` and
  `aws-codebuild` there only after the generated CodeBuild/IAM setup is ready.
- `generated/demoprojects.yaml` is the applied project desired state after
  bootstrap.
- `generated/<project>/` contains per-project Kubernetes review artifacts and
  unapplied Deployment templates. The registry operator reconciles the active
  resources from `DemoProject` specs.
- `generated/build-plan.json`, `generated/codebuild-setup-plan.json`, and
  `generated/external-resources-plan.json` describe the build runner, image
  repositories, and external-resource bootstrap contracts.

Regenerate project output from the repository root:

```sh
python3 scripts/generate_projects.py
```

Check that committed generated output is current:

```sh
python3 scripts/generate_projects.py --check
```
