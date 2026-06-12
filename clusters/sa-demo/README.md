# sa-demo

Cluster-specific generated output and Flux bootstrap configuration for the
`sa-demo` EKS cluster live here.

Generated files should be deterministic and reviewable.

- `bootstrap/` installs the `DemoProject` CRD, registry/operator Deployment,
  RBAC, platform ExternalSecrets, and the generated `DemoProject` CRs.
- `generated/demoprojects.yaml` is the applied project desired state after
  bootstrap.
- `generated/<project>/` contains per-project Kubernetes review artifacts and
  unapplied Deployment templates. The registry operator reconciles the active
  resources from `DemoProject` specs.

Regenerate project output from the repository root:

```sh
python3 scripts/generate_projects.py
```

Check that committed generated output is current:

```sh
python3 scripts/generate_projects.py --check
```
