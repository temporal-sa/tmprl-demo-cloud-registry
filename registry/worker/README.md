# Demo Registry Worker

Python Temporal worker for registry/operator workflows.

The worker hosts Temporal workflows and can also run a Kubernetes watch loop for
`DemoProject` custom resources. In `plan` mode it renders the resources it would
create. In `apply` mode it creates or patches the first operator-managed
Kubernetes slice:

- project namespace;
- component service accounts and tenant-baseline RBAC;
- platform metadata ConfigMap;
- Services, ExternalSecrets, and Deployments when every build has an immutable
  image override.
- Traefik middleware and IngressRoutes after a configured smoke check passes,
  or immediately for projects without a smoke check.

External ECR, AWS Secrets Manager, and Temporal Cloud resources are still
manual setup or future provider automation. The worker records those expected
paths in `DemoProject.status.externalResources`.

Environment variables:

- `TEMPORAL_ADDRESS`, default `localhost:7233`
- `TEMPORAL_NAMESPACE`, default `default`
- `TEMPORAL_API_KEY`, optional
- `TEMPORAL_TLS`, default `false`
- `DEMO_REGISTRY_TASK_QUEUE`, default `demo-registry-management`
- `DEMO_REGISTRY_ACTIVITY_WORKERS`, default `20`
- `DEMO_REGISTRY_WATCH_ENABLED`, default `false`
- `DEMO_REGISTRY_WATCH_INTERVAL_SECONDS`, default `15`
- `DEMO_REGISTRY_RECONCILE_MODE`, `plan` or `apply`, default `plan`

For a temporary deployment test before build automation exists, annotate a
`DemoProject` with immutable images for every declared build:

```sh
kubectl annotate demoproject demo-registry \
  'registry.tmprl-demo.cloud/image-overrides={"web":"<account>.dkr.ecr.<region>.amazonaws.com/tmprl-cloud-demo/demo-registry/web:<sha>","worker":"<account>.dkr.ecr.<region>.amazonaws.com/tmprl-cloud-demo/demo-registry/worker:<sha>"}' \
  --overwrite
```

Optionally record the source revision that produced those images:

```sh
kubectl annotate demoproject demo-registry \
  registry.tmprl-demo.cloud/source-commit-sha=<git-sha> \
  --overwrite
```
