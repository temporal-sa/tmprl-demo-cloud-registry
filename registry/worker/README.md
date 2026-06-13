# Demo Registry Worker

Python Temporal worker for registry/operator workflows.

The worker hosts Temporal workflows and can also run a Kubernetes watch loop for
`DemoProject` custom resources. In `plan` mode it renders the resources it would
create. In `apply` mode it creates or patches the first operator-managed
Kubernetes slice:

- project namespace;
- component service accounts and tenant-baseline RBAC;
- platform metadata ConfigMap;
- versioned candidate Deployments and Services when every build has an
  immutable image override;
- Traefik middleware and IngressRoutes after a configured smoke check passes,
  or immediately for projects without a smoke check.

External resource reconciliation runs before Kubernetes reconciliation. In the
default `plan` mode it records expected ECR repositories, Secrets Manager
paths, and Temporal Cloud resources in `DemoProject.status.externalResources`
without mutating cloud providers. In `aws` mode it creates or verifies missing
ECR repositories and verifies required Secrets Manager paths. Secret values and
Temporal Cloud namespace/API key lifecycle remain manual until those provider
contracts are implemented.

Candidate resource names use `git-<source-sha>` when the
`registry.tmprl-demo.cloud/source-commit-sha` annotation is present. Public
Traefik routes point at the versioned candidate Services only after the
configured smoke check succeeds, so a failed candidate does not repoint the
existing healthy route.

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
- `DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE`, `plan` or `aws`, default `plan`
- `DEMO_REGISTRY_AWS_REGION`, default from `AWS_REGION`,
  `AWS_DEFAULT_REGION`, then `us-west-1`
- `DEMO_REGISTRY_AWS_ACCOUNT_ID`, optional status metadata

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
