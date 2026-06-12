# Demo Registry Worker

Python Temporal worker for registry/operator workflows.

This scaffold is intentionally non-mutating. Activities return planned status
until the operator bootstrap, Kubernetes client, AWS permissions, and Temporal
Cloud provider integration are designed.

Environment variables:

- `TEMPORAL_ADDRESS`, default `localhost:7233`
- `TEMPORAL_NAMESPACE`, default `default`
- `TEMPORAL_API_KEY`, optional
- `TEMPORAL_TLS`, default `false`
- `DEMO_REGISTRY_TASK_QUEUE`, default `demo-registry-management`
- `DEMO_REGISTRY_ACTIVITY_WORKERS`, default `20`
