# QoS And Throughput Patterns

Use these patterns when the workflow design must control throughput, fairness,
priority, tenant isolation, or downstream pressure.

## Downstream Rate Limiting

Use when activities call a downstream system with a known throughput limit.

Shape:

1. Route limited activities to a dedicated task queue.
2. Configure worker or task-queue rate limits for that queue.
3. Keep unrelated work on other queues so it is not throttled accidentally.
4. Consider per-tenant fairness when one tenant can consume all capacity.

## Priority Task Queues

Use when some workflow or activity tasks should be dispatched before lower
priority work on the same queue.

Shape:

1. Set priority when starting workflows or scheduling activities/children.
2. Keep priority categories small and meaningful.
3. Combine with fairness when high-priority work also has tenant skew.

## Fairness

Use when multiple tenants, customers, vendors, or workload classes share a task
queue and one key can starve others.

Shape:

1. Assign a fairness key for the unit that needs isolation.
2. Optionally assign weights for relative share.
3. Configure fairness and rate limits at the queue level.
4. Keep keys stable and bounded in cardinality.

Use exact SDK and CLI syntax from the Temporal version in the project.
