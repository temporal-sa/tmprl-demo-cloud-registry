# Temporal Pattern Catalog

Use this catalog when a task is about designing the shape of a Temporal
application, choosing a pattern, or recognizing a recurring workflow structure.
Do not load every pattern file by default. Identify the closest family, read
only that file, then read language-specific implementation references as needed.

This catalog is for reusable Temporal design patterns. It is intentionally
language-neutral.

## Selection table

| If the design needs... | Read | Patterns covered |
|---|---|---|
| Multi-step external changes, rollback, safe transaction submission | `references/patterns/distributed-transactions.md` | Saga, Early Return, Idempotent Distributed Transactions |
| Long-lived business objects or workflow history lifecycle control | `references/patterns/entity-lifecycle.md` | Entity Workflow, Continue-As-New, Updatable Timer |
| Sending data into running workflows or returning synchronous workflow responses | `references/patterns/workflow-messaging.md` | Signal with Start, Request-Response via Updates, Event Accumulator |
| Coordinating multiple units of workflow work | `references/patterns/task-orchestration.md` | Child Workflows, Parallel Execution, Pick First |
| Waiting on people, callbacks, timers, or external asynchronous systems | `references/patterns/external-interaction.md` | Polling, Long-Running Activity, Approval, Delayed Start, Delayed Callback |
| Retry limits, wall-clock retry budgets, delayed retries, retry observability, or resumable work | `references/patterns/retry-error-handling.md` | Fixed Count, Fixed Wall-Time, Non-Retryable Errors, Delayed Retry, Fast/Slow Retries, Retry Metrics, Resumable Activity |
| Worker setup, dependency ownership, or host affinity | `references/patterns/worker-configuration.md` | Worker-Specific Task Queues, Activity Dependency Injection |
| Tenant fairness, priority, or downstream throughput limits | `references/patterns/qos-throughput.md` | Downstream Rate Limiting, Priority Task Queues, Fairness |
| Large record sets or batch processing | `references/patterns/batch-processing.md` | Fan-Out Child Workflows, Batch Iterator, Sliding Window, MapReduce Tree |

## Use rules

1. Start from the user's problem shape, not from a favorite Temporal primitive.
2. Read only the matching family file. Read a second family only when the
   design crosses boundaries, such as a batch workflow that also needs rate
   limiting.
3. After choosing a pattern, check the language-specific SDK reference for API
   syntax and determinism constraints.
4. Keep the Python rules in `references/temporal-python-rules.md` in force when
   implementing these patterns in Python.
