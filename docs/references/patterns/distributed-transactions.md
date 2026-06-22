# Distributed Transaction Patterns

Use these patterns when a workflow coordinates multiple external side effects
and must preserve business consistency without a distributed ACID transaction.

## Saga

Use for multi-step operations where each external side effect needs a
compensation on later failure or cancellation.

Shape:

1. Workflow executes one side-effecting activity at a time.
2. Workflow records a compensation for any side effect that might have happened.
3. On failure or cancellation, workflow runs compensations in reverse order.
4. Compensation activities are idempotent and tolerate "nothing to undo".

Register compensation early enough to cover the case where an activity performs
the external side effect but fails or times out before reporting completion to
Temporal.

## Early Return

Use when a caller needs a quick accepted or initialized result but the durable
process continues asynchronously.

Shape:

1. Client starts the workflow, commonly with Update-with-Start when available.
2. Workflow performs the small initialization step needed to return a stable
   handle, transaction id, or accepted result.
3. Client receives that early result.
4. Workflow continues the longer orchestration and eventually completes.

Use this for latency-sensitive initiation, not as a replacement for durable
completion tracking.

## Idempotent Distributed Transactions

Use when the same transaction request may be submitted more than once, or when
an external submit operation may succeed but fail to report completion.

Shape:

1. Workflow derives stable idempotency keys from business identifiers or the
   workflow id.
2. Submit activities use external idempotency APIs, check-and-skip reads, a
   write-ahead submission log, or a reconciliation activity.
3. Workflow keeps durable state for which transaction phase has completed.
4. Saga compensation handles partial completion.

Prefer external idempotency support when the downstream service provides it.
When it does not, use workflow-owned identifiers plus explicit reconciliation.
