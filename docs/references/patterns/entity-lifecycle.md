# Entity And Lifecycle Patterns

Use these patterns when the workflow represents durable business state over
time, or when a workflow execution must control history growth.

## Entity Workflow

Use when one workflow instance represents one business entity, such as an
account, cart, user session, subscription, device, or deployment.

Shape:

1. Workflow id is a stable business identifier.
2. Signals and Updates mutate entity state.
3. Queries expose current state.
4. Workflow serializes decisions for that entity.
5. Continue-As-New carries compact state forward before history grows too large.

Do not use entity workflows as a generic database substitute. Use them when
durable orchestration and serialized state transitions matter.

## Continue-As-New

Use to bound event history for long-lived workflows, loops, streams, entity
workflows, or batch processors.

Shape:

1. Workflow checks history length or the SDK's continue-as-new suggestion.
2. Workflow waits for in-flight handlers and required activities to settle.
3. Workflow passes compact state to the next run.
4. New run recreates timers, handlers, and derived in-memory state.

Remember that timers and non-abandoned children do not automatically carry over.

## Updatable Timer

Use when a workflow waits for a deadline that can be extended, shortened, or
cancelled by external input.

Shape:

1. Workflow stores the current wake-up time.
2. Signal or Update changes that wake-up time.
3. Workflow waits on a timer raced against "deadline changed" state.
4. On change, workflow recalculates the remaining duration and waits again.

Use for approvals, grace periods, SLA deadlines, and debounce windows.
