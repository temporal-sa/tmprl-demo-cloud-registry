# External Interaction Patterns

Use these patterns when a workflow waits for or coordinates with systems,
people, callbacks, or long-running operations outside Temporal.

## Polling

Use when an external system exposes state that must be checked until completion.

Shape:

1. Frequent polling belongs inside a single heartbeating activity loop.
2. Infrequent polling can use activity retries with fixed retry interval.
3. Very long or callback-capable processes may be better as Signals or async
   activity completion.

Avoid workflow loops that create one history event per short polling interval.

## Long-Running Activity

Use when one external operation can take a meaningful amount of time and should
be retried, cancelled, or resumed.

Shape:

1. Activity has `start_to_close` and a shorter heartbeat timeout.
2. Activity heartbeats progress or an operation handle.
3. Retry reads heartbeat details and resumes or checks external status.
4. Activity handles cancellation at heartbeat boundaries.

## Approval

Use when workflow progress depends on a human or external decision.

Shape:

1. Workflow records pending approval state.
2. Workflow waits on a condition with a timeout.
3. Signal or Update supplies approval decision and metadata.
4. Workflow branches on approve, reject, escalate, or timeout.

Use Updates when the caller needs synchronous validation or an immediate
accepted/rejected response.

## Delayed Start

Use when a workflow should be created now but defer work until a future time.

Shape:

1. Client starts the workflow immediately.
2. Workflow begins with a durable timer.
3. Signal or Update may cancel or alter the pending delay when required.
4. Workflow performs work after the timer fires.

Use schedules for recurring or calendar-based starts; use a workflow timer for
one-off relative delays inside an execution.

## Delayed Callback And Webhooks

Use when Temporal must wait for inbound webhooks, send outbound callbacks after
a delay, or bridge an external asynchronous process.

Shape:

1. Inbound webhook: external handler validates the webhook and Signals or
   Updates the workflow, often using Signal-with-Start.
2. Outbound delayed callback: workflow sleeps until the callback time and runs a
   callback activity with retries.
3. Async activity completion: activity returns a task token or handle; external
   process later completes or fails the activity.

Use async activity completion when the external process must heartbeat or receive
Temporal cancellation. Use Signals or Updates when the external system can be
trusted to call back and no activity-level heartbeat/cancellation is needed.
