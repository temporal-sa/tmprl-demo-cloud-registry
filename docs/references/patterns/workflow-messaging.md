# Workflow Messaging Patterns

Use these patterns when external callers need to send data into running
workflows, lazily create workflows, or receive synchronous responses from
workflow state changes.

## Signal With Start

Use when the first message should create the workflow if it does not already
exist, and later messages should go to the same running workflow.

Shape:

1. Client chooses a stable workflow id.
2. Client calls Signal-with-Start with the signal name and start input.
3. Temporal atomically starts the workflow if needed and delivers the signal.
4. Workflow handler records the event in deterministic workflow state.

Use for entity workflows consuming event streams or commands where clients
should not race on "does the workflow already exist?" checks.

## Request-Response Via Updates

Use when the caller needs validation, a state mutation, and a synchronous result.

Shape:

1. Client sends an Update.
2. Optional validator rejects invalid requests before they are written to
   history.
3. Update handler mutates workflow state deterministically.
4. Handler returns the result to the caller.

Use Updates instead of signal-plus-query when the caller needs a response tied
to the accepted state transition.

## Event Accumulator

Use when many incoming events should be accumulated, deduplicated, batched, or
reduced before downstream work runs.

Shape:

1. Workflow accepts events through Signals or Updates.
2. Workflow stores compact pending state, not unbounded payloads.
3. Workflow drains the accumulator by count, time, explicit flush, or business
   condition.
4. Workflow uses Continue-As-New when total message volume threatens history.

Keep signal/update handlers small. Let the main workflow loop perform activities
in response to accumulated state.
