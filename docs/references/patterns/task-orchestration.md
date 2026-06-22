# Task Orchestration Patterns

Use these patterns when a workflow coordinates multiple units of work.

## Child Workflows

Use when a subprocess needs its own workflow identity, event history, lifecycle,
task queue routing, queryability, or failure boundary.

Shape:

1. Parent starts a child with an explicit workflow id strategy.
2. Parent chooses whether to wait for the child result or continue.
3. Parent close policy is deliberate: terminate, request cancel, or abandon.
4. Parent and child communicate only through explicit workflow APIs.

Do not use child workflows purely for code organization.

## Parallel Execution

Use when independent activities or child workflows can run concurrently.

Shape:

1. Workflow starts all independent operations without awaiting each one
   immediately.
2. Workflow stores futures, promises, or handles.
3. Workflow waits for all or a selected subset.
4. Workflow aggregates results and handles partial failures deliberately.

Add explicit concurrency bounds when fan-out size is not small and fixed.

## Pick First

Use when several alternatives can produce an equivalent result and the workflow
should use the first successful result.

Shape:

1. Workflow starts competing activities or child workflows.
2. Workflow waits for the first successful completion.
3. Workflow requests cancellation for losing branches.
4. Losing activities must heartbeat or otherwise support cancellation if their
   work should stop promptly.

Use for redundant providers, fastest-region lookup, or fallback races. Avoid it
when losing side effects cannot be safely cancelled or ignored.
