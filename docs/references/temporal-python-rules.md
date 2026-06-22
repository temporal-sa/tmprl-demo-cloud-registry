# Temporal Python Rules

Use these rules when building Python applications with Temporal. They are
intended to keep Temporal code explicit, durable, and easy for a human to trace.

## 1. Use dataclasses for every Temporal boundary payload

Every workflow start input, signal input, update input, child workflow
input/result, and activity input/result must use a dataclass, even when it
currently contains only one field.

This preserves future-compatible evolution and prevents positional or
unstructured contract drift. The only acceptable non-dataclass input/output is
no input or no output.

## 2. Keep workflow imports and passthrough module-oriented

Workflow files may own their `workflow.unsafe.imports_passed_through()` block,
but passthrough must stay small and deliberate. The default is to pass through
and import a module, then call through qualified names so workflow dependencies
and Temporal boundaries stay visible.

Do not build sprawling passthrough blocks that import long lists of activity
functions, request classes, constants, helpers, and unrelated modules.

Only import individual symbols from a module when the module contains shared
pieces the workflow does not need, and even then only for a small number of
functions or boundary types. If that exception starts to grow, split the module
instead of growing the passthrough block.

## 3. Optimize activities for human traceability

An activity should usually be readable top-to-bottom without chasing helper
functions. Prefer small, local duplication over extracting helpers that fragment
the logic.

Create helpers only when the repeated code is substantial, mechanically
identical, and likely to be repeated many times.

## 4. One activity owns one external side effect

Treat external reads and writes as side effects. If code must check external
state, decide, then create/update/delete something, those are separate
activities unless they form one truly atomic externally visible operation. The
workflow owns the decision between independent side effects.

An activity may combine operations only when they are one atomic side effect,
such as generating certificate material and writing it to one secret when the
generated material has no independent external lifecycle.

Do not hide a chain like "check remote state, branch, generate material, upload
it, then update metadata" inside one activity.

## 5. Evaluate retry policy per activity

Every activity must have deliberate retry-policy design for that specific
operation. Shared policies are acceptable only after checking that the activity
has the same retryable failure modes, non-retryable failure modes, timeout
expectations, and idempotency characteristics.

Explicitly classify what should retry and what should fail fast.

## 6. Heartbeat any potentially long-running activity

Any activity that may run for a meaningful duration must heartbeat and must have
a heartbeat timeout separate from `start_to_close_timeout`.

If the underlying SDK or operation does not expose progressive progress, design
the activity to hold a handle or operation ID and heartbeat while polling or
waiting.

## 7. Disable SDK retry behavior inside activities

Temporal activity retry policy, timeouts, and heartbeats are the durable retry
surface. Disable SDK/client built-in retries inside activities when possible.

If a client's retry behavior cannot be disabled, make that explicit and keep it
tightly bounded.

## 8. Keep workflows as orchestrators

Workflows own sequencing, branching, durable state, retries, timers, child
workflows, and compensation ordering.

Activities perform bounded side effects and return results. Do not hide
orchestration inside activities.

## 9. Give self-contained orchestration its own workflow

If a process has its own lifecycle, progress states, retries/timeouts, and
result, model it as a workflow or child workflow.

In an entity workflow, delegate bounded orchestration to a child workflow or a
dedicated workflow-context class rather than letting the entity workflow
implement every process directly.

## 10. Use sagas for external resource creation

Any workflow that creates external resources must register compensations for
side effects and execute them in reverse order on failure or cancellation.

Register compensation early enough to cover the case where an activity performs
the external side effect but fails or times out before reporting completion to
Temporal. When the side effect might have happened, the compensation must be
idempotent and must tolerate "nothing to undo".

Cleanup must be part of the orchestration design, not a manual operational
fallback.

## 11. Split files by cognitive ownership early

Do not let one workflow file or one activity file accumulate every phase of the
system.

Workflows should have their own folders. Activity modules should be organized by
side-effect domain and lifecycle boundary before they become large mixed-purpose
files.
