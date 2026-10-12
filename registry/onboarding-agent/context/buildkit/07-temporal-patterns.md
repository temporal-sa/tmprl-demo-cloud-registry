# 07 · Temporal patterns for demos

Discuss the language and Temporal behavior the demo should teach. These examples use the Go SDK and offer patterns to select when they serve that story. Check APIs against the installed SDK version before using a pattern.

## Write the Temporal code as if it is the demo

This is not optional. Whatever the demo shows on screen, assume someone will open the repo and read the Workflow, Activity, and Worker code to learn how to build with Temporal. That code is part of what the demo teaches.

- **Follow current best practices** for the chosen SDK: deterministic Workflow code, idempotent Activities, timeouts set on purpose, and versioning when executions span a change. Check them against the docs, not memory ([principle 3](01-principles.md#3-verify-against-current-docs-and-source-not-memory)).
- **Use what Temporal does natively instead of hand-rolling it.** Retries come from a Retry Policy, not a loop. Waits use durable timers, not polling with wall-clock checks. Live interaction uses Signals, Updates, and Queries, not a side database the Workflow polls. Recurring work uses Schedules, history growth is handled by Continue-As-New, and duplicate starts are prevented by Workflow ID and conflict policies. If the demo needs something Temporal already provides, use that feature. A custom version teaches the wrong lesson.
- **Use easy-to-follow patterns.** The Workflow should read top to bottom as the business process, with Activities named after domain steps (`ReserveInventory`, `ChargeCard`). Prefer a [documented design pattern](#design-patterns) over a clever one-off. Avoid layers of single-caller helpers; indirection in Workflow code makes it harder to read against Event History.
- **Keep demo machinery out of the orchestration.** Failure injection, simulated latency, and traffic shaping belong inside Activities, worker config, or a clearly named generator. That way the Workflow looks like production code that happens to be running in a demo.
- **Comment where the context matters, not everywhere.** Explain *why*: why a timeout or retry value was chosen, why an error is non-retryable, why a pattern was chosen over the obvious alternative, what a versioning gate protects, and where a demo shortcut differs from a real system. Don't narrate what the code already says.

```go
// Card declines are business outcomes, not outages. Retrying won't change the
// answer, so the error is non-retryable and the Workflow compensates instead.
return temporal.NewNonRetryableApplicationError("card declined", "CardDeclined", nil)
```

When code is shown inside the demo itself, the same code serves both readers. An optional live code view can connect each observed step to the relevant excerpt; see [guide 11](11-design-options-and-planning.md#connected-experience-execution-and-code).

## Design patterns

Temporal maintains a catalog of [design patterns](https://docs.temporal.io/design-patterns) with problem statements, trade-offs, multi-SDK code, and pitfalls. Each page is available as raw Markdown by appending `.md` to its URL. The [Markdown index](https://docs.temporal.io/design-patterns/llms.txt) lists all of them. Fetch the one or two pages that fit the story when committing to an approach. Don't load the whole catalog; it's large and multi-language. The category pages (`*-patterns.md`) contain decision trees for when the choice isn't obvious.

Starting points for common demo stories:

| Story | Patterns to look at |
|---|---|
| A person (or audience member) makes a decision | [Approval](https://docs.temporal.io/design-patterns/approval.md), [Request-Response via Updates](https://docs.temporal.io/design-patterns/request-response-via-updates.md), [Updatable Timer](https://docs.temporal.io/design-patterns/updatable-timer.md) |
| Failure and recovery | [Saga](https://docs.temporal.io/design-patterns/saga-pattern.md), [Non-Retryable Errors](https://docs.temporal.io/design-patterns/non-retryable-errors.md), [Resumable Activity](https://docs.temporal.io/design-patterns/resumable-activity.md), [Error Handling & Retry Patterns](https://docs.temporal.io/design-patterns/error-handling-patterns.md) |
| Long-lived per-user or per-session state | [Entity Workflow](https://docs.temporal.io/design-patterns/entity-workflow.md), [Signal with Start](https://docs.temporal.io/design-patterns/signal-with-start.md), [Continue-As-New](https://docs.temporal.io/design-patterns/continue-as-new.md) |
| Fast response to a click or submission | [Early Return](https://docs.temporal.io/design-patterns/early-return.md), [Eager Workflow Start](https://docs.temporal.io/design-patterns/eager-workflow-start.md) |
| Scale, throughput, and fairness | [Fan-Out with Child Workflows](https://docs.temporal.io/design-patterns/fanout-child-workflows.md), [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns.md), [Fairness](https://docs.temporal.io/design-patterns/fairness.md), [Priority](https://docs.temporal.io/design-patterns/priority-task-queues.md) |
| Waiting on external systems | [Polling](https://docs.temporal.io/design-patterns/polling.md), [Delayed Callback](https://docs.temporal.io/design-patterns/delayed-callback.md), [Long-Running Activity](https://docs.temporal.io/design-patterns/long-running-activity.md) |

## Client configuration: environment only

```go
opts, err := envconfig.LoadClientOptions(envconfig.LoadClientOptionsRequest{DisableFile: true})
// defaults for local runs only
if opts.HostPort == "" { opts.HostPort = "localhost:7233" }
if opts.Namespace == "" { opts.Namespace = "default" }
```

- Reads `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, `TEMPORAL_TLS*`, the same variables the Worker Controller injects into pods. The same binary works locally, in a cluster, and against Temporal Cloud.
- `DisableFile: true` matters: a developer's default CLI profile may point at a Cloud namespace.
- A dashboard can use `client.NewLazyClient` to come up and show “reconnecting” before Temporal is reachable.

## Option: generate traffic with an entity workflow

Some stories need background traffic; others can be driven entirely by audience or presenter actions. Discuss that choice first. When a durable generator is useful, the reference runs it as a Temporal workflow on its own worker and task queue:

- A long-running **entity workflow** starts a batch of workflows each tick via a **local activity**, sized by its current traffic level.
- **Unattended demos:** it loops through a traffic curve on its own (idle → ramp up → peak → ramp down → cooldown, ~5 minutes) and triggers the special event every N cycles.
- **Presenter-driven demos:** it holds a quiet baseline (or zero) and changes only on signals: start a wave, surge, inject failures, roll out, reset. (Ask which the owner wants; principle 4.)
- **Signals** for staff controls (pause, resume, surge, trigger the special event), a validated **Update** for live config changes, and a **Query** that returns the current phase, rate, and progress (the dashboard renders it).
- **Continue-As-New** every cycle (and on `GetContinueAsNewSuggested()`), draining buffered signals first and waiting for `AllHandlersFinished`.
- On worker startup, start it with `WorkflowIDConflictPolicy: UseExisting`, and re-check every ~15s, so the entity recreates itself if the in-memory dev server restarts (important for unattended demos).
- Give generated workflow IDs a per-entity prefix (from `FirstRunID`) so a restarted entity never collides with old IDs.
- Keep the generator out of the demo's UI; it's plumbing, not the story.

## Choose worker behavior that supports the story

- **Small, fixed slot counts** (e.g. `MaxConcurrentActivityExecutionSize: 4`) can make a modest load saturate a pod when scaling is the story. Fixed-size slots also make `temporal_worker_task_slots_available` useful for slot-utilization metrics.
- **Poller autoscaling** (`worker.NewPollerBehaviorAutoscaling`) is an option to evaluate against current SDK and deployment recommendations.
- **Worker Versioning** can support a safe-change story. The reference uses `DeploymentOptions{UseVersioning: true, Version: {DeploymentName, BuildID}}` from controller-injected env vars, with workflows registered **Pinned** so in-flight work drains on its original version during a rollout. Discuss whether versioning is part of this demo and how old executions will remain supported.
- Simulated activity work and selected transient or business failures can make retries or compensation visible. Include a saga or chaos mode when it serves the scenarios chosen with the owner.
- SDK test-suite coverage can verify the behaviors actually selected, such as signals, Updates, Continue-As-New, or a compensation path.

## Logs that tell the story

Anyone opening a pod's logs (e.g. from k9s) should see the work happening:

- logfmt (`slog.NewTextHandler`), not JSON blobs.
- One readable line per business step: `payment captured order=… amount=$42.10`, `order shipped … version=aurora`.
- Wrap the SDK logger so injected failures read as what they are: a retryable activity failure becomes `WARN "activity attempt failed; Temporal will retry it"`, and a non-retryable business failure becomes `WARN "activity failed permanently; the workflow compensates"`. The SDK's own `ERROR "Activity error."` looks like breakage on a booth screen.

## Keep a long-running dev server healthy

- Run it in memory and have something re-create demo state on restart.
- For long unattended runs, set namespace retention to the minimum (1h on the dev server) so completed workflows don't grow memory all day; the generator's worker can enforce it periodically, since a restart resets it.
- Bind it to `0.0.0.0` only if pods need to reach it (`host.minikube.internal`); keep the UI on `127.0.0.1`.

## Metrics labels for per-version signals

Tag SDK metrics with `temporal_worker_deployment_name` (with `/` replaced by `_`), `temporal_worker_build_id`, and `temporal_namespace`. These match the Worker Controller's per-version tokens and HPA `matchLabels`.
