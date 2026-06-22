# Retry And Error Handling Patterns

Use these patterns when retry behavior is part of the business design, not just
a default SDK setting.

## Fixed Count Of Retries

Use when the business process allows only a specific number of attempts.

Shape:

1. Activity retry policy sets maximum attempts.
2. Non-retryable errors are classified explicitly.
3. Workflow catches exhausted retry failure if it needs a fallback path.

## Fixed Wall-Time Retries

Use when attempts may continue only within a total time budget or SLA.

Shape:

1. Activity has a per-attempt timeout.
2. Schedule-to-close or workflow timer enforces the total retry budget.
3. Workflow handles timeout as a business outcome.

## Non-Retryable Errors

Use when some failures are permanent and should not consume retry attempts.

Shape:

1. Activity throws typed non-retryable application errors for permanent cases.
2. Retry policy lists non-retryable error types where useful.
3. Workflow distinguishes permanent failure from retry exhaustion.

## Delayed Retry

Use when the next retry delay depends on downstream response data, rate-limit
headers, maintenance windows, or business timing.

Shape:

1. Activity failure carries next-delay information when the SDK supports it, or
   workflow schedules the next attempt with a timer.
2. Retry delay is bounded.
3. Non-retryable conditions still fail fast.

## Fast/Slow Retries

Use when a few quick retries should happen first, then retries should slow down.

Shape:

1. Initial phase uses short intervals for transient glitches.
2. Later phase uses longer intervals to reduce pressure.
3. Workflow or retry configuration makes the phase transition explicit.

## Retry Metrics

Use when long retry sequences need alerting before final failure.

Shape:

1. Activity reads the current attempt number.
2. Activity emits metrics when attempts cross thresholds.
3. Alerting observes retry health without changing retry behavior.

## Resumable Activity

Use when an activity can make progress, fail, then resume from a known checkpoint
or external operation handle.

Shape:

1. Activity heartbeats progress or a durable handle.
2. Retry reads heartbeat details.
3. Activity resumes, reconciles, or continues polling instead of starting over.
4. Workflow may accept correction or approval Signals when human repair is
   needed.
