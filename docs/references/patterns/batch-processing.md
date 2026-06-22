# Batch Processing Patterns

Use these patterns when one logical job processes many records or partitions.
Choose based on data size, desired parallelism, and need for workflow-level rate
control.

## Fan-Out With Child Workflows

Use when a large but bounded record set can be split into chunks and processed in
parallel.

Shape:

1. Parent partitions records into chunks.
2. Parent starts one child workflow per chunk, up to a bounded count.
3. Each child processes its chunk with activities.
4. Parent aggregates child results.

Watch parent event history and child count. Very large child counts need a
different pattern.

## Batch Iterator

Use when the record set may be unbounded and sequential page-by-page throughput
is acceptable.

Shape:

1. Workflow fetches one page.
2. Workflow processes records in that page.
3. Workflow Continue-As-New carries the next offset or cursor.
4. Each run has bounded history and a durable page checkpoint.

## Sliding Window

Use when the record set may be unbounded but needs bounded concurrency.

Shape:

1. Parent keeps exactly `window_size` children or activities in flight.
2. Completion frees a slot and starts the next record.
3. Parent Continue-As-New carries window state before history grows too large.
4. Child or activity completion is correlated with stable ids.

Use this instead of all-at-once fan-out when downstream protection matters.

## MapReduce Tree

Use when a very large set can be recursively partitioned and aggregated.

Shape:

1. Parent partitions input into subranges.
2. Child workflows process or further partition subranges.
3. Results are reduced up the tree.
4. Fan-out and payload sizes are bounded at every level.

Use for maximum parallelism when the workload is naturally decomposable and
intermediate results are compact.
