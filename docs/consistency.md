# Consistency

## Parallel reads

A parallel connector scan is a set of independent SQL queries. It is not a distributed transaction
and does not create a shared snapshot. Inserts, deletes, ClickHouse mutations and part merges, or
Doris compaction and tablet changes during a scan can change what different tasks observe.

The exact-once test guarantee applies to stable fixture tables: the union of partition or tablet
tasks must equal a single-task scan, and every stable primary row identifier must occur once.

## ClickHouse

Split metadata uses logical `partition_id`, not individual part names. A part merge inside the same
logical partition therefore does not invalidate the task predicate. The set of partition IDs is a
planning-time snapshot: writes to an already discovered partition follow each query's visibility,
but a partition first created after discovery is absent from every planned task and is not
guaranteed to appear. Stable-table equivalence tests do not claim to cover concurrent writes. Use
`split="single"`, immutable partitions, or a database-side snapshot strategy for a stricter boundary.

Automatic splitting also requires the configured endpoint to keep discovery and all physical-table
queries on the same ClickHouse server. A load balancer that can route tasks to different replicas or
shards violates that precondition; use a server-pinned endpoint or `split="single"`.

Replicated/Shared engines, views, Distributed tables, unverified engines, missing metadata, and
insufficient metadata access fall back to one task unless strict discovery is requested.

## Apache Doris

FE planning and task execution are separate operations. The connector uses the same projection and
supported predicate for planning and execution, but metadata can change between them. It does not
execute FE's opaque direct-BE plan. Stable-table IT compares tablet tasks with a single query.

A failure in an explicitly selected Doris transport terminates that task. The connector never
replays it through another protocol, before or after the first batch, so transport availability
cannot silently create duplicate rows. This also applies when `TABLET(...)` is rejected or a tablet
becomes stale before the first batch: `discovery_policy` does not enable execution-time fallback.

## Limit and ordering

A single task can apply an exact limit. Multiple tasks may each over-fetch up to the global limit;
Daft applies the final global limit. Without an ORDER BY contract, neither the database nor Daft
promises the same row subset across runs.
