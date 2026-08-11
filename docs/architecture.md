# Architecture

## Execution boundary

The driver discovers a canonical schema and safe split units. It creates immutable task
specifications containing a redacted connection configuration, transport-safe SQL, canonical Arrow
schema, and partition or tablet identifiers. Live clients are never serialized.

Before schema discovery, public query parameters and driver option mappings are copied through a
standard-library pickle round trip. This preserves supported nested container and custom value
types while severing caller-owned mutable references. Callables, cyclic containers, open runtime
resources, and values that cannot be reconstructed fail with a redacted `ConfigurationError` on
the driver. Private planner/task factories remain test seams and are not part of the public option
serialization contract. Each driver invocation receives fresh nested client/MySQL option values.

Network-backed split discovery is awaited from `get_tasks()` through a worker thread so synchronous
ClickHouse metadata, PyMySQL binding, and Doris FE HTTP calls do not block Daft's async planning
loop. Planning still completes before tasks are emitted and its latency remains part of DataSource
planning. A parameterless Doris planning query bypasses the temporary PyMySQL binding connection.

Workers create one selected transport per task:

- ClickHouse: `clickhouse-connect` async Arrow stream;
- Doris MySQL: `SSCursor.fetchmany()` on a task-dedicated single-thread executor;
- Doris Flight: ADBC Flight SQL `RecordBatchReader` on a task-dedicated single-thread executor.

Every batch is validated against the driver-planned Arrow schema before conversion with
`RecordBatch.from_arrow_record_batches()`. `_compat.py` is the only module that touches Daft details
whose stability is not yet established. Public read functions form the stable facade; provisional
DataSource classes remain available from their database subpackages so a future Daft adapter can be
added without rewriting planners, task specifications, predicate IR, or transport tests.

## Predicate contract

Daft expressions are converted into an immutable IR containing only columns, literals, comparisons,
AND, OR, NOT, NULL tests, and non-empty IN sets without NULL members. ClickHouse and Doris each have
an independent renderer. Identifiers are backtick quoted. ClickHouse and Doris MySQL values are
driver parameters. Doris Flight values are rendered by a strict typed encoder because Doris 4.0.6
does not implement Flight SQL query-parameter binding. Text uses `from_base64()`, bytes use a
hexadecimal literal, non-finite numbers and unknown types fail closed, and raw values are never
concatenated into SQL.

The compilation boundary is all-or-nothing. Unsupported casts, functions, literal types, or any
unsupported child leave the complete predicate to Daft. Because the current Python DataSource
wrapper does not declare filter absorption, Daft also reapplies successfully pushed filters. A
nonzero limit reaches database SQL only when there is no filter or the entire filter was compiled;
otherwise Daft applies the limit after its residual filter.

Timezone-aware `datetime` and `time` literals are rejected at the Predicate IR and QuerySpec
boundaries. PyMySQL would otherwise discard their timezone and the Flight renderer would encode a
different string contract. Query specifications redact SQL and every value from `repr`, exposing
only positional parameter count, named parameter names, and Arrow schema.

## Splits

Both connectors default to `split="single"`, so an omitted split produces one task without
partition or tablet discovery. Parallel planning is an explicit `split="auto"` opt-in.

ClickHouse automatic splitting is available only for an explicitly allowlisted, non-replicated MergeTree
physical engine whose complete active parts are observable from one server. ReplicatedMergeTree,
SharedMergeTree, Distributed tables, views, and unknown engines fail closed to one task. Active
`system.parts.partition_id` values are weighted by bytes and grouped under `target_tasks` and
`max_tasks`; task SQL uses bound `_partition_id` values. A real user column named `_partition_id`
shadows the virtual column, so schema and `system.columns` checks force one task before any virtual
partition predicate is generated. The configured endpoint must pin planning
and every task query to that same physical server. The connector cannot detect a load balancer that
routes physical-table queries across servers; such endpoints must use `split="single"` or a
server-pinned address. Replica-aware discovery is outside v1.

Doris automatic planning sends the projected, safely bound single-table SQL to FE `_query_plan`,
validates the response, and consumes only unique positive tablet IDs. Task SQL uses `TABLET(...)`. The Base64
opaque plan is never executed because that would require an undocumented direct-BE scanner,
authentication, topology, and lifecycle contract.

An empty pruned tablet set is a successful zero-row plan, not a discovery failure. The connector
emits one ordinary task with `LIMIT 0`; this preserves a concrete task/schema contract without
turning an empty result into an unrestricted table scan.

## Count

The source advertises count pushdown only when the active Daft release exposes the public
DataSource capability hook and the compatibility adapter can validate the corresponding native
count shape. The supported Daft 0.7.23 line can push down the exact global `CountMode.All` form. An
accepted count creates one database task and includes trusted source filtering. Filtered, limited,
grouped, valid-only, null-only, or unknown aggregation shapes fail closed or remain in Daft. Count
is unrelated to progress estimation; the connector does not issue extra count or sampling queries
for progress.

## Distributed execution evidence

Split planning and Ray placement are separate contracts. Real-infrastructure tests wrap production
tasks with a test-only recorder, then execute the unchanged ClickHouse or Doris transport under the
Ray runner on a local multi-node cluster with a zero-CPU head raylet and two one-CPU worker raylets.
The same process first executes a Ray Data dataset, proving that the host's Ray Data API and Daft's
Ray runner coexist on one runtime before database work begins.
The tests require all expected ClickHouse partition predicates and Doris `TABLET(...)` clauses to
be observed in Ray task contexts and require each database transport to execute on at least two
distinct Ray node IDs. This proves multi-raylet scheduling, worker-side driver creation, real
database reachability, and exact split coverage without pretending that a same-host test validates
cross-host networking or a user's production topology. Worker IDs may still be reused within a
node.

## Bounded connector resources

Batch rows delivered to Daft, task count, connector-created connection count, driver queue size, and
network timeouts are bounded. Decoded RecordBatches are sliced by both the row cap and a byte target.
PyMySQL uses the previous decoded batch width to reduce subsequent `fetchmany()` row counts. A
physical batch already allocated by a driver and a single row larger than the byte target remain
unavoidable peak allocations, so the byte setting is not an absolute memory ceiling. Transport
reads are demand-driven and do not fully materialize query results. The Flight queue limit applies
per endpoint; Doris controls how many
endpoints a query returns, so endpoint count remains outside the connector-side bound while the
pinned server behavior is exercised by integration tests. Doris FE query-plan responses are read
with a 16 MiB hard limit so an opaque plan or malformed metadata response cannot cause unbounded
driver-side buffering.

ADBC Flight SQL 1.12.0 exposes query and fetch RPC timeouts but no separate Python connect-timeout
option. Connection work runs on the task-dedicated thread; the connector sets `WITH_BLOCK=false`
for compatible driver versions, while current ADBC documentation marks that option as a no-op. The
configured query timeout applies to the first query RPC and every result fetch, and the real failure
suite verifies an unreachable endpoint terminates. Read-only Flight connections explicitly use
autocommit and a one-batch ADBC result queue. PyMySQL and ADBC connections are closed only on their
task-owned thread. If cancellation arrives during a blocking fetch, the close remains queued and
runs when that call returns; repeated cancellation cannot cancel the close, and latency remains
bounded by the configured driver timeout. A delayed close failure is logged only by its exception
category; driver text is never logged. Daft's upstream Python task bridge currently uses
unbounded producer/consumer channels, which an external source cannot retrofit with acknowledgements.
This project documents that limitation and does not monkey patch or fork Daft.
