# Daft OLAP Connectors

Daft OLAP Connectors is an independent, read-only integration for loading ClickHouse and Apache
Doris physical tables into [Daft](https://www.getdaft.io/) DataFrames. It uses Daft's public
`DataSource` and `DataSourceTask` interfaces and keeps database sockets, cursors, and Arrow readers
inside worker tasks.

This is a third-party project and a candidate for Daft's Community Extensions listing. It is not
an official Daft connector and is not affiliated with or endorsed by Eventual, Inc. It is designed
to remain independently installable and versioned. Upstream work is expected to focus first on
generic Daft API or runtime improvements; moving a database connector into Daft core would require
an explicit maintainer invitation and a separate design review.

## Installation

Install only the transports used by your workers:

```bash
pip install "daft-olap-connectors[clickhouse]"
pip install "daft-olap-connectors[doris]"
pip install "daft-olap-connectors[doris-flight]"
```

Ray workers need the same package version and extra as the driver.

Install the connector's `ray` extra when Daft executes on Ray:

```bash
pip install "daft-olap-connectors[ray,clickhouse]"
```

That extra delegates to Daft's official `daft[ray]` dependency instead of selecting an independent
Ray version. A host application such as Tributo can therefore own the remaining Ray extras without
installing a second runtime. The validated downstream profile is Daft 0.7.23, Ray 2.55.1, PyArrow
19.0.1, and Python 3.12 or 3.13. The public Daft compatibility line is intentionally narrow:
`daft>=0.7.23,<0.7.24`; the package supports Python 3.12 and 3.13. New Daft and Python releases are
tested before either range is widened.

## ClickHouse

```python
import daft
from daft_olap import SecretRef, read_clickhouse

events = read_clickhouse(
    host="clickhouse.example.net",
    database="analytics",
    table="events",
    password=SecretRef.env("CLICKHOUSE_PASSWORD"),
    filter=daft.col("event_date") >= "2026-01-01",
    columns=["event_id", "event_date", "amount"],
    split="auto",
)
```

ClickHouse has one fixed transport: `clickhouse-connect` async Arrow streaming. Automatic splitting
uses active `_partition_id` values only for an explicitly supported, non-replicated MergeTree-family
physical table observed on one server. Replicated/Shared engines, views, Distributed tables,
unknown engines, and unavailable metadata use one task. For `split="auto"`, the configured endpoint
must pin planning and task queries to the same physical server; use `split="single"` behind an
uncontrolled replica or shard load balancer.

A physical user column named `_partition_id` shadows ClickHouse's virtual column. The connector
checks both the discovered schema and `system.columns` and disables automatic splitting for such a
table, so user data cannot be silently filtered by physical partition identifiers.

`max_block_size` and `max_execution_time` are connector-managed ClickHouse settings derived from
`batch_rows` and `query_timeout_seconds`. Supplying either key through `settings` raises
`ConfigurationError` instead of being silently overwritten.

## Apache Doris

```python
import daft
from daft_olap import SecretRef, read_doris

events = read_doris(
    host="doris-fe.example.net",
    database="analytics",
    table="events",
    transport="mysql",
    password=SecretRef.env("DORIS_PASSWORD"),
    filter=daft.col("score") >= 80,
    columns=["event_id", "score"],
    split="auto",
)
```

Doris requires `transport="mysql"` or `transport="flight"` on every call. It never selects a
protocol automatically and never retries through another protocol. Flight SQL is experimental.
Automatic splitting asks FE's `_query_plan` endpoint for tablet IDs, then executes ordinary
`TABLET(...)` SQL through the selected transport. The opaque direct-BE plan is intentionally not
executed. For encrypted endpoints, set `http_secure=True` with the FE HTTPS port and
`flight_secure=True` with a TLS-enabled Flight endpoint; certificate options remain explicit in
`mysql_options` and `flight_options`.

## Safe filters and trusted SQL

Use Daft expressions for business filters. Supported comparisons, boolean operations, NULL tests,
and non-empty `IN` sets are parameterized and pushed to the database; Daft retains the filter and
reapplies it. If any subtree is unsupported, the complete Daft predicate stays out of the database,
and a nonzero limit also stays in Daft so rows are never truncated before residual filtering.

`unsafe_where_sql` is a trusted-only escape hatch. It is not parsed or sanitized. ClickHouse uses
the named placeholders supported by `clickhouse-connect`. Doris uses connector-neutral `:name`
markers and a `query_parameters` mapping. MySQL values use PyMySQL binding. Doris 4.0.6 does not
implement Flight SQL query-parameter binding, so Flight values use the connector's fail-closed typed
literal renderer: text is Base64 encoded, bytes are hexadecimal, and only finite numeric values are
accepted. Application string formatting is never used.

Write ordinary SQL percent operators and `LIKE 'prefix%'` patterns with one percent sign. When a
driver binding path is active, the connector protects literal percent signs before PyMySQL or
clickhouse-connect performs Python-style formatting. Timezone-aware `datetime` and `time` values
are rejected rather than silently losing or changing their timezone; normalize them explicitly to
the database's intended wall-clock convention before creating a filter or query parameter.

## Resource and consistency boundary

Each transport pulls on demand and closes its resources after success, error, cancellation, or
early generator close. Task count is capped at 1,024, delivered batches at 1,000,000 rows, decoded
batches use a configurable byte target, and timeouts are capped at 86,400 seconds. A driver may
allocate a larger physical batch before the connector can slice it, and one oversized row cannot be
split, so the byte target is not an absolute process-memory ceiling. Daft's current Python
DataSource consumer bridge contains upstream unbounded channels, so this package does not claim
that the full end-to-end execution path has a strict memory bound.

Schema support is evidence-based. ClickHouse applies explicit canonical casts for Date, DateTime,
UUID, IP, Enum, and supported nested children. Decimal precision above 38, Doris LARGEINT, and
unsupported complex or evolving types fail during schema discovery instead of being truncated or
silently converted. See the
[tested type matrix](https://github.com/jiangxt2/daft-olap-connectors/blob/main/docs/compatibility.md#type-policy).

Parallel tasks issue independent database queries and do not share a transaction snapshot. Use
stable tables, database-side snapshots, or `split="single"` when a single-query visibility boundary
is required. See the
[consistency contract](https://github.com/jiangxt2/daft-olap-connectors/blob/main/docs/consistency.md)
and [compatibility matrix](https://github.com/jiangxt2/daft-olap-connectors/blob/main/docs/compatibility.md).
Public failure categories, cancellation, timeout, and runtime wrapping are defined by the
[error contract](https://github.com/jiangxt2/daft-olap-connectors/blob/main/docs/errors.md).

## Development

The repository uses Hatchling and uv. Unit, native, Ray, and real ClickHouse/Doris tests are
required; database behavior is never accepted from mocks alone. The Ray database tests also record
each executed task and prove that ClickHouse partition and Doris tablet fan-out occurred, rather
than accepting a single-task fallback that happens to return the same rows. See
[CONTRIBUTING.md](https://github.com/jiangxt2/daft-olap-connectors/blob/main/CONTRIBUTING.md) and
[the architecture](https://github.com/jiangxt2/daft-olap-connectors/blob/main/docs/architecture.md).

Licensed under the Apache License, Version 2.0.
