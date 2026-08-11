# Compatibility

## Supported baseline

The package requires Python 3.12 or 3.13, the explicitly tested Daft patch release, and PyArrow 16
through 24. The required matrix validates the minimum Arrow boundary, the downstream Tributo
profile, and the upper Arrow boundary. A non-blocking scheduled canary resolves the current Daft
`main` commit on every run, records that exact SHA, and warns about API drift before dependency
ranges are widened.

| Component | Declared range | Initial fixed validation |
| --- | --- | --- |
| Daft | `>=0.7.23,<0.7.24` | 0.7.23 |
| PyArrow | `>=16,<25` | 16.1.0, 19.0.1, and 24.0.0 |
| Ray through `daft[ray]` | `>=2.11,<2.56` | 2.55.1 |
| clickhouse-connect | `>=1.0.1,<1.6` | 1.5.0 |
| PyMySQL | `>=1.1,<3` | 1.2.0 |
| ADBC Flight SQL | `>=1.6,<2` | 1.12.0 |
| ClickHouse server | fixed IT image | 25.3.2.39 |
| Apache Doris | fixed IT images | 4.0.6 |

The exact lock and fixed integration image tags are the development evidence. Published compatibility
claims must be narrowed to combinations that have passed unit, native, Ray, and real database tests.

The public `DataSource.supports_count_pushdown()` capability is present from Daft 0.7.22. This
project keeps 0.7.23 as its minimum because that is the fully validated connector and Tributo
combination baseline. The compatibility adapter still validates the exact native global-count
shape before enabling the connector's one-task server count. More complex or unknown aggregation
shapes fail closed.

## Tributo downstream profile

Tributo uses Ray Data as its stable dataset runtime while also installing Daft for native scans and
transform planning. The connector therefore validates this shared environment rather than
publishing a competing Ray constraint:

| Component | Fixed downstream validation |
| --- | --- |
| Python | 3.12 and 3.13 |
| Daft | 0.7.23 with the official `ray` extra |
| Ray | 2.55.1 with `client`, `data`, `default`, `serve`, and `tune` extras combined |
| PyArrow | 19.0.1 |

Python packaging installs one Ray distribution and unions all requested extras. The connector's
`ray` extra is only an alias for `daft[ray]`; the host application remains responsible for its
additional Ray libraries and exact deployment lock. The connector does not depend on Tributo and
keeps its broader Daft-compatible Python and PyArrow ranges for independent users.

Ray versions below 2.56.0 are affected by
[GHSA-hhrp-gw25-jr43](https://github.com/ray-project/ray/security/advisories/GHSA-hhrp-gw25-jr43)
in `ray.data.read_webdataset()`'s default decoder. This connector never invokes that API, but the
shared 2.55.1 baseline remains a known dependency risk until Daft officially supports a patched Ray
release. Applications must not treat this compatibility profile as a claim that the advisory is
fixed.

## Optional dependencies

Importing `daft_olap` loads neither ClickHouse, PyMySQL, nor ADBC. The selected connector raises a
targeted installation error if its extra is absent. Doris Flight includes PyMySQL because DESCRIBE
and safe `_query_plan` parameter materialization use the MySQL protocol as the schema/planning
authority; query data still uses only Flight.

The connector's `ray` extra delegates to the official Daft Ray extra. It does not directly select a
Ray version and cannot override a host application's compatible Ray pin.

Doris 4.0.6 accepts complete Flight SQL statements but its
`acceptPutPreparedStatementQuery` implementation is explicitly unimplemented. Flight filters
therefore use the connector's strict typed literal renderer instead of ADBC parameter binding.
This behavior is covered by injection-payload unit tests and real MySQL/Flight result comparisons.

## Serializable configuration values

Public query-parameter and driver-option values must support a standard-library pickle round trip.
This is intentionally stricter than accepting values that work only because Ray installs
`cloudpickle`: the base connector does not depend on Ray. Nested dict, list, tuple, set, frozenset,
database scalar, and serializable custom values retain their types and are isolated from later
caller mutation. Driver-specific validity is still enforced by the selected database driver.

Callables, cyclic containers, open files or sockets, live clients/cursors/readers, `SSLContext`, and
other runtime objects that cannot be safely reconstructed are unsupported. They fail on the driver
before schema discovery or worker startup, and the error exposes only a safe option path and type,
never the value or the underlying pickle exception text.

## Type policy

Schema mapping is fail closed. A type is supported only after its declaration, driver value, Arrow
representation, NULL behavior, and both relevant execution runners have real-infrastructure tests.
Unsupported complex or evolving types raise a schema error rather than silently becoming strings.

The initial real-infrastructure matrix is intentionally explicit:

| Database | Tested declarations | Canonical Arrow behavior |
| --- | --- | --- |
| ClickHouse | `Bool`; signed/unsigned 8/16/32/64-bit integers; `Float32/64`; `Decimal(P,S)` with P ≤ 38; `Decimal32/64/128` | Native boolean, integer, floating-point, and Decimal128 values |
| ClickHouse | `String`, `FixedString`, `LowCardinality`, `Nullable` | String, fixed-size binary, and declared nullability |
| ClickHouse | `Date`, `Date32`, `DateTime`, `DateTime64` | `Date` is cast to `Date32`; `DateTime` is cast to `DateTime64(0)`; timezone and subsecond precision are retained |
| ClickHouse | `UUID`, `IPv4`, `IPv6`, `Enum8/16` | Explicit, round-trip-tested `String` projection |
| ClickHouse | `Array`, `Map`, and named or unnamed `Tuple` over supported children, including quoted Tuple field names | Recursive Arrow list, map, and struct values; required child casts are applied recursively |
| Doris, MySQL and Flight | `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, Decimal P ≤ 38 | Common native Arrow scalar values |
| Doris, MySQL and Flight | `CHAR`, `VARCHAR`, `STRING`, `JSON`, `DATE`, `DATETIMEV2` | Common string, Date32, and microsecond timestamp values |

ClickHouse `Int128/256`, `UInt128/256`, `Decimal256`, and decimal precision above 38 are
unsupported because the supported Daft releases cannot consume the required Arrow representation.
Doris `LARGEINT`, decimal precision above 38, `ARRAY`, `MAP`, `STRUCT`, `VARIANT`, and aggregate
state types are likewise fail-closed. Real database tests include unsupported-type tables to ensure
these errors occur during schema discovery, before any scan task is emitted.

Doris Flight remains experimental even when its shared scalar results agree with MySQL. ClickHouse
ADBC is watched as an external work-in-progress and is not a runtime transport.
