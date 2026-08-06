# Error contract

## Public hierarchy

All connector-defined failures inherit from `daft_olap.DaftOlapError` and are importable from the
top-level package.

| Error | Contract |
| --- | --- |
| `ConfigurationError` | A public option, identifier, secret reference, or resource limit is invalid. |
| `DependencyError` | The explicitly selected optional transport is not installed. |
| `CompatibilityError` | The installed Daft API cannot safely represent the requested operation. |
| `SchemaError` | Metadata, a driver result, or a database type cannot produce the planned Arrow schema without loss. |
| `DiscoveryError` | Split metadata or a planning endpoint failed or returned invalid data. |
| `AuthenticationError` | A metadata endpoint explicitly rejected credentials. It is also a `DiscoveryError`. |
| `DatabasePermissionError` | A metadata endpoint explicitly reported insufficient privilege. It is also a `DiscoveryError`. |
| `TransportError` | The selected worker-side query transport, decoded batch, or resource close failed. |
| `UnsupportedPredicateError` | A directly invoked predicate adapter received an unsupported expression. Normal DataFrame scans retain unsupported filters in Daft instead. |

Driver error text, DSNs, parameter values, and credentials are not preserved in public messages.
Safe structural context such as a database/table name or selected transport may be included. The
original driver exception is deliberately not exposed as a chained cause when it could contain a
URL, SQL text, or secret.

## Cancellation and timeout

`asyncio.CancelledError`, `GeneratorExit`, `KeyboardInterrupt`, and `SystemExit` are never converted
to connector exceptions. A driver timeout is not cancellation: it is reported as the sanitized
phase-specific `SchemaError`, `DiscoveryError`, or `TransportError`, because the supported drivers
do not expose one portable timeout exception hierarchy.

Doris synchronous driver calls run on a task-owned thread. Cancellation stops awaiting the active
fetch but cannot safely interrupt PyMySQL or ADBC from another thread. The already submitted close
operation remains queued on the owner thread and runs after the driver call returns. Cancellation
latency is therefore bounded by the configured driver query/socket timeout, not by immediate
cross-thread connection closure.

## Daft and Ray wrapping

Direct source construction and direct task iteration expose the public connector exception types.
Daft native execution and Ray workers may wrap a connector exception in their own execution error;
callers must inspect that platform error without relying on driver-specific text.

The connector does not automatically retry a task. `discovery_policy="single"` applies only while
planning split metadata and may select one ordinary task before execution begins. Once a task is
emitted, a failure before or after its first batch terminates that task. Doris never replays failed
`TABLET(...)` SQL as a single-task read and never switches between MySQL and Flight.
