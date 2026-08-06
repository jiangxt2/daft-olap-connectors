# Security

## Credentials

`SecretRef.env()` resolves a password separately in the driver and worker process. Literal passwords
are accepted only for trusted worker environments because serialized tasks deliver their
configuration to workers. Passwords are excluded from connection, source, and task representations.

Do not place credentials in a hostname, database name, table name, client option value intended for
logging, benchmark label, or raw SQL. Exceptions deliberately omit DSNs, SQL parameter values, and
driver exception text that may contain credentials.

Public exception categories and Daft/Ray wrapping behavior are specified in the
[error contract](errors.md). Cancellation is preserved as `asyncio.CancelledError`; sanitized
timeout failures never include driver URLs or parameter values.

## SQL construction

Database, table, and column identifiers are separate values and are backtick quoted with embedded
backticks doubled. Structured filter literals use driver binding for ClickHouse and Doris MySQL.
Doris Flight uses a fail-closed typed renderer because the tested Doris server does not implement
Flight SQL query-parameter binding. Text is UTF-8/Base64 encoded, bytes become hex, only finite
numbers are accepted, and unsupported objects are rejected. Tablet IDs are accepted only as
positive integers before being placed in Doris grammar.

`unsafe_where_sql` is explicitly trusted input. It is neither a sandbox nor an injection defense.
Application data must use `query_parameters`: clickhouse-connect named parameters for ClickHouse,
and connector-neutral `:name` markers for Doris. Doris replaces markers only outside SQL quotes,
requires an exact parameter set, and delegates binding to PyMySQL or typed materialization to the
Flight renderer. Application-provided string formatting is never accepted as a substitute.

## Network security

ClickHouse supports its driver's secure/TLS configuration. Doris FE planning uses HTTPS when
`http_secure=True`, connects directly without environment HTTP proxies, and rejects redirects so
credentials and planning SQL cannot change origin. HTTPS uses the operating-system trust store.
Doris Flight uses a `grpc+tls://` URI when `flight_secure=True`, and MySQL/Flight certificate options
pass through their explicit option mappings while lifecycle-critical connection arguments remain
reserved. The default plaintext protocols are intended only for trusted networks; configure
certificate verification for production and ensure Ray workers can reach the advertised FE and,
for Flight result endpoints, BE proxy addresses.

## Reporting

Use the private process in the
[security policy](https://github.com/jiangxt2/daft-olap-connectors/security/policy). Never include
live secrets or production rows in a report.
