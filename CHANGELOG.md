# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A version entry describes source contents; a Git tag, GitHub Release, or PyPI publication exists
only after the separately authorized release workflow completes.

## 0.1.0a1 - Alpha candidate

### Added

- Initial ClickHouse and Apache Doris read-only Daft DataSource connectors.
- ClickHouse Arrow streaming through `clickhouse-connect` and Doris streaming through explicit
  MySQL or experimental Flight SQL transports.
- Structured predicate, projection, and safe limit/count pushdown with fail-closed schema and type
  handling.
- Explicit `split="auto"` planning for eligible ClickHouse partitions and Doris tablets, with
  bounded task counts and single-query fallback policies.
- Environment-backed `SecretRef` credentials, redacted public failures, compatibility matrices,
  and real ClickHouse 25.3.2.39 and Doris 4.0.6 integration suites.

### Changed

- Align the validated downstream runtime with Tributo on Daft 0.7.23, Ray 2.55.1, and PyArrow
  19.0.1, while delegating Ray installation to Daft's official extra.
- Bound delivered batches by row count and a decoded-byte target, with adaptive Doris MySQL fetch
  sizing and cancellation-safe task-thread cleanup.
- Restrict ClickHouse automatic splitting to explicitly supported non-replicated MergeTree engines.
- Publish the credential-safe connector exception hierarchy and its cancellation/timeout contract.
- Omitted `split` now means `split="single"`; automatic fan-out is an explicit opt-in because its
  queries do not share a transaction snapshot.
- Caller-owned query parameters and driver options are snapshotted before discovery and validated
  against the standard-library pickle boundary used by task specifications.
- Doris planning has a distinct per-blocking-operation timeout, and Flight connection setup uses
  the ADBC connect RPC timeout option.
- PyMySQL support is limited to `>=1.2,<1.3`, with a validated SSCursor lifecycle adapter that
  closes an early-stopped query without draining unread rows.

### Known limitations

- The supported Daft Python DataSource bridge contains upstream unbounded channels, so the package
  does not claim end-to-end bounded memory.
- Driver physical batches and a single oversized row can exceed the connector's decoded batch
  target.
- `split="auto"` tasks are independent queries and do not provide a shared snapshot.
- Doris Flight SQL remains experimental; the core Alpha profile is ClickHouse and Doris MySQL
  read support.
- Write, DDL, arbitrary-query, and cross-protocol fallback APIs are not provided.
