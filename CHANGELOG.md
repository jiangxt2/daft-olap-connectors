# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Initial ClickHouse and Apache Doris read-only Daft DataSource connectors.

### Changed

- Align the validated downstream runtime with Tributo on Daft 0.7.23, Ray 2.55.1, and PyArrow
  19.0.1, while delegating Ray installation to Daft's official extra.
- Bound delivered batches by row count and a decoded-byte target, with adaptive Doris MySQL fetch
  sizing and cancellation-safe task-thread cleanup.
- Restrict ClickHouse automatic splitting to explicitly supported non-replicated MergeTree engines.
- Publish the credential-safe connector exception hierarchy and its cancellation/timeout contract.
