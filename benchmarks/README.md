# Benchmark harness

The benchmark harness compares a raw driver stream, a single connector task, automatic connector
splitting, and an optional `daft.read_sql()` capability probe. Run each runner in a fresh process so
Daft's global runner selection cannot contaminate another result.

Examples:

```bash
uv run --all-extras python benchmarks/scan.py clickhouse --runner native --iterations 3
uv run --all-extras python benchmarks/scan.py doris --transport mysql --runner ray --iterations 3
uv run --all-extras python benchmarks/scan.py doris --transport flight --runner native --iterations 3
```

Connection values come from the same `CLICKHOUSE_*` and `DORIS_*` environment variables used by
integration tests. The output is JSON Lines. An unavailable optional baseline is recorded with its
exception class and never converted into a zero-duration measurement.

Connector records separate `schema_setup_seconds` and `execution_seconds` fields in addition to
end-to-end `elapsed_seconds`. Schema setup covers construction and zero-row schema discovery.
Execution includes Daft task planning, split metadata discovery, scheduling, and reading because
the public DataSource contract performs those operations together. Raw-driver and optional
`read_sql()` records leave phase-specific fields null rather than claiming incomparable timing.

Use the real-infrastructure `wide_events` fixture together with `--batch-bytes` to measure
variable-width rows, for example `--table wide_events --batch-rows 65536 --batch-bytes 1048576`.
The byte setting is a decoded-batch target; one oversized row and a physical batch already allocated
inside a driver remain observable peak allocations.

Benchmark claims are valid only with the database image, driver lock, Daft version, runner, table,
row count, row and byte batch settings, task count, and raw JSONL artifact attached.
