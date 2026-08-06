# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reproducible connector and raw-transport scan benchmark."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import quote

import daft
import pyarrow as pa

from daft_olap import read_clickhouse, read_doris
from daft_olap._common.identifiers import QualifiedTable

_MAX_BATCH_ROWS = 1_000_000
_MAX_BATCH_BYTES = 1024 * 1024 * 1024
_MAX_TARGET_TASKS = 1_024
_TRUE_ENV_VALUES = frozenset({"1", "true", "yes"})


@dataclass(frozen=True)
class Result:
    database: str
    transport: str
    runner: str
    mode: str
    iteration: int
    status: str
    elapsed_seconds: float | None
    schema_setup_seconds: float | None
    execution_seconds: float | None
    rows: int | None
    error_type: str | None
    daft_version: str
    pyarrow_version: str
    python_version: str
    batch_rows: int
    batch_bytes: int
    target_tasks: int


@dataclass(frozen=True)
class Measurement:
    rows: int
    schema_setup_seconds: float | None = None
    execution_seconds: float | None = None


def _environment_flag(name: str) -> bool:
    value = os.environ.get(name, "false").strip().lower()
    if value in _TRUE_ENV_VALUES:
        return True
    if value in {"0", "false", "no"}:
        return False
    raise ValueError(f"{name} must be one of: 0, 1, false, true, no, yes")


def _clickhouse_options() -> dict[str, Any]:
    return {
        "host": os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        "port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", "28123")),
        "username": os.environ.get("CLICKHOUSE_USER", "connector"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", "daft-olap-test"),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "analytics"),
    }


def _doris_options() -> dict[str, Any]:
    return {
        "host": os.environ.get("DORIS_HOST", "127.0.0.1"),
        "mysql_port": int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
        "http_port": int(os.environ.get("DORIS_HTTP_PORT", "28030")),
        "flight_port": int(os.environ.get("DORIS_FLIGHT_PORT", "28070")),
        "username": os.environ.get("DORIS_USER", "root"),
        "password": os.environ.get("DORIS_PASSWORD", ""),
        "database": os.environ.get("DORIS_DATABASE", "analytics"),
        "http_secure": _environment_flag("DORIS_HTTP_SECURE"),
        "flight_secure": _environment_flag("DORIS_FLIGHT_SECURE"),
    }


async def _raw_clickhouse(table: str, batch_rows: int) -> int:
    import clickhouse_connect

    options = _clickhouse_options()
    table_sql = QualifiedTable(options["database"], table).sql()
    client = await clickhouse_connect.get_async_client(**options)
    rows = 0
    try:
        context = await client.query_arrow_stream(
            f"SELECT * FROM {table_sql}",
            settings={"max_block_size": batch_rows},
            use_strings=True,
        )
        async with context as batches:
            async for batch in batches:
                rows += batch.num_rows
    finally:
        await client.close()
    return rows


def _raw_doris_mysql(table: str, batch_rows: int) -> int:
    import pymysql

    options = _doris_options()
    table_sql = QualifiedTable(options["database"], table).sql()
    connection = pymysql.connect(
        host=options["host"],
        port=options["mysql_port"],
        user=options["username"],
        password=options["password"],
        database=options["database"],
        cursorclass=pymysql.cursors.SSCursor,
    )
    rows = 0
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_sql}")
            while batch := cursor.fetchmany(batch_rows):
                rows += len(batch)
    finally:
        connection.close()
    return rows


def _raw_doris_flight(table: str, batch_rows: int) -> int:
    import adbc_driver_flightsql.dbapi as flight_sql
    from adbc_driver_manager import DatabaseOptions

    options = _doris_options()
    table_sql = QualifiedTable(options["database"], table).sql()
    scheme = "grpc+tls" if options["flight_secure"] else "grpc"
    host = f"[{options['host']}]" if ":" in options["host"] else options["host"]
    connection = flight_sql.connect(
        uri=f"{scheme}://{host}:{options['flight_port']}",
        db_kwargs={
            DatabaseOptions.USERNAME.value: options["username"],
            DatabaseOptions.PASSWORD.value: options["password"],
        },
    )
    rows = 0
    try:
        with connection.cursor() as cursor:
            cursor.arraysize = batch_rows
            cursor.execute(f"SELECT * FROM {table_sql}")
            for batch in cursor.fetch_record_batch():
                rows += batch.num_rows
    finally:
        connection.close()
    return rows


def _connector_measurement(args: argparse.Namespace, split: str) -> Measurement:
    setup_started = time.perf_counter()
    if args.database == "clickhouse":
        frame = read_clickhouse(
            **_clickhouse_options(),
            table=args.table,
            split=split,
            batch_rows=args.batch_rows,
            batch_bytes=args.batch_bytes,
            target_tasks=args.target_tasks,
        )
    else:
        frame = read_doris(
            **_doris_options(),
            table=args.table,
            transport=args.transport,
            split=split,
            batch_rows=args.batch_rows,
            batch_bytes=args.batch_bytes,
            target_tasks=args.target_tasks,
        )
    schema_setup_seconds = time.perf_counter() - setup_started
    execution_started = time.perf_counter()
    rows = frame.to_arrow().num_rows
    return Measurement(
        rows=rows,
        schema_setup_seconds=schema_setup_seconds,
        execution_seconds=time.perf_counter() - execution_started,
    )


def _read_sql_rows(args: argparse.Namespace) -> int:
    if args.database == "clickhouse":
        options = _clickhouse_options()
        username = quote(str(options["username"]), safe="")
        password = quote(str(options["password"]), safe="")
        database = quote(str(options["database"]), safe="")
        connection = (
            f"clickhouse://{username}:{password}@{options['host']}:{options['port']}/{database}"
        )
    else:
        options = _doris_options()
        username = quote(str(options["username"]), safe="")
        password = quote(str(options["password"]), safe="")
        database = quote(str(options["database"]), safe="")
        connection = (
            f"mysql://{username}:{password}@{options['host']}:{options['mysql_port']}/{database}"
        )
    table = QualifiedTable(options["database"], args.table).sql()
    return daft.read_sql(f"SELECT * FROM {table}", connection).to_arrow().num_rows


def _measure(
    args: argparse.Namespace,
    mode: str,
    iteration: int,
    operation: Callable[[], Measurement],
) -> Result:
    started = time.perf_counter()
    try:
        measurement = operation()
    except Exception as exc:
        return Result(
            database=args.database,
            transport=args.transport,
            runner=args.runner,
            mode=mode,
            iteration=iteration,
            status="unavailable" if mode == "daft-read-sql" else "failed",
            elapsed_seconds=None,
            schema_setup_seconds=None,
            execution_seconds=None,
            rows=None,
            error_type=type(exc).__name__,
            daft_version=daft.__version__,
            pyarrow_version=pa.__version__,
            python_version=platform.python_version(),
            batch_rows=args.batch_rows,
            batch_bytes=args.batch_bytes,
            target_tasks=args.target_tasks,
        )
    return Result(
        database=args.database,
        transport=args.transport,
        runner=args.runner,
        mode=mode,
        iteration=iteration,
        status="ok",
        elapsed_seconds=time.perf_counter() - started,
        schema_setup_seconds=measurement.schema_setup_seconds,
        execution_seconds=measurement.execution_seconds,
        rows=measurement.rows,
        error_type=None,
        daft_version=daft.__version__,
        pyarrow_version=pa.__version__,
        python_version=platform.python_version(),
        batch_rows=args.batch_rows,
        batch_bytes=args.batch_bytes,
        target_tasks=args.target_tasks,
    )


def _rows_only(operation: Callable[[], int]) -> Measurement:
    return Measurement(rows=operation())


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", choices=("clickhouse", "doris"))
    parser.add_argument("--transport", choices=("mysql", "flight"), default="mysql")
    parser.add_argument("--runner", choices=("native", "ray"), default="native")
    parser.add_argument("--table", default="events")
    parser.add_argument("--batch-rows", type=int, default=65_536)
    parser.add_argument("--batch-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--target-tasks", type=int, default=8)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--include-read-sql", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.batch_rows <= _MAX_BATCH_ROWS:
        parser.error("--batch-rows must be between 1 and 1,000,000")
    if not 1 <= args.batch_bytes <= _MAX_BATCH_BYTES:
        parser.error("--batch-bytes must be between 1 and 1,073,741,824")
    if not 1 <= args.target_tasks <= _MAX_TARGET_TASKS:
        parser.error("--target-tasks must be between 1 and 1,024")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    return args


def main() -> None:
    """Run each selected mode and emit one self-describing JSON object per iteration."""
    args = _parse_args()
    ray_runtime: Any = None
    if args.runner == "ray":
        import ray

        ray.init(include_dashboard=False)
        daft.set_runner_ray(noop_if_initialized=True)
        ray_runtime = ray
    try:
        operations: list[tuple[str, Callable[[], Measurement]]] = [
            ("connector-single", lambda: _connector_measurement(args, "single")),
            ("connector-auto", lambda: _connector_measurement(args, "auto")),
        ]
        if args.database == "clickhouse":
            operations.insert(
                0,
                (
                    "raw-driver",
                    lambda: _rows_only(
                        lambda: asyncio.run(_raw_clickhouse(args.table, args.batch_rows))
                    ),
                ),
            )
        elif args.transport == "mysql":
            operations.insert(
                0,
                (
                    "raw-driver",
                    lambda: _rows_only(lambda: _raw_doris_mysql(args.table, args.batch_rows)),
                ),
            )
        else:
            operations.insert(
                0,
                (
                    "raw-driver",
                    lambda: _rows_only(lambda: _raw_doris_flight(args.table, args.batch_rows)),
                ),
            )
        if args.include_read_sql:
            operations.append(("daft-read-sql", lambda: _rows_only(lambda: _read_sql_rows(args))))
        for iteration in range(1, args.iterations + 1):
            for mode, operation in operations:
                result = _measure(args, mode, iteration, operation)
                print(json.dumps(asdict(result), sort_keys=True))
    finally:
        if ray_runtime is not None:
            ray_runtime.shutdown()


if __name__ == "__main__":
    main()
