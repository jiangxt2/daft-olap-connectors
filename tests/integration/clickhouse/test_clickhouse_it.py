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

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import uuid
from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import clickhouse_connect
import daft
import pyarrow as pa
import pytest
import ray
from daft.io.source import DataSourceTask
from daft.recordbatch import RecordBatch

from daft_olap import read_clickhouse
from daft_olap._common.contracts import QuerySpec, ResourceLimits
from daft_olap._common.errors import SchemaError
from daft_olap._common.identifiers import QualifiedTable
from daft_olap.clickhouse.datasource import ClickHouseDataSource
from daft_olap.clickhouse.discovery import (
    ClickHouseConnection,
    discover_partitions,
)
from daft_olap.clickhouse.task import ClickHouseTask
from daft_olap.clickhouse.transport import stream_query

pytestmark = pytest.mark.integration
_PROBE_PATH_ENV = "DAFT_OLAP_IT_TASK_PROBE"


def _record_task(query: QuerySpec) -> None:
    context = ray.get_runtime_context()
    event = {
        "connector": "clickhouse",
        "sql": query.sql,
        "named_parameters": query.named_parameter_dict(),
        "pid": os.getpid(),
        "worker_id": str(context.get_worker_id()),
        "node_id": str(context.get_node_id()),
        "task_id": str(context.get_task_id()),
    }
    payload = (json.dumps(event, default=repr, sort_keys=True) + "\n").encode()
    descriptor = os.open(
        os.environ[_PROBE_PATH_ENV],
        os.O_APPEND | os.O_CREAT | os.O_WRONLY,
        0o600,
    )
    try:
        if os.write(descriptor, payload) != len(payload):
            raise OSError("incomplete ClickHouse integration probe write")
    finally:
        os.close(descriptor)


class RecordingClickHouseTask(ClickHouseTask):
    """Production ClickHouse task with test-only Ray execution evidence."""

    async def read(self) -> AsyncIterator[RecordBatch]:
        _record_task(self._query)
        async for batch in super().read():
            yield batch


def recording_clickhouse_task_factory(
    connection: ClickHouseConnection,
    query: QuerySpec,
    limits: ResourceLimits,
) -> DataSourceTask:
    return RecordingClickHouseTask(connection, query, limits)


def _read_task_events(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def _port() -> int:
    return int(os.environ.get("CLICKHOUSE_HTTP_PORT", "28123"))


def _password() -> str:
    return os.environ.get("CLICKHOUSE_PASSWORD", "daft-olap-test")


def _source(
    *, split: str = "auto", table: str = "events", **options: object
) -> ClickHouseDataSource:
    return ClickHouseDataSource(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
        table=table,
        split=split,
        batch_rows=2,
        target_tasks=3,
        max_tasks=4,
        **options,
    )


def _direct_client() -> object:
    return clickhouse_connect.get_client(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
    )


def test_projection_filter_limit_count_nulls_and_repeat_collect() -> None:
    frame = read_clickhouse(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
        table="events",
        columns=("id", "kind"),
        filter=daft.col("score") >= 25,
        split="auto",
        batch_rows=2,
        target_tasks=3,
    ).sort("id")
    expected = {
        "id": [3, 4, 5, 6, 7, 8],
        "kind": ["alpha", None, "gamma", "beta", "alpha", "delta"],
    }
    assert frame.to_pydict() == expected
    assert frame.to_pydict() == expected
    assert _source(split="single").read().sort("id").limit(3).select("id").to_pydict() == {
        "id": [1, 2, 3]
    }
    assert _source().read().count().to_pydict() == {"count": [8]}


def test_residual_filter_runs_before_global_limit() -> None:
    frame = read_clickhouse(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
        table="events",
        columns=("id",),
        filter=daft.functions.length(daft.col("kind")) > 4,
        split="auto",
        batch_rows=2,
        target_tasks=3,
    ).limit(2)
    ids = frame.to_pydict()["id"]
    assert len(ids) == 2
    assert set(ids).issubset({1, 3, 5, 7, 8})


def test_single_and_partitioned_scans_are_exactly_equivalent() -> None:
    connection = ClickHouseConnection(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
    )
    discovery = discover_partitions(
        connection,
        QualifiedTable("analytics", "events"),
        ResourceLimits(),
    )
    assert discovery.supported
    assert {partition.partition_id for partition in discovery.partitions} == {
        "202601",
        "202602",
        "202603",
    }
    single = _source(split="single").read().select("id").sort("id").to_pydict()
    partitioned = _source(split="auto").read().select("id").sort("id").to_pydict()
    assert partitioned == single == {"id": list(range(1, 9))}
    assert len(partitioned["id"]) == len(set(partitioned["id"]))


def test_trusted_filter_is_bound_and_empty_table_preserves_schema() -> None:
    filtered = ClickHouseDataSource(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
        table="events",
        split="single",
        unsafe_where_sql="score >= %(minimum)s AND kind != %(excluded)s",
        query_parameters={"minimum": 25, "excluded": "gamma"},
    )
    assert filtered.read().select("id").sort("id").to_pydict() == {"id": [3, 6, 7, 8]}
    empty = _source(table="empty_events", split="single").read().select("id", "kind")
    assert empty.to_pydict() == {"id": [], "kind": []}
    assert _source(table="empty_events", split="single").read().count().to_pydict() == {
        "count": [0]
    }
    empty_nested = _source(table="empty_nested_types", split="single").read().to_arrow()
    assert empty_nested.num_rows == 0
    assert empty_nested.schema.field("nullable_low_cardinality").type == pa.large_string()


def test_supported_clickhouse_type_matrix_uses_stable_arrow_values() -> None:
    table = _source(table="type_matrix", split="single").read().to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["int8_value"] == -8
    assert row["uint64_value"] == 6_400_000_000
    assert row["float32_value"] == pytest.approx(1.25)
    assert row["float64_value"] == pytest.approx(-2.5)
    assert row["decimal_value"] == Decimal("12345.67")
    assert row["decimal32_value"] == Decimal("12.34")
    assert row["decimal64_value"] == Decimal("-1234.5678")
    assert row["decimal128_value"] == Decimal("123456789.123456")
    assert row["date_value"] == date(2026, 4, 1)
    assert row["date32_value"] == date(1900, 1, 2)
    assert row["datetime_value"].isoformat() == "2026-04-01T01:02:03+00:00"
    assert row["datetime64_value"].isoformat() == "2026-04-01T01:02:03.123456+00:00"
    assert row["fixed_value"] == b"test"
    assert row["low_cardinality_value"] == "alpha"
    assert row["nullable_value"] is None
    assert row["uuid_value"] == "12345678-1234-5678-1234-567812345678"
    assert row["ipv4_value"] == "192.0.2.1"
    assert row["ipv6_value"] == "2001:db8::1"
    assert row["enum_value"] == "second"
    assert row["enum16_value"] == "second"
    assert row["array_value"] == [1, 2, 3]
    assert dict(row["map_value"]) == {"alpha": 1, "beta": 2}
    assert set(row["tuple_value"].values()) == {"tuple", 7}
    assert row["nested_date_values"] == [date(2026, 4, 1), date(2026, 4, 2)]
    assert row["semantic_tuple"]["observed"].isoformat() == "2026-04-01T01:02:03+00:00"
    assert row["semantic_tuple"]["request_id"] == "12345678-1234-5678-1234-567812345678"


def test_clickhouse_type_unsupported_by_daft_fails_during_schema_discovery() -> None:
    with pytest.raises(SchemaError, match="unsupported ClickHouse type"):
        _source(table="unsupported_types", split="single")


def test_count_pushdown_emits_one_real_server_query() -> None:
    marker = f"daft-olap-count-{uuid.uuid4()}"
    source = _source(split="auto", settings={"log_comment": marker, "log_queries": 1})
    assert source.read().count().to_pydict() == {"count": [8]}
    client = _direct_client()
    try:
        client.command("SYSTEM FLUSH LOGS")
        result = client.query(
            "SELECT query FROM system.query_log "
            "WHERE type = 'QueryFinish' AND log_comment = %(marker)s "
            "AND query LIKE 'SELECT count()%%'",
            parameters={"marker": marker},
        )
        assert len(result.result_rows) == 1
        assert "FROM `analytics`.`events`" in result.result_rows[0][0]
    finally:
        client.close()


@pytest.mark.asyncio
async def test_real_stream_is_bounded_exhaustive_and_can_close_early() -> None:
    connection = ClickHouseConnection(
        host="127.0.0.1",
        port=_port(),
        username="connector",
        password=_password(),
        database="analytics",
    )
    query = QuerySpec(
        sql="SELECT id, value FROM analytics.large_events ORDER BY id",
        arrow_schema=pa.schema([("id", pa.uint64()), ("value", pa.string())]),
    )
    limits = ResourceLimits(batch_rows=128)
    stream = stream_query(connection, query, limits)
    first = await anext(stream)
    assert 0 < first.num_rows <= 128
    await stream.aclose()

    row_count = 0
    batch_count = 0
    async for batch in stream_query(connection, query, limits):
        assert 0 < batch.num_rows <= 128
        row_count += batch.num_rows
        batch_count += 1
    assert row_count == 100_000
    assert batch_count > 1

    wide_query = QuerySpec(
        sql="SELECT id, payload FROM analytics.wide_events ORDER BY id",
        arrow_schema=pa.schema([("id", pa.uint64()), ("payload", pa.string())]),
    )
    wide_batches = [
        batch
        async for batch in stream_query(
            connection,
            wide_query,
            ResourceLimits(batch_rows=8, batch_bytes=9_000),
        )
    ]
    assert sum(batch.num_rows for batch in wide_batches) == 16
    assert all(batch.nbytes <= 9_000 or batch.num_rows == 1 for batch in wide_batches)


def test_bad_credentials_fail_closed_without_secret_disclosure() -> None:
    secret = "never-echo-this-password"
    with pytest.raises(SchemaError) as error:
        ClickHouseDataSource(
            host="127.0.0.1",
            port=_port(),
            username="connector",
            password=secret,
            database="analytics",
            table="events",
        )
    assert secret not in str(error.value)


@pytest.mark.ray
def test_real_clickhouse_partition_tasks_coexist_with_ray_data_on_multinode_cluster(
    tmp_path: Path,
) -> None:
    probe_path = tmp_path / "clickhouse-ray-tasks.jsonl"
    script = textwrap.dedent(
        """
        import os
        import daft
        import ray
        import ray.data
        from ray.cluster_utils import Cluster
        from daft_olap.clickhouse import ClickHouseDataSource
        from test_clickhouse_it import recording_clickhouse_task_factory

        cluster = Cluster()
        cluster.add_node(num_cpus=0, include_dashboard=False)
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        try:
            ray_rows = ray.data.from_items([{"engine": "ray-data"}]).take_all()
            assert ray_rows == [{"engine": "ray-data"}], ray_rows
            daft.set_runner_ray(noop_if_initialized=True)
            source = ClickHouseDataSource(
                host="127.0.0.1",
                port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "28123")),
                username="connector",
                password=os.environ.get("CLICKHOUSE_PASSWORD", "daft-olap-test"),
                database="analytics",
                table="events",
                split="auto",
                batch_rows=2,
                target_tasks=3,
                max_tasks=3,
                _task_factory=recording_clickhouse_task_factory,
            )
            result = (
                source.read()
                .filter(daft.col("score") >= 55)
                .select("id")
                .sort("id")
                .to_pydict()
            )
            assert result == {"id": [6, 7, 8]}, result
        finally:
            ray.shutdown()
            cluster.shutdown()
        """
    )
    environment = os.environ.copy()
    # Local workers reuse this test environment instead of rebuilding it through uv.
    environment["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "0"
    environment["DAFT_OLAP_IT_TASK_PROBE"] = str(probe_path)
    root = os.getcwd()
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            os.path.join(root, "src"),
            os.path.join(root, "tests", "integration", "clickhouse"),
            root,
            environment.get("PYTHONPATH"),
        )
        if value
    )
    process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=os.getcwd(),
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert process.returncode == 0, process.stdout + process.stderr
    events = _read_task_events(probe_path)
    assert len(events) == 3, events
    assert all(event["connector"] == "clickhouse" for event in events)
    assert all("_partition_id IN" in event["sql"] for event in events)
    assert all(event["worker_id"] and event["node_id"] and event["task_id"] for event in events)
    assert len({event["node_id"] for event in events}) >= 2, events
    partition_ids = {
        value
        for event in events
        for key, value in event["named_parameters"].items()
        if key.startswith("__daft_olap_partition_")
    }
    assert partition_ids == {"202601", "202602", "202603"}
