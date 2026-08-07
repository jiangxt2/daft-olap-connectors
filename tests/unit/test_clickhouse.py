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

import logging
import pickle
import threading
from typing import Any, cast

import daft
import pyarrow as pa
import pytest
from daft.expressions import Expression
from daft.io.pushdowns import Pushdowns

from daft_olap._common.errors import (
    AuthenticationError,
    CompatibilityError,
    ConfigurationError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DiscoveryError,
    SchemaError,
)
from daft_olap._common.identifiers import QualifiedTable
from daft_olap._common.predicate_ir import Column, Compare, Literal
from daft_olap.clickhouse.datasource import ClickHouseDataSource
from daft_olap.clickhouse.discovery import PartitionDiscovery, PartitionMetadata
from daft_olap.clickhouse.schema import (
    canonical_schema,
    parse_clickhouse_type,
    parse_describe_columns,
    render_schema_projection,
    split_type_arguments,
)
from daft_olap.clickhouse.sql import build_select
from daft_olap.clickhouse.task import ClickHouseTask

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("kind", pa.string()),
        pa.field("score", pa.int32()),
    ]
)


def _greater_than(column: str, value: object) -> Expression:
    return cast(Expression, cast(Any, daft.col(column)) > value)


def _unsupported_length_filter(column: str, value: int) -> Expression:
    return cast(Expression, cast(Any, daft.functions.length(daft.col(column))) > value)


def test_recursive_clickhouse_type_parser_handles_nested_and_quoted_arguments() -> None:
    assert split_type_arguments("String, Array(Nullable(Int64)), 'UTC,Etc'") == (
        "String",
        "Array(Nullable(Int64))",
        "'UTC,Etc'",
    )
    parsed = parse_clickhouse_type("LowCardinality(Nullable(String))")
    assert parsed.name == "LowCardinality"
    assert parsed.arguments == ("Nullable(String)",)


def test_clickhouse_canonical_schema_requires_declared_and_arrow_agreement() -> None:
    rows = [("id", "Int64"), ("kind", "LowCardinality(Nullable(String))")]
    arrow = pa.schema([pa.field("id", pa.int64()), pa.field("kind", pa.string())])
    assert canonical_schema(rows, arrow) == arrow
    with pytest.raises(SchemaError, match="disagree"):
        canonical_schema(rows, pa.schema([("kind", pa.string()), ("id", pa.int64())]))
    with pytest.raises(SchemaError, match="unsupported"):
        canonical_schema([("payload", "Dynamic")], pa.schema([("payload", pa.string())]))


def test_clickhouse_schema_projection_applies_only_lossless_transport_casts() -> None:
    columns = parse_describe_columns(
        [
            ("id", "UInt64"),
            ("created", "Date"),
            ("observed", "DateTime('UTC')"),
            ("address", "Nullable(IPv6)"),
            ("status", "Enum8('ready' = 1)"),
            ("history", "Array(Date)"),
            ("details", "Tuple(at DateTime, request_id UUID)"),
            ("quoted", "Tuple(`field name` DateTime, `request-id` UUID)"),
        ]
    )
    assert render_schema_projection(columns) == (
        "`id`, CAST(`created` AS Date32) AS `created`, "
        "CAST(`observed` AS DateTime64(0, 'UTC')) AS `observed`, "
        "CAST(`address` AS Nullable(String)) AS `address`, "
        "CAST(`status` AS String) AS `status`, "
        "CAST(`history` AS Array(Date32)) AS `history`, "
        "CAST(`details` AS Tuple(at DateTime64(0), request_id String)) AS `details`, "
        "CAST(`quoted` AS Tuple(`field name` DateTime64(0), `request-id` String)) AS `quoted`"
    )
    with pytest.raises(SchemaError, match="timezone"):
        parse_describe_columns([("observed", "DateTime('UTC OR 1=1')")])


def test_clickhouse_select_golden_sql_uses_binding_and_partition_virtual_column() -> None:
    sql, parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id", "kind"),
        predicate=Compare(">", Column("score"), Literal(10)),
        unsafe_where_sql="kind != %(excluded)s",
        query_parameters=(("excluded", "internal"),),
        partition_ids=("202601", "202602"),
        limit=25,
    )
    assert sql == (
        "SELECT `id`, `kind` FROM `analytics`.`events` WHERE "
        "(kind != %(excluded)s) AND (`score` > %(__daft_olap_0)s) AND "
        "(_partition_id IN (%(__daft_olap_partition_0)s, "
        "%(__daft_olap_partition_1)s)) LIMIT 25"
    )
    assert dict(parameters) == {
        "excluded": "internal",
        "__daft_olap_0": 10,
        "__daft_olap_partition_0": "202601",
        "__daft_olap_partition_1": "202602",
    }
    malicious_partition = "p') OR 1 = 1 --"
    malicious_sql, malicious_parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        predicate=None,
        unsafe_where_sql=None,
        query_parameters=(),
        partition_ids=(malicious_partition,),
        limit=None,
    )
    assert malicious_partition not in malicious_sql
    assert dict(malicious_parameters) == {
        "__daft_olap_partition_0": malicious_partition,
    }


def test_clickhouse_select_renders_validated_canonical_projection() -> None:
    sql, _ = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("created", "request_id"),
        predicate=None,
        unsafe_where_sql=None,
        query_parameters=(),
        partition_ids=None,
        limit=None,
        projection=(("created", "Date32"), ("request_id", "String")),
    )
    assert sql == (
        "SELECT CAST(`created` AS Date32) AS `created`, "
        "CAST(`request_id` AS String) AS `request_id` FROM `analytics`.`events`"
    )
    with pytest.raises(ConfigurationError, match="projection"):
        build_select(
            table=QualifiedTable("analytics", "events"),
            columns=("created",),
            predicate=None,
            unsafe_where_sql=None,
            query_parameters=(),
            partition_ids=None,
            limit=None,
            projection=(("other", "Date32"),),
        )
    with pytest.raises(SchemaError):
        build_select(
            table=QualifiedTable("analytics", "events"),
            columns=("created",),
            predicate=None,
            unsafe_where_sql=None,
            query_parameters=(),
            partition_ids=None,
            limit=None,
            projection=(("created", "String); DROP TABLE events"),),
        )


def test_clickhouse_query_parameters_require_unsafe_fragment() -> None:
    with pytest.raises(ConfigurationError, match="require"):
        build_select(
            table=QualifiedTable("db", "table"),
            columns=("id",),
            predicate=None,
            unsafe_where_sql=None,
            query_parameters=(("value", 1),),
            partition_ids=None,
            limit=None,
        )


@pytest.mark.asyncio
async def test_clickhouse_datasource_plans_bounded_partition_tasks_and_hidden_filter_column() -> (
    None
):
    planning_thread: int | None = None

    def discover() -> PartitionDiscovery:
        nonlocal planning_thread
        planning_thread = threading.get_ident()
        return PartitionDiscovery(
            True,
            (
                PartitionMetadata("p1", 100),
                PartitionMetadata("p2", 80),
                PartitionMetadata("p3", 10),
            ),
        )

    event_loop_thread = threading.get_ident()
    source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        password="secret-value",
        target_tasks=2,
        max_tasks=2,
        _arrow_schema=SCHEMA,
        _partition_discoverer=discover,
    )
    pushdowns = Pushdowns(
        filters=_greater_than("score", 5),
        columns=["kind"],
        limit=7,
    )
    tasks = [task async for task in source.get_tasks(pushdowns)]
    assert len(tasks) == 2
    assert planning_thread is not None and planning_thread != event_loop_thread
    assert all(isinstance(task, ClickHouseTask) for task in tasks)
    clickhouse_tasks = [cast(ClickHouseTask, task) for task in tasks]
    assert all(task.schema.column_names() == ["kind", "score"] for task in clickhouse_tasks)
    assert all("LIMIT 7" in task._query.sql for task in clickhouse_tasks)
    assert "secret-value" not in repr(source)
    assert "secret-value" not in repr(tasks[0])
    pickle.loads(pickle.dumps(tasks[0]))


@pytest.mark.asyncio
async def test_clickhouse_discovery_policy_warns_only_for_single_task_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "must-not-be-logged"

    def fail_discovery() -> PartitionDiscovery:
        raise DiscoveryError(f"driver detail: {secret}")

    source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        password=secret,
        _arrow_schema=SCHEMA,
        _partition_discoverer=fail_discovery,
    )
    with caplog.at_level(logging.WARNING, logger="daft_olap.clickhouse.datasource"):
        tasks = [task async for task in source.get_tasks(Pushdowns())]

    assert len(tasks) == 1
    assert "_partition_id" not in cast(ClickHouseTask, tasks[0])._query.sql
    assert caplog.messages == [
        "ClickHouse partition discovery failed for 'analytics'.'events' "
        "(DiscoveryError); falling back to one task"
    ]
    assert secret not in caplog.text

    caplog.clear()
    strict_source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        password=secret,
        discovery_policy="error",
        _arrow_schema=SCHEMA,
        _partition_discoverer=fail_discovery,
    )
    with (
        caplog.at_level(logging.WARNING, logger="daft_olap.clickhouse.datasource"),
        pytest.raises(DiscoveryError),
    ):
        _ = [task async for task in strict_source.get_tasks(Pushdowns())]
    assert caplog.messages == []


@pytest.mark.asyncio
async def test_clickhouse_physical_partition_column_disables_virtual_partition_splitting(
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = SCHEMA.append(pa.field("_partition_id", pa.string()))
    source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        _arrow_schema=schema,
        _partition_discoverer=lambda: pytest.fail("physical column must bypass discovery"),
    )
    with caplog.at_level(logging.WARNING, logger="daft_olap.clickhouse.datasource"):
        tasks = [cast(ClickHouseTask, task) async for task in source.get_tasks(Pushdowns())]
    assert len(tasks) == 1
    assert "(_partition_id IN" not in tasks[0]._query.sql
    assert "physical _partition_id column" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        AuthenticationError("auth"),
        DatabasePermissionError("permission"),
        DatabaseObjectNotFoundError("missing"),
    ],
)
async def test_clickhouse_nonrecoverable_discovery_errors_never_fall_back(
    failure: BaseException,
) -> None:
    def fail_discovery() -> PartitionDiscovery:
        raise failure

    source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        _arrow_schema=SCHEMA,
        _partition_discoverer=fail_discovery,
    )
    with pytest.raises(type(failure)):
        _ = [task async for task in source.get_tasks(Pushdowns())]


@pytest.mark.asyncio
async def test_clickhouse_limit_stays_in_daft_when_filter_is_residual() -> None:
    source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        split="single",
        _arrow_schema=SCHEMA,
    )
    tasks = [
        cast(ClickHouseTask, task)
        async for task in source.get_tasks(
            Pushdowns(
                filters=_unsupported_length_filter("kind", 2),
                columns=["id"],
                limit=3,
            )
        )
    ]
    assert len(tasks) == 1
    assert "LIMIT" not in tasks[0]._query.sql
    assert tasks[0].schema.column_names() == ["id", "kind"]

    zero_limit_tasks = [
        cast(ClickHouseTask, task)
        async for task in source.get_tasks(
            Pushdowns(filters=_unsupported_length_filter("kind", 2), columns=["id"], limit=0)
        )
    ]
    assert "LIMIT 0" in zero_limit_tasks[0]._query.sql


@pytest.mark.asyncio
async def test_clickhouse_count_honors_negotiated_daft_capability() -> None:
    source = ClickHouseDataSource(
        host="localhost",
        database="analytics",
        table="events",
        _arrow_schema=SCHEMA,
        _partition_discoverer=lambda: pytest.fail("count must not discover partitions"),
    )
    count_pushdowns = Pushdowns(aggregation=daft.col("id").count("all"))
    if not source.supports_count_pushdown():
        with pytest.raises(CompatibilityError):
            _ = [task async for task in source.get_tasks(count_pushdowns)]
        return

    tasks = [task async for task in source.get_tasks(count_pushdowns)]
    assert len(tasks) == 1
    assert tasks[0].schema.column_names() == ["id"]
    count_task = cast(ClickHouseTask, tasks[0])
    assert "count() AS `id`" in count_task._query.sql
    with pytest.raises(CompatibilityError):
        _ = [task async for task in source.get_tasks(Pushdowns(aggregation=daft.col("id").sum()))]
