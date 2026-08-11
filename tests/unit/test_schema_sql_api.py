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

from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, ClassVar, cast

import daft
import pyarrow as pa
import pytest
from daft.expressions import Expression

import daft_olap
import daft_olap._compat as compat
from daft_olap._common.errors import ConfigurationError, SchemaError, UnsupportedPredicateError
from daft_olap._common.identifiers import QualifiedTable
from daft_olap._common.predicate_ir import (
    And,
    Column,
    Compare,
    InValues,
    IsNull,
    Literal,
    Not,
    Or,
)
from daft_olap.clickhouse import api as clickhouse_api
from daft_olap.clickhouse.datasource import ClickHouseDataSource
from daft_olap.clickhouse.schema import (
    canonical_schema as clickhouse_schema,
)
from daft_olap.clickhouse.schema import (
    parse_clickhouse_type,
    split_type_arguments,
    validate_clickhouse_type,
)
from daft_olap.clickhouse.schema import (
    project_schema as project_clickhouse_schema,
)
from daft_olap.clickhouse.sql import build_select as build_clickhouse_select
from daft_olap.clickhouse.sql import render_predicate as render_clickhouse
from daft_olap.doris import api as doris_api
from daft_olap.doris.datasource import DorisDataSource
from daft_olap.doris.schema import (
    canonical_schema as doris_schema,
)
from daft_olap.doris.schema import (
    coerce_decimal,
    doris_type_to_arrow,
    parse_describe_rows,
)
from daft_olap.doris.schema import (
    project_schema as project_doris_schema,
)
from daft_olap.doris.sql import build_describe, render_flight_literal
from daft_olap.doris.sql import build_select as build_doris_select
from daft_olap.doris.sql import render_predicate as render_doris


@pytest.mark.parametrize(
    "declaration",
    [
        "Int64",
        "Nullable(String)",
        "LowCardinality(String)",
        "Array(UInt8)",
        "Map(String, Int32)",
        "Tuple(String, value Nullable(Int64))",
        "Tuple(`field name` String, `request-id` Int32, `tick``name` UUID)",
        "FixedString(1)",
        "FixedString(8)",
        "Decimal32(2)",
        "Decimal64(18)",
        "Decimal128(38)",
        "Decimal(20, 6)",
        "DateTime",
        "DateTime('UTC')",
        "DateTime64(6)",
        "DateTime64(9, 'UTC')",
        "Enum8('a' = 1, 'b' = 2)",
    ],
)
def test_clickhouse_declared_type_whitelist_accepts_complete_valid_forms(
    declaration: str,
) -> None:
    validate_clickhouse_type(declaration)


@pytest.mark.parametrize(
    "declaration",
    [
        "Nothing",
        "Int128",
        "Nullable()",
        "Map(String)",
        "Tuple()",
        "Tuple(named)",
        "FixedString(0)",
        "FixedString(text)",
        "Decimal32(10)",
        "Decimal64(19)",
        "Decimal128(39)",
        "Decimal(0, 0)",
        "Decimal(50, 4)",
        "Decimal(77, 1)",
        "Decimal(3, 4)",
        "Decimal256(4)",
        "DateTime('UTC', 'extra')",
        "DateTime(UTC)",
        "DateTime64()",
        "DateTime64(10)",
        "DateTime64(6, UTC)",
        "Enum8",
        "Dynamic",
    ],
)
def test_clickhouse_declared_type_whitelist_rejects_unsupported_or_malformed_forms(
    declaration: str,
) -> None:
    with pytest.raises(SchemaError):
        validate_clickhouse_type(declaration)


@pytest.mark.parametrize(
    "value",
    ["", "Bad Type", "Array(String", "1Type(String)", "Array(String))"],
)
def test_clickhouse_type_parser_rejects_incomplete_syntax(value: str) -> None:
    with pytest.raises(SchemaError):
        parse_clickhouse_type(value)


@pytest.mark.parametrize(
    "value",
    ["String,,Int64", "String,", "String), Int64", "'unterminated", "Array(String"],
)
def test_clickhouse_argument_splitter_rejects_unbalanced_or_empty_parts(value: str) -> None:
    with pytest.raises(SchemaError):
        split_type_arguments(value)


def test_clickhouse_schema_rejects_empty_duplicate_and_bad_describe_rows() -> None:
    with pytest.raises(SchemaError, match="no columns"):
        clickhouse_schema([], pa.schema([]))
    with pytest.raises(SchemaError, match="duplicate"):
        clickhouse_schema(
            [("id", "Int64"), ("id", "Int64")],
            pa.schema([("id", pa.int64()), ("other", pa.int64())]),
        )
    with pytest.raises(SchemaError, match="invalid row"):
        clickhouse_schema([("id",)], pa.schema([("id", pa.int64())]))
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())], metadata={b"x": b"y"})
    assert project_clickhouse_schema(schema, ("name",)).metadata == {b"x": b"y"}


@pytest.mark.parametrize(
    ("declaration", "expected"),
    [
        ("BOOLEAN", pa.bool_()),
        ("TINYINT", pa.int8()),
        ("SMALLINT", pa.int16()),
        ("INTEGER", pa.int32()),
        ("BIGINT", pa.int64()),
        ("FLOAT", pa.float32()),
        ("DOUBLE", pa.float64()),
        ("CHAR(255)", pa.string()),
        ("VARCHAR(32)", pa.string()),
        ("VARCHAR(65533)", pa.string()),
        ("text", pa.string()),
        ("JSON", pa.string()),
        ("DATEV2", pa.date32()),
        ("DATETIME(6)", pa.timestamp("us")),
        ("DATETIMEV2(3)", pa.timestamp("us")),
        ("DECIMALV3(18, 2)", pa.decimal128(18, 2)),
    ],
)
def test_doris_scalar_type_matrix(declaration: str, expected: pa.DataType) -> None:
    assert doris_type_to_arrow(declaration, column_name="value") == expected


@pytest.mark.parametrize(
    "declaration",
    [
        "ARRAY<INT>",
        "MAP<STRING,INT>",
        "STRUCT<a:INT>",
        "VARIANT",
        "UNKNOWN",
        "varchar bad",
        "VARCHAR",
        "VARCHAR(foo)",
        "VARCHAR(0)",
        "VARCHAR(65534)",
        "CHAR(256)",
        "INT(11)",
        "DATEV2(1)",
        "DATETIMEV2(foo)",
        "DATETIMEV2(7)",
        "DECIMAL",
        "DECIMAL(10)",
        "DECIMAL(a, 2)",
        "DECIMAL(0, 0)",
        "DECIMAL256(50, 4)",
        "LARGEINT",
        "DECIMAL(77, 1)",
        "DECIMAL(2, 3)",
    ],
)
def test_doris_type_matrix_fails_closed(declaration: str) -> None:
    with pytest.raises(SchemaError):
        doris_type_to_arrow(declaration, column_name="value")


def test_doris_describe_validation_projection_and_decimal_coercion() -> None:
    with pytest.raises(SchemaError, match="invalid row"):
        parse_describe_rows([("id", "BIGINT")])
    with pytest.raises(SchemaError, match="name or type"):
        parse_describe_rows([(1, "BIGINT", "NO")])
    with pytest.raises(SchemaError, match="name or type"):
        parse_describe_rows([("", "BIGINT", "NO")])
    with pytest.raises(SchemaError, match="name or type"):
        parse_describe_rows([("bad\x00name", "BIGINT", "NO")])
    with pytest.raises(SchemaError, match="nullability"):
        parse_describe_rows([("id", "BIGINT", "MAYBE")])
    with pytest.raises(SchemaError, match="no columns"):
        parse_describe_rows([])
    with pytest.raises(SchemaError, match="duplicate"):
        parse_describe_rows([("id", "BIGINT", "NO"), ("id", "BIGINT", "YES")])
    columns = parse_describe_rows([("id", "BIGINT", "NO"), ("value", "STRING", "YES")])
    schema = doris_schema(columns).with_metadata({b"key": b"value"})
    assert project_doris_schema(schema, ("value",)).metadata == {b"key": b"value"}
    assert coerce_decimal(None) is None
    assert coerce_decimal(Decimal("1.25")) == Decimal("1.25")
    assert coerce_decimal("2.50") == Decimal("2.50")


def test_doris_flight_literals_are_typed_and_never_embed_raw_strings() -> None:
    attack = "a' OR 1=1 --"
    rendered = render_flight_literal(attack)
    assert rendered == "from_base64('YScgT1IgMT0xIC0t')"
    assert attack not in rendered
    assert render_flight_literal(b"\x00\xff") == "X'00ff'"
    assert render_flight_literal(True) == "TRUE"
    assert render_flight_literal(Decimal("1.20")) == "1.20"
    assert render_flight_literal(date(2026, 1, 1)) == ("from_base64('MjAyNi0wMS0wMQ==')")
    for value in (float("nan"), float("inf"), Decimal("NaN"), object()):
        with pytest.raises(ConfigurationError):
            render_flight_literal(value)
    for value in (
        datetime(2026, 1, 1, tzinfo=UTC),
        time(12, 0, tzinfo=UTC),
    ):
        with pytest.raises(ConfigurationError, match="timezone-aware"):
            render_flight_literal(value)


def _predicate_tree() -> Or:
    return Or(
        And(
            Compare("=", Column("kind"), Literal("alpha")),
            Not(IsNull(Column("score"))),
        ),
        InValues(Column("id"), (Literal(1), Literal(2))),
    )


def test_predicate_renderers_cover_boolean_null_in_and_column_operands() -> None:
    clickhouse = render_clickhouse(_predicate_tree())
    assert " OR " in clickhouse.sql
    assert "NOT" in clickhouse.sql
    assert "IS NULL" in clickhouse.sql
    assert len(clickhouse.parameters) == 3
    mysql = render_doris(_predicate_tree(), "mysql")
    assert mysql.parameters == ("alpha", 1, 2)
    assert render_doris(IsNull(Column("value"), negated=True), "flight").sql == (
        "(`value` IS NOT NULL)"
    )
    columns = Compare("=", Column("left"), Column("right"))
    assert render_clickhouse(columns).parameters == ()
    assert render_doris(columns, "mysql").parameters == ()
    with pytest.raises(UnsupportedPredicateError, match="IS NULL"):
        render_clickhouse(Compare("=", Column("value"), Literal(None)))
    with pytest.raises(UnsupportedPredicateError, match="IS NULL"):
        render_doris(Compare("=", Column("value"), Literal(None)), "mysql")


def test_clickhouse_select_validates_count_partition_parameters_and_limit() -> None:
    sql, parameters = build_clickhouse_select(
        table=QualifiedTable("db", "events"),
        columns=("count",),
        predicate=None,
        unsafe_where_sql=None,
        query_parameters=(),
        partition_ids=None,
        limit=5,
        count_output_name="count",
    )
    assert sql == "SELECT count() AS `count` FROM `db`.`events`"
    assert parameters == ()
    with pytest.raises(ConfigurationError, match="must not start"):
        build_clickhouse_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            predicate=None,
            unsafe_where_sql="id = 1",
            query_parameters=(("__daft_olap_bad", 1),),
            partition_ids=None,
            limit=None,
        )
    with pytest.raises(ConfigurationError, match="partition_ids"):
        build_clickhouse_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            predicate=None,
            unsafe_where_sql=None,
            query_parameters=(),
            partition_ids=(),
            limit=None,
        )
    for limit in (True, 1.5, "1"):
        with pytest.raises(ConfigurationError, match="limit"):
            build_clickhouse_select(
                table=QualifiedTable("db", "events"),
                columns=("id",),
                predicate=None,
                unsafe_where_sql=None,
                query_parameters=(),
                partition_ids=None,
                limit=cast(Any, limit),
            )


def test_clickhouse_select_protects_literal_percent_signs_on_every_binding_path() -> None:
    sql, parameters = build_clickhouse_select(
        table=QualifiedTable("db", "events"),
        columns=("id",),
        predicate=Compare(">", Column("score"), Literal(1)),
        unsafe_where_sql="kind LIKE 'A%' AND modulo % 2 = 0 AND id >= %(minimum)s",
        query_parameters=(("minimum", 10),),
        partition_ids=("p1",),
        limit=None,
    )
    assert "LIKE 'A%%'" in sql
    assert "modulo %% 2" in sql
    assert "%(minimum)s" in sql
    assert dict(parameters)["minimum"] == 10
    assert sql % {name: repr(value) for name, value in parameters}

    unbound_sql, unbound_parameters = build_clickhouse_select(
        table=QualifiedTable("db", "events"),
        columns=("id",),
        predicate=None,
        unsafe_where_sql="kind LIKE 'A%'",
        query_parameters=(),
        partition_ids=None,
        limit=None,
    )
    assert "LIKE 'A%'" in unbound_sql
    assert unbound_parameters == ()
    with pytest.raises(ConfigurationError, match="unused"):
        build_clickhouse_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            predicate=None,
            unsafe_where_sql="id > 0",
            query_parameters=(("unused", 1),),
            partition_ids=None,
            limit=None,
        )


def test_clickhouse_select_rejects_undeclared_unsafe_placeholders() -> None:
    for partition_ids in (None, ("p1",)):
        with pytest.raises(ConfigurationError, match="missing parameter 'missing'"):
            build_clickhouse_select(
                table=QualifiedTable("db", "events"),
                columns=("id",),
                predicate=None,
                unsafe_where_sql="kind = '%(missing)s'",
                query_parameters=(),
                partition_ids=partition_ids,
                limit=None,
            )


def test_doris_select_validates_unsafe_markers_tablets_count_and_limit() -> None:
    sql, parameters = build_doris_select(
        table=QualifiedTable("db", "events"),
        columns=("count",),
        style="mysql",
        predicate=None,
        unsafe_where_sql=None,
        query_parameters=(),
        tablet_ids=None,
        limit=4,
        count_output_name="count",
    )
    assert sql == "SELECT count(*) AS `count` FROM `db`.`events`"
    assert parameters == ()
    assert build_describe(QualifiedTable("db", "events")) == "DESCRIBE `db`.`events`"
    with pytest.raises(ConfigurationError, match="missing"):
        build_doris_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            style="mysql",
            predicate=None,
            unsafe_where_sql="id = :missing",
            query_parameters=(),
            tablet_ids=None,
            limit=None,
        )
    with pytest.raises(ConfigurationError, match="duplicate"):
        build_doris_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            style="mysql",
            predicate=None,
            unsafe_where_sql="id = :id",
            query_parameters=(("id", 1), ("id", 2)),
            tablet_ids=None,
            limit=None,
        )
    for tablet_ids in ((), (0,), (cast(Any, True),)):
        with pytest.raises(ConfigurationError, match="tablet"):
            build_doris_select(
                table=QualifiedTable("db", "events"),
                columns=("id",),
                style="flight",
                predicate=None,
                unsafe_where_sql=None,
                query_parameters=(),
                tablet_ids=tablet_ids,
                limit=None,
            )
    for limit in (-1, 1.5, "1", True):
        with pytest.raises(ConfigurationError, match="limit"):
            build_doris_select(
                table=QualifiedTable("db", "events"),
                columns=("id",),
                style="mysql",
                predicate=None,
                unsafe_where_sql=None,
                query_parameters=(),
                tablet_ids=None,
                limit=cast(Any, limit),
            )


def test_doris_select_protects_percent_signs_and_rejects_unclosed_quotes() -> None:
    sql, parameters = build_doris_select(
        table=QualifiedTable("db", "events"),
        columns=("id",),
        style="mysql",
        predicate=Compare(">", Column("score"), Literal(1)),
        unsafe_where_sql="kind LIKE 'A%' AND modulo % 2 = 0 AND id >= :minimum",
        query_parameters=(("minimum", 10),),
        tablet_ids=(1,),
        limit=None,
    )
    assert "LIKE 'A%%'" in sql
    assert "modulo %% 2" in sql
    assert parameters == (10, 1)
    assert sql % tuple(repr(value) for value in parameters)

    flight_sql, flight_parameters = build_doris_select(
        table=QualifiedTable("db", "events"),
        columns=("id",),
        style="flight",
        predicate=None,
        unsafe_where_sql="kind LIKE 'A%' AND id >= :minimum",
        query_parameters=(("minimum", 10),),
        tablet_ids=None,
        limit=None,
    )
    assert "LIKE 'A%'" in flight_sql
    assert flight_parameters == ()
    with pytest.raises(ConfigurationError, match="unclosed"):
        build_doris_select(
            table=QualifiedTable("db", "events"),
            columns=("id",),
            style="mysql",
            predicate=None,
            unsafe_where_sql="kind = 'unterminated :value",
            query_parameters=(("value", "x"),),
            tablet_ids=None,
            limit=None,
        )


class FakeFrame:
    def __init__(self) -> None:
        self.filter_value: object | None = None
        self.columns: tuple[str, ...] | None = None

    def filter(self, expression: object) -> FakeFrame:
        self.filter_value = expression
        return self

    def select(self, *columns: str) -> FakeFrame:
        self.columns = columns
        return self


class FakeSource:
    last_options: ClassVar[dict[str, object]] = {}
    last_frame: ClassVar[FakeFrame | None] = None

    def __init__(self, **options: object) -> None:
        type(self).last_options = options

    def read(self) -> FakeFrame:
        frame = FakeFrame()
        type(self).last_frame = frame
        return frame


def test_top_level_api_exposes_stable_entry_points_and_error_contract() -> None:
    assert set(daft_olap.__all__) == {
        "AuthenticationError",
        "CompatibilityError",
        "ConfigurationError",
        "DaftOlapError",
        "DatabaseObjectNotFoundError",
        "DatabasePermissionError",
        "DependencyError",
        "DiscoveryError",
        "SchemaError",
        "SecretRef",
        "TransportError",
        "UnsupportedPredicateError",
        "read_clickhouse",
        "read_doris",
    }
    assert not hasattr(daft_olap, "ClickHouseDataSource")
    assert not hasattr(daft_olap, "DorisDataSource")


def test_sources_advertise_count_only_when_the_daft_adapter_is_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = pa.schema([("id", pa.int64())])
    clickhouse = ClickHouseDataSource(
        host="clickhouse",
        database="analytics",
        table="events",
        split="single",
        _arrow_schema=schema,
    )
    doris = DorisDataSource(
        host="doris",
        database="analytics",
        table="events",
        transport="mysql",
        split="single",
        _arrow_schema=schema,
    )
    expected = compat.count_pushdown_available()
    assert clickhouse.supports_count_pushdown() is expected
    assert doris.supports_count_pushdown() is expected

    monkeypatch.setattr(compat, "_count_mode_all", lambda: None)
    assert not clickhouse.supports_count_pushdown()
    assert not doris.supports_count_pushdown()


def test_public_read_apis_construct_lazy_sources_then_apply_filter_and_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expression = cast(Expression, cast(Any, daft.col("score")) >= 10)
    monkeypatch.setattr(clickhouse_api, "ClickHouseDataSource", FakeSource)
    clickhouse_frame = clickhouse_api.read_clickhouse(
        host="host",
        database="db",
        table="events",
        columns=("id",),
        filter=expression,
    )
    assert cast(Any, clickhouse_frame).columns == ("id",)
    assert cast(Any, clickhouse_frame).filter_value is expression
    assert FakeSource.last_options["split"] == "single"
    assert FakeSource.last_options["batch_bytes"] == 64 * 1024 * 1024

    monkeypatch.setattr(doris_api, "DorisDataSource", FakeSource)
    doris_frame = doris_api.read_doris(
        host="host",
        database="db",
        table="events",
        transport="flight",
        http_secure=True,
        flight_secure=True,
        planning_timeout_seconds=4.5,
        columns=("id", "kind"),
        filter=expression,
    )
    assert cast(Any, doris_frame).columns == ("id", "kind")
    assert cast(Any, doris_frame).filter_value is expression
    assert FakeSource.last_options["transport"] == "flight"
    assert FakeSource.last_options["http_secure"] is True
    assert FakeSource.last_options["flight_secure"] is True
    assert FakeSource.last_options["split"] == "single"
    assert FakeSource.last_options["batch_bytes"] == 64 * 1024 * 1024
    assert FakeSource.last_options["planning_timeout_seconds"] == 4.5


def test_public_read_apis_leave_unrequested_operations_out_of_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(clickhouse_api, "ClickHouseDataSource", FakeSource)
    frame = clickhouse_api.read_clickhouse(host="host", database="db", table="events")
    assert cast(Any, frame).columns is None
    assert cast(Any, frame).filter_value is None


def test_literal_rejects_arbitrary_values_and_supports_date_decimal() -> None:
    assert Literal(date(2026, 1, 1)).value == date(2026, 1, 1)
    assert Literal(Decimal("1.2")).value == Decimal("1.2")
    with pytest.raises(UnsupportedPredicateError, match="unsupported"):
        Literal(cast(Any, object()))
