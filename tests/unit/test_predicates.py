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

import operator
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Any, cast

import daft
import pyarrow as pa
import pytest
from daft.expressions import Expression
from daft.io.pushdowns import Pushdowns

import daft_olap._compat as compat
from daft_olap._common.errors import CompatibilityError, UnsupportedPredicateError
from daft_olap._common.predicate_ir import (
    And,
    Column,
    Compare,
    InValues,
    IsNull,
    Literal,
    Not,
    Or,
    referenced_columns,
)
from daft_olap._compat import (
    _PredicateCompiler,
    compile_filter,
    count_pushdown,
    count_pushdown_available,
    daft_record_batch,
    daft_schema,
    required_scan_columns,
    validate_daft_arrow_schema,
)
from daft_olap.clickhouse.sql import render_predicate as render_clickhouse
from daft_olap.doris.sql import render_predicate as render_doris


def _comparison(expression: Expression, operator: str, value: object) -> Expression:
    dynamic = cast(Any, expression)
    if operator == ">":
        return cast(Expression, dynamic > value)
    if operator == ">=":
        return cast(Expression, dynamic >= value)
    raise AssertionError(f"unsupported test operator: {operator}")


def test_daft_expression_compiles_as_one_ir_tree_and_renders_per_dialect() -> None:
    expression = _comparison(daft.col("id"), ">=", 10) & daft.col("kind").is_in(["a", "b"])
    predicate = compile_filter(expression)
    assert isinstance(predicate, And)
    assert isinstance(predicate.left, Compare)
    assert isinstance(predicate.right, InValues)

    clickhouse = render_clickhouse(predicate)
    assert clickhouse.sql == (
        "((`id` >= %(__daft_olap_0)s) AND (`kind` IN (%(__daft_olap_1)s, %(__daft_olap_2)s)))"
    )
    assert clickhouse.parameters == (
        ("__daft_olap_0", 10),
        ("__daft_olap_1", "a"),
        ("__daft_olap_2", "b"),
    )
    mysql = render_doris(predicate, "mysql")
    flight = render_doris(predicate, "flight")
    assert mysql.sql == "((`id` >= %s) AND (`kind` IN (%s, %s)))"
    assert flight.sql == (
        "((`id` >= 10) AND (`kind` IN (from_base64('YQ=='), from_base64('Yg=='))))"
    )
    assert mysql.parameters == (10, "a", "b")
    assert flight.parameters == ()


def test_null_predicates_compile_but_ordinary_null_comparisons_stay_in_daft() -> None:
    predicate = compile_filter(daft.col("value").not_null())
    assert isinstance(predicate, IsNull)
    assert predicate.negated
    assert render_doris(predicate, "mysql").sql == "(`value` IS NOT NULL)"
    assert compile_filter(cast(Expression, operator.eq(daft.col("value"), None))) is None
    assert compile_filter(cast(Expression, operator.ne(daft.col("value"), None))) is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_float_literals_leave_the_complete_predicate_in_daft(value: object) -> None:
    assert compile_filter(_comparison(daft.col("value"), ">", value)) is None


def test_non_finite_decimal_literals_are_rejected_at_the_ir_boundary() -> None:
    with pytest.raises(UnsupportedPredicateError, match="non-finite decimal"):
        Literal(Decimal("NaN"))


@pytest.mark.parametrize(
    "value",
    [datetime(2026, 1, 1, tzinfo=UTC), time(12, 0, tzinfo=UTC)],
)
def test_timezone_aware_temporal_literals_fail_closed(value: object) -> None:
    with pytest.raises(UnsupportedPredicateError, match="timezone-aware"):
        Literal(cast(Any, value))


def test_any_unsupported_subtree_disables_the_whole_pushdown() -> None:
    expression = _comparison(daft.col("id"), ">", 1) & _comparison(
        daft.functions.length(daft.col("name")), ">", 2
    )
    assert compile_filter(expression) is None
    assert compile_filter(daft.col("id").is_in([])) is None
    assert compile_filter(daft.col("id").is_in([1, None])) is None


def test_required_scan_columns_include_residual_filter_columns_in_schema_order() -> None:
    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("score", pa.int32())])
    pushdowns = Pushdowns(filters=_comparison(daft.col("score"), ">", 10), columns=["name"])
    assert required_scan_columns(pushdowns, schema) == ("name", "score")


def test_exact_count_shape_is_accepted_and_other_aggregations_fail_closed() -> None:
    if not count_pushdown_available():
        with pytest.raises(CompatibilityError, match="negotiated count-pushdown contract"):
            count_pushdown(Pushdowns(aggregation=daft.col("id").count("all")))
        return
    count = count_pushdown(Pushdowns(aggregation=daft.col("id").count("all")))
    assert count is not None
    assert count.output_name == "id"
    with pytest.raises(CompatibilityError):
        count_pushdown(Pushdowns(aggregation=daft.col("id").sum()))
    with pytest.raises(CompatibilityError):
        count_pushdown(
            Pushdowns(
                filters=_comparison(daft.col("id"), ">", 0),
                aggregation=daft.col("id").count("all"),
            )
        )


def test_predicate_compiler_visitor_covers_exact_supported_operator_set() -> None:
    compiler = _PredicateCompiler()
    column = daft.col("value")
    one = daft.lit(1)
    two = daft.lit(2)
    assert compiler.visit_col("value") == Column("value")
    assert compiler.visit_lit(1) == Literal(1)
    assert compiler.visit_list([one, two]) == [Literal(1), Literal(2)]
    assert compiler.visit_equal(column, one).operator == "="
    assert compiler.visit_not_equal(column, one).operator == "!="
    assert compiler.visit_less_than(column, one).operator == "<"
    assert compiler.visit_less_than_or_equal(column, one).operator == "<="
    assert compiler.visit_greater_than(column, one).operator == ">"
    assert compiler.visit_greater_than_or_equal(column, one).operator == ">="
    assert isinstance(compiler.visit_between(column, one, two), And)
    assert isinstance(compiler.visit_and(_comparison(column, ">", 0), column.not_null()), And)
    assert isinstance(compiler.visit_or(_comparison(column, ">", 0), column.not_null()), Or)
    assert isinstance(compiler.visit_not(column.not_null()), Not)
    assert compiler.visit_is_in(column, [one, two]).values == (Literal(1), Literal(2))
    assert compiler.visit_is_null(column) == IsNull(Column("value"))
    assert compiler.visit_not_null(column) == IsNull(Column("value"), negated=True)


def test_predicate_compiler_rejects_non_structural_subexpressions() -> None:
    compiler = _PredicateCompiler()
    column = daft.col("value")
    literal = daft.lit(1)
    rejected_calls = (
        lambda: compiler.visit_alias(column, "alias"),
        lambda: compiler.visit_cast(column, cast(Any, object())),
        lambda: compiler.visit_try_cast(column, cast(Any, object())),
        lambda: compiler.visit_function("fn", [column]),
        lambda: compiler.visit_coalesce([column]),
        lambda: compiler.visit_list([column]),
        lambda: compiler.visit_not(column),
        lambda: compiler.visit_equal(_comparison(column, ">", 0), literal),
        lambda: compiler.visit_is_in(literal, [literal]),
        lambda: compiler.visit_is_in(column, [column]),
        lambda: compiler.visit_is_null(literal),
        lambda: compiler.visit_not_null(literal),
    )
    for call in rejected_calls:
        with pytest.raises(UnsupportedPredicateError):
            call()


def test_compat_helpers_cover_no_pushdown_unknown_columns_and_arrow_conversion() -> None:
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    assert compile_filter(None) is None
    assert compile_filter(daft.col("id")) is None
    assert count_pushdown(Pushdowns()) is None
    assert required_scan_columns(Pushdowns(), schema) == ("id", "name")
    with pytest.raises(CompatibilityError, match="unknown"):
        required_scan_columns(Pushdowns(columns=["missing"]), schema)
    daft_result_schema = daft_schema(schema)
    assert daft_result_schema.column_names() == ["id", "name"]
    batch = pa.record_batch([[1], ["a"]], schema=schema)
    assert daft_record_batch(batch).to_pydict() == {"id": [1], "name": ["a"]}
    validate_daft_arrow_schema(schema)
    with pytest.raises(CompatibilityError, match="decimal256"):
        validate_daft_arrow_schema(pa.schema([("wide", pa.decimal256(50, 4))]))


def test_count_pushdown_rejects_missing_output_name_and_referenced_columns_recurse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_mode = compat._count_mode_all()
    assert expected_mode is not None
    monkeypatch.setattr(compat, "count_pushdown_available", lambda: True)
    native = cast(
        Any,
        type(
            "Native",
            (),
            {
                "aggregation_count_mode": lambda self: expected_mode,
                "aggregation_required_column_names": lambda self: [],
            },
        )(),
    )
    aggregation = cast(Any, type("Aggregation", (), {"name": lambda self: ""})())
    fake_pushdowns = cast(
        Any,
        type(
            "FakePushdowns",
            (),
            {
                "aggregation": aggregation,
                "filters": None,
                "_to_pypushdowns": lambda self: native,
            },
        )(),
    )
    with pytest.raises(CompatibilityError, match="output field"):
        count_pushdown(fake_pushdowns)
    tree = And(
        Compare("=", Column("left"), Column("right")),
        Or(Not(IsNull(Column("nullable"))), InValues(Column("id"), (Literal(1),))),
    )
    assert referenced_columns(tree) == {"left", "right", "nullable", "id"}
