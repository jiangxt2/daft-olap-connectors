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

"""ClickHouse-specific predicate rendering and SELECT construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from daft_olap._common.errors import ConfigurationError, UnsupportedPredicateError
from daft_olap._common.identifiers import QualifiedTable, quote_columns, quote_identifier
from daft_olap._common.predicate_ir import (
    And,
    Column,
    Compare,
    InValues,
    IsNull,
    Not,
    Or,
    Predicate,
    Value,
)
from daft_olap.clickhouse.schema import validate_clickhouse_type

_PARAMETER_PREFIX = "__daft_olap_"


@dataclass(frozen=True)
class RenderedClickHousePredicate:
    """Rendered SQL plus named values consumed by clickhouse-connect."""

    sql: str
    parameters: tuple[tuple[str, Any], ...]


class _Renderer:
    def __init__(self) -> None:
        self._parameters: list[tuple[str, Any]] = []

    def _value(self, value: Value) -> str:
        if isinstance(value, Column):
            return quote_identifier(value.name)
        if value.value is None:
            raise UnsupportedPredicateError("NULL comparisons must use IS NULL")
        name = f"{_PARAMETER_PREFIX}{len(self._parameters)}"
        self._parameters.append((name, value.value))
        return f"%({name})s"

    def render(self, predicate: Predicate) -> str:
        if isinstance(predicate, Compare):
            left = self._value(predicate.left)
            right = self._value(predicate.right)
            return f"({left} {predicate.operator} {right})"
        if isinstance(predicate, And):
            return f"({self.render(predicate.left)} AND {self.render(predicate.right)})"
        if isinstance(predicate, Or):
            return f"({self.render(predicate.left)} OR {self.render(predicate.right)})"
        if isinstance(predicate, Not):
            return f"(NOT {self.render(predicate.operand)})"
        if isinstance(predicate, IsNull):
            operator = "IS NOT NULL" if predicate.negated else "IS NULL"
            return f"({quote_identifier(predicate.operand.name)} {operator})"
        if isinstance(predicate, InValues):
            values = ", ".join(self._value(value) for value in predicate.values)
            return f"({quote_identifier(predicate.operand.name)} IN ({values}))"
        raise AssertionError(f"unhandled ClickHouse predicate: {type(predicate).__name__}")

    def result(self, predicate: Predicate) -> RenderedClickHousePredicate:
        return RenderedClickHousePredicate(self.render(predicate), tuple(self._parameters))


def render_predicate(predicate: Predicate) -> RenderedClickHousePredicate:
    """Render the connector IR independently for ClickHouse."""
    return _Renderer().result(predicate)


def _render_projection(
    columns: tuple[str, ...],
    count_output_name: str | None,
    projection: tuple[tuple[str, str | None], ...] | None,
) -> str:
    if count_output_name is not None:
        return f"count() AS {quote_identifier(count_output_name)}"
    if projection is None:
        return quote_columns(columns)
    if tuple(name for name, _ in projection) != columns:
        raise ConfigurationError("ClickHouse projection must match the selected columns")
    expressions: list[str] = []
    for name, cast_type in projection:
        identifier = quote_identifier(name)
        if cast_type is None:
            expressions.append(identifier)
        else:
            validate_clickhouse_type(cast_type)
            expressions.append(f"CAST({identifier} AS {cast_type}) AS {identifier}")
    return ", ".join(expressions)


def build_select(
    *,
    table: QualifiedTable,
    columns: tuple[str, ...],
    predicate: Predicate | None,
    unsafe_where_sql: str | None,
    query_parameters: tuple[tuple[str, Any], ...],
    partition_ids: tuple[str, ...] | None,
    limit: int | None,
    count_output_name: str | None = None,
    projection: tuple[tuple[str, str | None], ...] | None = None,
) -> tuple[str, tuple[tuple[str, Any], ...]]:
    """Build a parameterized ClickHouse table scan or exact count."""
    parameters = dict(query_parameters)
    if any(key.startswith(_PARAMETER_PREFIX) for key in parameters):
        raise ConfigurationError(f"query parameter names must not start with {_PARAMETER_PREFIX!r}")
    projection_sql = _render_projection(columns, count_output_name, projection)
    clauses: list[str] = []
    if unsafe_where_sql is not None:
        clauses.append(f"({unsafe_where_sql})")
    elif parameters:
        raise ConfigurationError("query_parameters require unsafe_where_sql")
    if predicate is not None:
        rendered = render_predicate(predicate)
        clauses.append(rendered.sql)
        parameters.update(rendered.parameters)
    if partition_ids is not None:
        if not partition_ids:
            raise ConfigurationError("partition_ids must be None or non-empty")
        placeholders: list[str] = []
        for partition_id in partition_ids:
            key = f"{_PARAMETER_PREFIX}partition_{len(placeholders)}"
            parameters[key] = partition_id
            placeholders.append(f"%({key})s")
        clauses.append(f"(_partition_id IN ({', '.join(placeholders)}))")
    sql = f"SELECT {projection_sql} FROM {table.sql()}"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if limit is not None and count_output_name is None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ConfigurationError("limit must be a non-negative integer")
        sql += f" LIMIT {limit}"
    return sql, tuple(parameters.items())
