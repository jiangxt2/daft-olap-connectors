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

"""Fail-closed ClickHouse type parsing and canonical Arrow schema validation."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from daft_olap._common.errors import SchemaError
from daft_olap._common.identifiers import quote_identifier

_TYPE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_INTEGER = re.compile(r"^[0-9]+$")
_SIMPLE_TYPES = {
    "Bool",
    "Date",
    "Date32",
    "Float32",
    "Float64",
    "IPv4",
    "IPv6",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "Nothing",
    "String",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    "UUID",
}
_UNSUPPORTED_SIMPLE = {"Nothing", "Int128", "Int256", "UInt128", "UInt256"}
_BINARY_TYPE_ARGUMENTS = 2
_DECIMAL_SCALE_LIMITS = {"Decimal32": 9, "Decimal64": 18, "Decimal128": 38}
_SINGLE_INTEGER_TYPES = {"FixedString", *_DECIMAL_SCALE_LIMITS}
_MAX_DECIMAL_PRECISION = 38
_MAX_DATETIME64_PRECISION = 9
_MIN_DESCRIBE_COLUMNS = 2
_TEXT_TRANSPORT_TYPES = {"Enum8", "Enum16", "IPv4", "IPv6", "UUID"}
_TUPLE_FIELD = re.compile(r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*|`(?:[^`\\]|\\.|``)+`)\s+(?P<type>.+)$")
_TIME_ZONE = re.compile(r"^'[A-Za-z0-9_+./:-]+'$")


@dataclass(frozen=True)
class ClickHouseType:
    """A parsed ClickHouse type constructor."""

    name: str
    arguments: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClickHouseColumn:
    """A declared column and the type emitted through ClickHouse Arrow."""

    name: str
    declared_type: str
    transport_type: str

    @property
    def cast_type(self) -> str | None:
        """Return the explicit lossless transport cast, when one is required."""
        return self.transport_type if self.transport_type != self.declared_type else None


@dataclass(frozen=True)
class ClickHouseSchemaPlan:
    """Canonical Arrow schema plus the safe projection used by every scan task."""

    arrow_schema: pa.Schema
    columns: tuple[ClickHouseColumn, ...]

    @classmethod
    def passthrough(cls, arrow_schema: pa.Schema) -> ClickHouseSchemaPlan:
        """Build a no-cast plan for injected schemas used by contract tests."""
        return cls(
            arrow_schema,
            tuple(ClickHouseColumn(name, "", "") for name in arrow_schema.names),
        )

    def projection_for(self, names: tuple[str, ...]) -> tuple[tuple[str, str | None], ...]:
        """Select ordered projection metadata for a validated schema subset."""
        by_name = {column.name: column for column in self.columns}
        try:
            return tuple((name, by_name[name].cast_type) for name in names)
        except KeyError as exc:
            raise SchemaError(f"unknown ClickHouse projection column {exc.args[0]!r}") from None


def split_type_arguments(value: str) -> tuple[str, ...]:
    """Split comma-delimited type arguments while respecting nesting and quoted strings."""
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for index, character in enumerate(value):
        if quote is not None:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            continue
        if character in {"'", '"', "`"}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                raise SchemaError(f"unbalanced ClickHouse type: {value!r}")
        elif character == "," and depth == 0:
            part = value[start:index].strip()
            if not part:
                raise SchemaError(f"empty ClickHouse type argument: {value!r}")
            parts.append(part)
            start = index + 1
    if quote is not None or depth != 0:
        raise SchemaError(f"unbalanced ClickHouse type: {value!r}")
    final = value[start:].strip()
    if not final:
        raise SchemaError(f"empty ClickHouse type argument: {value!r}")
    parts.append(final)
    return tuple(parts)


def parse_clickhouse_type(value: str) -> ClickHouseType:
    """Parse one complete type constructor without guessing malformed syntax."""
    text = value.strip()
    if not text:
        raise SchemaError("ClickHouse type must not be empty")
    open_index = text.find("(")
    if open_index < 0:
        if not _TYPE_NAME.fullmatch(text):
            raise SchemaError(f"invalid ClickHouse type: {value!r}")
        return ClickHouseType(text)
    if not text.endswith(")"):
        raise SchemaError(f"invalid ClickHouse type: {value!r}")
    name = text[:open_index].strip()
    if not _TYPE_NAME.fullmatch(name):
        raise SchemaError(f"invalid ClickHouse type constructor: {value!r}")
    return ClickHouseType(name, split_type_arguments(text[open_index + 1 : -1]))


def _validate_single_integer_type(parsed: ClickHouseType, value: str) -> None:
    if len(parsed.arguments) != 1 or not _INTEGER.fullmatch(parsed.arguments[0]):
        raise SchemaError(f"invalid ClickHouse {parsed.name} type: {value!r}")
    argument = int(parsed.arguments[0])
    if parsed.name == "FixedString":
        if argument < 1:
            raise SchemaError(f"invalid ClickHouse FixedString size: {value!r}")
        return
    if argument > _DECIMAL_SCALE_LIMITS[parsed.name]:
        raise SchemaError(f"invalid ClickHouse {parsed.name} scale: {value!r}")


def _tuple_field(argument: str) -> tuple[str, str] | None:
    match = _TUPLE_FIELD.fullmatch(argument)
    if match is None:
        return None
    return match.group("name"), match.group("type")


def _validate_type_expression(value: str) -> None:
    parsed = parse_clickhouse_type(value)
    if parsed.name in _SIMPLE_TYPES:
        if parsed.arguments or parsed.name in _UNSUPPORTED_SIMPLE:
            raise SchemaError(f"unsupported ClickHouse type: {value!r}")
        return
    if parsed.name in {"Nullable", "LowCardinality", "Array"}:
        if len(parsed.arguments) != 1:
            raise SchemaError(f"invalid ClickHouse {parsed.name} type: {value!r}")
        _validate_type_expression(parsed.arguments[0])
        return
    if parsed.name == "Map":
        if len(parsed.arguments) != _BINARY_TYPE_ARGUMENTS:
            raise SchemaError(f"invalid ClickHouse Map type: {value!r}")
        for argument in parsed.arguments:
            _validate_type_expression(argument)
        return
    if parsed.name == "Tuple":
        if not parsed.arguments:
            raise SchemaError(f"invalid ClickHouse Tuple type: {value!r}")
        for argument in parsed.arguments:
            try:
                _validate_type_expression(argument)
            except SchemaError:
                named_field = _tuple_field(argument)
                if named_field is None:
                    raise
                _validate_type_expression(named_field[1])
        return
    if parsed.name == "Decimal256":
        raise SchemaError(f"unsupported ClickHouse type: {value!r}")
    if parsed.name in _SINGLE_INTEGER_TYPES:
        _validate_single_integer_type(parsed, value)
        return
    if parsed.name == "Decimal":
        if len(parsed.arguments) != _BINARY_TYPE_ARGUMENTS or not all(
            _INTEGER.fullmatch(argument) for argument in parsed.arguments
        ):
            raise SchemaError(f"invalid ClickHouse Decimal type: {value!r}")
        precision, scale = (int(argument) for argument in parsed.arguments)
        if precision > _MAX_DECIMAL_PRECISION:
            raise SchemaError(f"unsupported ClickHouse type: {value!r}")
        if precision < 1 or scale < 0 or scale > precision:
            raise SchemaError(f"invalid ClickHouse Decimal bounds: {value!r}")
        return
    if parsed.name == "DateTime":
        if len(parsed.arguments) > 1:
            raise SchemaError(f"invalid ClickHouse DateTime type: {value!r}")
        if parsed.arguments and _TIME_ZONE.fullmatch(parsed.arguments[0]) is None:
            raise SchemaError(f"unsupported ClickHouse timezone: {parsed.arguments[0]!r}")
        return
    if parsed.name == "DateTime64":
        if not 1 <= len(parsed.arguments) <= _BINARY_TYPE_ARGUMENTS or not _INTEGER.fullmatch(
            parsed.arguments[0]
        ):
            raise SchemaError(f"invalid ClickHouse DateTime64 type: {value!r}")
        if not 0 <= int(parsed.arguments[0]) <= _MAX_DATETIME64_PRECISION:
            raise SchemaError(f"invalid ClickHouse DateTime64 precision: {value!r}")
        if (
            len(parsed.arguments) == _BINARY_TYPE_ARGUMENTS
            and _TIME_ZONE.fullmatch(parsed.arguments[1]) is None
        ):
            raise SchemaError(f"unsupported ClickHouse timezone: {parsed.arguments[1]!r}")
        return
    if parsed.name in {"Enum8", "Enum16"} and parsed.arguments:
        return
    raise SchemaError(f"unsupported ClickHouse type: {value!r}")


def validate_clickhouse_type(value: str) -> None:
    """Validate one complete supported ClickHouse declaration."""
    _validate_type_expression(value)


def _transport_type_expression(value: str) -> str:
    _validate_type_expression(value)
    parsed = parse_clickhouse_type(value)
    if parsed.name in _SIMPLE_TYPES:
        if parsed.name == "Date":
            return "Date32"
        if parsed.name in _TEXT_TRANSPORT_TYPES:
            return "String"
        return parsed.name
    if parsed.name in {"Nullable", "LowCardinality", "Array"}:
        return f"{parsed.name}({_transport_type_expression(parsed.arguments[0])})"
    if parsed.name == "Map":
        map_arguments = ", ".join(_transport_type_expression(item) for item in parsed.arguments)
        return f"Map({map_arguments})"
    if parsed.name == "Tuple":
        tuple_arguments: list[str] = []
        for argument in parsed.arguments:
            try:
                tuple_arguments.append(_transport_type_expression(argument))
            except SchemaError:
                named_field = _tuple_field(argument)
                if named_field is None:
                    raise
                field_name, declared_type = named_field
                field_type = _transport_type_expression(declared_type)
                tuple_arguments.append(f"{field_name} {field_type}")
        return f"Tuple({', '.join(tuple_arguments)})"
    if parsed.name == "DateTime":
        if not parsed.arguments:
            return "DateTime64(0)"
        timezone = parsed.arguments[0]
        if _TIME_ZONE.fullmatch(timezone) is None:
            raise SchemaError(f"unsupported ClickHouse timezone: {timezone!r}")
        return f"DateTime64(0, {timezone})"
    if parsed.name in _TEXT_TRANSPORT_TYPES:
        return "String"
    return value.strip()


def parse_describe_columns(describe_rows: Sequence[Sequence[Any]]) -> tuple[ClickHouseColumn, ...]:
    """Validate DESCRIBE rows and derive a deterministic Arrow transport type per column."""
    columns: list[ClickHouseColumn] = []
    for row in describe_rows:
        if (
            len(row) < _MIN_DESCRIBE_COLUMNS
            or not isinstance(row[0], str)
            or not row[0]
            or not isinstance(row[1], str)
        ):
            raise SchemaError("ClickHouse DESCRIBE returned an invalid row")
        declared_type = row[1].strip()
        columns.append(
            ClickHouseColumn(row[0], declared_type, _transport_type_expression(declared_type))
        )
    if not columns:
        raise SchemaError("ClickHouse DESCRIBE returned no columns")
    names = [column.name for column in columns]
    if len(set(names)) != len(names):
        raise SchemaError("ClickHouse DESCRIBE returned duplicate columns")
    return tuple(columns)


def render_schema_projection(columns: Sequence[ClickHouseColumn]) -> str:
    """Render the validated lossless casts used for schema probing and task scans."""
    projection: list[str] = []
    for column in columns:
        identifier = quote_identifier(column.name)
        if column.cast_type is None:
            projection.append(identifier)
        else:
            projection.append(f"CAST({identifier} AS {column.cast_type}) AS {identifier}")
    return ", ".join(projection)


def canonical_schema(describe_rows: Sequence[Sequence[Any]], arrow_schema: pa.Schema) -> pa.Schema:
    """Validate declared types and use the zero-row Arrow transport schema as canonical."""
    declared_names = [column.name for column in parse_describe_columns(describe_rows)]
    if tuple(declared_names) != tuple(arrow_schema.names):
        raise SchemaError("ClickHouse DESCRIBE and Arrow transport schemas disagree")
    for field in arrow_schema:
        try:
            pa.schema([field]).serialize()
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError) as exc:
            raise SchemaError(
                f"unsupported Arrow type for ClickHouse column {field.name!r}"
            ) from exc
    return arrow_schema


def project_schema(schema: pa.Schema, columns: tuple[str, ...]) -> pa.Schema:
    """Project a canonical schema in caller-provided order."""
    return pa.schema([schema.field(column) for column in columns], metadata=schema.metadata)
