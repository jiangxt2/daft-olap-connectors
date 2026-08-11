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
import pickle
import sys
import urllib.error
from collections.abc import Callable, Sequence
from email.message import Message
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest

from daft_olap._common.contracts import ResourceLimits
from daft_olap._common.errors import (
    AuthenticationError,
    ConfigurationError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DependencyError,
    DiscoveryError,
    SchemaError,
)
from daft_olap._common.identifiers import QualifiedTable
from daft_olap._common.redaction import SecretRef
from daft_olap.clickhouse import discovery as clickhouse
from daft_olap.clickhouse.errors import translate_clickhouse_error
from daft_olap.doris import discovery as doris
from daft_olap.doris.errors import translate_doris_error


@pytest.mark.parametrize(
    ("code", "name", "expected_type"),
    [
        (516, "AUTHENTICATION_FAILED", AuthenticationError),
        (497, "ACCESS_DENIED", DatabasePermissionError),
        (60, "UNKNOWN_TABLE", DatabaseObjectNotFoundError),
    ],
)
def test_clickhouse_structured_errors_are_classified_without_driver_text(
    code: int,
    name: str,
    expected_type: type[BaseException],
) -> None:
    error = RuntimeError("private driver text")
    structured_error = cast(Any, error)
    structured_error.code = code
    structured_error.name = name
    translated = translate_clickhouse_error(error, operation="test operation")
    assert isinstance(translated, expected_type)
    assert "private driver text" not in str(translated)
    assert translate_clickhouse_error(RuntimeError("private"), operation="test") is None


@pytest.mark.parametrize(
    ("vendor_code", "status_code", "sqlstate", "expected_type"),
    [
        (1045, None, None, AuthenticationError),
        (1142, None, None, DatabasePermissionError),
        (1146, None, None, DatabaseObjectNotFoundError),
        (None, 13, None, AuthenticationError),
        (None, 14, None, DatabasePermissionError),
        (None, 3, None, DatabaseObjectNotFoundError),
        (None, None, "28000", AuthenticationError),
    ],
)
def test_doris_structured_errors_are_classified_without_driver_text(
    vendor_code: int | None,
    status_code: int | None,
    sqlstate: str | None,
    expected_type: type[BaseException],
) -> None:
    error = RuntimeError(*(() if vendor_code is None else (vendor_code, "private text")))
    structured_error = cast(Any, error)
    structured_error.status_code = status_code
    structured_error.sqlstate = sqlstate
    translated = translate_doris_error(error, operation="test operation")
    assert isinstance(translated, expected_type)
    assert "private text" not in str(translated)


def test_doris_generic_missing_object_error_is_classified_without_driver_text() -> None:
    error = RuntimeError(1105, "errCode = 2, detailMessage = Unknown table 'private_table'")
    translated = translate_doris_error(error, operation="schema discovery")
    assert isinstance(translated, DatabaseObjectNotFoundError)
    assert "private_table" not in str(translated)
    assert translate_doris_error(RuntimeError(1105, "generic failure"), operation="test") is None


class ClickHouseResult:
    def __init__(self, rows: Sequence[Sequence[Any]]) -> None:
        self.result_rows = rows


class FakeClickHouseClient:
    def __init__(
        self,
        *,
        describe_rows: Sequence[Sequence[Any]] = (("id", "Int64"),),
        arrow_value: object | None = None,
        engine_rows: Sequence[Sequence[Any]] = (("MergeTree",),),
        physical_partition_column_rows: Sequence[Sequence[Any]] = (),
        partition_rows: Sequence[Sequence[Any]] = (("p1", 10), ("p2", 5)),
        query_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.describe_rows = describe_rows
        self.arrow_value = (
            pa.table({"id": pa.array([], type=pa.int64())}) if arrow_value is None else arrow_value
        )
        self.engine_rows = engine_rows
        self.physical_partition_column_rows = physical_partition_column_rows
        self.partition_rows = partition_rows
        self.query_error = query_error
        self.close_error = close_error
        self.calls: list[tuple[str, object]] = []
        self.closed = False

    def query(self, sql: str, parameters: object = None) -> ClickHouseResult:
        self.calls.append((sql, parameters))
        if self.query_error is not None:
            raise self.query_error
        if sql.startswith("DESCRIBE"):
            return ClickHouseResult(self.describe_rows)
        if "system.tables" in sql:
            return ClickHouseResult(self.engine_rows)
        if "system.columns" in sql:
            return ClickHouseResult(self.physical_partition_column_rows)
        if "system.parts" in sql:
            return ClickHouseResult(self.partition_rows)
        raise AssertionError(f"unexpected query: {sql}")

    def query_arrow(self, sql: str, **kwargs: object) -> object:
        self.calls.append((sql, kwargs))
        return self.arrow_value

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def _install_clickhouse_client(
    monkeypatch: pytest.MonkeyPatch, client: FakeClickHouseClient
) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def get_client(**kwargs: Any) -> FakeClickHouseClient:
        captured.update(kwargs)
        return client

    monkeypatch.setattr(clickhouse, "_driver", lambda: SimpleNamespace(get_client=get_client))
    return captured


def test_clickhouse_connection_validation_freezing_secret_and_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CH_PASSWORD", "resolved")
    connection = clickhouse.ClickHouseConnection.from_options(
        host="localhost",
        database="analytics",
        username="reader",
        password=SecretRef.env("CH_PASSWORD"),
        port=8443,
        secure=True,
        settings={"max_threads": 2},
        client_options={"compress": True},
    )
    kwargs = connection.client_kwargs(
        ResourceLimits(connect_timeout_seconds=3, query_timeout_seconds=7)
    )
    assert kwargs["password"] == "resolved"
    assert kwargs["compress"] is True
    assert kwargs["connect_timeout"] == 3
    assert "resolved" not in repr(connection)
    assert "max_threads" in repr(connection)
    with pytest.raises(ConfigurationError, match="managed option"):
        clickhouse.ClickHouseConnection.from_options(
            host="localhost",
            database="analytics",
            username="reader",
            password="",
            port=8123,
            secure=False,
            settings=None,
            client_options={"password": "forbidden"},
        )
    for managed_option in ("connect_timeout", "send_receive_timeout"):
        with pytest.raises(ConfigurationError, match="managed option"):
            clickhouse.ClickHouseConnection.from_options(
                host="localhost",
                database="analytics",
                username="reader",
                password="",
                port=8123,
                secure=False,
                settings=None,
                client_options={managed_option: 1},
            )
    for managed_setting in ("max_block_size", "max_execution_time"):
        with pytest.raises(ConfigurationError, match="managed option"):
            clickhouse.ClickHouseConnection.from_options(
                host="localhost",
                database="analytics",
                username="reader",
                password="",
                port=8123,
                secure=False,
                settings={managed_setting: 1},
                client_options=None,
            )


def test_clickhouse_connection_snapshots_nested_options_and_returns_fresh_kwargs() -> None:
    setting_value = {"labels": ["before"]}
    client_value = {"headers": ["before"]}
    settings = {"custom_setting": setting_value}
    client_options = {"custom_client": client_value}
    connection = clickhouse.ClickHouseConnection.from_options(
        host="localhost",
        database="analytics",
        username="reader",
        password="",
        port=8123,
        secure=False,
        settings=settings,
        client_options=client_options,
    )

    setting_value["labels"].append("after")
    client_value["headers"].append("after")

    settings = connection.settings_kwargs()
    assert settings == {"custom_setting": {"labels": ["before"]}}
    settings["custom_setting"]["labels"].append("mutated")
    assert connection.settings_kwargs() == {"custom_setting": {"labels": ["before"]}}
    first = connection.client_kwargs(ResourceLimits())
    assert first["custom_client"] == {"headers": ["before"]}
    cast(list[str], first["custom_client"]["headers"]).append("mutated")
    assert connection.client_kwargs(ResourceLimits())["custom_client"] == {"headers": ["before"]}
    assert pickle.loads(pickle.dumps(connection)) == connection


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: clickhouse.ClickHouseConnection(host="", database="db"), "host"),
        (lambda: clickhouse.ClickHouseConnection(host="host", database=""), "database"),
        (
            lambda: clickhouse.ClickHouseConnection(host="host", database="db", username=""),
            "username",
        ),
        (lambda: clickhouse.ClickHouseConnection(host="host", database="db", port=0), "port"),
        (lambda: clickhouse.ClickHouseConnection(host="host", database="db", port=True), "port"),
        (
            lambda: clickhouse.ClickHouseConnection(
                host="host", database="db", secure=cast(Any, "yes")
            ),
            "secure",
        ),
    ],
)
def test_clickhouse_connection_rejects_invalid_public_configuration(
    factory: Callable[[], clickhouse.ClickHouseConnection], message: str
) -> None:
    with pytest.raises(ConfigurationError, match=message):
        factory()


def test_clickhouse_schema_discovery_uses_arrow_authority_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClickHouseClient(
        describe_rows=(("id", "Int64"), ("name", "Nullable(String)")),
        arrow_value=pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "name": pa.array([], type=pa.string()),
            }
        ),
    )
    captured = _install_clickhouse_client(monkeypatch, client)
    plan = clickhouse.discover_schema(
        clickhouse.ClickHouseConnection(host="host", database="db"),
        QualifiedTable("db", "events"),
        ResourceLimits(),
    )
    assert plan.arrow_schema.names == ["id", "name"]
    assert plan.projection_for(("id", "name")) == (("id", None), ("name", None))
    assert captured["host"] == "host"
    assert client.closed
    assert client.calls[0][0] == "DESCRIBE TABLE `db`.`events`"
    assert client.calls[1][0] == "SELECT `id`, `name` FROM `db`.`events` LIMIT 0"


@pytest.mark.parametrize(
    "client",
    [
        FakeClickHouseClient(arrow_value="not-arrow"),
        FakeClickHouseClient(
            query_error=RuntimeError("driver detail must be hidden"),
            close_error=RuntimeError("close detail must not replace the primary error"),
        ),
    ],
)
def test_clickhouse_schema_discovery_fails_closed_and_closes(
    monkeypatch: pytest.MonkeyPatch, client: FakeClickHouseClient
) -> None:
    _install_clickhouse_client(monkeypatch, client)
    with pytest.raises(SchemaError, match=r"failed|non-Arrow") as error:
        clickhouse.discover_schema(
            clickhouse.ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert "driver detail" not in str(error.value)
    assert client.closed


@pytest.mark.parametrize(
    ("engine_rows", "partition_rows", "supported", "partition_ids"),
    [
        ((("View",),), (), False, ()),
        ((("MergeTree",),), (("p2", 5), ("p1", 10)), True, ("p2", "p1")),
        ((("ReplacingMergeTree",),), (("p1", 10),), True, ("p1",)),
        ((("ReplicatedMergeTree",),), (("p1", 10),), False, ()),
        ((("SharedMergeTree",),), (("p1", 10),), False, ()),
        ((("FutureMergeTree",),), (("p1", 10),), False, ()),
    ],
)
def test_clickhouse_partition_discovery_engine_gate_and_metadata(
    monkeypatch: pytest.MonkeyPatch,
    engine_rows: Sequence[Sequence[Any]],
    partition_rows: Sequence[Sequence[Any]],
    supported: bool,
    partition_ids: tuple[str, ...],
) -> None:
    client = FakeClickHouseClient(engine_rows=engine_rows, partition_rows=partition_rows)
    _install_clickhouse_client(monkeypatch, client)
    result = clickhouse.discover_partitions(
        clickhouse.ClickHouseConnection(host="host", database="db"),
        QualifiedTable("db", "events"),
        ResourceLimits(),
    )
    assert result.supported is supported
    assert tuple(item.partition_id for item in result.partitions) == partition_ids
    assert client.closed


def test_clickhouse_partition_discovery_distinguishes_missing_and_shadowing_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = FakeClickHouseClient(engine_rows=())
    _install_clickhouse_client(monkeypatch, missing)
    with pytest.raises(DatabaseObjectNotFoundError, match="not found"):
        clickhouse.discover_partitions(
            clickhouse.ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "missing"),
            ResourceLimits(),
        )

    shadowed = FakeClickHouseClient(
        physical_partition_column_rows=(("_partition_id",),),
        partition_rows=(("must-not-be-read", 1),),
    )
    _install_clickhouse_client(monkeypatch, shadowed)
    result = clickhouse.discover_partitions(
        clickhouse.ClickHouseConnection(host="host", database="db"),
        QualifiedTable("db", "events"),
        ResourceLimits(),
    )
    assert not result.supported
    assert not any("system.parts" in sql for sql, _ in shadowed.calls)


def test_clickhouse_partition_discovery_rejects_bad_rows_and_hides_driver_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed_rows: tuple[tuple[Sequence[Any], ...], ...] = (
        ((1, 5),),
        (("p1", -1),),
        (("p1", "5"),),
    )
    for partition_rows in malformed_rows:
        malformed = FakeClickHouseClient(partition_rows=partition_rows)
        _install_clickhouse_client(monkeypatch, malformed)
        with pytest.raises(DiscoveryError, match="malformed"):
            clickhouse.discover_partitions(
                clickhouse.ClickHouseConnection(host="host", database="db"),
                QualifiedTable("db", "events"),
                ResourceLimits(),
            )

    duplicate = FakeClickHouseClient(partition_rows=(("p1", 5), ("p1", 7)))
    _install_clickhouse_client(monkeypatch, duplicate)
    with pytest.raises(DiscoveryError, match="duplicate"):
        clickhouse.discover_partitions(
            clickhouse.ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    failing = FakeClickHouseClient(
        query_error=RuntimeError("secret driver detail"),
        close_error=RuntimeError("close detail must not replace the primary error"),
    )
    _install_clickhouse_client(monkeypatch, failing)
    with pytest.raises(DiscoveryError, match="failed") as error:
        clickhouse.discover_partitions(
            clickhouse.ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert "secret driver detail" not in str(error.value)


def test_clickhouse_discovery_reports_cleanup_failures_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema_client = FakeClickHouseClient(close_error=RuntimeError("private close detail"))
    _install_clickhouse_client(monkeypatch, schema_client)
    with pytest.raises(SchemaError, match="close") as schema_error:
        clickhouse.discover_schema(
            clickhouse.ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert "private close detail" not in str(schema_error.value)

    partition_client = FakeClickHouseClient(close_error=RuntimeError("private close detail"))
    _install_clickhouse_client(monkeypatch, partition_client)
    with pytest.raises(DiscoveryError, match="close") as discovery_error:
        clickhouse.discover_partitions(
            clickhouse.ClickHouseConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert "private close detail" not in str(discovery_error.value)


class FakeDorisCursor:
    def __init__(
        self,
        *,
        batches: list[list[tuple[Any, ...]]] | None = None,
        mogrified: str | bytes = b"SELECT 1",
        error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.batches = list(batches or [])
        self.mogrified = mogrified
        self.error = error
        self.close_error = close_error
        self.executed: list[tuple[str, object]] = []
        self.closed = False

    def execute(self, sql: str, parameters: object = None) -> None:
        self.executed.append((sql, parameters))
        if self.error is not None:
            raise self.error

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        assert size == 256
        return self.batches.pop(0) if self.batches else []

    def mogrify(self, sql: str, parameters: tuple[Any, ...]) -> str | bytes:
        self.executed.append((sql, parameters))
        if self.error is not None:
            raise self.error
        return self.mogrified

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeDorisDatabaseConnection:
    def __init__(
        self, cursor: FakeDorisCursor, *, close_error: BaseException | None = None
    ) -> None:
        self._cursor = cursor
        self.close_error = close_error
        self.closed = False

    def cursor(self) -> FakeDorisCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def _install_doris_connection(
    monkeypatch: pytest.MonkeyPatch,
    connection: FakeDorisDatabaseConnection,
) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def connect(**kwargs: Any) -> FakeDorisDatabaseConnection:
        captured.update(kwargs)
        return connection

    monkeypatch.setattr(doris, "mysql_driver", lambda: SimpleNamespace(connect=connect))
    return captured


def test_doris_connection_validation_options_secret_and_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DORIS_SECRET", "resolved")
    connection = doris.DorisConnection.from_options(
        host="host",
        database="db",
        username="reader",
        password=SecretRef.env("DORIS_SECRET"),
        mysql_port=9031,
        http_port=8031,
        flight_port=8071,
        mysql_options={"ssl": None},
        flight_options={"adbc.option": "value"},
        http_secure=True,
        flight_secure=True,
    )
    kwargs = connection.mysql_kwargs(
        ResourceLimits(connect_timeout_seconds=2, query_timeout_seconds=9)
    )
    assert kwargs["password"] == "resolved"
    assert kwargs["ssl"] is None
    assert kwargs["read_timeout"] == 9
    assert "resolved" not in repr(connection)
    assert "http_secure=True" in repr(connection)
    assert "flight_secure=True" in repr(connection)
    with pytest.raises(ConfigurationError, match="managed option"):
        doris.DorisConnection.from_options(
            host="host",
            database="db",
            username="reader",
            password="",
            mysql_port=9030,
            http_port=8030,
            flight_port=8070,
            mysql_options={"password": "forbidden"},
            flight_options=None,
        )
    with pytest.raises(ConfigurationError, match="values"):
        doris.DorisConnection.from_options(
            host="host",
            database="db",
            username="reader",
            password="",
            mysql_port=9030,
            http_port=8030,
            flight_port=8070,
            mysql_options=None,
            flight_options=cast(Any, {"option": 1}),
        )
    for mysql_option in ("defer_connect", "read_default_file", "conv"):
        with pytest.raises(ConfigurationError, match="managed option"):
            doris.DorisConnection.from_options(
                host="host",
                database="db",
                username="reader",
                password="",
                mysql_port=9030,
                http_port=8030,
                flight_port=8070,
                mysql_options={mysql_option: True},
                flight_options=None,
            )
    for flight_option in (
        "adbc.flight.sql.client_option.with_block",
        "adbc.flight.sql.rpc.timeout_seconds.fetch",
        "adbc.flight.sql.rpc.timeout_seconds.query",
    ):
        with pytest.raises(ConfigurationError, match="managed option"):
            doris.DorisConnection.from_options(
                host="host",
                database="db",
                username="reader",
                password="",
                mysql_port=9030,
                http_port=8030,
                flight_port=8070,
                mysql_options=None,
                flight_options={flight_option: "0"},
            )


def test_doris_connection_snapshots_nested_options_and_returns_fresh_kwargs() -> None:
    ssl_value = {"ca": ["before"]}
    mysql_options = {"ssl": ssl_value}
    flight_options = {"adbc.option": "before"}
    connection = doris.DorisConnection.from_options(
        host="host",
        database="db",
        username="reader",
        password="",
        mysql_port=9030,
        http_port=8030,
        flight_port=8070,
        mysql_options=mysql_options,
        flight_options=flight_options,
    )

    ssl_value["ca"].append("after")
    flight_options["adbc.option"] = "after"

    first = connection.mysql_kwargs(ResourceLimits())
    assert first["ssl"] == {"ca": ["before"]}
    cast(list[str], first["ssl"]["ca"]).append("mutated")
    assert connection.mysql_kwargs(ResourceLimits())["ssl"] == {"ca": ["before"]}
    assert dict(connection.flight_options) == {"adbc.option": "before"}
    assert pickle.loads(pickle.dumps(connection)) == connection


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: doris.DorisConnection(host="", database="db"), "host"),
        (lambda: doris.DorisConnection(host="host", database=""), "database"),
        (lambda: doris.DorisConnection(host="host", database="db", username=""), "username"),
        (
            lambda: doris.DorisConnection(host="host", database="db", mysql_port=0),
            "mysql_port",
        ),
        (
            lambda: doris.DorisConnection(host="host", database="db", http_port=True),
            "http_port",
        ),
        (
            lambda: doris.DorisConnection(host="host", database="db", flight_port=70_000),
            "flight_port",
        ),
        (
            lambda: doris.DorisConnection(host="https://host", database="db"),
            "bare hostname",
        ),
        (
            lambda: doris.DorisConnection(host="[::1]", database="db"),
            "unbracketed",
        ),
        (
            lambda: doris.DorisConnection(host="host", database="db", http_secure=cast(Any, "yes")),
            "http_secure",
        ),
        (
            lambda: doris.DorisConnection(host="host", database="db", flight_secure=cast(Any, 1)),
            "flight_secure",
        ),
    ],
)
def test_doris_connection_rejects_invalid_public_configuration(
    factory: Callable[[], doris.DorisConnection], message: str
) -> None:
    with pytest.raises(ConfigurationError, match=message):
        factory()


def test_doris_schema_discovery_streams_describe_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = FakeDorisCursor(
        batches=[
            [("id", "BIGINT", "NO")],
            [("name", "VARCHAR(20)", "YES")],
        ]
    )
    connection = FakeDorisDatabaseConnection(cursor)
    captured = _install_doris_connection(monkeypatch, connection)
    schema = doris.discover_schema(
        doris.DorisConnection(host="host", database="db"),
        QualifiedTable("db", "events"),
        ResourceLimits(),
    )
    assert schema == pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())]
    )
    assert captured["database"] == "db"
    assert cursor.executed == [("DESCRIBE `db`.`events`", None)]
    assert cursor.closed and connection.closed


def test_doris_schema_and_plan_binding_failures_close_and_hide_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = FakeDorisCursor(
        error=RuntimeError("private detail"), close_error=RuntimeError("cursor close detail")
    )
    connection = FakeDorisDatabaseConnection(
        cursor, close_error=RuntimeError("connection close detail")
    )
    _install_doris_connection(monkeypatch, connection)
    with pytest.raises(SchemaError, match="failed") as schema_error:
        doris.discover_schema(
            doris.DorisConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert "private detail" not in str(schema_error.value)
    assert cursor.closed and connection.closed

    cursor = FakeDorisCursor(
        error=RuntimeError("private bind detail"),
        close_error=RuntimeError("cursor close detail"),
    )
    connection = FakeDorisDatabaseConnection(
        cursor, close_error=RuntimeError("connection close detail")
    )
    _install_doris_connection(monkeypatch, connection)
    with pytest.raises(DiscoveryError, match="bind") as bind_error:
        doris._materialize_plan_sql(
            doris.DorisConnection(host="host", database="db"),
            "SELECT %s",
            (1,),
            ResourceLimits(),
        )
    assert "private bind detail" not in str(bind_error.value)
    assert cursor.closed and connection.closed


def test_doris_discovery_reports_cleanup_failures_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = FakeDorisCursor(
        batches=[[("id", "BIGINT", "NO")]],
        close_error=RuntimeError("private cursor close detail"),
    )
    connection = FakeDorisDatabaseConnection(cursor)
    _install_doris_connection(monkeypatch, connection)
    with pytest.raises(SchemaError, match="close") as schema_error:
        doris.discover_schema(
            doris.DorisConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            ResourceLimits(),
        )
    assert "private cursor close detail" not in str(schema_error.value)
    assert cursor.closed and connection.closed

    cursor = FakeDorisCursor(mogrified="SELECT 1")
    connection = FakeDorisDatabaseConnection(
        cursor, close_error=RuntimeError("private connection close detail")
    )
    _install_doris_connection(monkeypatch, connection)
    with pytest.raises(DiscoveryError, match="close") as discovery_error:
        doris._materialize_plan_sql(
            doris.DorisConnection(host="host", database="db"),
            "SELECT %s",
            (1,),
            ResourceLimits(),
        )
    assert "private connection close detail" not in str(discovery_error.value)
    assert cursor.closed and connection.closed


def test_doris_plan_binding_without_parameters_does_not_open_mysql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doris,
        "mysql_driver",
        lambda: pytest.fail("parameterless planning SQL must not open MySQL"),
    )
    assert (
        doris._materialize_plan_sql(
            doris.DorisConnection(host="host", database="db"),
            "SELECT 1",
            (),
            ResourceLimits(),
        )
        == "SELECT 1"
    )


@pytest.mark.parametrize("mogrified", [b"SELECT 7", "SELECT 7"])
def test_doris_plan_binding_accepts_driver_text_and_bytes(
    monkeypatch: pytest.MonkeyPatch, mogrified: str | bytes
) -> None:
    cursor = FakeDorisCursor(mogrified=mogrified)
    connection = FakeDorisDatabaseConnection(cursor)
    _install_doris_connection(monkeypatch, connection)
    assert (
        doris._materialize_plan_sql(
            doris.DorisConnection(host="host", database="db"),
            "SELECT %s",
            (7,),
            ResourceLimits(),
        )
        == "SELECT 7"
    )


@pytest.mark.parametrize(
    ("payload", "error_type", "message"),
    [
        ([], DiscoveryError, "object"),
        ({"code": 2}, DiscoveryError, "request failed"),
        ({"code": 0}, DiscoveryError, "data"),
        ({"code": 0, "data": {"status": "1", "exception": "bad"}}, DiscoveryError, "rejected"),
        ({"code": 0, "data": {"status": 201}}, DiscoveryError, "status"),
        ({"code": 0, "data": {"status": 200}}, DiscoveryError, "partitions"),
        (
            {"code": 0, "data": {"status": 200, "partitions": {"bad": {}}}},
            DiscoveryError,
            "non-integer",
        ),
        (
            {"code": 0, "data": {"status": 200, "partitions": {"0": {}}}},
            DiscoveryError,
            "non-positive",
        ),
        ({"code": 401}, AuthenticationError, "credentials"),
        (
            {"code": 0, "data": {"status": "1", "exception": "Access denied: table"}},
            DatabasePermissionError,
            "SELECT",
        ),
        (
            {"code": 0, "data": {"status": "1", "exception": "Unknown table events"}},
            DatabaseObjectNotFoundError,
            "not found",
        ),
    ],
)
def test_doris_query_plan_response_rejects_every_malformed_envelope(
    payload: object, error_type: type[BaseException], message: str
) -> None:
    with pytest.raises(error_type, match=message):
        doris.parse_query_plan_response(payload)


class FakeHttpResponse:
    def __init__(self, *, status: int = 200, body: bytes = b"") -> None:
        self.status = status
        self._body = body
        self.read_sizes: list[int] = []

    def __enter__(self) -> FakeHttpResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return self._body if size < 0 else self._body[:size]


def _plan_payload() -> bytes:
    return json.dumps(
        {"code": 0, "data": {"status": 200, "partitions": {"4": {}, "2": {}}}}
    ).encode()


def test_doris_query_plan_redirects_are_rejected() -> None:
    handler = doris._RejectRedirects()
    redirect = cast(Any, handler.redirect_request)
    assert redirect(object(), object(), 302, "redirect", Message(), "https://other") is None
    assert not any(
        isinstance(candidate, urllib.request.ProxyHandler)
        for candidate in cast(Any, doris._QUERY_PLAN_OPENER).handlers
    )


def test_doris_tablet_discovery_builds_authenticated_encoded_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(doris, "_materialize_plan_sql", lambda *args: "SELECT 1")
    captured: dict[str, object] = {}

    def open_request(request: Any, timeout: float) -> FakeHttpResponse:
        captured["request"] = request
        captured["timeout"] = timeout
        return FakeHttpResponse(body=_plan_payload())

    monkeypatch.setattr(doris, "_open_query_plan", open_request)
    result = doris.discover_tablets(
        doris.DorisConnection(
            host="host", database="db name", username="reader", password="secret", http_port=8080
        ),
        QualifiedTable("db name", "event/table"),
        "SELECT 1",
        (),
        ResourceLimits(connect_timeout_seconds=4),
    )
    assert result == (2, 4)
    request = cast(urllib.request.Request, captured["request"])
    assert request.full_url == "http://host:8080/api/db%20name/event%2Ftable/_query_plan"
    authorization = request.get_header("Authorization")
    assert authorization is not None
    assert authorization.startswith("Basic ")
    assert captured["timeout"] == 4


def test_doris_tablet_discovery_supports_https_and_ipv6(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(doris, "_materialize_plan_sql", lambda *args: "SELECT 1")
    captured: dict[str, object] = {}

    def open_request(request: Any, timeout: float) -> FakeHttpResponse:
        captured["url"] = request.full_url
        return FakeHttpResponse(body=_plan_payload())

    monkeypatch.setattr(doris, "_open_query_plan", open_request)
    tablets = doris.discover_tablets(
        doris.DorisConnection(host="::1", database="db", http_port=8050, http_secure=True),
        QualifiedTable("db", "events"),
        "SELECT 1",
        (),
        ResourceLimits(),
    )
    assert tablets == (2, 4)
    assert captured["url"] == "https://[::1]:8050/api/db/events/_query_plan"


def test_doris_tablet_discovery_bounds_query_plan_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(doris, "_materialize_plan_sql", lambda *args: "SELECT 1")
    monkeypatch.setattr(doris, "_MAX_QUERY_PLAN_RESPONSE_BYTES", 8)
    response = FakeHttpResponse(body=b"123456789")
    monkeypatch.setattr(doris, "_open_query_plan", lambda *args, **kwargs: response)
    with pytest.raises(DiscoveryError, match="safety limit"):
        doris.discover_tablets(
            doris.DorisConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            "SELECT 1",
            (),
            ResourceLimits(),
        )
    assert response.read_sizes == [9]


@pytest.mark.parametrize(
    ("effect", "error_type", "message"),
    [
        (FakeHttpResponse(status=503), DiscoveryError, "HTTP 503"),
        (
            urllib.error.HTTPError("http://host", 401, "unauthorized", Message(), None),
            AuthenticationError,
            "credentials",
        ),
        (
            urllib.error.HTTPError("http://host", 403, "forbidden", Message(), None),
            DatabasePermissionError,
            "denied",
        ),
        (
            urllib.error.HTTPError("http://host", 500, "failed", Message(), None),
            DiscoveryError,
            "HTTP 500",
        ),
        (OSError("offline"), DiscoveryError, "unavailable"),
        (FakeHttpResponse(body=b"not-json"), DiscoveryError, "invalid JSON"),
    ],
)
def test_doris_tablet_discovery_classifies_http_and_payload_failures(
    monkeypatch: pytest.MonkeyPatch,
    effect: FakeHttpResponse | BaseException,
    error_type: type[BaseException],
    message: str,
) -> None:
    monkeypatch.setattr(doris, "_materialize_plan_sql", lambda *args: "SELECT 1")

    def open_request(*args: object, **kwargs: object) -> FakeHttpResponse:
        if isinstance(effect, BaseException):
            raise effect
        return effect

    monkeypatch.setattr(doris, "_open_query_plan", open_request)
    with pytest.raises(error_type, match=message):
        doris.discover_tablets(
            doris.DorisConnection(host="host", database="db"),
            QualifiedTable("db", "events"),
            "SELECT 1",
            (),
            ResourceLimits(),
        )


def test_optional_driver_imports_have_actionable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "clickhouse_connect", None)
    monkeypatch.setitem(sys.modules, "pymysql", None)
    with pytest.raises(DependencyError, match="clickhouse"):
        clickhouse._driver()
    with pytest.raises(DependencyError, match="Doris"):
        doris.mysql_driver()
