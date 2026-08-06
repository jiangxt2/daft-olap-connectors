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

import asyncio
import threading
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest

from daft_olap._common.contracts import QuerySpec, ResourceLimits
from daft_olap._common.errors import DependencyError
from daft_olap.clickhouse import transport as clickhouse_transport
from daft_olap.clickhouse.discovery import ClickHouseConnection
from daft_olap.doris.discovery import DorisConnection
from daft_olap.doris.transports import flight, mysql


class FakeClickHouseContext:
    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        self._batches = batches
        self.closed = False

    async def __aenter__(self) -> Any:
        async def iterate() -> Any:
            for batch in self._batches:
                yield batch

        return iterate()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.closed = True


class FakeClickHouseClient:
    def __init__(self, context: FakeClickHouseContext) -> None:
        self.context = context
        self.closed = False
        self.call: dict[str, Any] = {}

    async def query_arrow_stream(self, sql: str, **kwargs: Any) -> FakeClickHouseContext:
        self.call = {"sql": sql, **kwargs}
        return self.context

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_clickhouse_async_stream_closes_context_and_client_on_early_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = FakeClickHouseContext(
        [pa.record_batch([[1, 2]], schema=pa.schema([("id", pa.int64())]))]
    )
    client = FakeClickHouseClient(context)

    async def get_async_client(**kwargs: Any) -> FakeClickHouseClient:
        assert kwargs["password"] == "secret"
        return client

    monkeypatch.setattr(
        clickhouse_transport,
        "_driver",
        lambda: SimpleNamespace(get_async_client=get_async_client),
    )
    stream = cast(
        AsyncGenerator[pa.RecordBatch, None],
        clickhouse_transport.stream_query(
            ClickHouseConnection(host="localhost", database="db", password="secret"),
            QuerySpec(
                sql="SELECT id FROM events",
                named_parameters=(("value", 1),),
                arrow_schema=pa.schema([("id", pa.int64())]),
            ),
            ResourceLimits(batch_rows=2, query_timeout_seconds=12),
        ),
    )
    assert (await anext(stream)).to_pydict() == {"id": [1, 2]}
    await stream.aclose()
    assert context.closed and client.closed
    assert client.call["parameters"] == {"value": 1}
    assert client.call["settings"]["max_block_size"] == 2
    assert client.call["settings"]["max_execution_time"] == 12


class FakeMySqlCursor:
    def __init__(self, rows: list[tuple[Any, ...]], events: list[tuple[str, int]]) -> None:
        self.description = (("id",), ("active",))
        self._rows = rows
        self._events = events
        self._result = SimpleNamespace(unbuffered_active=True, connection=object())
        self.closed = False

    def execute(self, sql: str, parameters: tuple[Any, ...] | None) -> None:
        self._events.append(("execute", threading.get_ident()))
        assert sql == "SELECT id, active FROM events"
        assert parameters is None

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self._events.append(("fetch", threading.get_ident()))
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def close(self) -> None:
        self._events.append(("cursor_close", threading.get_ident()))
        self.closed = True


class FakeMySqlConnection:
    def __init__(self, cursor: FakeMySqlCursor, events: list[tuple[str, int]]) -> None:
        self._cursor = cursor
        self._events = events
        self.closed = False

    def cursor(self) -> FakeMySqlCursor:
        self._events.append(("cursor", threading.get_ident()))
        return self._cursor

    def close(self) -> None:
        self._events.append(("connection_close", threading.get_ident()))
        self.closed = True


@pytest.mark.asyncio
async def test_mysql_reader_is_demand_driven_single_threaded_and_closes_early() -> None:
    events: list[tuple[str, int]] = []
    cursor = FakeMySqlCursor([(1, 1), (2, 0), (3, None)], events)
    connection = FakeMySqlConnection(cursor, events)
    query = QuerySpec(
        sql="SELECT id, active FROM events",
        arrow_schema=pa.schema([("id", pa.int64()), ("active", pa.bool_())]),
    )
    limits = ResourceLimits(batch_rows=2)
    reader = mysql.MySqlBatchReader(
        DorisConnection(host="localhost", database="db"),
        query,
        limits,
        connection_factory=lambda **kwargs: connection,
    )
    await reader.start()
    first = await reader.next_batch()
    assert first is not None
    assert first.to_pydict() == {"id": [1, 2], "active": [True, False]}
    assert [name for name, _ in events].count("fetch") == 1
    await reader.close()
    assert cursor.closed and connection.closed
    assert cursor._result.unbuffered_active is False
    assert cursor._result.connection is None
    event_names = [name for name, _ in events]
    assert event_names.index("connection_close") < event_names.index("cursor_close")
    worker_threads = {thread_id for _, thread_id in events}
    assert len(worker_threads) == 1
    assert threading.get_ident() not in worker_threads


@pytest.mark.asyncio
async def test_mysql_blocked_fetch_and_repeated_cancellation_still_close_on_owner_thread() -> None:
    fetch_started = threading.Event()
    release_fetch = threading.Event()
    resources_closed = threading.Event()
    events: list[tuple[str, int]] = []

    class BlockingCursor(FakeMySqlCursor):
        def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
            events.append(("fetch", threading.get_ident()))
            fetch_started.set()
            assert release_fetch.wait(timeout=2)
            return []

    class BlockingConnection(FakeMySqlConnection):
        def close(self) -> None:
            super().close()
            resources_closed.set()

    cursor = BlockingCursor([], events)
    connection = BlockingConnection(cursor, events)
    reader = mysql.MySqlBatchReader(
        DorisConnection(host="localhost", database="db"),
        QuerySpec(
            sql="SELECT id, active FROM events",
            arrow_schema=pa.schema([("id", pa.int64()), ("active", pa.bool_())]),
        ),
        ResourceLimits(query_timeout_seconds=2),
        connection_factory=lambda **kwargs: connection,
    )
    await reader.start()
    fetch = asyncio.create_task(reader.next_batch())
    assert await asyncio.to_thread(fetch_started.wait, 1)
    fetch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fetch

    close = asyncio.create_task(reader.close())
    await asyncio.sleep(0)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    release_fetch.set()
    assert await asyncio.to_thread(resources_closed.wait, 1)
    await reader.close()
    assert cursor.closed and connection.closed
    assert len({thread_id for _, thread_id in events}) == 1


class FakeStatement:
    def __init__(self, events: list[tuple[str, int]]) -> None:
        self.events = events
        self.options: dict[str, str] = {}

    def set_options(self, **options: str) -> None:
        self.events.append(("set_options", threading.get_ident()))
        self.options.update(options)


class FakeFlightReader:
    def __init__(self, batches: list[pa.RecordBatch], events: list[tuple[str, int]]) -> None:
        self._batches = iter(batches)
        self._events = events
        self.closed = False

    def __iter__(self) -> FakeFlightReader:
        return self

    def __next__(self) -> pa.RecordBatch:
        self._events.append(("fetch", threading.get_ident()))
        return next(self._batches)

    def close(self) -> None:
        self._events.append(("reader_close", threading.get_ident()))
        self.closed = True


class FakeFlightCursor:
    def __init__(self, reader: FakeFlightReader, events: list[tuple[str, int]]) -> None:
        self.reader = reader
        self.events = events
        self.arraysize = 0
        self.adbc_statement = FakeStatement(events)
        self.closed = False

    def execute(self, sql: str) -> None:
        self.events.append(("execute", threading.get_ident()))
        assert sql == "SELECT id FROM events"

    def fetch_record_batch(self) -> FakeFlightReader:
        self.events.append(("reader", threading.get_ident()))
        return self.reader

    def close(self) -> None:
        self.events.append(("cursor_close", threading.get_ident()))
        self.closed = True


class FakeFlightConnection:
    def __init__(self, cursor: FakeFlightCursor, events: list[tuple[str, int]]) -> None:
        self._cursor = cursor
        self.events = events
        self.closed = False

    def cursor(self) -> FakeFlightCursor:
        self.events.append(("cursor", threading.get_ident()))
        return self._cursor

    def close(self) -> None:
        self.events.append(("connection_close", threading.get_ident()))
        self.closed = True


@pytest.mark.asyncio
async def test_flight_reader_has_queue_cap_timeout_and_single_thread_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, int]] = []
    batch = pa.record_batch([[1, 2]], names=["id"])
    record_reader = FakeFlightReader([batch], events)
    cursor = FakeFlightCursor(record_reader, events)
    connection = FakeFlightConnection(cursor, events)
    username = SimpleNamespace(value="username")
    password = SimpleNamespace(value="password")
    timeout_query = SimpleNamespace(value="adbc.flight.sql.rpc.timeout_seconds.query")
    timeout_fetch = SimpleNamespace(value="adbc.flight.sql.rpc.timeout_seconds.fetch")
    with_block = SimpleNamespace(value="adbc.flight.sql.client_option.with_block")
    database_options = SimpleNamespace(
        USERNAME=username,
        PASSWORD=password,
    )
    flight_options = SimpleNamespace(
        TIMEOUT_QUERY=timeout_query,
        TIMEOUT_FETCH=timeout_fetch,
        WITH_BLOCK=with_block,
    )
    monkeypatch.setattr(
        flight,
        "flight_driver",
        lambda: (SimpleNamespace(), database_options, flight_options),
    )
    captured: dict[str, Any] = {}

    def connect(**kwargs: Any) -> FakeFlightConnection:
        captured.update(kwargs)
        return connection

    config = DorisConnection(host="localhost", database="db", password="secret", flight_secure=True)
    reader = flight.FlightBatchReader(
        config,
        QuerySpec(
            sql="SELECT id FROM events",
            arrow_schema=pa.schema([("id", pa.int64())]),
        ),
        ResourceLimits(batch_rows=2, query_timeout_seconds=12),
        connection_factory=connect,
    )
    await reader.start()
    assert cursor.adbc_statement.options == {"adbc.rpc.result_queue_size": "1"}
    assert captured["db_kwargs"]["adbc.flight.sql.rpc.timeout_seconds.query"] == "12"
    assert captured["db_kwargs"]["adbc.flight.sql.rpc.timeout_seconds.fetch"] == "12"
    assert captured["db_kwargs"]["adbc.flight.sql.client_option.with_block"] == "false"
    assert captured["autocommit"] is True
    assert captured["uri"] == "grpc+tls://localhost:8070"
    assert "secret" not in repr(config)
    next_batch = await reader.next_batch()
    assert next_batch is not None
    assert next_batch.to_pydict() == {"id": [1, 2]}
    await reader.close()
    assert record_reader.closed and cursor.closed and connection.closed
    assert len({thread_id for _, thread_id in events}) == 1


@pytest.mark.asyncio
async def test_explicit_flight_dependency_error_never_invokes_mysql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing() -> tuple[Any, Any, Any]:
        raise DependencyError("flight missing")

    monkeypatch.setattr(flight, "flight_driver", missing)
    mysql_called = False

    def forbidden_mysql(**kwargs: Any) -> None:
        nonlocal mysql_called
        mysql_called = True

    monkeypatch.setattr(mysql, "mysql_driver", forbidden_mysql)
    stream = flight.stream_query(
        DorisConnection(host="localhost", database="db"),
        QuerySpec(sql="SELECT id", arrow_schema=pa.schema([("id", pa.int64())])),
        ResourceLimits(),
    )
    with pytest.raises(DependencyError, match="flight missing"):
        await anext(stream)
    assert not mysql_called
