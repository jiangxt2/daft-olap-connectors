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
import sys
from collections.abc import AsyncIterator
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pyarrow as pa
import pytest

from daft_olap._common.contracts import QuerySpec, ResourceLimits
from daft_olap._common.errors import DependencyError, SchemaError, TransportError
from daft_olap.clickhouse import task as clickhouse_task_module
from daft_olap.clickhouse import transport as clickhouse
from daft_olap.clickhouse.discovery import ClickHouseConnection
from daft_olap.clickhouse.task import ClickHouseTask
from daft_olap.doris import task as doris_task_module
from daft_olap.doris.discovery import DorisConnection
from daft_olap.doris.task import DorisTask
from daft_olap.doris.transports import flight, mysql


def test_arrow_batch_casts_preserve_schema_and_fail_closed() -> None:
    int32_batch = pa.record_batch([[1, 2]], names=["id"])
    int64_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    clickhouse_cast = clickhouse.cast_batch(int32_batch, int64_schema)
    flight_cast = flight.cast_batch(int32_batch, int64_schema)
    assert clickhouse_cast.schema == int64_schema
    assert flight_cast.schema == int64_schema
    assert clickhouse_cast.to_pydict() == {"id": [1, 2]}
    with pytest.raises(SchemaError, match="columns"):
        clickhouse.cast_batch(int32_batch, pa.schema([("other", pa.int64())]))
    with pytest.raises(SchemaError, match="columns"):
        flight.cast_batch(int32_batch, pa.schema([("other", pa.int64())]))
    string_batch = pa.record_batch([pa.array(["not-an-int"])], names=["id"])
    with pytest.raises(SchemaError, match="planned"):
        clickhouse.cast_batch(string_batch, int64_schema)
    with pytest.raises(SchemaError, match="planned"):
        flight.cast_batch(string_batch, int64_schema)


def test_flight_reader_rejects_bound_parameters_before_loading_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver_loaded = False

    def forbidden_driver() -> tuple[Any, Any, Any]:
        nonlocal driver_loaded
        driver_loaded = True
        raise AssertionError("driver must not load")

    monkeypatch.setattr(flight, "flight_driver", forbidden_driver)
    reader = flight.FlightBatchReader(
        DorisConnection(host="host", database="db"),
        QuerySpec(
            sql="SELECT id WHERE id = ?",
            positional_parameters=(1,),
            arrow_schema=pa.schema([("id", pa.int64())]),
        ),
        ResourceLimits(),
    )
    with pytest.raises(TransportError, match="fully materialized"):
        reader._open()
    assert not driver_loaded


class AsyncContext:
    def __init__(self, values: list[object], error: BaseException | None = None) -> None:
        self.values = values
        self.error = error
        self.closed = False

    async def __aenter__(self) -> AsyncIterator[object]:
        async def iterate() -> AsyncIterator[object]:
            for value in self.values:
                yield value
            if self.error is not None:
                raise self.error

        return iterate()

    async def __aexit__(self, *args: object) -> None:
        self.closed = True


class AsyncClient:
    def __init__(
        self,
        context: AsyncContext,
        *,
        query_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.context = context
        self.query_error = query_error
        self.close_error = close_error
        self.closed = False

    async def query_arrow_stream(self, *args: object, **kwargs: object) -> AsyncContext:
        if self.query_error is not None:
            raise self.query_error
        return self.context

    async def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def _install_async_client(monkeypatch: pytest.MonkeyPatch, client: AsyncClient) -> None:
    async def get_async_client(**kwargs: object) -> AsyncClient:
        return client

    monkeypatch.setattr(
        clickhouse,
        "_driver",
        lambda: SimpleNamespace(get_async_client=get_async_client),
    )


def _clickhouse_stream() -> tuple[ClickHouseConnection, QuerySpec, ResourceLimits]:
    return (
        ClickHouseConnection(host="host", database="db"),
        QuerySpec(sql="SELECT id", arrow_schema=pa.schema([("id", pa.int64())])),
        ResourceLimits(),
    )


@pytest.mark.asyncio
async def test_clickhouse_stream_classifies_non_batch_driver_and_close_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    non_batch = AsyncClient(AsyncContext(["bad"]))
    _install_async_client(monkeypatch, non_batch)
    with pytest.raises(TransportError, match="non-RecordBatch"):
        await anext(clickhouse.stream_query(*_clickhouse_stream()))
    assert non_batch.closed

    query_failure = AsyncClient(
        AsyncContext([]),
        query_error=RuntimeError("driver detail"),
        close_error=RuntimeError("close"),
    )
    _install_async_client(monkeypatch, query_failure)
    with pytest.raises(TransportError, match="query failed") as error:
        await anext(clickhouse.stream_query(*_clickhouse_stream()))
    assert "driver detail" not in str(error.value)
    assert query_failure.closed

    close_failure = AsyncClient(AsyncContext([]), close_error=RuntimeError("close detail"))
    _install_async_client(monkeypatch, close_failure)
    with pytest.raises(TransportError, match="close"):
        _ = [batch async for batch in clickhouse.stream_query(*_clickhouse_stream())]


@pytest.mark.asyncio
async def test_clickhouse_stream_preserves_cancellation_and_sync_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = AsyncClient(AsyncContext([], asyncio.CancelledError()))
    _install_async_client(monkeypatch, cancelled)
    with pytest.raises(asyncio.CancelledError):
        await anext(clickhouse.stream_query(*_clickhouse_stream()))
    assert cancelled.closed
    interrupted = AsyncClient(AsyncContext([], KeyboardInterrupt()))
    _install_async_client(monkeypatch, interrupted)
    with pytest.raises(KeyboardInterrupt):
        await anext(clickhouse.stream_query(*_clickhouse_stream()))
    assert interrupted.closed
    closed: list[bool] = []
    await clickhouse._close(SimpleNamespace(close=lambda: closed.append(True)))
    assert closed == [True]


@pytest.mark.asyncio
async def test_clickhouse_stream_slices_oversized_driver_batches_to_declared_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch = pa.record_batch([[1, 2, 3, 4, 5]], schema=pa.schema([("id", pa.int64())]))
    client = AsyncClient(AsyncContext([batch]))
    _install_async_client(monkeypatch, client)
    connection, query, _ = _clickhouse_stream()
    batches = [
        value
        async for value in clickhouse.stream_query(connection, query, ResourceLimits(batch_rows=2))
    ]
    assert [value.num_rows for value in batches] == [2, 2, 1]
    assert pa.Table.from_batches(batches).column("id").to_pylist() == [1, 2, 3, 4, 5]


def test_transport_optional_import_errors_are_targeted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "clickhouse_connect", None)
    monkeypatch.setitem(sys.modules, "adbc_driver_flightsql.dbapi", None)
    with pytest.raises(DependencyError, match="ClickHouse"):
        clickhouse._driver()
    with pytest.raises(DependencyError, match="Flight"):
        flight.flight_driver()


def test_mysql_row_conversion_validates_names_width_values_and_decimal() -> None:
    schema = pa.schema(
        [("amount", pa.decimal128(8, 2)), ("active", pa.bool_()), ("id", pa.int64())]
    )
    batch = mysql.rows_to_batch(
        [("1.25", 1, 7), (Decimal("2.50"), None, 8)],
        ("amount", "active", "id"),
        schema,
    )
    assert batch.to_pydict() == {
        "amount": [Decimal("1.25"), Decimal("2.50")],
        "active": [True, None],
        "id": [7, 8],
    }
    with pytest.raises(SchemaError, match="columns"):
        mysql.rows_to_batch([(1,)], ("wrong",), pa.schema([("id", pa.int64())]))
    with pytest.raises(SchemaError, match="column count"):
        mysql.rows_to_batch([(1, 2)], ("id",), pa.schema([("id", pa.int64())]))
    with pytest.raises(SchemaError, match="BOOLEAN"):
        mysql.rows_to_batch([(2,)], ("active",), pa.schema([("active", pa.bool_())]))
    with pytest.raises(SchemaError, match="planned type"):
        mysql.rows_to_batch([("bad",)], ("id",), pa.schema([("id", pa.int64())]))
    with pytest.raises(SchemaError, match="planned type"):
        mysql.rows_to_batch(
            [("not-a-decimal",)],
            ("amount",),
            pa.schema([("amount", pa.decimal128(8, 2))]),
        )


class ReaderCursor:
    def __init__(self, *, description: object, rows: list[tuple[Any, ...]]) -> None:
        self.description = description
        self.rows = rows
        self.executed_parameters: object = object()
        self.close_calls = 0

    def execute(self, sql: str, parameters: object) -> None:
        self.executed_parameters = parameters

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch

    def close(self) -> None:
        self.close_calls += 1


class ReaderConnection:
    def __init__(self, cursor: ReaderCursor) -> None:
        self._cursor = cursor
        self.close_calls = 0

    def cursor(self) -> ReaderCursor:
        return self._cursor

    def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_mysql_reader_parameters_empty_result_metadata_error_and_idempotent_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = SimpleNamespace(cursors=SimpleNamespace(SSCursor=object()), connect=None)
    monkeypatch.setattr(mysql, "mysql_driver", lambda: driver)
    cursor = ReaderCursor(description=(("id",),), rows=[])
    connection = ReaderConnection(cursor)
    reader = mysql.MySqlBatchReader(
        DorisConnection(host="host", database="db"),
        QuerySpec(
            sql="SELECT id WHERE id = %s",
            positional_parameters=(1,),
            arrow_schema=pa.schema([("id", pa.int64())]),
        ),
        ResourceLimits(),
        connection_factory=lambda **kwargs: connection,
    )
    await reader.start()
    assert cursor.executed_parameters == (1,)
    assert await reader.next_batch() is None
    await reader.close()
    await reader.close()
    assert cursor.close_calls == connection.close_calls == 1

    bad_cursor = ReaderCursor(description=None, rows=[])
    bad_connection = ReaderConnection(bad_cursor)
    bad_reader = mysql.MySqlBatchReader(
        DorisConnection(host="host", database="db"),
        QuerySpec(sql="SELECT id", arrow_schema=pa.schema([("id", pa.int64())])),
        ResourceLimits(),
        connection_factory=lambda **kwargs: bad_connection,
    )
    with pytest.raises(TransportError, match="metadata"):
        await bad_reader.start()
    await bad_reader.close()


class StubBatchReader:
    batches: ClassVar[list[pa.RecordBatch | None]] = []
    start_error: ClassVar[BaseException | None] = None
    close_error: ClassVar[BaseException | None] = None
    close_calls: ClassVar[int] = 0

    def __init__(self, *args: object, **kwargs: object) -> None:
        self._batches = list(type(self).batches)

    async def start(self) -> None:
        error = type(self).start_error
        if error is not None:
            raise error

    async def next_batch(self) -> pa.RecordBatch | None:
        return self._batches.pop(0)

    async def close(self) -> None:
        type(self).close_calls += 1
        error = type(self).close_error
        if error is not None:
            raise error


class CloseableIterator:
    def __init__(self, values: tuple[object, ...]) -> None:
        self._values = iter(values)
        self.closed = False

    def __iter__(self) -> CloseableIterator:
        return self

    def __next__(self) -> object:
        return next(self._values)

    def close(self) -> None:
        self.closed = True


def _reset_stub(
    batches: list[pa.RecordBatch | None],
    *,
    start_error: BaseException | None = None,
    close_error: BaseException | None = None,
) -> None:
    StubBatchReader.batches = batches
    StubBatchReader.start_error = start_error
    StubBatchReader.close_error = close_error
    StubBatchReader.close_calls = 0


@pytest.mark.asyncio
@pytest.mark.parametrize("module", [mysql, flight])
async def test_doris_stream_orchestration_success_errors_cancellation_and_cleanup(
    monkeypatch: pytest.MonkeyPatch, module: Any
) -> None:
    batch = pa.record_batch([[1]], names=["id"])
    monkeypatch.setattr(
        module,
        "MySqlBatchReader" if module is mysql else "FlightBatchReader",
        StubBatchReader,
    )
    args = (
        DorisConnection(host="host", database="db"),
        QuerySpec(sql="SELECT id", arrow_schema=pa.schema([("id", pa.int64())])),
        ResourceLimits(),
    )
    _reset_stub([batch, None])
    assert [value async for value in module.stream_query(*args)] == [batch]
    assert StubBatchReader.close_calls == 1
    _reset_stub([], start_error=RuntimeError("driver detail"), close_error=RuntimeError("close"))
    with pytest.raises(TransportError, match="query failed"):
        await anext(module.stream_query(*args))
    _reset_stub([], start_error=SchemaError("schema"))
    with pytest.raises(SchemaError, match="schema"):
        await anext(module.stream_query(*args))
    _reset_stub([], start_error=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await anext(module.stream_query(*args))
    _reset_stub([], start_error=SystemExit())
    with pytest.raises(SystemExit):
        await anext(module.stream_query(*args))
    _reset_stub([None], close_error=RuntimeError("close"))
    with pytest.raises(TransportError, match="close"):
        _ = [value async for value in module.stream_query(*args)]


@pytest.mark.asyncio
@pytest.mark.parametrize("module", [mysql, flight])
async def test_doris_stream_slices_oversized_driver_batches_by_rows_and_bytes(
    monkeypatch: pytest.MonkeyPatch, module: Any
) -> None:
    monkeypatch.setattr(
        module,
        "MySqlBatchReader" if module is mysql else "FlightBatchReader",
        StubBatchReader,
    )
    batch = pa.record_batch(
        [pa.array(["a" * 8, "b" * 8, "c" * 8, "d" * 8, "e" * 8])],
        names=["payload"],
    )
    _reset_stub([batch, None])
    batches = [
        value
        async for value in module.stream_query(
            DorisConnection(host="host", database="db"),
            QuerySpec(sql="SELECT payload", arrow_schema=batch.schema),
            ResourceLimits(batch_rows=3, batch_bytes=25),
        )
    ]
    assert all(value.num_rows <= 3 for value in batches)
    assert all(value.nbytes <= 25 or value.num_rows == 1 for value in batches)
    assert pa.Table.from_batches(batches).column("payload").to_pylist() == [
        "a" * 8,
        "b" * 8,
        "c" * 8,
        "d" * 8,
        "e" * 8,
    ]


@pytest.mark.asyncio
async def test_flight_reader_empty_nonbatch_and_idempotent_unopened_close() -> None:
    query = QuerySpec(sql="SELECT id", arrow_schema=pa.schema([("id", pa.int64())]))
    empty = flight.FlightBatchReader(
        DorisConnection(host="host", database="db"), query, ResourceLimits()
    )
    empty._reader = CloseableIterator(())
    assert empty._fetch() is None
    await empty.close()
    await empty.close()
    non_batch = flight.FlightBatchReader(
        DorisConnection(host="host", database="db"), query, ResourceLimits()
    )
    non_batch._reader = CloseableIterator(("bad",))
    with pytest.raises(TransportError, match="non-RecordBatch"):
        non_batch._fetch()
    await non_batch.close()


@pytest.mark.asyncio
async def test_serializable_task_read_methods_delegate_and_convert_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arrow_batch = pa.record_batch([[1]], schema=pa.schema([("id", pa.int64())]))

    async def stream(*args: object) -> AsyncIterator[pa.RecordBatch]:
        yield arrow_batch

    monkeypatch.setattr(clickhouse_task_module, "stream_query", stream)
    clickhouse_task = ClickHouseTask(
        ClickHouseConnection(host="host", database="db"),
        QuerySpec(sql="SELECT id", arrow_schema=arrow_batch.schema),
        ResourceLimits(),
    )
    clickhouse_batches = [batch async for batch in clickhouse_task.read()]
    assert clickhouse_batches[0].to_pydict() == {"id": [1]}
    assert "ClickHouseTask" in repr(clickhouse_task)

    monkeypatch.setattr(doris_task_module, "stream_mysql", stream)
    monkeypatch.setattr(doris_task_module, "stream_flight", stream)
    for transport in ("mysql", "flight"):
        doris_task = DorisTask(
            DorisConnection(host="host", database="db"),
            QuerySpec(sql="SELECT id", arrow_schema=arrow_batch.schema),
            ResourceLimits(),
            cast(Any, transport),
        )
        doris_batches = [batch async for batch in doris_task.read()]
        assert doris_batches[0].to_pydict() == {"id": [1]}
        assert transport in repr(doris_task)
