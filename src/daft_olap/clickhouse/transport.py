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

"""Worker-side clickhouse-connect async Arrow streaming."""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa

from daft_olap._common.contracts import QuerySpec, ResourceLimits, iter_batch_slices
from daft_olap._common.errors import DaftOlapError, DependencyError, SchemaError, TransportError
from daft_olap.clickhouse.discovery import ClickHouseConnection
from daft_olap.clickhouse.errors import translate_clickhouse_error


def _driver() -> Any:
    try:
        import clickhouse_connect
    except ImportError:
        raise DependencyError(
            'ClickHouse support is not installed; install "daft-olap-connectors[clickhouse]"'
        ) from None
    return clickhouse_connect


def cast_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Validate result names and cast without lossy conversion to the canonical schema."""
    if tuple(batch.schema.names) != tuple(schema.names):
        raise SchemaError("ClickHouse result columns do not match the planned schema")
    try:
        if batch.schema.equals(schema, check_metadata=False):
            return pa.RecordBatch.from_arrays(batch.columns, schema=schema)
        table = pa.Table.from_batches([batch]).cast(schema, safe=True).combine_chunks()
        if table.num_rows == 0:
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in schema], schema=schema
            )
        batches = table.to_batches(max_chunksize=max(table.num_rows, 1))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, ValueError):
        raise SchemaError("ClickHouse result does not match the planned Arrow schema") from None
    if len(batches) != 1:
        raise SchemaError("ClickHouse batch cast produced an unexpected batch count")
    return batches[0]


async def _close(client: Any) -> None:
    close_result = client.close()
    if inspect.isawaitable(close_result):
        await close_result


async def stream_query(
    connection: ClickHouseConnection,
    query: QuerySpec,
    limits: ResourceLimits,
) -> AsyncIterator[pa.RecordBatch]:
    """Yield driver batches and close both stream and client on every exit path."""
    client = None
    failure: BaseException | None = None
    try:
        client = await _driver().get_async_client(**connection.client_kwargs(limits))
        settings = dict(connection.settings)
        settings["max_block_size"] = limits.batch_rows
        settings["max_execution_time"] = max(1, math.ceil(limits.query_timeout_seconds))
        context = await client.query_arrow_stream(
            query.sql,
            parameters=query.named_parameter_dict(),
            settings=settings,
            use_strings=True,
        )
        async with context as batches:
            async for batch in batches:
                if not isinstance(batch, pa.RecordBatch):
                    raise TransportError("clickhouse-connect yielded a non-RecordBatch value")
                canonical = cast_batch(batch, query.arrow_schema)
                for bounded in iter_batch_slices(canonical, limits):
                    yield bounded
    except (asyncio.CancelledError, GeneratorExit, KeyboardInterrupt, SystemExit) as exc:
        failure = exc
        raise
    except DaftOlapError as exc:
        failure = exc
        raise
    except Exception as exc:
        failure = exc
        translated = translate_clickhouse_error(exc, operation="query execution")
        if translated is not None:
            raise translated from None
        raise TransportError("ClickHouse async Arrow query failed") from None
    finally:
        if client is not None:
            try:
                await _close(client)
            except Exception:
                if failure is None:
                    raise TransportError("failed to close the ClickHouse client") from None
