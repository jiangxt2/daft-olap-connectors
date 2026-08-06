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

"""Serializable ClickHouse DataSource task."""

from __future__ import annotations

from collections.abc import AsyncIterator

from daft.io.source import DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_olap._common.contracts import QuerySpec, ResourceLimits
from daft_olap._compat import daft_record_batch, daft_schema
from daft_olap.clickhouse.discovery import ClickHouseConnection
from daft_olap.clickhouse.transport import stream_query


class ClickHouseTask(DataSourceTask):
    """Read one immutable SQL task through clickhouse-connect."""

    def __init__(
        self,
        connection: ClickHouseConnection,
        query: QuerySpec,
        limits: ResourceLimits,
    ) -> None:
        self._connection = connection
        self._query = query
        self._limits = limits

    @property
    def schema(self) -> Schema:
        return daft_schema(self._query.arrow_schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        async for batch in stream_query(self._connection, self._query, self._limits):
            yield daft_record_batch(batch)

    def __repr__(self) -> str:
        return (
            f"ClickHouseTask(connection={self._connection!r}, schema={self._query.arrow_schema!r})"
        )
