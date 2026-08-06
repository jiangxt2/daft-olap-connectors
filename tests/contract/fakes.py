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

from collections.abc import AsyncIterator

import pyarrow as pa
from daft.io.source import DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_olap._common.contracts import QuerySpec, ResourceLimits
from daft_olap._compat import daft_record_batch, daft_schema
from daft_olap.clickhouse.discovery import ClickHouseConnection
from daft_olap.doris.discovery import DorisConnection
from daft_olap.doris.task import DorisTransport

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("kind", pa.string()),
        pa.field("score", pa.int32()),
    ]
)
_DATA = {
    "id": [1, 2, 3, 4],
    "kind": ["a", "b", "a", None],
    "score": [5, 15, 25, 35],
}


class InMemoryQueryTask(DataSourceTask):
    """A serializable deterministic task used to test Daft's source bridge."""

    def __init__(self, query: QuerySpec) -> None:
        self._query = query

    @property
    def schema(self) -> Schema:
        return daft_schema(self._query.arrow_schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        if "count(" in self._query.sql.lower():
            field = self._query.arrow_schema.field(0)
            batch = pa.record_batch(
                [pa.array([4], type=field.type)], schema=self._query.arrow_schema
            )
            yield daft_record_batch(batch)
            return
        arrays = [
            pa.array(_DATA[field.name], type=field.type) for field in self._query.arrow_schema
        ]
        full = pa.RecordBatch.from_arrays(arrays, schema=self._query.arrow_schema)
        for batch in pa.Table.from_batches([full]).to_batches(max_chunksize=2):
            yield daft_record_batch(batch)


def clickhouse_task_factory(
    connection: ClickHouseConnection,
    query: QuerySpec,
    limits: ResourceLimits,
) -> DataSourceTask:
    """Build a fake task with the production ClickHouse factory signature."""
    return InMemoryQueryTask(query)


def doris_task_factory(
    connection: DorisConnection,
    query: QuerySpec,
    limits: ResourceLimits,
    transport: DorisTransport,
) -> DataSourceTask:
    """Build a fake task with the production Doris factory signature."""
    return InMemoryQueryTask(query)
