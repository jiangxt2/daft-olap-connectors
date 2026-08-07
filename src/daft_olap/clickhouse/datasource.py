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

"""Daft DataSource implementation for ClickHouse physical tables."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

import pyarrow as pa
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.schema import Schema

from daft_olap._common.contracts import (
    DiscoveryPolicy,
    QuerySpec,
    ResourceLimits,
    SplitMode,
    group_weighted_items,
    validate_query_parameter_values,
)
from daft_olap._common.errors import CompatibilityError, ConfigurationError, DiscoveryError
from daft_olap._common.identifiers import QualifiedTable
from daft_olap._common.redaction import Secret
from daft_olap._compat import (
    compile_filter,
    count_pushdown,
    count_pushdown_available,
    daft_schema,
    required_scan_columns,
    safe_database_limit,
)
from daft_olap.clickhouse.discovery import (
    ClickHouseConnection,
    PartitionDiscovery,
    discover_partitions,
    discover_schema,
)
from daft_olap.clickhouse.schema import ClickHouseSchemaPlan, project_schema
from daft_olap.clickhouse.sql import build_select
from daft_olap.clickhouse.task import ClickHouseTask

PartitionDiscoverer = Callable[[], PartitionDiscovery]
ClickHouseTaskFactory = Callable[[ClickHouseConnection, QuerySpec, ResourceLimits], DataSourceTask]

logger = logging.getLogger(__name__)


class ClickHouseDataSource(DataSource):
    """A read-only ClickHouse physical-table source using async Arrow streaming."""

    def __init__(
        self,
        *,
        host: str,
        database: str,
        table: str,
        port: int = 8123,
        username: str = "default",
        password: Secret = "",
        secure: bool = False,
        split: SplitMode = "auto",
        discovery_policy: DiscoveryPolicy = "single",
        batch_rows: int = 65_536,
        batch_bytes: int = 64 * 1024 * 1024,
        target_tasks: int = 8,
        max_tasks: int = 256,
        connect_timeout_seconds: float = 10.0,
        query_timeout_seconds: float = 300.0,
        unsafe_where_sql: str | None = None,
        query_parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
        client_options: Mapping[str, Any] | None = None,
        _arrow_schema: pa.Schema | None = None,
        _partition_discoverer: PartitionDiscoverer | None = None,
        _task_factory: ClickHouseTaskFactory = ClickHouseTask,
    ) -> None:
        if split not in {"single", "auto"}:
            raise ConfigurationError("ClickHouse split must be 'single' or 'auto'")
        if discovery_policy not in {"single", "error"}:
            raise ConfigurationError("discovery_policy must be 'single' or 'error'")
        if unsafe_where_sql is not None and (
            not isinstance(unsafe_where_sql, str) or not unsafe_where_sql.strip()
        ):
            raise ConfigurationError("unsafe_where_sql must be None or a non-empty SQL fragment")
        parameters = tuple((query_parameters or {}).items())
        if any(not isinstance(key, str) or not key for key, _ in parameters):
            raise ConfigurationError("query parameter names must be non-empty strings")
        validate_query_parameter_values(value for _, value in parameters)
        self._table = QualifiedTable(database, table)
        self._limits = ResourceLimits(
            batch_rows=batch_rows,
            batch_bytes=batch_bytes,
            target_tasks=target_tasks,
            max_tasks=max_tasks,
            connect_timeout_seconds=connect_timeout_seconds,
            query_timeout_seconds=query_timeout_seconds,
        )
        self._connection = ClickHouseConnection.from_options(
            host=host,
            database=database,
            username=username,
            password=password,
            port=port,
            secure=secure,
            settings=dict(settings) if settings is not None else None,
            client_options=dict(client_options) if client_options is not None else None,
        )
        self._split = split
        self._discovery_policy = discovery_policy
        self._unsafe_where_sql = unsafe_where_sql.strip() if unsafe_where_sql is not None else None
        self._query_parameters = parameters
        schema_plan = (
            ClickHouseSchemaPlan.passthrough(_arrow_schema)
            if _arrow_schema is not None
            else discover_schema(self._connection, self._table, self._limits)
        )
        self._arrow_schema = schema_plan.arrow_schema
        self._schema_plan = schema_plan
        self._partition_discoverer = _partition_discoverer
        self._task_factory = _task_factory

    @property
    def name(self) -> str:
        return f"clickhouse:{self._table.database}.{self._table.table}"

    @property
    def schema(self) -> Schema:
        return daft_schema(self._arrow_schema)

    def supports_count_pushdown(self) -> bool:
        return count_pushdown_available()

    async def _partition_groups(self, limit: int | None) -> tuple[tuple[str, ...] | None, ...]:
        if self._split == "single" or limit == 0:
            return (None,)
        if "_partition_id" in self._arrow_schema.names:
            logger.warning(
                "ClickHouse table %r.%r has a physical _partition_id column; "
                "falling back to one task",
                self._table.database,
                self._table.table,
            )
            return (None,)
        try:
            if self._partition_discoverer is not None:
                discovery = await asyncio.to_thread(self._partition_discoverer)
            else:
                discovery = await asyncio.to_thread(
                    discover_partitions,
                    self._connection,
                    self._table,
                    self._limits,
                )
        except DiscoveryError as error:
            if self._discovery_policy == "error":
                raise
            logger.warning(
                "ClickHouse partition discovery failed for %r.%r (%s); falling back to one task",
                self._table.database,
                self._table.table,
                type(error).__name__,
            )
            return (None,)
        if not discovery.supported or not discovery.partitions:
            return (None,)
        groups = group_weighted_items(
            tuple((item.partition_id, item.bytes_on_disk) for item in discovery.partitions),
            target_groups=self._limits.target_tasks,
            max_groups=self._limits.max_tasks,
        )
        return tuple(groups)

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        count = count_pushdown(pushdowns)
        if count is not None:
            if pushdowns.limit is not None:
                raise CompatibilityError("count pushdown with a limit is not supported")
            count_schema = pa.schema([pa.field(count.output_name, pa.uint64(), nullable=False)])
            sql, parameters = build_select(
                table=self._table,
                columns=(count.output_name,),
                predicate=None,
                unsafe_where_sql=self._unsafe_where_sql,
                query_parameters=self._query_parameters,
                partition_ids=None,
                limit=None,
                count_output_name=count.output_name,
            )
            yield self._task_factory(
                self._connection,
                QuerySpec(sql=sql, named_parameters=parameters, arrow_schema=count_schema),
                self._limits,
            )
            return

        columns = required_scan_columns(pushdowns, self._arrow_schema)
        if not columns:
            raise CompatibilityError("an empty non-count ClickHouse projection is not supported")
        task_schema = project_schema(self._arrow_schema, columns)
        predicate = compile_filter(pushdowns.filters)
        database_limit = safe_database_limit(pushdowns, predicate)
        for partition_ids in await self._partition_groups(database_limit):
            sql, parameters = build_select(
                table=self._table,
                columns=columns,
                predicate=predicate,
                unsafe_where_sql=self._unsafe_where_sql,
                query_parameters=self._query_parameters,
                partition_ids=partition_ids,
                limit=database_limit,
                projection=self._schema_plan.projection_for(columns),
            )
            yield self._task_factory(
                self._connection,
                QuerySpec(sql=sql, named_parameters=parameters, arrow_schema=task_schema),
                self._limits,
            )

    def __repr__(self) -> str:
        return (
            "ClickHouseDataSource("
            f"name={self.name!r}, connection={self._connection!r}, split={self._split!r}, "
            f"schema={self._arrow_schema!r})"
        )
