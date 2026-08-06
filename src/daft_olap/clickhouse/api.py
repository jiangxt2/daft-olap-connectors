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

"""Public ClickHouse convenience API."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from daft.dataframe import DataFrame
from daft.expressions import Expression

from daft_olap._common.contracts import DiscoveryPolicy, SplitMode
from daft_olap._common.identifiers import normalize_columns
from daft_olap._common.redaction import Secret
from daft_olap.clickhouse.datasource import ClickHouseDataSource


def read_clickhouse(
    *,
    host: str,
    database: str,
    table: str,
    port: int = 8123,
    username: str = "default",
    password: Secret = "",
    secure: bool = False,
    columns: Iterable[str] | None = None,
    filter: Expression | None = None,
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
) -> DataFrame:
    """Read one ClickHouse physical table as a lazy Daft DataFrame.

    ``filter`` is a Daft expression and remains in Daft's plan even when safely pushed to the
    database. ``unsafe_where_sql`` is trusted SQL, is not sanitized, and must bind dynamic values
    through ``query_parameters``.
    """
    projection = normalize_columns(columns)
    source = ClickHouseDataSource(
        host=host,
        database=database,
        table=table,
        port=port,
        username=username,
        password=password,
        secure=secure,
        split=split,
        discovery_policy=discovery_policy,
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
        target_tasks=target_tasks,
        max_tasks=max_tasks,
        connect_timeout_seconds=connect_timeout_seconds,
        query_timeout_seconds=query_timeout_seconds,
        unsafe_where_sql=unsafe_where_sql,
        query_parameters=query_parameters,
        settings=settings,
        client_options=client_options,
    )
    dataframe = source.read()
    if filter is not None:
        dataframe = dataframe.filter(filter)
    if projection is not None:
        dataframe = dataframe.select(*projection)
    return dataframe
