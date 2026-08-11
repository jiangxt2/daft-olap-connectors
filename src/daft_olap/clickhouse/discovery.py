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

"""Driver-side ClickHouse schema and MergeTree partition discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from daft_olap._common.contracts import ResourceLimits, freeze_options, thaw_options
from daft_olap._common.errors import (
    CompatibilityError,
    ConfigurationError,
    DaftOlapError,
    DatabaseObjectNotFoundError,
    DependencyError,
    DiscoveryError,
    SchemaError,
)
from daft_olap._common.identifiers import QualifiedTable
from daft_olap._common.redaction import (
    Secret,
    option_keys,
    resolve_secret,
    validate_secret,
)
from daft_olap._compat import validate_daft_arrow_schema
from daft_olap.clickhouse.errors import translate_clickhouse_error
from daft_olap.clickhouse.schema import (
    ClickHouseSchemaPlan,
    canonical_schema,
    parse_describe_columns,
    render_schema_projection,
)

_RESERVED_CLIENT_OPTIONS = {
    "access_token",
    "connect_timeout",
    "database",
    "dsn",
    "host",
    "interface",
    "password",
    "port",
    "secure",
    "send_receive_timeout",
    "settings",
    "username",
}
_RESERVED_SETTINGS = {
    "max_block_size",
    "max_execution_time",
}
_MAX_PORT = 65_535
_PARTITION_METADATA_COLUMNS = 2
_LOCAL_MERGETREE_ENGINES = frozenset(
    {
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "GraphiteMergeTree",
        "MergeTree",
        "ReplacingMergeTree",
        "SummingMergeTree",
        "VersionedCollapsingMergeTree",
    }
)


@dataclass(frozen=True)
class ClickHouseConnection:
    """Serializable ClickHouse connection settings with a redacted representation."""

    host: str
    database: str
    username: str = "default"
    password: Secret = field(default="", repr=False, compare=False)
    port: int = 8123
    secure: bool = False
    settings: tuple[tuple[str, Any], ...] = field(default_factory=tuple, repr=False)
    client_options: tuple[tuple[str, Any], ...] = field(default_factory=tuple, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host:
            raise ConfigurationError("ClickHouse host must be a non-empty string")
        if not isinstance(self.database, str) or not self.database:
            raise ConfigurationError("ClickHouse database must be a non-empty string")
        if not isinstance(self.username, str) or not self.username:
            raise ConfigurationError("ClickHouse username must be a non-empty string")
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= _MAX_PORT
        ):
            raise ConfigurationError("ClickHouse port must be between 1 and 65535")
        if not isinstance(self.secure, bool):
            raise ConfigurationError("ClickHouse secure must be a boolean")
        validate_secret(self.password)

    @classmethod
    def from_options(
        cls,
        *,
        host: str,
        database: str,
        username: str,
        password: Secret,
        port: int,
        secure: bool,
        settings: Mapping[str, Any] | None,
        client_options: Mapping[str, Any] | None,
    ) -> ClickHouseConnection:
        """Freeze caller-owned settings and protect connector-managed options."""
        return cls(
            host=host,
            database=database,
            username=username,
            password=password,
            port=port,
            secure=secure,
            settings=freeze_options(
                settings,
                reserved=_RESERVED_SETTINGS,
                option_name="settings",
            ),
            client_options=freeze_options(
                client_options,
                reserved=_RESERVED_CLIENT_OPTIONS,
                option_name="client_options",
            ),
        )

    def client_kwargs(self, limits: ResourceLimits) -> dict[str, Any]:
        """Build fresh clickhouse-connect arguments in the current process."""
        kwargs = {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": resolve_secret(self.password),
            "database": self.database,
            "secure": self.secure,
            "connect_timeout": limits.connect_timeout_seconds,
            "send_receive_timeout": limits.query_timeout_seconds,
        }
        kwargs.update(thaw_options(self.client_options, option_name="client_options"))
        return kwargs

    def settings_kwargs(self) -> dict[str, Any]:
        """Build fresh clickhouse-connect settings for one driver invocation."""
        return thaw_options(self.settings, option_name="settings")

    def __repr__(self) -> str:
        return (
            "ClickHouseConnection("
            f"host={self.host!r}, database={self.database!r}, username={self.username!r}, "
            "password=<redacted>, "
            f"port={self.port}, secure={self.secure}, "
            f"settings={option_keys(self.settings)!r}, "
            f"client_options={option_keys(self.client_options)!r})"
        )


@dataclass(frozen=True)
class PartitionMetadata:
    """One ClickHouse partition identifier and its active on-disk weight."""

    partition_id: str
    bytes_on_disk: int


@dataclass(frozen=True)
class PartitionDiscovery:
    """Whether partition pruning is valid and the discovered physical units."""

    supported: bool
    partitions: tuple[PartitionMetadata, ...] = ()


def _table_engine(rows: list[tuple[Any, ...]]) -> str:
    if not rows:
        raise DatabaseObjectNotFoundError(
            "ClickHouse table was not found during partition discovery"
        )
    if len(rows) != 1:
        raise DiscoveryError("ClickHouse system.tables returned duplicate table metadata")
    row = rows[0]
    if len(row) != 1 or not isinstance(row[0], str):
        raise DiscoveryError("ClickHouse system.tables returned malformed metadata")
    return row[0]


def _has_physical_partition_column(rows: list[tuple[Any, ...]]) -> bool:
    if len(rows) > 1:
        raise DiscoveryError("ClickHouse system.columns returned duplicate column metadata")
    if not rows:
        return False
    row = rows[0]
    if len(row) != 1 or row[0] != "_partition_id":
        raise DiscoveryError("ClickHouse system.columns returned malformed metadata")
    return True


def _driver() -> Any:
    try:
        import clickhouse_connect
    except ImportError:
        raise DependencyError(
            'ClickHouse support is not installed; install "daft-olap-connectors[clickhouse]"'
        ) from None
    return clickhouse_connect


def _close_client(client: Any | None) -> bool:
    """Close a synchronous client and report ordinary cleanup failures."""
    if client is None:
        return False
    try:
        client.close()
    except Exception:
        return True
    return False


def discover_schema(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> ClickHouseSchemaPlan:
    """Combine DESCRIBE declarations with a zero-row Arrow transport schema."""
    client = None
    failure: BaseException | None = None
    try:
        client = _driver().get_client(**connection.client_kwargs(limits))
        describe = client.query(f"DESCRIBE TABLE {table.sql()}")
        columns = parse_describe_columns(describe.result_rows)
        projection = render_schema_projection(columns)
        arrow_table = client.query_arrow(
            f"SELECT {projection} FROM {table.sql()} LIMIT 0",
            settings=connection.settings_kwargs(),
            use_strings=True,
        )
        if not isinstance(arrow_table, pa.Table):
            raise SchemaError("clickhouse-connect returned a non-Arrow schema probe")
        arrow_schema = canonical_schema(describe.result_rows, arrow_table.schema)
        validate_daft_arrow_schema(arrow_schema)
        return ClickHouseSchemaPlan(arrow_schema, columns)
    except (CompatibilityError, DependencyError, SchemaError) as exc:
        failure = exc
        raise
    except Exception as exc:
        translated = translate_clickhouse_error(exc, operation="schema discovery")
        failure = translated or exc
        if translated is not None:
            raise translated from None
        raise SchemaError(
            f"failed to discover ClickHouse schema for {table.database!r}.{table.table!r}"
        ) from None
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if _close_client(client) and failure is None:
            raise SchemaError("failed to close the ClickHouse schema discovery client") from None


def discover_partitions(
    connection: ClickHouseConnection, table: QualifiedTable, limits: ResourceLimits
) -> PartitionDiscovery:
    """Return active MergeTree partition IDs, or mark the table unsafe for parallel splitting."""
    client = None
    failure: BaseException | None = None
    try:
        client = _driver().get_client(**connection.client_kwargs(limits))
        engine_result = client.query(
            "SELECT engine FROM system.tables WHERE database = %(database)s AND name = %(table)s",
            parameters={"database": table.database, "table": table.table},
        )
        engine = _table_engine(engine_result.result_rows)
        if engine not in _LOCAL_MERGETREE_ENGINES:
            return PartitionDiscovery(False)
        physical_partition_column = client.query(
            "SELECT name FROM system.columns "
            "WHERE database = %(database)s AND table = %(table)s "
            "AND name = '_partition_id'",
            parameters={"database": table.database, "table": table.table},
        )
        if _has_physical_partition_column(physical_partition_column.result_rows):
            return PartitionDiscovery(False)
        result = client.query(
            "SELECT partition_id, sum(bytes_on_disk) AS bytes_on_disk "
            "FROM system.parts "
            "WHERE active AND database = %(database)s AND table = %(table)s "
            "GROUP BY partition_id ORDER BY partition_id",
            parameters={"database": table.database, "table": table.table},
        )
        partitions: list[PartitionMetadata] = []
        for row in result.result_rows:
            if (
                len(row) != _PARTITION_METADATA_COLUMNS
                or not isinstance(row[0], str)
                or not row[0]
                or isinstance(row[1], bool)
                or not isinstance(row[1], int)
                or row[1] < 0
            ):
                raise DiscoveryError("ClickHouse system.parts returned malformed metadata")
            partitions.append(PartitionMetadata(row[0], row[1]))
        if len({partition.partition_id for partition in partitions}) != len(partitions):
            raise DiscoveryError("ClickHouse system.parts returned duplicate partition IDs")
        return PartitionDiscovery(True, tuple(partitions))
    except DaftOlapError as exc:
        failure = exc
        raise
    except Exception as exc:
        translated = translate_clickhouse_error(exc, operation="partition discovery")
        failure = translated or exc
        if translated is not None:
            raise translated from None
        raise DiscoveryError(
            f"failed to discover ClickHouse partitions for {table.database!r}.{table.table!r}"
        ) from None
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if _close_client(client) and failure is None:
            raise DiscoveryError(
                "failed to close the ClickHouse partition discovery client"
            ) from None
