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

"""Immutable cross-database resource and query contracts."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Literal

import pyarrow as pa

from daft_olap._common.errors import ConfigurationError

SplitMode = Literal["single", "auto"]
DiscoveryPolicy = Literal["single", "error"]
_MAX_BATCH_ROWS = 1_000_000
_MAX_BATCH_BYTES = 1024 * 1024 * 1024
_MAX_TASKS = 1_024
_MAX_TIMEOUT_SECONDS = 86_400


def freeze_options(
    options: dict[str, Any] | None,
    *,
    reserved: set[str],
    option_name: str,
) -> tuple[tuple[str, Any], ...]:
    """Copy a mapping into a deterministic, serializable tuple and protect managed keys."""
    if options is None:
        return ()
    frozen: list[tuple[str, Any]] = []
    for key, value in options.items():
        if not isinstance(key, str) or not key:
            raise ConfigurationError(f"{option_name} keys must be non-empty strings")
        if key in reserved:
            raise ConfigurationError(f"{option_name} must not override managed option {key!r}")
        frozen.append((key, value))
    return tuple(sorted(frozen, key=lambda item: item[0]))


@dataclass(frozen=True)
class ResourceLimits:
    """Validated connector bounds applied to every connector invocation."""

    batch_rows: int = 65_536
    batch_bytes: int = 64 * 1024 * 1024
    target_tasks: int = 8
    max_tasks: int = 256
    connect_timeout_seconds: float = 10.0
    query_timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.batch_rows, bool)
            or not isinstance(self.batch_rows, int)
            or not 1 <= self.batch_rows <= _MAX_BATCH_ROWS
        ):
            raise ConfigurationError("batch_rows must be between 1 and 1,000,000")
        if (
            isinstance(self.batch_bytes, bool)
            or not isinstance(self.batch_bytes, int)
            or not 1 <= self.batch_bytes <= _MAX_BATCH_BYTES
        ):
            raise ConfigurationError("batch_bytes must be between 1 and 1,073,741,824")
        if (
            isinstance(self.target_tasks, bool)
            or not isinstance(self.target_tasks, int)
            or not 1 <= self.target_tasks <= _MAX_TASKS
        ):
            raise ConfigurationError("target_tasks must be between 1 and 1,024")
        if (
            isinstance(self.max_tasks, bool)
            or not isinstance(self.max_tasks, int)
            or not 1 <= self.max_tasks <= _MAX_TASKS
        ):
            raise ConfigurationError("max_tasks must be between 1 and 1,024")
        if self.target_tasks > self.max_tasks:
            raise ConfigurationError("target_tasks must not exceed max_tasks")
        for name, value in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("query_timeout_seconds", self.query_timeout_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0 < value <= _MAX_TIMEOUT_SECONDS
            ):
                raise ConfigurationError(f"{name} must be between 0 and 86,400 seconds")


def iter_batch_slices(
    batch: pa.RecordBatch,
    limits: ResourceLimits,
) -> Iterator[pa.RecordBatch]:
    """Slice one decoded batch by row cap and byte target without dropping oversized rows."""
    offset = 0
    while offset < batch.num_rows:
        upper = min(limits.batch_rows, batch.num_rows - offset)
        if batch.slice(offset, upper).nbytes <= limits.batch_bytes:
            size = upper
        else:
            low = 1
            high = upper
            size = 1
            while low <= high:
                candidate_size = (low + high) // 2
                if batch.slice(offset, candidate_size).nbytes <= limits.batch_bytes:
                    size = candidate_size
                    low = candidate_size + 1
                else:
                    high = candidate_size - 1
        yield batch.slice(offset, size)
        offset += size


@dataclass(frozen=True)
class QuerySpec:
    """A picklable SQL statement, bound values, and canonical Arrow result schema."""

    sql: str
    positional_parameters: tuple[Any, ...] = ()
    named_parameters: tuple[tuple[str, Any], ...] = ()
    arrow_schema: pa.Schema = field(default_factory=lambda: pa.schema([]))

    def __post_init__(self) -> None:
        if not self.sql:
            raise ConfigurationError("query SQL must not be empty")
        if self.positional_parameters and self.named_parameters:
            raise ConfigurationError("a query cannot mix positional and named parameters")

    def named_parameter_dict(self) -> dict[str, Any]:
        """Return a fresh parameter mapping for a database driver."""
        return dict(self.named_parameters)


def group_weighted_items(
    items: tuple[tuple[str, int], ...], *, target_groups: int, max_groups: int
) -> tuple[tuple[str, ...], ...]:
    """Greedily balance deterministic weighted items without exceeding a hard group cap."""
    if not items:
        return ()
    group_count = min(len(items), target_groups, max_groups)
    buckets: list[list[str]] = [[] for _ in range(group_count)]
    weights = [0 for _ in range(group_count)]
    for name, weight in sorted(items, key=lambda item: (-item[1], item[0])):
        bucket_index = min(range(group_count), key=lambda index: (weights[index], index))
        buckets[bucket_index].append(name)
        weights[bucket_index] += max(weight, 0)
    return tuple(tuple(sorted(bucket)) for bucket in buckets if bucket)


def group_adjacent_ids(
    values: tuple[int, ...], *, target_groups: int, max_groups: int
) -> tuple[tuple[int, ...], ...]:
    """Group sorted integer identifiers deterministically under the task limit."""
    if not values:
        return ()
    group_count = min(len(values), target_groups, max_groups)
    group_size = (len(values) + group_count - 1) // group_count
    ordered = tuple(sorted(values))
    return tuple(
        ordered[index : index + group_size] for index in range(0, len(ordered), group_size)
    )
