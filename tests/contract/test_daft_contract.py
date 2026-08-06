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

import os
import subprocess
import sys
import textwrap
from collections.abc import Callable
from typing import Any, cast

import daft
import pytest
from daft.dataframe import DataFrame
from daft.expressions import Expression

from daft_olap.clickhouse.datasource import ClickHouseDataSource
from daft_olap.doris.datasource import DorisDataSource
from tests.contract.fakes import (
    ARROW_SCHEMA,
    clickhouse_task_factory,
    doris_task_factory,
)

SourceFactory = Callable[[], ClickHouseDataSource | DorisDataSource]


def clickhouse_source() -> ClickHouseDataSource:
    """Build a real connector planner backed by a deterministic task transport."""
    return ClickHouseDataSource(
        host="fake-clickhouse",
        database="analytics",
        table="events",
        split="single",
        _arrow_schema=ARROW_SCHEMA,
        _task_factory=clickhouse_task_factory,
    )


def doris_source() -> DorisDataSource:
    """Build a real connector planner backed by a deterministic task transport."""
    return DorisDataSource(
        host="fake-doris",
        database="analytics",
        table="events",
        transport="mysql",
        split="single",
        _arrow_schema=ARROW_SCHEMA,
        _task_factory=doris_task_factory,
    )


@pytest.fixture(params=[clickhouse_source, doris_source], ids=["clickhouse", "doris"])
def source_factory(request: pytest.FixtureRequest) -> SourceFactory:
    return cast(SourceFactory, request.param)


def _greater_than_or_equal(column: str, value: object) -> Expression:
    return cast(Expression, cast(Any, daft.col(column)) >= value)


def _query(source_factory: SourceFactory) -> DataFrame:
    return (
        source_factory().read().filter(_greater_than_or_equal("score", 15)).select("kind").limit(2)
    )


def test_native_projection_filter_limit_multibatch_and_repeat_collect(
    source_factory: SourceFactory,
) -> None:
    dataframe = _query(source_factory)
    assert dataframe.to_pydict() == {"kind": ["b", "a"]}
    assert dataframe.to_pydict() == {"kind": ["b", "a"]}


def test_native_count_semantics_and_join_schema(source_factory: SourceFactory) -> None:
    assert source_factory().read().count().to_pydict() == {"count": [4]}
    dimensions = daft.from_pydict({"kind": ["a", "b"], "label": ["A", "B"]})
    result = (
        source_factory()
        .read()
        .join(dimensions, on="kind", how="inner")
        .select("id", "label")
        .sort("id")
        .to_pydict()
    )
    assert result == {"id": [1, 2, 3], "label": ["A", "B", "A"]}


@pytest.mark.ray
@pytest.mark.parametrize("factory_name", ["clickhouse_source", "doris_source"])
def test_ray_data_and_local_daft_ray_runner_coexist(factory_name: str) -> None:
    script = textwrap.dedent(
        f"""
        import daft
        import ray
        import ray.data
        from tests.contract.test_daft_contract import {factory_name}

        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
        try:
            ray_rows = ray.data.from_items([{{"engine": "ray-data"}}]).take_all()
            assert ray_rows == [{{"engine": "ray-data"}}], ray_rows
            daft.set_runner_ray(noop_if_initialized=True)
            source = {factory_name}()
            result = (
                source.read()
                .filter(daft.col("score") >= 15)
                .select("kind")
                .limit(2)
                .to_pydict()
            )
            assert result == {{"kind": ["b", "a"]}}, result
            assert source.read().count().to_pydict() == {{"count": [4]}}
        finally:
            ray.shutdown()
        """
    )
    environment = os.environ.copy()
    # Local workers reuse this test environment. Ray's uv hook would rebuild the
    # project environment per session and can exceed its worker registration timeout.
    environment["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "0"
    root = os.getcwd()
    environment["PYTHONPATH"] = os.pathsep.join(
        value for value in (os.path.join(root, "src"), root, environment.get("PYTHONPATH")) if value
    )
    process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert process.returncode == 0, process.stdout + process.stderr
