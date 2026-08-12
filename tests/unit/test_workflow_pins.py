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

from pathlib import Path

import pytest

from scripts.check_workflow_pins import pin_failures


def _workflow(tmp_path: Path, uses: str) -> Path:
    path = tmp_path / "workflow.yml"
    path.write_text(f"jobs:\n  test:\n    uses: {uses}\n", encoding="utf-8")
    return path


def test_full_sha_with_exact_version_comment_is_accepted(tmp_path: Path) -> None:
    path = _workflow(tmp_path, f"actions/checkout@{'a' * 40} # v4.4.0")
    assert pin_failures(path) == ()


def test_local_reusable_workflow_is_accepted(tmp_path: Path) -> None:
    path = _workflow(tmp_path, "./.github/workflows/ci.yml")
    assert pin_failures(path) == ()


@pytest.mark.parametrize(
    "reference",
    [
        "actions/checkout@v4 # v4.4.0",
        "actions/checkout@main # v4.4.0",
        "actions/checkout@abc123 # v4.4.0",
        "docker://alpine:latest",
    ],
)
def test_mutable_or_non_sha_references_are_rejected(tmp_path: Path, reference: str) -> None:
    failures = pin_failures(_workflow(tmp_path, reference))
    assert len(failures) == 1
    assert "not pinned" in failures[0]


@pytest.mark.parametrize("comment", ["", " # v4", " # release/v1", " # latest"])
def test_missing_or_inexact_version_comment_is_rejected(tmp_path: Path, comment: str) -> None:
    failures = pin_failures(_workflow(tmp_path, f"actions/checkout@{'a' * 40}{comment}"))
    assert len(failures) == 1
    assert "version comment" in failures[0]
