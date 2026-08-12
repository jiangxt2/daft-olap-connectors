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

import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.verify_release_tag import TagError, remote_tag_commit, verify_remote_tag

_VERSION = "0.1.0a1"


def _git(root: Path, *arguments: str) -> str:
    git = shutil.which("git")
    assert git is not None
    return subprocess.run(
        [git, *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def remote_repository(tmp_path: Path) -> tuple[Path, str, str]:
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    remote.mkdir()
    source.mkdir()
    _git(remote, "init", "--bare")
    _git(source, "init", "--initial-branch=master")
    _git(source, "config", "user.name", "Release Test")
    _git(source, "config", "user.email", "release@example.com")
    (source / "payload").write_text("initial\n", encoding="utf-8")
    _git(source, "add", "payload")
    _git(source, "commit", "-m", "initial")
    _git(source, "remote", "add", "origin", str(remote))
    return source, str(remote), _git(source, "rev-parse", "HEAD")


@pytest.mark.parametrize("annotated", [False, True])
def test_remote_tag_verifies_lightweight_and_annotated_tags(
    remote_repository: tuple[Path, str, str], annotated: bool
) -> None:
    source, remote, sha = remote_repository
    tag = f"v{_VERSION}"
    arguments = ("tag", "-a", tag, "-m", "release") if annotated else ("tag", tag)
    _git(source, *arguments)
    _git(source, "push", "origin", tag)

    assert remote_tag_commit(source, remote=remote, tag=tag) == sha
    verify_remote_tag(source, remote=remote, tag=tag, expected_sha=sha)


def test_remote_tag_rejects_moved_candidate(
    remote_repository: tuple[Path, str, str],
) -> None:
    source, remote, candidate_sha = remote_repository
    tag = f"v{_VERSION}"
    _git(source, "tag", tag)
    _git(source, "push", "origin", tag)
    (source / "payload").write_text("moved\n", encoding="utf-8")
    _git(source, "commit", "-am", "moved")
    _git(source, "tag", "--force", tag)
    _git(source, "push", "--force", "origin", tag)

    with pytest.raises(TagError, match="no longer matches"):
        verify_remote_tag(source, remote=remote, tag=tag, expected_sha=candidate_sha)


def test_remote_tag_rejects_missing_tag(remote_repository: tuple[Path, str, str]) -> None:
    source, remote, sha = remote_repository
    with pytest.raises(TagError, match="unavailable"):
        verify_remote_tag(source, remote=remote, tag=f"v{_VERSION}", expected_sha=sha)


def test_remote_tag_rejects_lookup_failure(remote_repository: tuple[Path, str, str]) -> None:
    source, _, sha = remote_repository
    with pytest.raises(TagError, match="lookup failed"):
        verify_remote_tag(
            source,
            remote=str(source / "missing-remote.git"),
            tag=f"v{_VERSION}",
            expected_sha=sha,
        )


@pytest.mark.parametrize(
    ("tag", "sha", "message"),
    [("not-a-tag", "a" * 40, "must start with v"), (f"v{_VERSION}", "abc", "full commit")],
)
def test_remote_tag_rejects_invalid_identity(
    remote_repository: tuple[Path, str, str], tag: str, sha: str, message: str
) -> None:
    source, remote, _ = remote_repository
    with pytest.raises(TagError, match=message):
        verify_remote_tag(source, remote=remote, tag=tag, expected_sha=sha)
