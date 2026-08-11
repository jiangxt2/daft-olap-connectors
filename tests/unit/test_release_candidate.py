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

from scripts.validate_release_candidate import (
    CandidateError,
    ReleaseCandidate,
    main,
    project_version,
    validate_candidate,
    write_github_outputs,
)

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
def release_repository(tmp_path: Path) -> tuple[Path, str]:
    _git(tmp_path, "init", "--initial-branch=master")
    _git(tmp_path, "config", "user.name", "Release Test")
    _git(tmp_path, "config", "user.email", "release@example.com")
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "release-test"\nversion = "{_VERSION}"\n',
        encoding="utf-8",
    )
    _git(tmp_path, "add", "pyproject.toml")
    _git(tmp_path, "commit", "-m", "initial")
    return tmp_path, _git(tmp_path, "rev-parse", "HEAD")


def test_dry_run_candidate_requires_full_master_commit(
    release_repository: tuple[Path, str],
) -> None:
    root, sha = release_repository
    candidate = validate_candidate(
        root=root,
        mode="dry-run",
        candidate_ref=sha,
        expected_version=_VERSION,
        tag="",
        master_ref="master",
    )
    assert candidate == ReleaseCandidate(
        mode="dry-run",
        sha=sha,
        version=_VERSION,
        tag="",
        master_sha=sha,
    )


def test_candidate_version_is_read_from_commit_not_working_tree(
    release_repository: tuple[Path, str],
) -> None:
    root, sha = release_repository
    (root / "pyproject.toml").write_text(
        '[project]\nname = "release-test"\nversion = "9.9.9"\n',
        encoding="utf-8",
    )

    candidate = validate_candidate(
        root=root,
        mode="dry-run",
        candidate_ref=sha,
        expected_version=_VERSION,
        tag="",
        master_ref="master",
    )

    assert candidate.version == _VERSION


@pytest.mark.parametrize("candidate", ["", "abc", "g" * 40, "0" * 39])
def test_dry_run_candidate_rejects_non_full_sha(
    release_repository: tuple[Path, str], candidate: str
) -> None:
    root, _ = release_repository
    with pytest.raises(CandidateError, match="full 40-character"):
        validate_candidate(
            root=root,
            mode="dry-run",
            candidate_ref=candidate,
            expected_version=_VERSION,
            tag="",
            master_ref="master",
        )


def test_dry_run_candidate_rejects_unknown_commit(
    release_repository: tuple[Path, str],
) -> None:
    root, _ = release_repository
    with pytest.raises(CandidateError, match="Git validation"):
        validate_candidate(
            root=root,
            mode="dry-run",
            candidate_ref="0" * 40,
            expected_version=_VERSION,
            tag="",
            master_ref="master",
        )


def test_candidate_rejects_commit_not_reachable_from_master(
    release_repository: tuple[Path, str],
) -> None:
    root, master_sha = release_repository
    _git(root, "checkout", "--orphan", "other")
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "release-test"\nversion = "{_VERSION}"\n',
        encoding="utf-8",
    )
    _git(root, "add", "pyproject.toml")
    _git(root, "commit", "-m", "other")
    other_sha = _git(root, "rev-parse", "HEAD")
    assert other_sha != master_sha
    with pytest.raises(CandidateError, match="not reachable"):
        validate_candidate(
            root=root,
            mode="dry-run",
            candidate_ref=other_sha,
            expected_version=_VERSION,
            tag="",
            master_ref="master",
        )


@pytest.mark.parametrize("annotated", [False, True])
def test_tag_candidate_resolves_lightweight_and_annotated_tags(
    release_repository: tuple[Path, str], annotated: bool
) -> None:
    root, sha = release_repository
    arguments = (
        ("tag", "-a", f"v{_VERSION}", "-m", "release")
        if annotated
        else (
            "tag",
            f"v{_VERSION}",
        )
    )
    _git(root, *arguments)
    event_sha = _git(root, "rev-parse", f"v{_VERSION}")
    candidate = validate_candidate(
        root=root,
        mode="tag",
        candidate_ref=f"refs/tags/v{_VERSION}",
        expected_version=_VERSION,
        tag=f"v{_VERSION}",
        master_ref="master",
        event_sha=event_sha,
        event_created=True,
    )
    assert candidate.sha == sha
    assert candidate.version == _VERSION


@pytest.mark.parametrize(
    ("candidate_ref", "tag", "message"),
    [
        ("refs/tags/v0.1.0a1", "not-a-tag", "must start with v"),
        ("refs/tags/v0.1.0a1", "v0.2.0", "does not match"),
    ],
)
def test_tag_candidate_rejects_invalid_tag_identity(
    release_repository: tuple[Path, str], candidate_ref: str, tag: str, message: str
) -> None:
    root, _ = release_repository
    with pytest.raises(CandidateError, match=message):
        validate_candidate(
            root=root,
            mode="tag",
            candidate_ref=candidate_ref,
            expected_version="",
            tag=tag,
            master_ref="master",
        )


@pytest.mark.parametrize("mode", ["dry-run", "tag"])
def test_candidate_rejects_version_mismatch(
    release_repository: tuple[Path, str], mode: str
) -> None:
    root, sha = release_repository
    if mode == "tag":
        _git(root, "tag", "v0.2.0")
        candidate_ref = "refs/tags/v0.2.0"
        tag = "v0.2.0"
        event_sha = _git(root, "rev-parse", tag)
    else:
        candidate_ref = sha
        tag = ""
        event_sha = ""
    with pytest.raises(CandidateError, match="version"):
        validate_candidate(
            root=root,
            mode=mode,
            candidate_ref=candidate_ref,
            expected_version="0.2.0",
            tag=tag,
            master_ref="master",
            event_sha=event_sha,
            event_created=mode == "tag",
        )


@pytest.mark.parametrize(
    ("created", "deleted", "forced"),
    [(False, False, False), (True, True, False), (True, False, True)],
)
def test_tag_candidate_rejects_non_creation_or_destructive_push(
    release_repository: tuple[Path, str], created: bool, deleted: bool, forced: bool
) -> None:
    root, sha = release_repository
    tag = f"v{_VERSION}"
    _git(root, "tag", tag)

    with pytest.raises(CandidateError, match="new, non-forced"):
        validate_candidate(
            root=root,
            mode="tag",
            candidate_ref=f"refs/tags/{tag}",
            expected_version="",
            tag=tag,
            master_ref="master",
            event_sha=sha,
            event_created=created,
            event_deleted=deleted,
            event_forced=forced,
        )


def test_tag_candidate_rejects_live_tag_moved_after_event(
    release_repository: tuple[Path, str],
) -> None:
    root, event_sha = release_repository
    tag = f"v{_VERSION}"
    _git(root, "tag", tag)
    (root / "later").write_text("later\n", encoding="utf-8")
    _git(root, "add", "later")
    _git(root, "commit", "-m", "later")
    _git(root, "tag", "--force", tag)

    with pytest.raises(CandidateError, match="triggering push"):
        validate_candidate(
            root=root,
            mode="tag",
            candidate_ref=f"refs/tags/{tag}",
            expected_version="",
            tag=tag,
            master_ref="master",
            event_sha=event_sha,
            event_created=True,
        )


def test_dry_run_candidate_rejects_tag_event_metadata(
    release_repository: tuple[Path, str],
) -> None:
    root, sha = release_repository
    with pytest.raises(CandidateError, match="must not receive tag push"):
        validate_candidate(
            root=root,
            mode="dry-run",
            candidate_ref=sha,
            expected_version=_VERSION,
            tag="",
            master_ref="master",
            event_sha=sha,
        )


def test_project_version_rejects_missing_non_string_or_multiline_metadata(tmp_path: Path) -> None:
    with pytest.raises(CandidateError, match="unavailable"):
        project_version(tmp_path)
    (tmp_path / "pyproject.toml").write_text("[project]\nversion = 1\n", encoding="utf-8")
    with pytest.raises(CandidateError, match="non-empty single-line string"):
        project_version(tmp_path)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nversion = "0.1.0\\nmode=tag"\n', encoding="utf-8"
    )
    with pytest.raises(CandidateError, match="non-empty single-line string"):
        project_version(tmp_path)


def test_github_outputs_are_complete_and_append_only(tmp_path: Path) -> None:
    output = tmp_path / "github-output"
    output.write_text("existing=value\n", encoding="utf-8")
    write_github_outputs(
        output,
        ReleaseCandidate(
            mode="dry-run",
            sha="a" * 40,
            version=_VERSION,
            tag="",
            master_sha="b" * 40,
        ),
    )
    assert output.read_text(encoding="utf-8").splitlines() == [
        "existing=value",
        "mode=dry-run",
        f"sha={'a' * 40}",
        f"version={_VERSION}",
        "tag=",
        f"master_sha={'b' * 40}",
    ]


def test_tag_cli_matches_release_workflow_invocation(
    release_repository: tuple[Path, str], tmp_path: Path
) -> None:
    root, sha = release_repository
    tag = f"v{_VERSION}"
    _git(root, "tag", tag)
    output = tmp_path / "github-output"

    assert (
        main(
            [
                "--root",
                str(root),
                "--mode",
                "tag",
                "--candidate-ref",
                f"refs/tags/{tag}",
                "--expected-version",
                "",
                "--tag",
                tag,
                "--master-ref",
                "master",
                "--event-sha",
                _git(root, "rev-parse", tag),
                "--event-created",
                "true",
                "--event-deleted",
                "false",
                "--event-forced",
                "false",
                "--github-output",
                str(output),
            ]
        )
        == 0
    )
    assert output.read_text(encoding="utf-8").splitlines() == [
        "mode=tag",
        f"sha={sha}",
        f"version={_VERSION}",
        f"tag={tag}",
        f"master_sha={sha}",
    ]
