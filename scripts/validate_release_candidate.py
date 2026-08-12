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

"""Resolve and validate an immutable release candidate from repository state."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

_FULL_SHA = re.compile(r"[0-9a-f]{40}")
_TAG = re.compile(r"v[^\s]+")


class CandidateError(ValueError):
    """A release candidate failed a local, deterministic validation."""


@dataclass(frozen=True)
class ReleaseCandidate:
    """Validated values shared by release workflow jobs."""

    mode: str
    sha: str
    version: str
    tag: str
    master_sha: str


class GitRepository:
    """Run fixed Git operations against one repository root."""

    def __init__(self, root: Path) -> None:
        git = shutil.which("git")
        if git is None:
            raise CandidateError("git is required to validate a release candidate")
        self._git = git
        self._root = root

    def output(self, *arguments: str) -> str:
        """Return stdout for a successful Git command."""
        # The executable is resolved locally and arguments are passed without a shell.
        result = subprocess.run(  # noqa: S603
            [self._git, *arguments],
            cwd=self._root,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise CandidateError("release candidate Git validation failed")
        return result.stdout.strip()

    def is_ancestor(self, candidate: str, master_ref: str) -> bool:
        """Return whether candidate is reachable from the configured master ref."""
        # The executable is resolved locally and arguments are passed without a shell.
        result = subprocess.run(  # noqa: S603
            [self._git, "merge-base", "--is-ancestor", candidate, master_ref],
            cwd=self._root,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode not in (0, 1):
            raise CandidateError("release candidate ancestry validation failed")
        return result.returncode == 0

    def resolve_commit(self, revision: str) -> str:
        """Resolve one revision to an immutable commit SHA."""
        sha = self.output("rev-parse", "--verify", f"{revision}^{{commit}}")
        if _FULL_SHA.fullmatch(sha) is None:
            raise CandidateError("release candidate Git validation returned an invalid commit SHA")
        return sha


def _parse_project_version(source: str) -> str:
    try:
        data = tomllib.loads(source)
        version = data["project"]["version"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise CandidateError("project version metadata is unavailable") from exc
    if (
        not isinstance(version, str)
        or not version
        or version.strip() != version
        or "\n" in version
        or "\r" in version
    ):
        raise CandidateError("project version metadata must be a non-empty single-line string")
    return version


def project_version(root: Path) -> str:
    """Read the working tree project version without importing the package."""
    try:
        source = (root / "pyproject.toml").read_text(encoding="utf-8")
    except OSError as exc:
        raise CandidateError("project version metadata is unavailable") from exc
    return _parse_project_version(source)


def _candidate_project_version(repository: GitRepository, sha: str) -> str:
    return _parse_project_version(repository.output("show", f"{sha}:pyproject.toml"))


def _resolve_tag_candidate(
    repository: GitRepository,
    candidate_ref: str,
    tag: str,
    *,
    event_sha: str,
    event_created: bool,
    event_deleted: bool,
    event_forced: bool,
) -> str:
    if _TAG.fullmatch(tag) is None:
        raise CandidateError("release tag must start with v and contain no whitespace")
    if candidate_ref != f"refs/tags/{tag}":
        raise CandidateError("tag candidate ref does not match the release tag")
    if not event_created or event_deleted or event_forced:
        raise CandidateError("release tag must come from a new, non-forced tag push")
    if _FULL_SHA.fullmatch(event_sha) is None:
        raise CandidateError("tag push event must provide a full commit SHA")
    event_commit = repository.resolve_commit(event_sha)
    tag_commit = repository.resolve_commit(candidate_ref)
    if tag_commit != event_commit:
        raise CandidateError("release tag no longer matches the triggering push")
    return tag_commit


def _resolve_dry_run_candidate(repository: GitRepository, candidate_ref: str) -> str:
    if _FULL_SHA.fullmatch(candidate_ref) is None:
        raise CandidateError("dry-run candidate_sha must be a full 40-character commit SHA")
    sha = repository.output("rev-parse", "--verify", f"{candidate_ref}^{{commit}}")
    if sha != candidate_ref:
        raise CandidateError("dry-run candidate_sha must name a commit object")
    return sha


def validate_candidate(
    *,
    root: Path,
    mode: str,
    candidate_ref: str,
    expected_version: str,
    tag: str,
    master_ref: str,
    event_sha: str = "",
    event_created: bool = False,
    event_deleted: bool = False,
    event_forced: bool = False,
) -> ReleaseCandidate:
    """Validate candidate identity, ancestry, and source version."""
    repository = GitRepository(root)
    master_sha = repository.resolve_commit(master_ref)
    if mode == "tag":
        sha = _resolve_tag_candidate(
            repository,
            candidate_ref,
            tag,
            event_sha=event_sha,
            event_created=event_created,
            event_deleted=event_deleted,
            event_forced=event_forced,
        )
        version = tag.removeprefix("v")
        if expected_version and expected_version != version:
            raise CandidateError("tag and expected version do not match")
    elif mode == "dry-run":
        if event_sha or event_created or event_deleted or event_forced:
            raise CandidateError("dry-run mode must not receive tag push event metadata")
        if tag:
            raise CandidateError("dry-run mode must not receive a release tag")
        if not expected_version or expected_version.strip() != expected_version:
            raise CandidateError("dry-run expected_version must be a non-empty string")
        sha = _resolve_dry_run_candidate(repository, candidate_ref)
        version = expected_version
    else:
        raise CandidateError("release candidate mode must be tag or dry-run")

    if not repository.is_ancestor(sha, master_sha):
        raise CandidateError("release candidate is not reachable from the protected master ref")
    if _candidate_project_version(repository, sha) != version:
        raise CandidateError("release candidate version does not match project metadata")
    return ReleaseCandidate(mode=mode, sha=sha, version=version, tag=tag, master_sha=master_sha)


def write_github_outputs(path: Path, candidate: ReleaseCandidate) -> None:
    """Append validated candidate fields to the GitHub Actions output file."""
    with path.open("a", encoding="utf-8") as output:
        output.write(f"mode={candidate.mode}\n")
        output.write(f"sha={candidate.sha}\n")
        output.write(f"version={candidate.version}\n")
        output.write(f"tag={candidate.tag}\n")
        output.write(f"master_sha={candidate.master_sha}\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--mode", required=True, choices=("tag", "dry-run"))
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--expected-version", default="")
    parser.add_argument("--tag", default="")
    parser.add_argument("--master-ref", default="origin/master")
    parser.add_argument("--event-sha", default="")
    parser.add_argument("--event-created", choices=("true", "false"), default="false")
    parser.add_argument("--event-deleted", choices=("true", "false"), default="false")
    parser.add_argument("--event-forced", choices=("true", "false"), default="false")
    parser.add_argument("--github-output", type=Path)
    return parser


def _fail(message: str) -> NoReturn:
    raise SystemExit(f"release candidate validation failed: {message}")


def main(arguments: Sequence[str] | None = None) -> int:
    """Validate CLI inputs and optionally write GitHub Actions outputs."""
    options = _parser().parse_args(arguments)
    try:
        candidate = validate_candidate(
            root=options.root.resolve(),
            mode=options.mode,
            candidate_ref=options.candidate_ref,
            expected_version=options.expected_version,
            tag=options.tag,
            master_ref=options.master_ref,
            event_sha=options.event_sha,
            event_created=options.event_created == "true",
            event_deleted=options.event_deleted == "true",
            event_forced=options.event_forced == "true",
        )
    except CandidateError as exc:
        _fail(str(exc))
    if options.github_output is not None:
        write_github_outputs(options.github_output, candidate)
    print(
        f"release candidate verified: {candidate.sha} "
        f"({candidate.version}, {candidate.mode}; master {candidate.master_sha})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
