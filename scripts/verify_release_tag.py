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

"""Fail closed unless a remote release tag still resolves to the candidate commit."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

_FULL_SHA = re.compile(r"[0-9a-f]{40}")
_TAG = re.compile(r"v[^\s]+")
_REMOTE_FIELD_COUNT = 2


class TagError(ValueError):
    """A remote release tag failed immutable identity validation."""


def _remote_lines(root: Path, remote: str, tag: str) -> tuple[str, ...]:
    git = shutil.which("git")
    if git is None:
        raise TagError("git is required to verify the remote release tag")
    reference = f"refs/tags/{tag}"
    result = subprocess.run(  # noqa: S603
        [git, "ls-remote", remote, reference, f"{reference}^{{}}"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise TagError("remote release tag lookup failed")
    return tuple(line for line in result.stdout.splitlines() if line)


def remote_tag_commit(root: Path, *, remote: str, tag: str) -> str:
    """Resolve a lightweight or annotated remote tag to exactly one commit SHA."""
    if _TAG.fullmatch(tag) is None:
        raise TagError("release tag must start with v and contain no whitespace")
    reference = f"refs/tags/{tag}"
    allowed = {reference, f"{reference}^{{}}"}
    identities: dict[str, str] = {}
    for line in _remote_lines(root, remote, tag):
        fields = line.split("\t")
        if len(fields) != _REMOTE_FIELD_COUNT:
            raise TagError("remote release tag lookup returned malformed output")
        sha, resolved_ref = fields
        if (
            resolved_ref not in allowed
            or resolved_ref in identities
            or _FULL_SHA.fullmatch(sha) is None
        ):
            raise TagError("remote release tag lookup returned an invalid identity")
        identities[resolved_ref] = sha
    if reference not in identities:
        raise TagError("remote release tag is unavailable")
    return identities.get(f"{reference}^{{}}", identities[reference])


def verify_remote_tag(root: Path, *, remote: str, tag: str, expected_sha: str) -> None:
    """Require the live remote tag to resolve to the expected candidate commit."""
    if _FULL_SHA.fullmatch(expected_sha) is None:
        raise TagError("expected candidate must be a full commit SHA")
    if remote_tag_commit(root, remote=remote, tag=tag) != expected_sha:
        raise TagError("remote release tag no longer matches the release candidate")


def _fail(message: str) -> NoReturn:
    raise SystemExit(f"release tag validation failed: {message}")


def main(arguments: Sequence[str] | None = None) -> int:
    """Verify the remote release tag immediately before an external write."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-sha", required=True)
    options = parser.parse_args(arguments)
    try:
        verify_remote_tag(
            options.root.resolve(),
            remote=options.remote,
            tag=options.tag,
            expected_sha=options.expected_sha,
        )
    except TagError as exc:
        _fail(str(exc))
    print(f"remote release tag verified: {options.tag} -> {options.expected_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
