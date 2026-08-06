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

"""Validate a matching DCO trailer for every non-merge commit in a range."""

from __future__ import annotations

import subprocess
import sys

_EXPECTED_ARGUMENT_COUNT = 3


def _commit_records(base: str, head: str) -> tuple[tuple[str, str, str, str], ...]:
    output = subprocess.run(
        [
            "git",
            "log",
            "--no-merges",
            "--format=%H%x00%an%x00%ae%x00%B%x00",
            f"{base}..{head}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    fields = output.split("\x00")
    while fields and not fields[-1].strip():
        fields.pop()
    if len(fields) % 4 != 0:
        raise RuntimeError("git log returned an unexpected DCO record format")
    return tuple(
        (fields[index], fields[index + 1], fields[index + 2], fields[index + 3])
        for index in range(0, len(fields), 4)
    )


def main() -> int:
    """Check the requested commit range and print all unsigned commits."""
    if len(sys.argv) != _EXPECTED_ARGUMENT_COUNT:
        print("usage: check_dco.py BASE HEAD", file=sys.stderr)
        return 2
    failures: list[str] = []
    for commit, author_name, author_email, message in _commit_records(sys.argv[1], sys.argv[2]):
        expected = f"Signed-off-by: {author_name} <{author_email}>"
        trailers = {line.strip().casefold() for line in message.splitlines()}
        if expected.casefold() not in trailers:
            failures.append(f"{commit[:12]} missing {expected}")
    if failures:
        print("DCO validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print("DCO trailers verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
