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

"""Fail when a source, executable, or build configuration lacks Apache-2.0 text."""

from __future__ import annotations

import sys
from pathlib import Path

_SUFFIXES = {".conf", ".py", ".sh", ".sql", ".toml", ".yaml", ".yml"}
_SPECIAL_NAMES = {"Dockerfile"}
_EXCLUDED_PARTS = {
    ".artifacts",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "build",
    "dist",
    "site",
}
_REQUIRED_TEXT = (
    "Licensed under the Apache License, Version 2.0",
    "http://www.apache.org/licenses/LICENSE-2.0",
)


def candidates(root: Path) -> tuple[Path, ...]:
    """Return deterministic, human-authored files covered by the policy."""
    paths: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file() or _EXCLUDED_PARTS.intersection(path.parts):
            continue
        if path.name == "uv.lock":
            continue
        if (
            path.suffix in _SUFFIXES
            or path.name in _SPECIAL_NAMES
            or path.name.endswith(".Dockerfile")
        ):
            paths.append(path)
    return tuple(sorted(paths))


def main() -> int:
    """Check every candidate and report all failures in one run."""
    root = Path(__file__).resolve().parents[1]
    missing: list[str] = []
    for path in candidates(root):
        prefix = "\n".join(path.read_text(encoding="utf-8").splitlines()[:16])
        if not all(text in prefix for text in _REQUIRED_TEXT):
            missing.append(str(path.relative_to(root)))
    if missing:
        print("Apache-2.0 header missing from:", file=sys.stderr)
        for path in missing:
            print(f"  {path}", file=sys.stderr)
        return 1
    print(f"Apache-2.0 headers verified in {len(candidates(root))} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
