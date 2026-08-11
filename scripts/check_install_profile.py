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

"""Smoke-test one clean installation profile for the built distribution."""

from __future__ import annotations

import argparse
import importlib
import importlib.util
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version
from typing import NoReturn

_DISTRIBUTION = "daft-olap-connectors"
_ALWAYS_REQUIRED = frozenset(("daft", "daft_olap"))
_OPTIONAL_MODULES = frozenset(
    ("adbc_driver_flightsql", "adbc_driver_manager", "clickhouse_connect", "pymysql", "ray")
)
_PROFILE_MODULES = {
    "base": frozenset(),
    "clickhouse": frozenset(("clickhouse_connect",)),
    "doris": frozenset(("pymysql",)),
    "doris-flight": frozenset(("adbc_driver_flightsql", "adbc_driver_manager", "pymysql")),
    "ray": frozenset(("ray",)),
}


class InstallProfileError(RuntimeError):
    """An installation profile did not match its declared dependency boundary."""


def _module_available(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


def verify_install_profile(profile: str, expected_version: str) -> None:
    """Validate package identity, required imports, and optional dependency isolation."""
    try:
        installed_version = version(_DISTRIBUTION)
    except PackageNotFoundError as exc:
        raise InstallProfileError("daft-olap-connectors is not installed") from exc
    if installed_version != expected_version:
        raise InstallProfileError("installed distribution version does not match the candidate")

    profile_modules = _PROFILE_MODULES.get(profile)
    if profile_modules is None:
        raise InstallProfileError("unknown installation profile")
    required = _ALWAYS_REQUIRED | profile_modules
    forbidden = _OPTIONAL_MODULES - profile_modules
    missing = sorted(module for module in required if not _module_available(module))
    unexpected = sorted(module for module in forbidden if _module_available(module))
    if missing:
        raise InstallProfileError(f"required modules are unavailable: {', '.join(missing)}")
    if unexpected:
        unexpected_names = ", ".join(unexpected)
        raise InstallProfileError(f"undeclared optional modules are installed: {unexpected_names}")
    for module in sorted(required):
        importlib.import_module(module)


def _fail(message: str) -> NoReturn:
    raise SystemExit(f"installation profile validation failed: {message}")


def main(arguments: Sequence[str] | None = None) -> int:
    """Validate one named installation profile."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=tuple(_PROFILE_MODULES))
    parser.add_argument("--version", required=True)
    options = parser.parse_args(arguments)
    try:
        verify_install_profile(options.profile, options.version)
    except InstallProfileError as exc:
        _fail(str(exc))
    print(f"installation profile verified: {options.profile} ({options.version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
