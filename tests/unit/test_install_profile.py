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

import importlib.metadata

import pytest

from scripts import check_install_profile
from scripts.check_install_profile import InstallProfileError, verify_install_profile


@pytest.fixture
def installed_modules(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    modules = {"daft", "daft_olap"}
    monkeypatch.setattr(check_install_profile, "_module_available", modules.__contains__)
    monkeypatch.setattr(check_install_profile.importlib, "import_module", lambda module: object())
    monkeypatch.setattr(check_install_profile, "version", lambda distribution: "0.1.0a1")
    return modules


@pytest.mark.parametrize(
    ("profile", "optional_modules"),
    [
        ("base", set()),
        ("clickhouse", {"clickhouse_connect"}),
        ("doris", {"pymysql"}),
        ("doris-flight", {"adbc_driver_flightsql", "adbc_driver_manager", "pymysql"}),
        ("ray", {"ray"}),
    ],
)
def test_accepts_isolated_install_profile(
    installed_modules: set[str], profile: str, optional_modules: set[str]
) -> None:
    installed_modules.update(optional_modules)

    verify_install_profile(profile, "0.1.0a1")


def test_rejects_wrong_version(installed_modules: set[str]) -> None:
    with pytest.raises(InstallProfileError, match="version does not match"):
        verify_install_profile("base", "0.1.0a2")


def test_rejects_missing_distribution(
    monkeypatch: pytest.MonkeyPatch, installed_modules: set[str]
) -> None:
    def missing(_: str) -> str:
        raise importlib.metadata.PackageNotFoundError

    monkeypatch.setattr(check_install_profile, "version", missing)

    with pytest.raises(InstallProfileError, match="not installed"):
        verify_install_profile("base", "0.1.0a1")


def test_rejects_missing_required_module(installed_modules: set[str]) -> None:
    installed_modules.remove("daft_olap")

    with pytest.raises(InstallProfileError, match=r"required modules.*daft_olap"):
        verify_install_profile("base", "0.1.0a1")


def test_rejects_extra_module(installed_modules: set[str]) -> None:
    installed_modules.add("ray")

    with pytest.raises(InstallProfileError, match=r"undeclared optional modules.*ray"):
        verify_install_profile("base", "0.1.0a1")


def test_rejects_unknown_profile(installed_modules: set[str]) -> None:
    with pytest.raises(InstallProfileError, match="unknown installation profile"):
        verify_install_profile("unknown", "0.1.0a1")
