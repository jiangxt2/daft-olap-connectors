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

import io
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts.verify_release_artifacts import (
    ArtifactError,
    main,
    prepare_release_artifacts,
    verify_release_artifacts,
)

_NAME = "daft-olap-connectors"
_VERSION = "0.1.0a1"


def _metadata(*, name: str = _NAME, version: str = _VERSION) -> bytes:
    return f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n\n".encode()


def _write_distributions(
    directory: Path,
    *,
    wheel_name: str = _NAME,
    wheel_version: str = _VERSION,
    sdist_name: str = _NAME,
    sdist_version: str = _VERSION,
) -> tuple[Path, Path]:
    wheel = directory / "daft_olap_connectors-0.1.0a1-py3-none-any.whl"
    with zipfile.ZipFile(wheel, mode="w") as archive:
        archive.writestr(
            "daft_olap_connectors-0.1.0a1.dist-info/METADATA",
            _metadata(name=wheel_name, version=wheel_version),
        )

    sdist = directory / "daft_olap_connectors-0.1.0a1.tar.gz"
    payload = _metadata(name=sdist_name, version=sdist_version)
    info = tarfile.TarInfo("daft_olap_connectors-0.1.0a1/PKG-INFO")
    info.size = len(payload)
    with tarfile.open(sdist, mode="w:gz") as archive:
        archive.addfile(info, io.BytesIO(payload))
    return wheel, sdist


def test_prepare_and_verify_release_artifacts(tmp_path: Path) -> None:
    _write_distributions(tmp_path)

    prepare_release_artifacts(tmp_path, name=_NAME, version=_VERSION)
    verify_release_artifacts(tmp_path, name=_NAME, version=_VERSION)

    manifest = (tmp_path / "SHA256SUMS").read_text(encoding="utf-8")
    assert manifest.count("\n") == 2
    assert "daft_olap_connectors-0.1.0a1-py3-none-any.whl" in manifest
    assert "daft_olap_connectors-0.1.0a1.tar.gz" in manifest


def test_supports_manifest_outside_publish_directory(tmp_path: Path) -> None:
    packages = tmp_path / "packages"
    packages.mkdir()
    _write_distributions(packages)
    manifest = tmp_path / "SHA256SUMS"

    prepare_release_artifacts(packages, name=_NAME, version=_VERSION, manifest=manifest)
    verify_release_artifacts(packages, name=_NAME, version=_VERSION, manifest=manifest)

    assert manifest.is_file()
    assert not (packages / "SHA256SUMS").exists()


def test_cli_requires_explicit_manifest(tmp_path: Path) -> None:
    _write_distributions(tmp_path)

    with pytest.raises(SystemExit) as exc_info:
        main([str(tmp_path), "--name", _NAME, "--version", _VERSION, "--prepare"])

    assert exc_info.value.code == 2
    assert not (tmp_path / "SHA256SUMS").exists()


def test_cli_prepares_explicit_manifest_outside_publish_directory(tmp_path: Path) -> None:
    packages = tmp_path / "packages"
    packages.mkdir()
    _write_distributions(packages)
    manifest = tmp_path / "SHA256SUMS"

    assert (
        main(
            [
                str(packages),
                "--name",
                _NAME,
                "--version",
                _VERSION,
                "--manifest",
                str(manifest),
                "--prepare",
            ]
        )
        == 0
    )
    assert manifest.is_file()
    assert not (packages / "SHA256SUMS").exists()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("wheel_name", "other", "wheel Name or Version"),
        ("wheel_version", "9.9.9", "wheel Name or Version"),
        ("sdist_name", "other", "sdist Name or Version"),
        ("sdist_version", "9.9.9", "sdist Name or Version"),
    ],
)
def test_rejects_distribution_identity_mismatch(
    tmp_path: Path, field: str, value: str, message: str
) -> None:
    arguments = {field: value}
    _write_distributions(tmp_path, **arguments)

    with pytest.raises(ArtifactError, match=message):
        prepare_release_artifacts(tmp_path, name=_NAME, version=_VERSION)


def test_rejects_missing_or_duplicate_distribution(tmp_path: Path) -> None:
    wheel, _ = _write_distributions(tmp_path)
    wheel.unlink()

    with pytest.raises(ArtifactError, match="exactly one wheel and one sdist"):
        prepare_release_artifacts(tmp_path, name=_NAME, version=_VERSION)


def test_rejects_symbolic_link_distribution(tmp_path: Path) -> None:
    wheel, _ = _write_distributions(tmp_path)
    target = tmp_path / "wheel-target"
    wheel.rename(target)
    wheel.symlink_to(target)

    with pytest.raises(ArtifactError, match="regular files"):
        prepare_release_artifacts(tmp_path, name=_NAME, version=_VERSION)


def test_rejects_tampered_distribution(tmp_path: Path) -> None:
    wheel, _ = _write_distributions(tmp_path)
    prepare_release_artifacts(tmp_path, name=_NAME, version=_VERSION)
    wheel.write_bytes(wheel.read_bytes() + b"tampered")

    with pytest.raises(ArtifactError, match="digest does not match"):
        verify_release_artifacts(tmp_path, name=_NAME, version=_VERSION)


@pytest.mark.parametrize(
    "manifest",
    [
        "malformed\n",
        f"{'0' * 64}  ../outside.whl\n",
        f"{'0' * 64}  duplicate.whl\n{'1' * 64}  duplicate.whl\n",
        "",
    ],
)
def test_rejects_invalid_manifest(tmp_path: Path, manifest: str) -> None:
    _write_distributions(tmp_path)
    (tmp_path / "SHA256SUMS").write_text(manifest, encoding="utf-8")

    with pytest.raises(ArtifactError):
        verify_release_artifacts(tmp_path, name=_NAME, version=_VERSION)
