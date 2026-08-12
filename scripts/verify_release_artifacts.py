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

"""Validate release distribution identity and its SHA-256 manifest."""

from __future__ import annotations

import argparse
import hashlib
import re
import tarfile
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path
from typing import IO, NoReturn

_MANIFEST_NAME = "SHA256SUMS"
_MANIFEST_LINE = re.compile(r"([0-9a-f]{64})  ([^/\\\r\n]+)")
_SDIST_METADATA_DEPTH = 2


class ArtifactError(ValueError):
    """A release artifact failed deterministic validation."""


@dataclass(frozen=True)
class DistributionIdentity:
    """Core metadata read from one built distribution."""

    name: str
    version: str


def _identity(stream: IO[bytes]) -> DistributionIdentity:
    metadata = BytesParser().parse(stream, headersonly=True)
    name = metadata.get("Name")
    version = metadata.get("Version")
    if not name or not version:
        raise ArtifactError("distribution metadata is missing Name or Version")
    return DistributionIdentity(name=name, version=version)


def _wheel_identity(path: Path) -> DistributionIdentity:
    try:
        with zipfile.ZipFile(path) as archive:
            metadata_files = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA") and not name.startswith("/")
            ]
            if len(metadata_files) != 1:
                raise ArtifactError("wheel must contain exactly one METADATA file")
            with archive.open(metadata_files[0]) as stream:
                return _identity(stream)
    except (OSError, zipfile.BadZipFile) as exc:
        raise ArtifactError("wheel metadata is unreadable") from exc


def _sdist_identity(path: Path) -> DistributionIdentity:
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            metadata_files = [
                member
                for member in archive.getmembers()
                if member.isfile()
                and Path(member.name).name == "PKG-INFO"
                and len(Path(member.name).parts) == _SDIST_METADATA_DEPTH
            ]
            if len(metadata_files) != 1:
                raise ArtifactError("sdist must contain exactly one top-level PKG-INFO file")
            stream = archive.extractfile(metadata_files[0])
            if stream is None:
                raise ArtifactError("sdist PKG-INFO is unreadable")
            with stream:
                return _identity(stream)
    except (OSError, tarfile.TarError) as exc:
        raise ArtifactError("sdist metadata is unreadable") from exc


def distribution_paths(directory: Path) -> tuple[Path, Path]:
    """Return the only wheel and sdist in a release directory."""
    wheels = sorted(directory.glob("*.whl"))
    sdists = sorted(directory.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ArtifactError("release directory must contain exactly one wheel and one sdist")
    if any(path.is_symlink() or not path.is_file() for path in (*wheels, *sdists)):
        raise ArtifactError("release distributions must be regular files")
    return wheels[0], sdists[0]


def verify_distribution_metadata(directory: Path, *, name: str, version: str) -> tuple[Path, Path]:
    """Require both built distributions to carry the requested identity."""
    wheel, sdist = distribution_paths(directory)
    expected = DistributionIdentity(name=name, version=version)
    if _wheel_identity(wheel) != expected:
        raise ArtifactError("wheel Name or Version does not match the release candidate")
    if _sdist_identity(sdist) != expected:
        raise ArtifactError("sdist Name or Version does not match the release candidate")
    return wheel, sdist


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_manifest(
    directory: Path, distributions: Sequence[Path], *, manifest: Path | None = None
) -> Path:
    """Write a stable SHA-256 manifest for the supplied distributions."""
    manifest_path = directory / _MANIFEST_NAME if manifest is None else manifest
    if manifest_path.is_symlink():
        raise ArtifactError("SHA256SUMS must not be a symbolic link")
    entries = sorted(distributions, key=lambda path: path.name)
    manifest_path.write_text(
        "".join(f"{_sha256(path)}  {path.name}\n" for path in entries),
        encoding="utf-8",
    )
    return manifest_path


def verify_manifest(
    directory: Path, distributions: Sequence[Path], *, manifest: Path | None = None
) -> None:
    """Require the manifest to name and authenticate exactly the distributions."""
    expected_names = {path.name for path in distributions}
    entries: dict[str, str] = {}
    manifest_path = directory / _MANIFEST_NAME if manifest is None else manifest
    try:
        lines = manifest_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ArtifactError("SHA256SUMS is unavailable") from exc
    for line in lines:
        match = _MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise ArtifactError("SHA256SUMS contains a malformed entry")
        digest, filename = match.groups()
        if filename in entries:
            raise ArtifactError("SHA256SUMS contains a duplicate filename")
        entries[filename] = digest
    if set(entries) != expected_names:
        raise ArtifactError("SHA256SUMS must cover exactly the wheel and sdist")
    for path in distributions:
        if _sha256(path) != entries[path.name]:
            raise ArtifactError("release artifact SHA-256 digest does not match")


def prepare_release_artifacts(
    directory: Path, *, name: str, version: str, manifest: Path | None = None
) -> None:
    """Validate distributions, write their manifest, and verify it immediately."""
    distributions = verify_distribution_metadata(directory, name=name, version=version)
    write_manifest(directory, distributions, manifest=manifest)
    verify_manifest(directory, distributions, manifest=manifest)


def verify_release_artifacts(
    directory: Path, *, name: str, version: str, manifest: Path | None = None
) -> None:
    """Validate distribution identity and a pre-existing manifest."""
    distributions = verify_distribution_metadata(directory, name=name, version=version)
    verify_manifest(directory, distributions, manifest=manifest)


def _fail(message: str) -> NoReturn:
    raise SystemExit(f"release artifact validation failed: {message}")


def main(arguments: Sequence[str] | None = None) -> int:
    """Prepare or verify a release artifact directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    options = parser.parse_args(arguments)
    try:
        if options.prepare:
            prepare_release_artifacts(
                options.directory,
                name=options.name,
                version=options.version,
                manifest=options.manifest,
            )
        else:
            verify_release_artifacts(
                options.directory,
                name=options.name,
                version=options.version,
                manifest=options.manifest,
            )
    except ArtifactError as exc:
        _fail(str(exc))
    print(f"release artifacts verified: {options.name} {options.version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
