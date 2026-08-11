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

"""Enforce fail-closed release orchestration invariants across workflows."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from pathlib import Path

_CI_JOB_NAMES = {
    "static": "Static and package checks",
    "contract": "${{ matrix.profile }} / Python ${{ matrix.python }} / Daft ${{ matrix.daft }} / "
    "Ray ${{ matrix.ray }} / PyArrow ${{ matrix.pyarrow }}",
    "clickhouse-it": "ClickHouse 25.3.2.39 IT",
    "doris-it": "Doris 4.0.6 IT",
}
_CONTRACT_MATRIX_ENTRY = re.compile(
    r"(?m)^          - profile: ([^\n]+)\n"
    r'            python: "([^"]+)"\n'
    r'            daft: "([^"]+)"\n'
    r'            pyarrow: "([^"]+)"\n'
    r'            ray: "([^"]+)"\n'
    r'            ray_requirement: "([^"]+)"$'
)
_CONTRACT_MATRIX = (
    ("minimum-pyarrow", "3.12", "0.7.23", "16.1.0", "2.55.1", "ray==2.55.1"),
    (
        "tributo",
        "3.12",
        "0.7.23",
        "19.0.1",
        "2.55.1",
        "ray[default,serve,tune]==2.55.1",
    ),
    (
        "tributo",
        "3.13",
        "0.7.23",
        "19.0.1",
        "2.55.1",
        "ray[default,serve,tune]==2.55.1",
    ),
    ("upper-pyarrow", "3.13", "0.7.23", "24.0.0", "2.55.1", "ray==2.55.1"),
)
_CANDIDATE_CHECKOUT = "ref: ${{ needs.candidate.outputs.sha }}"
_TAG_REVALIDATION = "scripts/verify_release_tag.py"
_ARTIFACT_REVALIDATION = "scripts/verify_release_artifacts.py release/packages"
_ARTIFACT_ID_DOWNLOAD = "artifact-ids: ${{ needs.build.outputs.distributions_artifact_id }}"
_ARTIFACT_ID_OUTPUT = (
    "distributions_artifact_id: ${{ steps.upload-distributions.outputs.artifact-id }}"
)
_ARTIFACT_DIGEST_OUTPUT = (
    "distributions_artifact_digest: ${{ steps.upload-distributions.outputs.artifact-digest }}"
)
_PUBLISH_GATES = (
    "github.event_name == 'push'",
    "startsWith(github.ref, 'refs/tags/v')",
    "needs.candidate.result == 'success'",
    "needs.gates.result == 'success'",
    "needs.build.result == 'success'",
    "needs.install-smoke.result == 'success'",
    "needs.candidate.outputs.mode == 'tag'",
)


def _job(workflow: str, job_id: str) -> str:
    jobs_start = workflow.find("\njobs:\n")
    if jobs_start < 0:
        return ""
    jobs = workflow[jobs_start:]
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        jobs,
    )
    return "" if match is None else match.group(1)


def _require(failures: list[str], condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def release_policy_failures(ci_workflow: str, release_workflow: str) -> tuple[str, ...]:
    """Return all known release orchestration policy violations."""
    failures: list[str] = []
    for trigger in ("pull_request:", "push:", "workflow_call:"):
        _require(failures, trigger in ci_workflow, f"CI is missing the {trigger} trigger")
    for job_id, job_name in _CI_JOB_NAMES.items():
        block = _job(ci_workflow, job_id)
        _require(failures, bool(block), f"CI is missing the {job_id} job")
        _require(
            failures,
            f"name: {job_name}" in block,
            f"CI changed the required check name for {job_id}",
        )
    _require(
        failures,
        not _job(ci_workflow, "dco") and "check_dco.py" not in ci_workflow,
        "CI must not add a repository DCO gate absent from Daft",
    )
    contract = _job(ci_workflow, "contract")
    _require(
        failures,
        tuple(_CONTRACT_MATRIX_ENTRY.findall(contract)) == _CONTRACT_MATRIX,
        "CI changed the four supported contract matrix entries",
    )
    checkout_count = ci_workflow.count("uses: actions/checkout@")
    _require(failures, checkout_count > 0, "CI has no checkout steps")
    _require(
        failures,
        ci_workflow.count("ref: ${{ inputs.candidate_sha || github.sha }}") == checkout_count,
        "every CI checkout must select the optional immutable candidate SHA",
    )

    for trigger in ('tags: ["v*"]', "workflow_dispatch:", "candidate_sha:", "expected_version:"):
        _require(failures, trigger in release_workflow, f"release is missing {trigger}")
    _require(
        failures,
        "group: release-${{ github.event_name == 'push' && github.ref || inputs.candidate_sha }}"
        in release_workflow,
        "release attempts for the same tag must share one concurrency group",
    )

    candidate = _job(release_workflow, "candidate")
    gates = _job(release_workflow, "gates")
    build = _job(release_workflow, "build")
    install = _job(release_workflow, "install-smoke")
    publish = _job(release_workflow, "publish")
    github_release = _job(release_workflow, "github-release")
    for job_id, block in (
        ("candidate", candidate),
        ("gates", gates),
        ("build", build),
        ("install-smoke", install),
        ("publish", publish),
        ("github-release", github_release),
    ):
        _require(failures, bool(block), f"release is missing the {job_id} job")

    for token in (
        "fetch-depth: 0",
        "ref: master",
        "refs/heads/master:refs/remotes/origin/master",
        "scripts/validate_release_candidate.py",
        "--master-ref origin/master",
        "github.event.after",
        "github.event.created",
        "github.event.deleted",
        "github.event.forced",
        '--event-sha "${EVENT_SHA}"',
        '--event-created "${EVENT_CREATED}"',
        '--event-deleted "${EVENT_DELETED}"',
        '--event-forced "${EVENT_FORCED}"',
    ):
        _require(failures, token in candidate, f"candidate validation is missing {token}")
    _require(
        failures,
        "uses: ./.github/workflows/ci.yml" in gates
        and "candidate_sha: ${{ needs.candidate.outputs.sha }}" in gates,
        "release gates must call CI with the validated candidate SHA",
    )
    _require(
        failures,
        release_workflow.count("uv run python -m build --outdir release-artifacts/packages") == 1,
        "release must build distributions exactly once",
    )
    _require(failures, _CANDIDATE_CHECKOUT in build, "build must checkout the candidate SHA")
    _require(
        failures,
        _CANDIDATE_CHECKOUT in install,
        "install smoke must checkout the candidate SHA",
    )
    _require(
        failures,
        "needs: [candidate, gates, build]" in install,
        "install smoke must explicitly depend on candidate gates and build",
    )
    _require(
        failures,
        "scripts/verify_release_artifacts.py release-artifacts/packages" in build
        and "--manifest release-artifacts/SHA256SUMS" in build
        and "--prepare" in build,
        "build must prepare and verify the distribution manifest",
    )
    _require(
        failures,
        _ARTIFACT_ID_OUTPUT in build
        and _ARTIFACT_DIGEST_OUTPUT in build
        and "GITHUB_STEP_SUMMARY" in build,
        "build must expose and record the immutable distribution artifact identity",
    )
    _require(
        failures,
        "scripts/verify_release_artifacts.py" in install,
        "install smoke must verify downloaded distribution identity and hashes",
    )
    install_profiles = ("base", "clickhouse", "doris", "doris-flight", "ray")
    _require(
        failures,
        all(profile in install for profile in install_profiles),
        "install profiles missing",
    )
    for job_id, block in (("publish", publish), ("github-release", github_release)):
        for gate in _PUBLISH_GATES:
            _require(failures, gate in block, f"{job_id} is missing fail-closed condition {gate}")
        _require(
            failures,
            _CANDIDATE_CHECKOUT in block,
            f"{job_id} must checkout the immutable candidate SHA",
        )
        _require(
            failures,
            _TAG_REVALIDATION in block,
            f"{job_id} must revalidate the live remote tag",
        )
        _require(
            failures,
            _ARTIFACT_REVALIDATION in block
            and "--name daft-olap-connectors" in block
            and '--version "${{ needs.candidate.outputs.version }}"' in block
            and "--manifest release/SHA256SUMS" in block,
            f"{job_id} must revalidate distribution identity and manifest",
        )
        _require(
            failures,
            _ARTIFACT_ID_DOWNLOAD in block and "merge-multiple: true" in block,
            f"{job_id} must download the exact distribution artifact ID",
        )
    _require(
        failures,
        _ARTIFACT_ID_DOWNLOAD in install and "merge-multiple: true" in install,
        "install smoke must download the exact distribution artifact ID",
    )
    _require(
        failures,
        "packages-dir: release/packages/" in publish,
        "PyPI publish must exclude SHA256SUMS from the packages directory",
    )
    _require(
        failures,
        "needs: [candidate, gates, build, install-smoke]" in publish,
        "publish must depend on every read-only gate",
    )
    _require(
        failures,
        "id-token: write" in publish
        and "contents: read" in publish
        and "contents: write" not in publish,
        "publish must have only OIDC write and contents read permissions",
    )
    _require(
        failures,
        "needs: [candidate, gates, build, install-smoke, publish]" in github_release,
        "GitHub Release must depend on successful publishing",
    )
    _require(
        failures,
        "needs.publish.result == 'success'" in github_release,
        "GitHub Release must fail closed unless PyPI publish succeeded",
    )
    _require(
        failures,
        "contents: write" in github_release and "id-token: write" not in github_release,
        "GitHub Release must have only contents write permission",
    )
    return tuple(failures)


def main(arguments: Sequence[str] | None = None) -> int:
    """Check the CI and release workflows and report every policy failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ci", type=Path)
    parser.add_argument("release", type=Path)
    options = parser.parse_args(arguments)
    failures = release_policy_failures(
        options.ci.read_text(encoding="utf-8"),
        options.release.read_text(encoding="utf-8"),
    )
    if failures:
        print("release workflow policy failed:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print("release workflow policy verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
