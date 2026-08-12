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

from pathlib import Path

import pytest

from scripts.check_release_workflow import release_policy_failures

_ROOT = Path(__file__).parents[2]


@pytest.fixture
def workflows() -> tuple[str, str]:
    return (
        (_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        (_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"),
    )


def test_repository_release_workflow_satisfies_policy(workflows: tuple[str, str]) -> None:
    assert release_policy_failures(*workflows) == ()


@pytest.mark.parametrize(
    ("workflow_index", "old", "new", "expected_failure"),
    [
        (0, "  workflow_call:\n", "", "workflow_call"),
        (
            0,
            "      - run: uv run codespell\n",
            "      - run: uv run codespell\n"
            "      - run: uv run --no-project python scripts/check_dco.py HEAD~1 HEAD\n",
            "must not add a repository DCO gate",
        ),
        (0, "name: Static and package checks", "name: Static checks", "required check name"),
        (
            0,
            "          - profile: upper-pyarrow\n",
            "          - profile: tributo\n",
            "four supported contract matrix entries",
        ),
        (
            0,
            '            pyarrow: "16.1.0"\n',
            '            pyarrow: "16.2.0"\n',
            "four supported contract matrix entries",
        ),
        (
            0,
            "ref: ${{ inputs.candidate_sha || github.sha }}",
            "ref: ${{ github.sha }}",
            "every CI checkout",
        ),
        (1, "  workflow_dispatch:\n", "", "workflow_dispatch"),
        (1, "uses: ./.github/workflows/ci.yml", "uses: ./other.yml", "must call CI"),
        (
            1,
            "uv run python -m build --outdir release-artifacts/packages",
            "python -m build --outdir release-artifacts/packages",
            "exactly once",
        ),
        (
            1,
            "needs.install-smoke.result == 'success'",
            "needs.install-smoke.result != 'cancelled'",
            "fail-closed condition",
        ),
        (
            1,
            "scripts/verify_release_tag.py",
            "scripts/tag_check_removed.py",
            "revalidate the live remote tag",
        ),
        (
            1,
            "      - name: Revalidate distribution identity and manifest\n"
            "        run: >-\n"
            "          python scripts/verify_release_artifacts.py release/packages",
            "      - name: Revalidate distribution identity and manifest\n"
            "        run: >-\n"
            "          python scripts/verify_hashes_only.py release/packages",
            "revalidate distribution identity",
        ),
        (
            1,
            "distributions_artifact_digest: "
            "${{ steps.upload-distributions.outputs.artifact-digest }}",
            "distributions_artifact_digest: missing",
            "expose and record the immutable distribution artifact identity",
        ),
        (
            1,
            "artifact-ids: ${{ needs.build.outputs.distributions_artifact_id }}",
            "name: distributions",
            "download the exact distribution artifact ID",
        ),
        (
            1,
            "needs: [candidate, gates, build]",
            "needs: [candidate, build]",
            "explicitly depend on candidate gates",
        ),
        (
            1,
            "group: release-${{ github.event_name == 'push' && github.ref || "
            "inputs.candidate_sha }}",
            "group: release-${{ github.ref }}-${{ inputs.candidate_sha || github.sha }}",
            "same tag must share one concurrency group",
        ),
        (
            1,
            "EVENT_FORCED: ${{ github.event_name == 'push' && github.event.forced || false }}",
            "EVENT_FORCED: false",
            "github.event.forced",
        ),
    ],
)
def test_policy_rejects_weakened_workflow(
    workflows: tuple[str, str],
    workflow_index: int,
    old: str,
    new: str,
    expected_failure: str,
) -> None:
    modified = list(workflows)
    assert old in modified[workflow_index]
    modified[workflow_index] = modified[workflow_index].replace(old, new, 1)

    failures = release_policy_failures(*modified)

    assert any(expected_failure in failure for failure in failures)
