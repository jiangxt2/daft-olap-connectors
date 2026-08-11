# Releasing

This document defines the repository-side Alpha release procedure. It does not authorize a tag,
workflow dispatch, GitHub environment change, GitHub Release, or PyPI publication. Each external
operation requires separate maintainer approval.

## Release candidate identity

The package version is static in `pyproject.toml`. Release tags use exactly `v<version>`, so the
current source version `0.1.0a1` corresponds to `v0.1.0a1`. A tag may be lightweight or annotated,
but it must resolve to a commit reachable from protected `master`.

The Release workflow has two modes:

- A newly created, non-forced `v*` tag push binds the event SHA to the fetched tag commit and requires
  its version to match project metadata. Deleted, moved, or mismatched tag events fail closed.
- A manual `workflow_dispatch` is a non-publishing dry-run. It requires a full 40-character
  `candidate_sha` and an exact `expected_version`; the commit must be reachable from `master`.

Branch names, short hashes, pull-request merge refs, unknown objects, and unmerged feature commits
are rejected. The manual workflow definition is available only after it exists on GitHub's default
branch; therefore the first real dry-run occurs after this release-readiness change is merged.

## Candidate gates

Both modes call the same reusable CI workflow against the validated SHA. The gate includes static
and documentation checks, the Python/Daft/Ray/PyArrow compatibility matrix, the Ray contract, and
real ClickHouse 25.3.2.39 and Doris 4.0.6 integration tests.

After the read-only gates pass, one job builds one wheel and one sdist, runs Twine metadata checks,
scans the candidate with TruffleHog, and generates an SPDX JSON SBOM. It writes a sorted
`SHA256SUMS` manifest for the two distributions. Five clean Python environments then install the
same downloaded wheel as base, ClickHouse, Doris, Doris Flight, and Ray profiles; each verifies the
candidate version, required imports, optional-dependency isolation, and manifest first.

The build exposes the immutable GitHub artifact ID and archive SHA-256 digest and records both in
the `Build and scan distributions` job summary. Every distribution consumer downloads that exact
artifact ID; `SHA256SUMS` then authenticates the extracted wheel and sdist independently of the
GitHub artifact archive.

Every non-local GitHub Action in the release execution chain is pinned to a full commit SHA. The
adjacent workflow comment records the upstream release tag that was used to verify the pin. Action
updates require a new tag-to-commit verification and ordinary code review.

The install jobs explicitly depend on candidate gates and build. Immediately before each external
write, the workflow resolves the live remote tag again and requires it to match the candidate SHA.
It also revalidates the downloaded wheel and sdist Name, Version, and `SHA256SUMS`, rather than
trusting filenames or an earlier job's working directory.

## Dry-run checklist

Before creating any release tag:

- merge the release-readiness source change through the protected `master` checks;
- select the exact merged `master` commit and confirm its `pyproject.toml` version;
- obtain separate approval to dispatch Release with the full SHA and version;
- require candidate, reusable gates, build/scan/SBOM, and all five install jobs to succeed;
- confirm `Publish to PyPI` and `Create GitHub Release` are explicitly skipped;
- record the workflow run URL, candidate SHA, version, distributions artifact ID and digest from
  the `Build and scan distributions` job summary, and `SHA256SUMS` contents;
- confirm no PyPI version or GitHub Release was created.

Dry-run jobs have read-only repository permissions. The two write-capable jobs are excluded by
event, tag-ref, candidate-mode, dependency-result, and success conditions; missing credentials or
environment approval is not the dry-run safety mechanism.

## External publishing prerequisites

Before the first tag is pushed, maintainers separately verify that GitHub has a `pypi` environment
with the intended reviewers and a PyPI Trusted Publisher restricted to this repository, Release
workflow, and environment. Repository source contains no long-lived PyPI token. These remote
settings are not created or proven by this pull request.

An active tag ruleset matching release tags is also required before the first tag. It must allow the
authorized release procedure to create a new tag while preventing that tag from being updated or
deleted after creation, without an ordinary maintainer bypass. Event-SHA binding and pre-write
remote checks fail closed across known tag moves, but no client-side check can make the interval
between its final lookup and an external publish atomic. The remote ruleset closes that interval.

After a successful dry-run and explicit release approval, create the exact version tag on the
validated commit and push it. The tag run requests `id-token: write` only in the PyPI job and uses
OIDC trusted publishing. Provenance attestations are not enabled by this initial Alpha workflow.
Only after PyPI succeeds does the final job request `contents: write` and create the GitHub Release
from the same wheel, sdist, `SHA256SUMS`, and SPDX JSON SBOM.

## Failures and recovery

Any failed, cancelled, timed-out, or unexpectedly skipped candidate gate prevents publishing. A
missing, moved, or deleted remote tag also prevents the next write step. Do not move a public tag,
overwrite a PyPI file, bypass a database integration test, or publish a locally rebuilt artifact.
Fix source failures through a new pull request and use a new version when an already-visible tag or
package version cannot be safely reused.

If PyPI succeeds but GitHub Release creation fails, preserve the successful publish evidence and
rerun only the failed job while the retained run artifacts remain available. If artifacts are no
longer available or identity cannot be proven, stop and obtain a separately reviewed recovery plan.

Community Extensions submission is downstream of a verified public release. Its documentation PR
is a separate external contribution and must not claim Daft maintenance or endorsement.
