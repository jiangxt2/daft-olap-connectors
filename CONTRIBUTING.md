# Contributing

Thank you for improving Daft OLAP Connectors. This project welcomes focused changes that preserve
the public Daft DataSource contract and the database-specific correctness boundaries.

Before opening a pull request:

- discuss public API or architecture changes in an issue;
- add unit tests for isolated logic and real ClickHouse or Doris tests for database behavior;
- run the static, documentation, native, Ray, and relevant integration checks below;
- update compatibility and consistency documentation when behavior changes;
- add a `Signed-off-by` trailer to every commit.

Please keep ClickHouse and Doris transport logic separate unless a shared invariant has been proven
for both databases. Do not add protocol fallback, raw arbitrary-query APIs, or credential-bearing
diagnostics.

## Branch and DCO policy

The canonical and default branch is `master`. Pull requests must target `master`; pushes to that
branch run the CI workflow, while the DCO job remains intentionally limited to pull requests.

Every non-merge commit must contain a `Signed-off-by` trailer that matches the commit author's name
and email. Automated dependency updates are not exempt. If Dependabot or another automation account
creates an unsigned commit, do not merge it or treat a pull request description as a substitute for
the trailer. A maintainer must review and reproduce the update in an independent worktree as a new
commit with matching author and trailer metadata, then submit it through the normal pull request
checks.

```bash
uv sync --all-extras --group dev
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/check_license_headers.py
uv run codespell
uv run mkdocs build --strict
uv run pytest tests/unit tests/contract --cov=daft_olap --cov-branch
./scripts/run_clickhouse_it.sh
./scripts/run_doris_it.sh
uv run python -m build
uvx twine check dist/*
```

Both database scripts use project-scoped Compose resources and perform exact teardown. Never use a
Docker prune command to prepare or clean up integration tests.

The Doris environment defaults to `10.250.128.0/24`. If that range overlaps an existing network,
set `DORIS_CONTAINER_SUBNET`, `DORIS_FE_CONTAINER_IP`, and `DORIS_BE_CONTAINER_IP` together to one
unused private subnet. Do not remove an unrelated Docker network to make the default range fit.
