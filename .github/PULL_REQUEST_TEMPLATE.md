## What changed

Describe the connector behavior and public contract affected by this change.

## Why

Explain the user problem and why this design fits Daft's DataSource direction.

## How tested

- [ ] Unit and native contract tests
- [ ] Local Ray contract tests
- [ ] Relevant real ClickHouse or Doris integration tests
- [ ] Ruff, mypy, license headers, and strict documentation build

## Compatibility and safety

- [ ] No credential appears in logs, exceptions, plans, or representations
- [ ] Unsupported behavior fails closed or has a documented single-task fallback
- [ ] New public behavior is documented in the compatibility matrix
- [ ] Every commit contains a matching `Signed-off-by` trailer
