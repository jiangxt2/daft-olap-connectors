# Daft OLAP Connectors

Daft OLAP Connectors provides independent third-party Daft DataSources for ClickHouse and Apache
Doris. It is intended to remain independently installed and versioned as a Daft Community
Extension. Inclusion in Daft's community listing is a documentation contribution, not an assertion
that the connector is maintained or endorsed by Daft. The public surface is deliberately small:

- `daft_olap.read_clickhouse()`
- `daft_olap.read_doris()`
- `daft_olap.clickhouse.ClickHouseDataSource`
- `daft_olap.doris.DorisDataSource`
- `daft_olap.SecretRef`
- `daft_olap.DaftOlapError` and its documented public subclasses

Start with the repository [README](https://github.com/jiangxt2/daft-olap-connectors#readme), then
read the architecture, compatibility, consistency, error, and security contracts before production
use.
