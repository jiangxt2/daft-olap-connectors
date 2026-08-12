# Community and upstream contributions

Daft maintainers have directed database integrations that can use the custom DataSource API toward
independent third-party packages. Daft also provides a Community Extensions catalog for projects
that are installed and versioned outside core. This project follows that model as its default,
long-lived destination; a listing does not imply Daft maintenance or endorsement.

After the first public release has verifiable native, Ray, ClickHouse, and Doris results, the first
external proposal should be a small documentation PR adding the package to Daft's Community
Extensions page. The package must already have installation instructions, compatibility claims,
governance files, and real-infrastructure test evidence before that proposal.

Repository metadata, a merged release-readiness change, or a successful non-publishing dry-run is
not a public release. The Community Extensions proposal starts only after the exact PyPI version,
GitHub tag, release assets, distribution hashes, and release-run evidence have been verified. Its
body and submission require their own review and authorization.

Core contributions are limited by default to generic improvements that benefit all custom
DataSources, such as a more stable capability API, documented task metadata, or bounded async bridge
behavior. Each such change needs its own maintainer discussion and cannot be a runtime prerequisite
for a released connector version.

Moving ClickHouse or Doris implementation into Daft core is not the planned end state. It is
considered only after an explicit maintainer invitation and a separate design review covering
dependency cost, ownership, CI infrastructure, and compatibility. If that occurs, connectors move
independently and the third-party facade receives a documented deprecation or delegation path. Until
then, the package remains the canonical implementation and does not duplicate a private Daft fork.

The project keeps this future option inexpensive by isolating unstable Daft details in `_compat.py`,
keeping database planners and transports independent, and testing observable contracts rather than
Daft internal layouts. It never patches Daft at runtime or depends on unmerged upstream work.
