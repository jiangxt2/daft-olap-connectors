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

"""Independent Daft DataSource connectors for ClickHouse and Apache Doris."""

from importlib.metadata import version

from daft_olap._common.errors import (
    AuthenticationError,
    CompatibilityError,
    ConfigurationError,
    DaftOlapError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DependencyError,
    DiscoveryError,
    SchemaError,
    TransportError,
    UnsupportedPredicateError,
)
from daft_olap._common.redaction import SecretRef
from daft_olap.clickhouse.api import read_clickhouse
from daft_olap.doris.api import read_doris

__all__ = [
    "AuthenticationError",
    "CompatibilityError",
    "ConfigurationError",
    "DaftOlapError",
    "DatabaseObjectNotFoundError",
    "DatabasePermissionError",
    "DependencyError",
    "DiscoveryError",
    "SchemaError",
    "SecretRef",
    "TransportError",
    "UnsupportedPredicateError",
    "read_clickhouse",
    "read_doris",
]
__version__ = version("daft-olap-connectors")
