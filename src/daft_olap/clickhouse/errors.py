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

"""Credential-safe translation of structured ClickHouse server errors."""

from __future__ import annotations

from daft_olap._common.errors import (
    AuthenticationError,
    DaftOlapError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
)

_AUTHENTICATION_CODES = frozenset({192, 193, 194, 516})
_AUTHENTICATION_NAMES = frozenset(
    {"AUTHENTICATION_FAILED", "REQUIRED_PASSWORD", "UNKNOWN_USER", "WRONG_PASSWORD"}
)
_PERMISSION_CODES = frozenset({497})
_PERMISSION_NAMES = frozenset({"ACCESS_DENIED"})
_NOT_FOUND_CODES = frozenset({60})
_NOT_FOUND_NAMES = frozenset({"UNKNOWN_TABLE"})


def _integer_attribute(error: BaseException, *names: str) -> int | None:
    for name in names:
        value = getattr(error, name, None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _symbolic_name(error: BaseException) -> str | None:
    value = getattr(error, "name", None)
    return value.upper() if isinstance(value, str) else None


def translate_clickhouse_error(error: BaseException, *, operation: str) -> DaftOlapError | None:
    """Translate only structured ClickHouse codes without retaining driver text."""
    code = _integer_attribute(error, "code", "error_code")
    name = _symbolic_name(error)
    if code in _AUTHENTICATION_CODES or name in _AUTHENTICATION_NAMES:
        return AuthenticationError(f"ClickHouse authentication failed during {operation}")
    if code in _PERMISSION_CODES or name in _PERMISSION_NAMES:
        return DatabasePermissionError(f"ClickHouse denied access during {operation}")
    if code in _NOT_FOUND_CODES or name in _NOT_FOUND_NAMES:
        return DatabaseObjectNotFoundError(
            f"ClickHouse database object was not found during {operation}"
        )
    return None
