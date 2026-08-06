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

"""Initialize the fixed Doris integration-test schema."""

from __future__ import annotations

import os
from pathlib import Path

import pymysql


def _statements(sql: str) -> tuple[str, ...]:
    return tuple(statement.strip() for statement in sql.split(";") if statement.strip())


def main() -> None:
    """Execute the version-controlled fixture SQL against the test FE."""
    root = Path(__file__).resolve().parents[1]
    sql = (root / "docker" / "doris" / "init.sql").read_text(encoding="utf-8")
    connection = pymysql.connect(
        host="127.0.0.1",
        port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
        user="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        autocommit=True,
        connect_timeout=10,
        read_timeout=120,
        write_timeout=120,
    )
    try:
        with connection.cursor() as cursor:
            for statement in _statements(sql):
                cursor.execute(statement)
        print("Doris integration fixtures initialized", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
