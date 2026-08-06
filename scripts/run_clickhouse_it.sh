#!/usr/bin/env bash
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

set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${project_root}/docker/clickhouse/compose.yaml"
compose_project="daft-olap-it-clickhouse"
artifact_dir="${project_root}/.artifacts/it"

mkdir -p "${artifact_dir}"

cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then
    docker compose --project-name "${compose_project}" --file "${compose_file}" logs --no-color \
      >"${artifact_dir}/clickhouse-compose.log" 2>&1 || true
  fi
  docker compose --project-name "${compose_project}" --file "${compose_file}" down \
    --volumes --remove-orphans
  return "${status}"
}
trap cleanup EXIT

cd "${project_root}"
docker compose --project-name "${compose_project}" --file "${compose_file}" up --detach --wait
uv run --all-extras python scripts/wait_for_service.py clickhouse
uv run --all-extras pytest -p no:cacheprovider tests/integration/clickhouse -vv \
  --junitxml="${artifact_dir}/clickhouse-junit.xml"
