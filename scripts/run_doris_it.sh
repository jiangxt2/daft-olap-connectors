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
compose_file="${project_root}/docker/doris/compose.yaml"
compose_project="daft-olap-it-doris"
artifact_dir="${project_root}/.artifacts/it"
readiness_pid=""

mkdir -p "${artifact_dir}"

cleanup() {
  local status=$?
  if [[ -n "${readiness_pid}" ]] && kill -0 "${readiness_pid}" 2>/dev/null; then
    kill "${readiness_pid}" 2>/dev/null || true
    wait "${readiness_pid}" 2>/dev/null || true
  fi
  if [[ ${status} -ne 0 ]]; then
    docker compose --project-name "${compose_project}" --file "${compose_file}" logs --no-color \
      >"${artifact_dir}/doris-compose.log" 2>&1 || true
  fi
  docker compose --project-name "${compose_project}" --file "${compose_file}" down \
    --volumes --remove-orphans
  return "${status}"
}
trap cleanup EXIT

wait_for_doris() {
  uv run --all-extras python scripts/wait_for_service.py doris &
  readiness_pid=$!

  while kill -0 "${readiness_pid}" 2>/dev/null; do
    local exited_services
    exited_services="$(
      docker compose --project-name "${compose_project}" --file "${compose_file}" \
        ps --all --status exited --services
    )"
    if [[ -n "${exited_services}" ]]; then
      printf 'Doris services exited before readiness:\n%s\n' "${exited_services}" >&2
      kill "${readiness_pid}" 2>/dev/null || true
      wait "${readiness_pid}" 2>/dev/null || true
      readiness_pid=""
      return 1
    fi
    sleep 2
  done

  local status
  if wait "${readiness_pid}"; then
    readiness_pid=""
    return 0
  else
    status=$?
    readiness_pid=""
    return "${status}"
  fi
}

cd "${project_root}"
docker compose --project-name "${compose_project}" --file "${compose_file}" up --detach --build
wait_for_doris
uv run --all-extras python scripts/init_doris.py
uv run --all-extras pytest -p no:cacheprovider tests/integration/doris -vv \
  --junitxml="${artifact_dir}/doris-junit.xml"
