#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Production-style automation for Ranger audit-server Docker stack.
#
# Usage:
#   ./audit-stack.sh up --tier 1              # ingestor + Kafka only
#   ./audit-stack.sh up --tier 3 --verify     # full E2E (same as tier 4)
#   ./audit-stack.sh up --tier 4 --verify     # full E2E (HDFS + Ozone + dispatchers)
#   ./audit-stack.sh down --tier 4
#   ./audit-stack.sh verify --tier 4
#
# Prefer: ./setup-audit-e2e.sh
# From repo root: ./audit_in_docker up --tier 4 --verify

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=scripts/audit/audit-stack-lib.sh
source "${SCRIPT_DIR}/scripts/audit/audit-stack-lib.sh"

ACTION="${1:-}"
shift || true

TIER="4"
DO_BUILD=true
DOCKER_BUILD=false
DO_VERIFY=false

usage() {
  sed -n '22,32p' "${BASH_SOURCE[0]}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier) TIER="${2:?}"; shift 2 ;;
    --no-build) DO_BUILD=false; shift ;;
    --docker-build) DOCKER_BUILD=true; DO_BUILD=true; shift ;;
    --verify) DO_VERIFY=true; shift ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${ACTION}" ]]; then
  echo "ERROR: action required (up|down|restart|verify|prepare)" >&2
  usage >&2
  exit 1
fi

export RANGER_HOME="${RANGER_HOME:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/.env" 2>/dev/null || true
export RANGER_VERSION="${RANGER_VERSION:-3.0.0-SNAPSHOT}"
export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
export KERBEROS_ENABLED="${KERBEROS_ENABLED:-true}"

chmod +x prepare-audit-dist.sh setup-audit-e2e.sh scripts/audit/*.sh scripts/**/*.sh 2>/dev/null || true

run_prepare() {
  local -a prep=(./prepare-audit-dist.sh)
  if [[ "${DOCKER_BUILD}" == "true" ]]; then
    prep+=(--docker-build)
  elif [[ "${DO_BUILD}" == "true" ]]; then
    prep+=(--build-audit)
  else
    prep+=(--strict)
  fi
  "${prep[@]}"
}

load_compose_files() {
  COMPOSE_FILES=()
  while IFS= read -r f; do
    [[ -n "${f}" ]] && COMPOSE_FILES+=("${f}")
  done < <(audit_stack_compose_files "${TIER}")
}

e2e_args() {
  E2E_ARGS=(./setup-audit-e2e.sh)
  case "${ACTION}" in
    up) E2E_ARGS+=(up) ;;
    down) E2E_ARGS+=(down) ;;
    restart) E2E_ARGS+=(restart) ;;
    verify) E2E_ARGS+=(verify) ;;
  esac
  [[ "${DO_BUILD}" == "false" ]] && E2E_ARGS+=(--no-build)
  [[ "${DOCKER_BUILD}" == "true" ]] && E2E_ARGS+=(--docker-build)
  [[ "${DO_VERIFY}" == "false" ]] && E2E_ARGS+=(--no-verify)
}

case "${ACTION}" in
  prepare)
    run_prepare
    ;;
  up|down|restart|verify)
    if [[ "${TIER}" == "3" ]] || [[ "${TIER}" == "4" ]]; then
      e2e_args
      "${E2E_ARGS[@]}"
      exit 0
    fi
    if [[ "${TIER}" == "2" ]]; then
      echo "Tier 2 standalone stack removed. Use ./setup-audit-e2e.sh for full audit E2E." >&2
      exit 1
    fi
    if [[ "${ACTION}" == "up" ]]; then
      run_prepare
      docker network create rangernw 2>/dev/null || true
      load_compose_files
      docker compose "${COMPOSE_FILES[@]}" up -d --build
      ./scripts/audit/wait-for-audit-health.sh --tier "${TIER}" --timeout 600
      echo ""
      echo "Audit stack Tier ${TIER} is up."
      echo "  Ingestor: http://localhost:7081/api/audit/health"
      exit 0
    fi
    if [[ "${ACTION}" == "down" ]]; then
      load_compose_files
      docker compose "${COMPOSE_FILES[@]}" down
      exit 0
    fi
    ./scripts/audit/wait-for-audit-health.sh --tier "${TIER}" --timeout 120
    ;;
  *)
    echo "ERROR: unknown action '${ACTION}' (use up|down|restart|verify|prepare)" >&2
    exit 1
    ;;
esac
