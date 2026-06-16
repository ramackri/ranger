#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run full partition-plan E2E suite on Tier 3 Docker.
#
# Usage:
#   ./scripts/audit/verify-partition-plan-e2e-all.sh
#   ./scripts/audit/verify-partition-plan-e2e-all.sh --skip-kafka-down
#   ./scripts/audit/verify-partition-plan-e2e-all.sh --with-audit-smoke

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

SKIP_KAFKA_DOWN=false
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-kafka-down) SKIP_KAFKA_DOWN=true; shift ;;
    --with-audit-smoke) EXTRA_ARGS+=(--with-audit-smoke); shift ;;
    --timeout) EXTRA_ARGS+=(--timeout "${2:?}"); shift 2 ;;
    -h|--help)
      sed -n '19,23p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

chmod +x scripts/audit/verify-partition-plan-e2e.sh \
  scripts/audit/verify-partition-plan-multipod-e2e.sh \
  scripts/audit/verify-partition-plan-brownfield-e2e.sh \
  scripts/audit/verify-partition-plan-kafka-down-e2e.sh 2>/dev/null || true

run_step() {
  local name="$1"
  shift
  echo ""
  echo "########## ${name} ##########"
  if "$@"; then
    echo "########## ${name}: OK ##########"
  else
    echo "########## ${name}: FAILED ##########" >&2
    exit 1
  fi
}

run_step "Core (static + dynamic)" \
  ./scripts/audit/verify-partition-plan-e2e.sh --static-only

run_step "Core (dynamic REST)" \
  ./scripts/audit/verify-partition-plan-e2e.sh --dynamic ${EXTRA_ARGS+"${EXTRA_ARGS[@]}"} --restore-static

run_step "Multi-pod" \
  ./scripts/audit/verify-partition-plan-multipod-e2e.sh

run_step "Brownfield pre-seed" \
  ./scripts/audit/verify-partition-plan-brownfield-e2e.sh --restore-static

if [[ "${SKIP_KAFKA_DOWN}" != "true" ]]; then
  run_step "Kafka down (NEG-9)" \
    ./scripts/audit/verify-partition-plan-kafka-down-e2e.sh
fi

echo ""
echo "All partition-plan E2E suites passed."
