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

# POS-D1/D2: two ingestor replicas share the same Kafka plan via watcher.
#
# Usage:
#   ./scripts/audit/verify-partition-plan-multipod-e2e.sh
#   ./scripts/audit/verify-partition-plan-multipod-e2e.sh --no-cleanup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"

CLEANUP=true
TIMEOUT=300
PROMOTE_PLUGIN="storm"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-cleanup) CLEANUP=false; shift ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '19,22p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

cleanup() {
  if [[ "${CLEANUP}" == "true" ]]; then
    pp_stop_ingestor_replica
  fi
}
trap cleanup EXIT

echo "=== Partition plan multi-pod E2E ==="
pp_preflight_tier3 "${TIMEOUT}"

echo ""
echo "Preparing primary ingestor (dynamic mode)..."
pp_set_dynamic_enabled "${CONTAINER}" "true"
if ! pp_wait_watcher "${CONTAINER}" "${TIMEOUT}"; then
  pp_record_fail "primary watcher not ready"
  pp_print_results; exit 1
fi
pp_record_pass "primary watcher ready"

echo ""
echo "Starting replica ${REPLICA_CONTAINER} on :7082..."
if ! pp_start_ingestor_replica; then
  pp_record_fail "replica failed to start"
  pp_print_results; exit 1
fi
if ! pp_wait_watcher "${REPLICA_CONTAINER}" "${TIMEOUT}"; then
  pp_record_fail "replica watcher not ready"
  pp_print_results; exit 1
fi
pp_record_pass "replica watcher ready"

PRIMARY_PLAN_URL="$(pp_ingestor_plan_url "${CONTAINER}")"
REPLICA_PLAN_URL="$(pp_ingestor_plan_url "${REPLICA_CONTAINER}")"
pp_ingestor_request "${CONTAINER}" GET "${PRIMARY_PLAN_URL}"
v1="$(pp_json_field "${HTTP_BODY}" version)"
if [[ "${HTTP_CODE}" == "200" && -n "${v1}" ]]; then
  pp_record_pass "primary GET version=${v1}"
else
  pp_record_fail "primary GET failed: ${HTTP_CODE}"
fi

pp_ingestor_request "${REPLICA_CONTAINER}" GET "${REPLICA_PLAN_URL}"
v2="$(pp_json_field "${HTTP_BODY}" version)"
if [[ "${HTTP_CODE}" == "200" && "${v2}" == "${v1}" ]]; then
  pp_record_pass "replica GET matches primary version ${v1}"
else
  pp_record_fail "replica version ${v2} != primary ${v1}"
fi

echo ""
promote_plugin="$(pp_pick_buffer_promote_plugin "${CONTAINER}" "${PRIMARY_PLAN_URL}")"
echo "Promote ${promote_plugin} on primary only (buffer plugin)..."
promote_body="{\"pluginId\":\"${promote_plugin}\",\"partitionCount\":2,\"expectedVersion\":${v1}}"
pp_ingestor_request "${CONTAINER}" POST "${PRIMARY_PLAN_URL}/plugins" "${promote_body}"
new_v="$(pp_json_field "${HTTP_BODY}" version)"
if [[ "${HTTP_CODE}" == "200" && "${new_v}" -gt "${v1}" ]]; then
  pp_record_pass "primary promote -> version ${new_v}"
else
  pp_record_fail "primary promote failed: ${HTTP_CODE} ${HTTP_BODY}"
fi

echo "Waiting for replica watcher (≤35s)..."
sleep 35
pp_ingestor_request "${REPLICA_CONTAINER}" GET "${REPLICA_PLAN_URL}"
replica_v="$(pp_json_field "${HTTP_BODY}" version)"
if [[ "${HTTP_CODE}" == "200" && "${replica_v}" == "${new_v}" ]]; then
  pp_record_pass "replica converged to version ${new_v} after promote"
else
  pp_record_fail "replica version ${replica_v} expected ${new_v}"
fi

pp_ingestor_request "${CONTAINER}" GET "${PRIMARY_PLAN_URL}"
if [[ "${HTTP_CODE}" == "200" && "$(pp_json_field "${HTTP_BODY}" version)" == "${new_v}" ]]; then
  pp_record_pass "primary still at version ${new_v}"
else
  pp_record_fail "primary version drift after promote"
fi

pp_print_results
