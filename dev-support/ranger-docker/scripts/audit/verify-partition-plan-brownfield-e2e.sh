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

# POS-F1/F2: pre-seed plan v1 in Kafka, enable dynamic, verify; optional rollback to static.
#
# Usage:
#   ./scripts/audit/verify-partition-plan-brownfield-e2e.sh
#   ./scripts/audit/verify-partition-plan-brownfield-e2e.sh --restore-static

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"

RESTORE_STATIC=false
TIMEOUT=180
SEED_FILE="/tmp/pp-brownfield-seed-$$.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --restore-static) RESTORE_STATIC=true; shift ;;
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
  rm -f "${SEED_FILE}"
}
trap cleanup EXIT

echo "=== Partition plan brownfield E2E ==="
pp_preflight_tier3 "${TIMEOUT}"

PLAN_URL="$(pp_ingestor_plan_url "${CONTAINER}")"

echo ""
echo "Step 1: bootstrap reference plan (temporary enable)..."
pp_set_dynamic_enabled "${CONTAINER}" "true"
if ! pp_wait_watcher "${CONTAINER}" "${TIMEOUT}"; then
  pp_record_fail "watcher not ready for reference plan"
  pp_print_results; exit 1
fi
pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
if [[ "${HTTP_CODE}" != "200" ]]; then
  pp_record_fail "could not GET reference plan: ${HTTP_CODE}"
  pp_print_results; exit 1
fi
printf '%s' "${HTTP_BODY}" > "${SEED_FILE}"
python3 -c "import json,sys; json.load(open('${SEED_FILE}'))" || { pp_record_fail "invalid reference plan JSON"; exit 1; }
ref_version="$(pp_json_field "${HTTP_BODY}" version)"
pp_record_pass "captured reference plan version ${ref_version}"

echo ""
echo "Step 2: return to static and wipe plan topic..."
pp_set_dynamic_enabled "${CONTAINER}" "false"
pp_delete_plan_topic
pp_record_pass "plan topic deleted (static mode)"

echo ""
echo "Step 3: pre-seed plan in Kafka (Path A brownfield)..."
python3 <<PY
import json
plan = json.load(open("${SEED_FILE}"))
plan["updatedBy"] = "brownfield-e2e-seed"
plan["version"] = 1
json.dump(plan, open("${SEED_FILE}", "w"))
PY
pp_seed_plan_file "${SEED_FILE}"
pp_record_pass "seeded plan v1 with updatedBy=brownfield-e2e-seed"

echo ""
echo "Step 4: enable dynamic — ingestor must read seed, not re-bootstrap..."
pp_set_dynamic_enabled "${CONTAINER}" "true"
if ! pp_wait_watcher "${CONTAINER}" "${TIMEOUT}"; then
  pp_record_fail "watcher not ready after brownfield enable"
  pp_print_results; exit 1
fi
pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
seed_by="$(pp_json_field "${HTTP_BODY}" updatedBy)"
got_v="$(pp_json_field "${HTTP_BODY}" version)"
if [[ "${HTTP_CODE}" == "200" && "${seed_by}" == "brownfield-e2e-seed" ]]; then
  pp_record_pass "GET shows pre-seeded updatedBy (no XML bootstrap overwrite)"
else
  pp_record_fail "expected updatedBy=brownfield-e2e-seed got '${seed_by}' (code ${HTTP_CODE})"
fi
if [[ "${got_v}" == "1" ]]; then
  pp_record_pass "plan version is 1 after pre-seed"
else
  pp_record_fail "expected version 1 got ${got_v}"
fi

topic_count="$(pp_json_field "${HTTP_BODY}" topicPartitionCount)"
kafka_parts="$(pp_kafka_topic_partition_count "${AUDIT_TOPIC}")"
if [[ -n "${topic_count}" && "${topic_count}" == "${kafka_parts}" ]]; then
  pp_record_pass "topicPartitionCount matches Kafka (${topic_count})"
else
  pp_record_fail "topicPartitionCount ${topic_count} vs kafka ${kafka_parts}"
fi

if [[ "${RESTORE_STATIC}" == "true" ]]; then
  echo ""
  echo "Step 5: rollback to static mode (POS-F2)..."
  pp_set_dynamic_enabled "${CONTAINER}" "false"
  pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
  if [[ "${HTTP_CODE}" == "503" ]]; then
    pp_record_pass "partition-plan API 503 after rollback to static"
  else
    pp_record_fail "expected 503 after static rollback, got ${HTTP_CODE}"
  fi
  if curl -sf "http://localhost:7081/api/audit/health" >/dev/null; then
    pp_record_pass "ingestor healthy in static mode after rollback"
  else
    pp_record_fail "ingestor unhealthy after static rollback"
  fi
fi

pp_print_results
