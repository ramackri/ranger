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

# Core E2E: static regression + dynamic REST (promote/scale/409/400).
#
# Usage:
#   ./scripts/audit/verify-partition-plan-e2e.sh --static-only
#   ./scripts/audit/verify-partition-plan-e2e.sh --dynamic --restore-static
#   ./scripts/audit/verify-partition-plan-e2e.sh --dynamic --with-audit-smoke
#
# Full suite: ./scripts/audit/verify-partition-plan-e2e-all.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"

RUN_STATIC=false
RUN_DYNAMIC=false
DO_ENABLE=true
RESTORE_STATIC=false
AUDIT_SMOKE=false
TIMEOUT=300
PROMOTE_PLUGIN="storm"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --static-only) RUN_STATIC=true; shift ;;
    --dynamic) RUN_DYNAMIC=true; shift ;;
    --no-enable) DO_ENABLE=false; shift ;;
    --restore-static) RESTORE_STATIC=true; shift ;;
    --with-audit-smoke) AUDIT_SMOKE=true; shift ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    --promote-plugin) PROMOTE_PLUGIN="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '19,25p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "${RUN_STATIC}" != "true" && "${RUN_DYNAMIC}" != "true" ]]; then
  RUN_STATIC=true
  RUN_DYNAMIC=true
fi

PLAN_URL="$(pp_ingestor_plan_url "${CONTAINER}")"

run_static_tests() {
  echo ""
  echo "=== Static mode (dynamic.enabled=false) ==="
  if pp_dynamic_enabled "${CONTAINER}" "${SITE_XMLS[0]}"; then
    pp_record_fail "dynamic.enabled should be false (use --restore-static)"
    return
  fi
  pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
  if [[ "${HTTP_CODE}" == "503" ]]; then
    pp_record_pass "GET partition-plan returns 503 when disabled"
  else
    pp_record_fail "GET expected 503, got ${HTTP_CODE}"
  fi
  if curl -sf "http://localhost:7081/api/audit/health" >/dev/null; then
    pp_record_pass "GET /health still 200"
  else
    pp_record_fail "GET /health not reachable"
  fi
}

run_dynamic_tests() {
  echo ""
  echo "=== Dynamic mode ==="
  if [[ "${DO_ENABLE}" == "true" ]]; then
    echo "Preparing greenfield Kafka layout (static mode)..."
    pp_set_dynamic_enabled "${CONTAINER}" "false" "false" || true
    pp_prepare_audit_topic_for_greenfield "${CONTAINER}"
    pp_delete_plan_topic
    echo "Enabling dynamic partition plan..."
    pp_set_dynamic_enabled "${CONTAINER}" "true" || { pp_record_fail "enable dynamic"; return; }
    pp_wait_health 7081 "Ingestor after enable" "${TIMEOUT}" || { pp_record_fail "health after enable"; return; }
  fi
  if ! pp_wait_watcher "${CONTAINER}" "${TIMEOUT}"; then
    pp_record_fail "PartitionPlanWatcher not ready"
    return
  fi
  pp_record_pass "PartitionPlanWatcher ready"

  pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    pp_record_fail "GET expected 200, got ${HTTP_CODE}: ${HTTP_BODY}"
    return
  fi
  pp_record_pass "GET partition-plan returns 200"

  local version topic_count kafka_parts new_version plan_plugins expected_plugin_count promote_plugin
  version="$(pp_json_field "${HTTP_BODY}" version)"
  if [[ -n "${version}" && "${version}" -ge 1 ]]; then
    pp_record_pass "plan version=${version}"
  else
    pp_record_fail "invalid version: ${version}"
  fi

  plan_plugins="$(printf '%s' "${HTTP_BODY}" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('plugins') or {}))" 2>/dev/null || echo "")"
  local configured_plugins
  configured_plugins="$(pp_read_site_prop "${CONTAINER}" "ranger.audit.ingestor.kafka.configured.plugins")"
  if [[ -n "${configured_plugins}" ]]; then
    expected_plugin_count="$(printf '%s' "${configured_plugins}" | python3 -c "import sys; print(len([p for p in sys.stdin.read().split(',') if p.strip()]))")"
  else
    expected_plugin_count="0"
  fi
  topic_count="$(pp_json_field "${HTTP_BODY}" topicPartitionCount)"
  kafka_parts="$(pp_kafka_topic_partition_count "${AUDIT_TOPIC}")"
  if [[ "${version}" == "1" && "${plan_plugins}" == "${expected_plugin_count}" ]]; then
    if [[ "${expected_plugin_count}" == "0" ]]; then
      pp_record_pass "greenfield plan is buffer-only (configured.plugins empty in site XML)"
    else
      pp_record_pass "greenfield plan lists ${plan_plugins} configured plugins (site XML layout)"
    fi
  elif [[ "${version}" != "1" ]]; then
    pp_record_pass "plan has ${plan_plugins} plugins at v${version}"
  else
    pp_record_fail "greenfield expected ${expected_plugin_count} configured plugins, plan has ${plan_plugins}"
  fi
  if [[ -n "${topic_count}" && "${topic_count}" == "${kafka_parts}" ]]; then
    pp_record_pass "topicPartitionCount matches Kafka (${topic_count})"
  else
    pp_record_fail "topicPartitionCount=${topic_count} kafka=${kafka_parts}"
  fi

  local services_count
  services_count="$(printf '%s' "${HTTP_BODY}" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('services') or {}))" 2>/dev/null || echo "0")"
  if [[ "${services_count}" -ge 1 ]]; then
    pp_record_pass "plan includes services allowlist (${services_count} repos)"
  else
    pp_record_fail "plan missing services allowlist map"
  fi

  promote_plugin="${PROMOTE_PLUGIN}"
  if [[ "${PROMOTE_PLUGIN}" == "storm" ]]; then
    promote_plugin="$(pp_pick_buffer_promote_plugin "${CONTAINER}" "${PLAN_URL}")"
  fi

  if pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --list" | grep -qx "${PLAN_TOPIC}"; then
    pp_record_pass "plan topic ${PLAN_TOPIC} exists"
  else
    pp_record_fail "plan topic missing"
  fi

  echo ""
  echo "  Promote ${promote_plugin} (buffer plugin, not in configured.plugins)..."
  pp_ingestor_request "${CONTAINER}" POST "${PLAN_URL}/promote" \
    "{\"pluginId\":\"${promote_plugin}\",\"partitionCount\":2,\"expectedVersion\":${version}}"
  if [[ "${HTTP_CODE}" == "200" ]]; then
    new_version="$(pp_json_field "${HTTP_BODY}" version)"
    if [[ "${new_version}" -gt "${version}" ]] && echo "${HTTP_BODY}" | grep -q "\"${promote_plugin}\""; then
      pp_record_pass "promote ${promote_plugin} -> v${new_version}"
      version="${new_version}"
    else
      pp_record_fail "promote response invalid"
    fi
  else
    pp_record_fail "promote expected 200, got ${HTTP_CODE}: ${HTTP_BODY}"
  fi

  echo "  Stale expectedVersion (409)..."
  pp_ingestor_request "${CONTAINER}" POST "${PLAN_URL}/promote" \
    "{\"pluginId\":\"e2eStale409\",\"partitionCount\":2,\"expectedVersion\":1}"
  if [[ "${HTTP_CODE}" == "409" ]]; then
    pp_record_pass "stale expectedVersion -> 409"
  else
    pp_record_fail "stale expectedVersion expected 409, got ${HTTP_CODE}"
  fi

  echo "  Promote hdfs again (400)..."
  pp_ingestor_request "${CONTAINER}" POST "${PLAN_URL}/promote" \
    "{\"pluginId\":\"hdfs\",\"partitionCount\":2,\"expectedVersion\":${version}}"
  if [[ "${HTTP_CODE}" == "400" ]]; then
    pp_record_pass "duplicate hdfs promote -> 400"
  else
    pp_record_fail "duplicate promote expected 400, got ${HTTP_CODE}"
  fi

  echo "  Scale ${promote_plugin}..."
  pp_ingestor_request "${CONTAINER}" POST "${PLAN_URL}/scale" \
    "{\"pluginId\":\"${promote_plugin}\",\"additionalPartitions\":1,\"expectedVersion\":${version}}"
  if [[ "${HTTP_CODE}" == "200" ]]; then
    new_version="$(pp_json_field "${HTTP_BODY}" version)"
    if [[ "${new_version}" -gt "${version}" ]]; then
      pp_record_pass "scale -> v${new_version}"
      version="${new_version}"
    else
      pp_record_fail "scale did not bump version"
    fi
  else
    pp_record_fail "scale expected 200, got ${HTTP_CODE}: ${HTTP_BODY}"
  fi

  sleep 5
  pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
  if [[ "${HTTP_CODE}" == "200" && "$(pp_json_field "${HTTP_BODY}" version)" == "${version}" ]]; then
    pp_record_pass "GET stable at v${version}"
  else
    pp_record_fail "GET version mismatch after mutations"
  fi

  if [[ "${AUDIT_SMOKE}" == "true" ]]; then
    echo "  Audit pipeline smoke..."
    if ./scripts/audit/verify-audit-tier3-e2e.sh --timeout "${TIMEOUT}"; then
      pp_record_pass "verify-audit-tier3-e2e after mutations"
    else
      pp_record_fail "verify-audit-tier3-e2e failed"
    fi
  fi
}

echo "Partition plan E2E (static=${RUN_STATIC}, dynamic=${RUN_DYNAMIC})"
pp_preflight_tier3 "${TIMEOUT}"

if [[ "${RUN_STATIC}" == "true" && "${RUN_DYNAMIC}" == "true" && "${DO_ENABLE}" == "true" ]]; then
  run_static_tests
  run_dynamic_tests
elif [[ "${RUN_STATIC}" == "true" ]]; then
  run_static_tests
elif [[ "${RUN_DYNAMIC}" == "true" ]]; then
  run_dynamic_tests
fi

if [[ "${RESTORE_STATIC}" == "true" ]]; then
  echo ""
  echo "Restoring static mode..."
  pp_set_dynamic_enabled "${CONTAINER}" "false" || true
fi

pp_print_results
