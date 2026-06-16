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

# NEG-9: dynamic.enabled=true with Kafka down should fail ingestor startup.
#
# Usage:
#   ./scripts/audit/verify-partition-plan-kafka-down-e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"

TIMEOUT=120

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '17,19p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

restore_stack() {
  echo ""
  echo "Restoring Kafka and ingestor..."
  docker start "${KAFKA_CONTAINER}" >/dev/null 2>&1 || true
  sleep 25
  pp_set_dynamic_enabled "${CONTAINER}" "false" || true
  docker restart "${CONTAINER}" >/dev/null 2>&1 || true
  sleep 35
  pp_wait_health 7081 "Ingestor recovery" "${TIMEOUT}" || true
}
trap restore_stack EXIT

echo "=== Partition plan Kafka-down E2E (NEG-9) ==="
pp_require_container "${CONTAINER}"

echo ""
echo "Enabling dynamic mode in config (no restart yet)..."
pp_set_dynamic_enabled "${CONTAINER}" "true" "false"

echo "Stopping Kafka..."
docker stop "${KAFKA_CONTAINER}" >/dev/null
sleep 5

echo "Restarting ingestor with dynamic=true and Kafka down..."
log_since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker restart "${CONTAINER}" >/dev/null
# createAuditsTopic retries (~11 × 3s) before watcher fails when Kafka is down
sleep 90

failed=false
collect_ingestor_logs() {
  {
    docker logs --since "${log_since}" "${CONTAINER}" 2>&1
    docker exec "${CONTAINER}" bash -c \
      'for f in /var/log/ranger/audit-ingestor/catalina.out /var/log/ranger/audit-ingestor/ranger-audit-ingestor.log; do
         test -f "$f" && tail -n 300 "$f";
       done' 2>/dev/null
  }
}
# Avoid pipefail + grep -q SIGPIPE when docker logs is large (set -euo pipefail).
logs="$(collect_ingestor_logs)"

if curl -sf "http://localhost:7081/api/audit/health" >/dev/null 2>&1; then
  if grep -qiE 'PartitionPlan|watcher|Failed to.*partition plan|Kafka' <<< "${logs}"; then
    if grep -q "Partition plan watcher ready" <<< "${logs}"; then
      pp_record_fail "ingestor health OK and watcher ready with Kafka down (expected startup failure)"
      failed=true
    else
      pp_record_pass "health up but partition plan watcher not ready (degraded)"
    fi
  else
    pp_record_fail "ingestor healthy with Kafka down — expected partition plan startup failure in logs"
    failed=true
  fi
else
  pp_record_pass "ingestor not healthy while Kafka is down"
fi

if grep -qiE 'Failed to start partition plan watcher|Failed to read partition plan|Failed to update partition plan|PartitionPlanException|partition plan watcher|Audit topic must be created before starting partition plan watcher|createAuditsTopicIfNotExists|Failed to connect to Kafka|createPartitionPlanTopic|NetworkClient|TimeoutException|Broker may not be available|BeanCreationException|Context \[\] startup failed' <<< "${logs}"; then
  pp_record_pass "ingestor logs show partition-plan/Kafka error"
else
  pp_record_fail "no partition-plan/Kafka error found in ingestor logs"
  failed=true
fi

restore_stack
trap - EXIT

echo ""
echo "Verifying recovery after Kafka restore..."
if curl -sf "http://localhost:7081/api/audit/health" >/dev/null; then
  pp_record_pass "ingestor healthy after Kafka restore + static mode"
else
  pp_record_fail "ingestor not healthy after recovery"
fi

pp_print_results
