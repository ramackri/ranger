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

# Tier 3 end-to-end: HDFS plugin audit -> ingestor -> Kafka -> Solr (+ Admin API).
# CI-friendly: exit 0 on pass, 1 on fail.
#
# Usage:
#   ./scripts/audit/verify-audit-tier3-e2e.sh
#   ./scripts/audit/verify-audit-tier3-e2e.sh --skip-smoke   # health + config only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

SMOKE=true
TIMEOUT=120
TEST_PATH="/tmp/audit-stack-e2e-$$"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-smoke) SMOKE=false; shift ;;
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

chmod +x scripts/audit/wait-for-audit-health.sh 2>/dev/null || true
./scripts/audit/wait-for-audit-health.sh --tier 3 --timeout "${TIMEOUT}"

echo ""
echo "Admin accessAudit API..."
if curl -sf -u 'admin:rangerR0cks!' \
  'http://localhost:6080/service/assets/accessAudit?pageSize=1&startIndex=0' >/dev/null; then
  echo "  accessAudit API: OK"
else
  echo "  accessAudit API: FAIL (Admin may need ensure-admin-audit-solr or Solr auth fix)" >&2
  exit 1
fi

if [[ "${SMOKE}" != "true" ]]; then
  echo "E2E smoke skipped (--skip-smoke)"
  exit 0
fi

echo ""
echo "HDFS audit smoke (Kerberos testuser1)..."

if [[ -x scripts/hadoop/run-hdfs-as-hdfs.sh ]]; then
  ./scripts/hadoop/run-hdfs-as-hdfs.sh mkdir -p "${TEST_PATH}" 2>/dev/null || \
    docker exec ranger-hadoop bash -c \
      "kinit -kt /etc/keytabs/hdfs.keytab hdfs/ranger-hadoop.rangernw@EXAMPLE.COM && hdfs dfs -mkdir -p ${TEST_PATH}" || true
else
  docker exec ranger-hadoop bash -c \
    "kinit -kt /etc/keytabs/hdfs.keytab hdfs/ranger-hadoop.rangernw@EXAMPLE.COM && hdfs dfs -mkdir -p ${TEST_PATH}" || true
fi

BEFORE="$(curl -sf "http://localhost:8983/solr/ranger_audits/select?q=reqUser:testuser1&rows=0&wt=json" \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('response',{}).get('numFound',0))" 2>/dev/null || echo 0)"

if [[ -x scripts/hadoop/run-hdfs-as-testuser1.sh ]]; then
  ./scripts/hadoop/run-hdfs-as-testuser1.sh ls "${TEST_PATH}" || exit 1
else
  docker exec ranger-hadoop bash -c \
    "kinit -kt /etc/keytabs/testuser1.keytab testuser1/ranger-hadoop.rangernw@EXAMPLE.COM && hdfs dfs -ls ${TEST_PATH}" || exit 1
fi

deadline=$((SECONDS + 90))
FOUND="${BEFORE}"
while (( SECONDS < deadline )); do
  sleep 10
  FOUND="$(curl -sf "http://localhost:8983/solr/ranger_audits/select?q=reqUser:testuser1&rows=0&wt=json" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('response',{}).get('numFound',0))" 2>/dev/null || echo 0)"
  if [[ "${FOUND}" -gt "${BEFORE}" ]]; then
    echo "  Solr reqUser:testuser1 numFound: ${BEFORE} -> ${FOUND} (PASS)"
    exit 0
  fi
done

echo "  Solr audit count did not increase (before=${BEFORE}, after=${FOUND})" >&2
echo "  Check: docker logs ranger-audit-ingestor --tail 40" >&2
echo "  Repair: ./scripts/hadoop/ensure-hdfs-plugin-audit-config.sh" >&2
exit 1
