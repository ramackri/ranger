#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Health check for the full audit E2E stack (infra only — no plugin audits).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/audit/verify-audit-e2e-infrastructure.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=audit-stack-lib.sh
source "${SCRIPT_DIR}/scripts/audit/audit-stack-lib.sh"

export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"

fail=0
check_container() {
  local name="$1"
  if docker ps --filter "name=^${name}$" --filter status=running -q | grep -q .; then
    echo "OK  container ${name}"
  else
    echo "FAIL container ${name}" >&2
    fail=1
  fi
}

check_url() {
  local url="$1"
  local label="$2"
  if curl -sf -o /dev/null "${url}" 2>/dev/null; then
    echo "OK  ${label}"
  else
    echo "FAIL ${label} (${url})" >&2
    fail=1
  fi
}

echo "=== Audit E2E infrastructure ==="

for c in \
  ranger "ranger-${RANGER_DB_TYPE}" ranger-kdc ranger-zk ranger-solr ranger-kafka \
  ranger-audit-ingestor ranger-audit-dispatcher-solr ranger-audit-dispatcher-hdfs \
  ranger-hadoop ranger-hive ranger-hbase ozone-scm ozone-datanode ozone-om; do
  if docker ps -a --format '{{.Names}}' | grep -qx "${c}"; then
    check_container "${c}" || true
  fi
done

check_url "http://localhost:6080/login.jsp" "Ranger Admin UI"
check_url "http://localhost:7081/api/audit/health" "Audit ingestor"
check_url "http://localhost:7091/api/health/ping" "Solr audit dispatcher"
check_url "http://localhost:7092/api/health/ping" "HDFS audit dispatcher"

if audit_stack_solr_curl '/solr/ranger_audits/admin/ping?wt=json' 2>/dev/null \
  | grep -q '"status"[[:space:]]*:[[:space:]]*0'; then
  echo "OK  Solr ranger_audits ping"
else
  echo "FAIL Solr ranger_audits ping" >&2
  fail=1
fi

if docker exec ranger-kafka timeout 2 bash -c 'echo >/dev/tcp/localhost/9092' 2>/dev/null; then
  echo "OK  Kafka broker :9092"
else
  echo "FAIL Kafka broker :9092" >&2
  fail=1
fi

if docker exec ranger-zk bash -c 'echo ruok | nc localhost 2181' 2>/dev/null | grep -q imok; then
  echo "OK  Zookeeper ruok"
else
  echo "WARN Zookeeper ruok (nc may be unavailable)"
fi

if [[ "${fail}" -eq 0 ]]; then
  echo "PASS: audit E2E infrastructure"
else
  echo "FAIL: audit E2E infrastructure" >&2
fi
exit "${fail}"
