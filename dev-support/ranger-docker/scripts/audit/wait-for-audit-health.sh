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

# Poll audit stack health until ready or timeout. Exit 0 = all checks passed.
#
# Usage:
#   ./scripts/audit/wait-for-audit-health.sh --tier 3
#   ./scripts/audit/wait-for-audit-health.sh --tier 3 --timeout 600

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=audit-stack-lib.sh
source "${SCRIPT_DIR}/scripts/audit/audit-stack-lib.sh"

TIER=""
TIMEOUT=600
TIER2_FULL=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier) TIER="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    --tier2-full) TIER2_FULL=true; shift ;;
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

[[ -n "${TIER}" ]] || { echo "ERROR: --tier required" >&2; exit 1; }

FAIL=0
echo "Waiting for audit stack Tier ${TIER} (timeout ${TIMEOUT}s)..."

audit_stack_wait_url "http://localhost:6080/login.jsp" "Admin UI (6080)" "${TIMEOUT}" || FAIL=1
audit_stack_wait_url "http://localhost:7081/api/audit/health" "Ingestor (7081)" "${TIMEOUT}" || FAIL=1

if [[ "${TIER}" != "1" ]]; then
  audit_stack_wait_url "http://localhost:7091/api/health/ping" "Solr dispatcher (7091)" "${TIMEOUT}" || FAIL=1
  audit_stack_wait_solr "Solr collection" "${TIMEOUT}" || FAIL=1
fi

if [[ "${TIER}" == "3" ]] || [[ "${TIER}" == "4" ]] || [[ "${TIER2_FULL}" == "true" ]]; then
  audit_stack_wait_container "ranger-hadoop" "Hadoop (ranger-hadoop)" "${TIMEOUT}" || FAIL=1
  if docker exec ranger-hadoop test -f /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml 2>/dev/null; then
    echo -n "  HDFS auditserver config..."
    if docker exec ranger-hadoop python3 - /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
for prop in root.findall("property"):
    n = prop.find("name")
    if n is not None and (n.text or "").strip() == "xasecure.audit.destination.auditserver":
        v = prop.find("value")
        ok = v is not None and (v.text or "").strip().lower() == "true"
        sys.exit(0 if ok else 1)
sys.exit(1)
PY
    then
      echo " OK"
    else
      echo " FAIL (auditserver not true)"
      FAIL=1
    fi
  else
    echo "  HDFS audit XML: missing"
    FAIL=1
  fi
fi

if [[ "${TIER}" == "4" ]]; then
  audit_stack_wait_container "ozone-scm" "Ozone SCM" "${TIMEOUT}" || FAIL=1
  audit_stack_wait_container "ozone-datanode" "Ozone datanode" "${TIMEOUT}" || FAIL=1
  audit_stack_wait_container "ozone-om" "Ozone OM" "${TIMEOUT}" || FAIL=1
  audit_stack_wait_container "ranger-kafka" "Kafka (ranger-kafka)" "${TIMEOUT}" || FAIL=1
  audit_stack_wait_url "http://localhost:7092/api/health/ping" "HDFS dispatcher (7092)" "${TIMEOUT}" || FAIL=1
  if docker exec ozone-om test -f /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml 2>/dev/null; then
    echo -n "  Ozone auditserver config..."
    if docker exec ozone-om python3 - /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
for prop in root.findall("property"):
    n = prop.find("name")
    if n is not None and (n.text or "").strip() == "xasecure.audit.destination.auditserver":
        v = prop.find("value")
        ok = v is not None and (v.text or "").strip().lower() == "true"
        sys.exit(0 if ok else 1)
sys.exit(1)
PY
    then
      echo " OK"
    else
      echo " FAIL (auditserver not true)"
      FAIL=1
    fi
  else
    echo "  Ozone audit XML: missing"
    FAIL=1
  fi
fi

if [[ "${FAIL}" -ne 0 ]]; then
  echo "Health gate FAILED" >&2
  exit 1
fi

echo "Health gate PASSED"
exit 0
