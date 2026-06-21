#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Smoke: ranger-hbase + audit-server destination → ingestor (dev_hbase).
# RANGER-5644: Jersey JSON writers must be in plugin-impl (unlike Kafka #1020).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/hbase/verify-hbase-plugin-audit-e2e.sh
#   ./scripts/hbase/verify-hbase-plugin-audit-e2e.sh --full-e2e

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../audit/audit-stack-lib.sh
source "${DOCKER_DIR}/scripts/audit/audit-stack-lib.sh"

CONTAINER="${RANGER_HBASE_CONTAINER:-ranger-hbase}"
RANGER_SCRIPTS="${RANGER_SCRIPTS:-/home/ranger/scripts}"
FULL_E2E=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --full-e2e) FULL_E2E=true; shift ;;
    -h|--help)
      sed -n '10,14p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

fail=0

if ! docker inspect -f '{{.State.Running}}' "${CONTAINER}" 2>/dev/null | grep -q true; then
  echo "FAIL: ${CONTAINER} is not running" >&2
  exit 1
fi

echo "=== HBase Master port 16000 ==="
if docker exec "${CONTAINER}" timeout 3 bash -c 'echo >/dev/tcp/ranger-hbase.rangernw/16000' 2>/dev/null \
  || docker exec "${CONTAINER}" timeout 3 bash -c 'echo >/dev/tcp/localhost/16000' 2>/dev/null; then
  echo "OK: HBase Master listening"
else
  echo "FAIL: HBase Master not on port 16000" >&2
  fail=1
fi

echo "=== Repo name dev_hbase ==="
if docker exec "${CONTAINER}" grep -q 'dev_hbase' /opt/hbase/conf/ranger-hbase-security.xml 2>/dev/null; then
  echo "OK: ranger-hbase-security.xml uses dev_hbase"
else
  echo "WARN: dev_hbase not found in ranger-hbase-security.xml"
fi

echo "=== Audit XML ==="
if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/apply-hbase-plugin-audit-config.sh" --check-only; then
  echo "OK: auditserver URL + kerberos authn"
else
  echo "Applying audit ingestor URL..."
  "${SCRIPT_DIR}/apply-hbase-plugin-audit-config.sh" --no-restart || fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Ingestor allowlist dev_hbase ==="
  "${SCRIPT_DIR}/ensure-hbase-ingestor-allowlist.sh" || fail=1
  echo "=== Full E2E: trigger DENIED scan ==="
  SOLR_BEFORE="$(audit_stack_solr_count 'repo:dev_hbase' || echo 0)"
  echo "Solr repo:dev_hbase before: ${SOLR_BEFORE}"
  "${SCRIPT_DIR}/trigger-hbase-plugin-audit-e2e.sh" || fail=1
  echo "Waiting 45s for audit batch → ingestor → dispatcher → Solr..."
  sleep 45
fi

echo "=== HBase audit errors (MessageBodyWriter / batch send) ==="
RECENT="$(docker exec "${CONTAINER}" bash -c '
  grep -iE "MessageBodyWriter|Failed to send audit batch|Authentication failure|LinkageError" \
    /opt/hbase/logs/*.log 2>/dev/null | tail -12
' || true)"
if [[ -z "${RECENT}" ]]; then
  echo "OK: no audit REST client errors in HBase logs"
else
  echo "${RECENT}"
  if echo "${RECENT}" | grep -qiE 'MessageBodyWriter|LinkageError'; then
    echo "FAIL: audit REST client errors present (RANGER-5644 — check plugin-impl JARs)" >&2
    fail=1
  elif echo "${RECENT}" | grep -qi 'Failed to send audit batch'; then
    if [[ "${FULL_E2E}" == "true" ]] && echo "${RECENT}" | grep -qi 'HTTP status: 403'; then
      echo "FAIL: ingestor rejected dev_hbase audits (check allowlist)" >&2
      fail=1
    elif [[ "${FULL_E2E}" != "true" ]]; then
      echo "WARN: audit batch errors in logs (run --full-e2e after config fixes)"
    fi
  fi
fi

echo "=== Ingestor dev_hbase ==="
INGESTOR_LINES="$(docker logs ranger-audit-ingestor 2>&1 | grep -iE 'dev_hbase|service=dev_hbase|Unauthorized user.*hbase' | tail -8 || true)"
if [[ -n "${INGESTOR_LINES}" ]]; then
  echo "${INGESTOR_LINES}"
else
  echo "WARN: no dev_hbase lines in ingestor log"
  [[ "${FULL_E2E}" == "true" ]] && fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Solr repo:dev_hbase ==="
  SOLR_AFTER="$(audit_stack_solr_count 'repo:dev_hbase' || echo 0)"
  echo "Solr repo:dev_hbase after: ${SOLR_AFTER}"
  if [[ "${SOLR_AFTER}" -gt "${SOLR_BEFORE}" ]]; then
    echo "OK: Solr indexed new dev_hbase audit(s)"
    audit_stack_solr_curl '/solr/ranger_audits/select?q=repo:dev_hbase&rows=1&sort=evtTime+desc&wt=json' \
      | python3 -c "
import sys, json
doc = json.load(sys.stdin).get('response', {}).get('docs', [{}])[0]
for k in ('repo', 'reqUser', 'access', 'resource', 'evtTime'):
    if k in doc:
        print(f'  {k}: {doc[k]}')
" 2>/dev/null || true
  else
    echo "FAIL: Solr repo:dev_hbase count did not increase (${SOLR_BEFORE} -> ${SOLR_AFTER})" >&2
    fail=1
  fi
fi

echo "=== Tarball / assembly verify ==="
(
  cd "${DOCKER_DIR}"
  ./scripts/audit/verify-plugin-auditserver-jars.sh --hbase-only --check-assembly
) || fail=1

if [[ "${fail}" -eq 0 ]]; then
  if [[ "${FULL_E2E}" == "true" ]]; then
    echo "PASS: HBase plugin full audit E2E (dev_hbase → ingestor → Solr)"
  else
    echo "PASS: HBase plugin audit-server smoke (RANGER-5644)"
  fi
else
  echo "FAIL: HBase plugin audit verification" >&2
fi
exit "${fail}"
