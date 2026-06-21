#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Smoke + optional full E2E: Knox plugin → audit-ingestor (dev_knox) → Solr.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/knox/verify-knox-plugin-audit-e2e.sh
#   ./scripts/knox/verify-knox-plugin-audit-e2e.sh --full-e2e

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../audit/audit-stack-lib.sh
source "${DOCKER_DIR}/scripts/audit/audit-stack-lib.sh"

CONTAINER="${RANGER_KNOX_CONTAINER:-ranger-knox}"
RANGER_SCRIPTS="${RANGER_SCRIPTS:-/home/ranger/scripts}"
GATEWAY_URL="${KNOX_GATEWAY_URL:-https://localhost:8443}"
GATEWAY_USER="${KNOX_GATEWAY_USER:-guest}"
GATEWAY_PASSWORD="${KNOX_GATEWAY_PASSWORD:-guest-password}"
WEBHDFS_PATH="${KNOX_E2E_WEBHDFS_PATH:-/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS}"
FULL_E2E=false

knox_gateway_http_code() {
  curl -sk -o /dev/null -w '%{http_code}' \
    -u "${GATEWAY_USER}:${GATEWAY_PASSWORD}" \
    "${GATEWAY_URL}${WEBHDFS_PATH}" 2>/dev/null || echo "000"
}

knox_gateway_ok() {
  local code
  code="$(knox_gateway_http_code)"
  [[ "${code}" =~ ^(200|401|403)$ ]]
}

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

echo "=== Knox gateway port 8443 (WebHDFS) ==="
GATEWAY_CODE="$(knox_gateway_http_code)"
echo "HTTP ${GATEWAY_CODE} on ${WEBHDFS_PATH}"
if knox_gateway_ok; then
  echo "OK: Knox gateway responding (200/401/403 on WebHDFS path)"
else
  echo "FAIL: Knox gateway not reachable on 8443 WebHDFS path" >&2
  fail=1
fi

echo "=== Repo name dev_knox ==="
if docker exec "${CONTAINER}" grep -q 'dev_knox' /opt/knox/conf/ranger-knox-security.xml 2>/dev/null; then
  echo "OK: ranger-knox-security.xml uses dev_knox"
else
  echo "WARN: dev_knox not found in ranger-knox-security.xml"
fi

echo "=== Audit XML ==="
if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/apply-knox-plugin-audit-config.sh" --check-only 2>/dev/null; then
  echo "OK: auditserver URL + kerberos authn"
else
  echo "Applying audit ingestor URL..."
  "${SCRIPT_DIR}/apply-knox-plugin-audit-config.sh" --no-restart || fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Hadoop backend (WebHDFS) ==="
  if docker ps --filter name=^ranger-hadoop$ --filter status=running -q | grep -q .; then
    echo "OK: ranger-hadoop running"
  else
    echo "FAIL: ranger-hadoop required for Knox sandbox WebHDFS E2E" >&2
    fail=1
  fi
  echo "=== Ingestor allowlist dev_knox ==="
  "${SCRIPT_DIR}/ensure-knox-ingestor-allowlist.sh" || fail=1
  echo "=== Full E2E: trigger WebHDFS via gateway ==="
  SOLR_BEFORE="$(audit_stack_solr_count 'repo:dev_knox' || echo 0)"
  echo "Solr repo:dev_knox before: ${SOLR_BEFORE}"
  "${SCRIPT_DIR}/trigger-knox-plugin-audit-e2e.sh" || fail=1
  echo "Waiting 45s for audit batch → ingestor → dispatcher → Solr..."
  sleep 45
fi

echo "=== Knox audit errors ==="
# Recent gateway.log tail only — ignore historical batch errors when Solr count increased.
RECENT="$(docker exec "${CONTAINER}" bash -c '
  log=/opt/knox/logs/gateway.log
  [[ -f "${log}" ]] || exit 0
  tail -80 "${log}" | grep -iE "MessageBodyWriter|Failed to send audit batch|Authentication failure|LinkageError" | tail -12
' || true)"
SOLR_PROVED=false
if [[ "${FULL_E2E}" == "true" ]]; then
  SOLR_AFTER_CHECK="$(audit_stack_solr_count 'repo:dev_knox' || echo 0)"
  [[ "${SOLR_AFTER_CHECK}" -gt "${SOLR_BEFORE:-0}" ]] && SOLR_PROVED=true
fi
if [[ -z "${RECENT}" ]]; then
  echo "OK: no recent audit REST client errors in Knox logs"
else
  echo "${RECENT}"
  if echo "${RECENT}" | grep -qiE 'MessageBodyWriter|LinkageError'; then
    echo "FAIL: audit REST client errors in Knox logs" >&2
    fail=1
  elif echo "${RECENT}" | grep -qi 'Failed to send audit batch'; then
    if [[ "${FULL_E2E}" == "true" ]] && echo "${RECENT}" | grep -qi 'HTTP status: 403'; then
      echo "FAIL: ingestor rejected dev_knox audits (check allowlist)" >&2
      fail=1
    elif [[ "${SOLR_PROVED}" == "true" ]]; then
      echo "WARN: stale Knox batch errors in log; Solr count increased — treating as non-fatal"
    elif [[ "${FULL_E2E}" == "true" ]]; then
      echo "FAIL: Knox audit batch errors and Solr count did not increase" >&2
      fail=1
    fi
  fi
fi

echo "=== Ingestor dev_knox ==="
INGESTOR_LINES="$(docker logs ranger-audit-ingestor 2>&1 | grep -iE 'dev_knox|service=dev_knox|Unauthorized user.*knox' | tail -8 || true)"
if [[ -n "${INGESTOR_LINES}" ]]; then
  echo "${INGESTOR_LINES}"
elif [[ "${SOLR_PROVED}" == "true" ]]; then
  echo "WARN: no dev_knox lines in ingestor log (Solr count increased — pipeline OK)"
else
  echo "WARN: no dev_knox lines in ingestor log"
  [[ "${FULL_E2E}" == "true" ]] && fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Solr repo:dev_knox ==="
  SOLR_AFTER="$(audit_stack_solr_count 'repo:dev_knox' || echo 0)"
  echo "Solr repo:dev_knox after: ${SOLR_AFTER}"
  if [[ "${SOLR_AFTER}" -gt "${SOLR_BEFORE}" ]]; then
    echo "OK: Solr indexed new dev_knox audit(s)"
    audit_stack_solr_curl '/solr/ranger_audits/select?q=repo:dev_knox&rows=1&sort=evtTime+desc&wt=json' \
      | python3 -c "
import sys, json
doc = json.load(sys.stdin).get('response', {}).get('docs', [{}])[0]
for k in ('repo', 'reqUser', 'access', 'resource', 'evtTime'):
    if k in doc:
        print(f'  {k}: {doc[k]}')
" 2>/dev/null || true
  else
    echo "FAIL: Solr repo:dev_knox count did not increase (${SOLR_BEFORE} -> ${SOLR_AFTER})" >&2
    fail=1
  fi
fi

if [[ "${fail}" -eq 0 ]]; then
  if [[ "${FULL_E2E}" == "true" ]]; then
    echo "PASS: Knox plugin full audit E2E (dev_knox → ingestor → Solr)"
  else
    echo "PASS: Knox plugin audit-server smoke"
  fi
else
  echo "FAIL: Knox plugin audit verification" >&2
fi
exit "${fail}"
