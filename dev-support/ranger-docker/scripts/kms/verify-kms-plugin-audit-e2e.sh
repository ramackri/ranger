#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Smoke + optional full E2E: KMS plugin → audit-ingestor (dev_kms) → Solr.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kms/verify-kms-plugin-audit-e2e.sh
#   ./scripts/kms/verify-kms-plugin-audit-e2e.sh --full-e2e

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../audit/audit-stack-lib.sh
source "${DOCKER_DIR}/scripts/audit/audit-stack-lib.sh"
# shellcheck source=kms-e2e-lib.sh
source "${SCRIPT_DIR}/kms-e2e-lib.sh"

CONTAINER="${RANGER_KMS_CONTAINER:-ranger-kms}"
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
SOLR_PROVED=false
SOLR_BEFORE=0

if ! docker inspect -f '{{.State.Running}}' "${CONTAINER}" 2>/dev/null | grep -q true; then
  echo "FAIL: ${CONTAINER} is not running" >&2
  exit 1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== KMS plugin-impl Jackson/Jersey (audit client) ==="
  docker network connect rangernw ranger 2>/dev/null || true
  "${SCRIPT_DIR}/ensure-kms-plugin-audit-jars.sh" || fail=1
fi

echo "=== Audit XML ==="
if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/apply-kms-plugin-audit-config.sh" --check-only 2>/dev/null; then
  echo "OK: auditserver URL + kerberos authn"
else
  echo "Applying audit ingestor URL (with KMS restart)..."
  "${SCRIPT_DIR}/apply-kms-plugin-audit-config.sh" || fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "Restarting KMS (audit XML + plugin-impl classpath)..."
  docker restart "${CONTAINER}" >/dev/null
  for _ in $(seq 1 30); do
    kms_e2e_ok && break
    sleep 5
  done
fi

echo "=== Ranger KMS port 9292 (simple auth) ==="
KMS_CODE="$(kms_e2e_http_code GET '/kms/v1/keys/names')"
echo "GET /keys/names HTTP: ${KMS_CODE}"
if [[ "${KMS_CODE}" == "200" ]]; then
  echo "OK: KMS REST responding on 9292"
else
  echo "FAIL: KMS not reachable or auth failed (expected 200 with user.name=${KMS_E2E_USER:-keyadmin})" >&2
  fail=1
fi

echo "=== Repo name dev_kms ==="
if docker exec "${CONTAINER}" grep -q 'dev_kms' /opt/ranger/kms/ews/webapp/WEB-INF/classes/conf/ranger-kms-security.xml 2>/dev/null; then
  echo "OK: ranger-kms-security.xml uses dev_kms"
else
  echo "WARN: dev_kms not found in ranger-kms-security.xml"
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== dev_kms keyadmin policy ==="
  "${SCRIPT_DIR}/ensure-dev-kms-e2e-policy.sh" || fail=1
  echo "=== dev_kms audit exclude ==="
  "${SCRIPT_DIR}/clear-dev-kms-audit-exclude.sh" || fail=1
  echo "=== Ingestor allowlist dev_kms ==="
  "${SCRIPT_DIR}/ensure-kms-ingestor-allowlist.sh" || fail=1
  echo "=== Full E2E: trigger KMS key ops ==="
  SOLR_BEFORE="$(audit_stack_solr_count 'repo:dev_kms' || echo 0)"
  echo "Solr repo:dev_kms before: ${SOLR_BEFORE}"
  "${SCRIPT_DIR}/trigger-kms-plugin-audit-e2e.sh" || fail=1
  echo "Waiting 45s for audit batch → ingestor → dispatcher → Solr..."
  sleep 45
  SOLR_AFTER_CHECK="$(audit_stack_solr_count 'repo:dev_kms' || echo 0)"
  [[ "${SOLR_AFTER_CHECK}" -gt "${SOLR_BEFORE:-0}" ]] && SOLR_PROVED=true
fi

echo "=== KMS audit errors ==="
RECENT="$(docker exec "${CONTAINER}" bash -c '
  for log in /var/log/ranger/kms/kms-audit-*.log /var/log/ranger/kms/ranger-kms-*.log; do
    [[ -f "${log}" ]] || continue
    tail -30 "${log}" | grep -iE "Failed to send audit batch|MessageBodyWriter|Authentication failure" | tail -8
  done
' || true)"
if [[ -z "${RECENT}" ]]; then
  echo "OK: no audit REST client errors in KMS logs"
else
  echo "${RECENT}"
  if echo "${RECENT}" | grep -qiE 'MessageBodyWriter'; then
    if [[ "${SOLR_PROVED:-false}" == "true" ]]; then
      echo "WARN: stale KMS batch errors in log; Solr count increased — treating as non-fatal"
    else
      echo "FAIL: audit REST client errors in KMS logs" >&2
      fail=1
    fi
  elif echo "${RECENT}" | grep -qi 'Failed to send audit batch'; then
    if [[ "${FULL_E2E}" == "true" ]] && echo "${RECENT}" | grep -qi 'HTTP status: 403'; then
      echo "FAIL: ingestor rejected dev_kms audits (check allowlist)" >&2
      fail=1
    elif [[ "${SOLR_PROVED:-false}" == "true" ]]; then
      echo "WARN: stale KMS batch errors in log; Solr count increased — treating as non-fatal"
    elif [[ "${FULL_E2E}" == "true" ]]; then
      echo "FAIL: KMS audit batch errors and Solr count did not increase" >&2
      fail=1
    fi
  fi
fi

SOLR_PROVED="${SOLR_PROVED:-false}"
echo "=== Ingestor dev_kms ==="
INGESTOR_LINES="$(docker logs ranger-audit-ingestor 2>&1 | grep -iE 'dev_kms|service=dev_kms|Unauthorized user.*rangerkms' | tail -8 || true)"
if [[ -n "${INGESTOR_LINES}" ]]; then
  echo "${INGESTOR_LINES}"
elif [[ "${SOLR_PROVED}" == "true" ]]; then
  echo "WARN: no dev_kms lines in ingestor log (Solr count increased — pipeline OK)"
else
  echo "WARN: no dev_kms lines in ingestor log"
  [[ "${FULL_E2E}" == "true" ]] && fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Solr repo:dev_kms ==="
  SOLR_AFTER="$(audit_stack_solr_count 'repo:dev_kms' || echo 0)"
  echo "Solr repo:dev_kms after: ${SOLR_AFTER}"
  if [[ "${SOLR_AFTER}" -gt "${SOLR_BEFORE}" ]]; then
    echo "OK: Solr indexed new dev_kms audit(s)"
    audit_stack_solr_curl '/solr/ranger_audits/select?q=repo:dev_kms&rows=1&sort=evtTime+desc&wt=json' \
      | python3 -c "
import sys, json
doc = json.load(sys.stdin).get('response', {}).get('docs', [{}])[0]
for k in ('repo', 'reqUser', 'access', 'resource', 'evtTime'):
    if k in doc:
        print(f'  {k}: {doc[k]}')
" 2>/dev/null || true
  else
    echo "FAIL: Solr repo:dev_kms count did not increase (${SOLR_BEFORE} -> ${SOLR_AFTER})" >&2
    fail=1
  fi
fi

if [[ "${fail}" -eq 0 ]]; then
  if [[ "${FULL_E2E}" == "true" ]]; then
    echo "PASS: KMS plugin full audit E2E (dev_kms → ingestor → Solr)"
  else
    echo "PASS: KMS plugin audit-server smoke"
  fi
else
  echo "FAIL: KMS plugin audit verification" >&2
fi
exit "${fail}"
