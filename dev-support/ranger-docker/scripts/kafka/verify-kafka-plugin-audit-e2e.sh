#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Smoke: ranger-kafka + audit-server destination → ingestor (dev_kafka).
# RANGER-5642: no duplicate Jersey/Jackson in plugin-impl; no WadlAutoDiscoverable errors.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kafka/verify-kafka-plugin-audit-e2e.sh
#   ./scripts/kafka/verify-kafka-plugin-audit-e2e.sh --enable-authorizer
#   ./scripts/kafka/verify-kafka-plugin-audit-e2e.sh --full-e2e

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../audit/audit-stack-lib.sh
source "${DOCKER_DIR}/scripts/audit/audit-stack-lib.sh"

CONTAINER="${RANGER_KAFKA_CONTAINER:-ranger-kafka}"
RANGER_SCRIPTS="${RANGER_SCRIPTS:-/home/ranger/scripts}"
ENABLE_AUTHORIZER=false
FULL_E2E=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --enable-authorizer) ENABLE_AUTHORIZER=true; shift ;;
    --full-e2e) FULL_E2E=true; ENABLE_AUTHORIZER=true; shift ;;
    -h|--help)
      sed -n '10,15p' "$0"
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

echo "=== Align plugin-impl JARs (Jersey + Jackson) ==="
if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/ensure-kafka-plugin-audit-jars.sh"; then
  echo "OK: plugin-impl aligned with broker classpath"
else
  echo "FAIL: ensure-kafka-plugin-audit-jars.sh" >&2
  fail=1
fi

echo "=== Repo name dev_kafka ==="
if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/apply-kafka-plugin-repo-config.sh" --check-only; then
  echo "OK: ranger-kafka-security.xml uses dev_kafka"
else
  echo "Applying dev_kafka repo config..."
  "${SCRIPT_DIR}/apply-kafka-plugin-repo-config.sh" || fail=1
fi

echo "=== Audit XML ==="
if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/apply-kafka-plugin-audit-config.sh" --check-only; then
  echo "OK: auditserver URL + kerberos authn"
else
  echo "FAIL: ranger-kafka-audit.xml not configured for ingestor" >&2
  fail=1
fi

if [[ "${ENABLE_AUTHORIZER}" == "true" ]]; then
  echo "=== RangerKafkaAuthorizer ==="
  if docker exec "${CONTAINER}" bash "${RANGER_SCRIPTS}/apply-kafka-plugin-repo-config.sh" --check-only \
    && docker exec "${CONTAINER}" grep -q '^[[:space:]]*authorizer.class.name=org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer' /opt/kafka/config/server.properties 2>/dev/null; then
    echo "OK: authorizer enabled"
  else
    "${SCRIPT_DIR}/enable-kafka-authorizer-docker.sh" || fail=1
  fi
  if [[ "${FULL_E2E}" == "true" ]] && [[ -x "${SCRIPT_DIR}/ensure-kafka-audit-bus-acls.sh" ]]; then
    echo "=== Audit bus (ingestor/dispatchers consume ranger_audits) ==="
    "${SCRIPT_DIR}/ensure-kafka-audit-bus-acls.sh" || fail=1
  fi
fi

echo "=== Kafka broker port 9092 ==="
if docker exec "${CONTAINER}" timeout 3 bash -c 'echo >/dev/tcp/localhost/9092' 2>/dev/null; then
  echo "OK: Kafka listening"
else
  echo "FAIL: Kafka not on port 9092" >&2
  fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Ingestor allowlist dev_kafka ==="
  "${SCRIPT_DIR}/ensure-kafka-ingestor-allowlist.sh" || fail=1
  echo "=== Full E2E: trigger DENIED authorization ==="
  SOLR_BEFORE="$(audit_stack_solr_count 'repo:dev_kafka' || echo 0)"
  echo "Solr repo:dev_kafka before: ${SOLR_BEFORE}"
  "${SCRIPT_DIR}/trigger-kafka-plugin-audit-e2e.sh" || fail=1
  echo "Waiting 45s for audit batch → ingestor → dispatcher → Solr..."
  sleep 45
fi

echo "=== Broker audit errors (Wadl / MessageBodyWriter / batch send) ==="
RECENT="$(docker logs "${CONTAINER}" 2>&1 | grep -iE 'WadlAutoDiscoverable|MessageBodyWriter|LinkageError|Failed to send audit batch|Authentication failure' | tail -12 || true)"
if [[ -z "${RECENT}" ]]; then
  echo "OK: no audit REST client errors in broker logs"
else
  echo "${RECENT}"
  if echo "${RECENT}" | grep -qiE 'WadlAutoDiscoverable|MessageBodyWriter|LinkageError'; then
    # Ignore stale errors from before JAR alignment / broker restart
    RECENT_OK="$(docker logs "${CONTAINER}" 2>&1 | tail -400 | grep -iE 'WadlAutoDiscoverable|MessageBodyWriter|LinkageError' || true)"
    if [[ -n "${RECENT_OK}" ]]; then
      echo "FAIL: recent audit REST client errors present" >&2
      fail=1
    else
      echo "OK: no recent Wadl/MessageBodyWriter/LinkageError (older log lines ignored)"
    fi
  elif echo "${RECENT}" | grep -qi 'Failed to send audit batch'; then
    if [[ "${FULL_E2E}" == "true" ]] && echo "${RECENT}" | grep -qi 'HTTP status: 403'; then
      echo "FAIL: ingestor rejected dev_kafka audits (check repo name / allowlist)" >&2
      fail=1
    elif [[ "${FULL_E2E}" != "true" ]]; then
      echo "WARN: audit batch errors in logs (run --full-e2e after repo/JAR fixes)"
    fi
  fi
fi

echo "=== Ingestor dev_kafka ==="
INGESTOR_LINES="$(docker logs ranger-audit-ingestor 2>&1 | grep -iE 'dev_kafka|service=dev_kafka|Unauthorized user.*kafka' | tail -8 || true)"
if [[ -n "${INGESTOR_LINES}" ]]; then
  echo "${INGESTOR_LINES}"
else
  echo "WARN: no dev_kafka lines in ingestor log"
  [[ "${FULL_E2E}" == "true" ]] && fail=1
fi

if [[ "${FULL_E2E}" == "true" ]]; then
  echo "=== Solr repo:dev_kafka ==="
  SOLR_AFTER="$(audit_stack_solr_count 'repo:dev_kafka' || echo 0)"
  echo "Solr repo:dev_kafka after: ${SOLR_AFTER}"
  if [[ "${SOLR_AFTER}" -gt "${SOLR_BEFORE}" ]]; then
    echo "OK: Solr indexed new dev_kafka audit(s)"
    audit_stack_solr_curl '/solr/ranger_audits/select?q=repo:dev_kafka&rows=1&sort=evtTime+desc&wt=json' \
      | python3 -c "
import sys, json
doc = json.load(sys.stdin).get('response', {}).get('docs', [{}])[0]
for k in ('repo', 'reqUser', 'access', 'resource', 'evtTime'):
    if k in doc:
        print(f'  {k}: {doc[k]}')
" 2>/dev/null || true
  else
    echo "FAIL: Solr repo:dev_kafka count did not increase (${SOLR_BEFORE} -> ${SOLR_AFTER})" >&2
    fail=1
  fi
fi

echo "=== Tarball / assembly verify ==="
(
  cd "${DOCKER_DIR}"
  ./scripts/audit/verify-plugin-auditserver-jars.sh --kafka-only --check-assembly
) || fail=1

if [[ "${fail}" -eq 0 ]]; then
  if [[ "${FULL_E2E}" == "true" ]]; then
    echo "PASS: Kafka plugin full audit E2E (dev_kafka → ingestor → Solr)"
  else
    echo "PASS: Kafka plugin audit-server smoke (RANGER-5642)"
  fi
else
  echo "FAIL: Kafka plugin audit verification" >&2
fi
exit "${fail}"
