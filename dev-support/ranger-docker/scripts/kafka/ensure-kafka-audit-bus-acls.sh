#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# When RangerKafkaAuthorizer is enabled, audit ingestor + dispatchers must still
# produce/consume topic ranger_audits. Grant rangerauditserver broker super-user
# (Docker E2E only — bypasses Ranger on the audit bus, not plugin authz tests).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kafka/ensure-kafka-audit-bus-acls.sh
#   ./scripts/kafka/ensure-kafka-audit-bus-acls.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONTAINER="${RANGER_KAFKA_CONTAINER:-ranger-kafka}"
PROP=/opt/kafka/config/server.properties
CHECK_ONLY=false
RESTART_DISPATCHERS=true
# Kafka broker admin + audit bus clients (ingestor, Solr/HDFS dispatchers).
SUPER_USERS='User:kafka;User:rangerauditserver'

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    --no-restart-dispatchers) RESTART_DISPATCHERS=false; shift ;;
    -h|--help)
      sed -n '11,14p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

super_users_ok() {
  docker exec "${CONTAINER}" grep -q "^[[:space:]]*super.users=${SUPER_USERS}" "${PROP}" 2>/dev/null
}

if [[ "${CHECK_ONLY}" == "true" ]]; then
  if super_users_ok; then
    echo "OK: super.users includes kafka + rangerauditserver (audit bus)"
    exit 0
  fi
  echo "FAIL: super.users must be '${SUPER_USERS}' for audit bus with authorizer" >&2
  docker exec "${CONTAINER}" grep 'super.users' "${PROP}" 2>/dev/null || true
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "ERROR: ${CONTAINER} is not running" >&2
  exit 1
fi

docker exec "${CONTAINER}" bash -c "
set -e
PROP=${PROP}
TARGET='${SUPER_USERS}'
if grep -q '^[[:space:]]*super.users=' \"\${PROP}\"; then
  sed -i 's|^[[:space:]]*super.users=.*|    super.users='\${TARGET}'|' \"\${PROP}\"
else
  echo '    super.users='\${TARGET} >> \"\${PROP}\"
fi
grep 'super.users' \"\${PROP}\"
"

echo "Restarting Kafka broker (pick up super.users)..."
"${SCRIPT_DIR}/restart-kafka-broker-docker.sh"

if [[ "${RESTART_DISPATCHERS}" == "true" ]]; then
  echo "Restarting audit dispatchers (re-subscribe to ranger_audits)..."
  for c in ranger-audit-dispatcher-solr ranger-audit-dispatcher-hdfs; do
    if docker ps -a --format '{{.Names}}' | grep -qx "${c}"; then
      docker restart "${c}" >/dev/null
    fi
  done
  sleep 20
  for c in ranger-audit-dispatcher-solr ranger-audit-dispatcher-hdfs; do
    if docker logs "${c}" 2>&1 | tail -200 | grep -q 'TopicAuthorizationException'; then
      echo "WARN: ${c} still shows TopicAuthorizationException — check super.users" >&2
    else
      echo "OK: ${c} restarted"
    fi
  done
fi

echo "Audit bus ACL fix applied"
