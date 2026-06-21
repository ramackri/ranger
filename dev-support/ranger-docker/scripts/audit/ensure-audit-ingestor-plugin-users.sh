#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Sync audit ingestor service-user allowlists from repo source site.xml.
# Required for plugin E2E: dev_hdfs, dev_hive, dev_hbase, dev_ozone, dev_kafka, dev_knox, dev_kms, etc.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/audit/ensure-audit-ingestor-plugin-users.sh
#   ./scripts/audit/ensure-audit-ingestor-plugin-users.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONTAINER="${AUDIT_INGESTOR_CONTAINER:-ranger-audit-ingestor}"
CHECK_ONLY=false
SOURCE_SITE="${REPO_ROOT}/audit-server/audit-ingestor/src/main/resources/conf/ranger-audit-ingestor-site.xml"
SITE_XMLS=(
  "/opt/ranger/audit-ingestor/conf/ranger-audit-ingestor-site.xml"
  "/opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf/ranger-audit-ingestor-site.xml"
)
REQUIRED_PROPS=(
  "ranger.audit.ingestor.service.dev_hdfs.allowed.users"
  "ranger.audit.ingestor.service.dev_hive.allowed.users"
  "ranger.audit.ingestor.service.dev_hbase.allowed.users"
  "ranger.audit.ingestor.service.dev_ozone.allowed.users"
  "ranger.audit.ingestor.service.dev_kafka.allowed.users"
  "ranger.audit.ingestor.service.dev_knox.allowed.users"
  "ranger.audit.ingestor.service.dev_kms.allowed.users"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    -h|--help)
      sed -n '10,13p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if ! docker ps --filter "name=^${CONTAINER}$" --filter status=running --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "ERROR: ${CONTAINER} is not running" >&2
  exit 1
fi

site_has_prop() {
  local path="$1"
  local prop="$2"
  docker exec "${CONTAINER}" grep -q "${prop}" "${path}" 2>/dev/null
}

all_props_present() {
  local path="$1"
  local prop
  for prop in "${REQUIRED_PROPS[@]}"; do
    site_has_prop "${path}" "${prop}" || return 1
  done
}

if [[ "${CHECK_ONLY}" == "true" ]]; then
  for SITE_XML in "${SITE_XMLS[@]}"; do
    docker exec "${CONTAINER}" test -f "${SITE_XML}" 2>/dev/null || continue
    if all_props_present "${SITE_XML}"; then
      echo "OK: plugin allowlists present in ${SITE_XML}"
      exit 0
    fi
  done
  echo "FAIL: missing plugin allowlist properties in ingestor site.xml" >&2
  exit 1
fi

if [[ ! -f "${SOURCE_SITE}" ]]; then
  echo "ERROR: missing ${SOURCE_SITE}" >&2
  exit 1
fi

patched="no"
for SITE_XML in "${SITE_XMLS[@]}"; do
  if ! docker exec "${CONTAINER}" test -f "${SITE_XML}" 2>/dev/null; then
    continue
  fi
  if all_props_present "${SITE_XML}"; then
    echo "Ingestor: allowlists OK in ${SITE_XML}"
    continue
  fi
  docker cp "${SOURCE_SITE}" "${CONTAINER}:${SITE_XML}"
  patched="yes"
  echo "Ingestor: synced ${SOURCE_SITE} -> ${SITE_XML}"
done

if [[ "${patched}" == "yes" ]]; then
  echo "Ingestor: restarting ${CONTAINER}..."
  docker restart "${CONTAINER}" >/dev/null
  for _ in $(seq 1 36); do
    if curl -sf "http://localhost:7081/api/audit/health" >/dev/null 2>&1; then
      echo "OK: ingestor healthy after allowlist sync"
      break
    fi
    sleep 5
  done
fi

"${0}" --check-only
