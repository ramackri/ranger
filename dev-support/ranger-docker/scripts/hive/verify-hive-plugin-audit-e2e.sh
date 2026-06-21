#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Smoke: ranger-hive HS2 + audit-server destination → ingestor (dev_hive).
# See dev-support/RANGER-HIVE-PLUGIN-RBAC-E2E.md

set -euo pipefail

CONTAINER="${RANGER_HIVE_CONTAINER:-ranger-hive}"
INGESTOR_URL="${RANGER_AUDIT_INGESTOR_URL:-http://ranger-audit-ingestor.rangernw:7081}"
PRINCIPAL="${HIVE_KERBEROS_PRINCIPAL:-hive/ranger-hive.rangernw@EXAMPLE.COM}"

fail=0

if ! docker inspect -f '{{.State.Running}}' "${CONTAINER}" 2>/dev/null | grep -q true; then
  echo "FAIL: ${CONTAINER} is not running" >&2
  exit 1
fi

echo "=== HS2 port 10000 ==="
if docker exec "${CONTAINER}" timeout 3 bash -c 'echo >/dev/tcp/localhost/10000' 2>/dev/null; then
  echo "OK: HiveServer2 listening"
else
  echo "FAIL: HiveServer2 not on port 10000" >&2
  fail=1
fi

echo "=== Plugin-impl duplicates (RANGER-5646) ==="
DUPES="$(docker exec "${CONTAINER}" bash -c '
  impl=/opt/hive/lib/ranger-hive-plugin-impl
  ls "${impl}"/hive-*-*.jar 2>/dev/null | wc -l
  ls "${impl}"/hppc-*.jar "${impl}"/httpclient-*.jar "${impl}"/jackson-core-2.17*.jar 2>/dev/null || true
' 2>/dev/null || true)"
HIVE_JARS="$(echo "${DUPES}" | head -1 | tr -d ' ')"
if [[ "${HIVE_JARS:-0}" != "0" ]]; then
  echo "WARN: ${HIVE_JARS} hive-* jar(s) still in plugin-impl (expect 0 after enable-hive-plugin-docker.sh)"
fi

echo "=== Audit XML ==="
if docker exec "${CONTAINER}" bash -c "grep -A5 'xasecure.audit.destination.auditserver.url' /opt/hive/conf/ranger-hive-audit.xml | grep -Fq '${INGESTOR_URL}'"; then
  echo "OK: audit URL ${INGESTOR_URL}"
else
  echo "FAIL: audit URL not ${INGESTOR_URL}" >&2
  docker exec "${CONTAINER}" grep -A5 'xasecure.audit.destination.auditserver.url' /opt/hive/conf/ranger-hive-audit.xml || true
  fail=1
fi

echo "=== Beeline SHOW DATABASES ==="
if docker exec "${CONTAINER}" bash -c "
  kinit -kt /etc/keytabs/hive.keytab '${PRINCIPAL}' &&
  beeline -u 'jdbc:hive2://localhost:10000/default;principal=${PRINCIPAL}' --silent=true -e 'SHOW DATABASES' 2>&1 | tail -3
"; then
  echo "OK: beeline query"
else
  echo "FAIL: beeline query" >&2
  fail=1
fi

sleep 15

echo "=== HS2 audit errors (401 / MessageBodyWriter) ==="
RECENT="$(docker exec "${CONTAINER}" bash -c "
  grep -iE '401|403|MessageBodyWriter|Authentication failure|Failed to send audit batch' \
    /opt/hive/logs/hiveserver2.log 2>/dev/null | tail -5
" || true)"
if [[ -z "${RECENT}" ]]; then
  echo "OK: no recent audit errors in hiveserver2.log"
else
  echo "${RECENT}"
  if echo "${RECENT}" | grep -qiE '401|MessageBodyWriter|Authentication failure'; then
    echo "FAIL: audit delivery errors present" >&2
    fail=1
  else
    echo "WARN: non-fatal audit log lines (check ingestor allowlist for 403)"
  fi
fi

echo "=== Ingestor dev_hive (optional) ==="
docker logs ranger-audit-ingestor 2>&1 | grep -iE 'dev_hive|Unauthorized' | tail -3 || true

exit "${fail}"
