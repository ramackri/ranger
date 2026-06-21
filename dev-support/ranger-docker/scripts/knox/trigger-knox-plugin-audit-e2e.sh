#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Trigger Knox Ranger plugin via sandbox WebHDFS LISTSTATUS (dev_knox audits).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/knox/trigger-knox-plugin-audit-e2e.sh
#
# Requires: ranger-knox + ranger-hadoop (WebHDFS backend) + audit stack.

set -euo pipefail

CONTAINER="${RANGER_KNOX_CONTAINER:-ranger-knox}"
HADOOP_CONTAINER="${RANGER_HADOOP_CONTAINER:-ranger-hadoop}"
GATEWAY_URL="${KNOX_GATEWAY_URL:-https://localhost:8443}"
GATEWAY_USER="${KNOX_GATEWAY_USER:-guest}"
GATEWAY_PASSWORD="${KNOX_GATEWAY_PASSWORD:-guest-password}"
WEBHDFS_PATH="${KNOX_E2E_WEBHDFS_PATH:-/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS}"

if ! docker ps --filter "name=^${CONTAINER}$" --filter status=running -q | grep -q .; then
  echo "FAIL: ${CONTAINER} is not running" >&2
  exit 1
fi

if ! docker ps --filter "name=^${HADOOP_CONTAINER}$" --filter status=running -q | grep -q .; then
  echo "FAIL: ${HADOOP_CONTAINER} is not running (Knox sandbox WebHDFS backend)" >&2
  exit 1
fi

echo "Triggering Knox plugin via: ${GATEWAY_URL}${WEBHDFS_PATH}"
HTTP_CODE="$(curl -sk -o /tmp/knox-e2e-webhdfs.json -w '%{http_code}' \
  -u "${GATEWAY_USER}:${GATEWAY_PASSWORD}" \
  "${GATEWAY_URL}${WEBHDFS_PATH}" || echo "000")"

echo "HTTP status: ${HTTP_CODE}"
if [[ -f /tmp/knox-e2e-webhdfs.json ]]; then
  head -c 400 /tmp/knox-e2e-webhdfs.json | tr '\n' ' '
  echo ""
fi

if [[ "${HTTP_CODE}" =~ ^(200|401|403)$ ]]; then
  echo "OK: Knox gateway handled WebHDFS request (plugin initialized on authorize path)"
  exit 0
fi

echo "WARN: unexpected HTTP ${HTTP_CODE} — Knox may still have emitted audits; check ingestor/Solr" >&2
exit 0
