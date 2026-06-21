#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Full audit E2E: infrastructure + all plugin pipelines (HDFS, Ozone, Hive, HBase, Kafka, Knox, KMS).
#
# Prerequisites: stack up (see README-AUDIT-E2E.md).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/audit/verify-audit-e2e-full.sh
#   ./scripts/audit/verify-audit-e2e-full.sh --infra-only
#   ./scripts/audit/verify-audit-e2e-full.sh --with-dynamic-partition
#   ./scripts/audit/verify-audit-e2e-full.sh --with-auth-access

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
export KERBEROS_ENABLED="${KERBEROS_ENABLED:-true}"

INFRA_ONLY=false
WITH_DYNAMIC_PARTITION=false
WITH_AUTH_ACCESS=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --infra-only) INFRA_ONLY=true; shift ;;
    --with-dynamic-partition) WITH_DYNAMIC_PARTITION=true; shift ;;
    --with-auth-access) WITH_AUTH_ACCESS=true; shift ;;
    -h|--help)
      sed -n '11,16p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

echo "========== Phase 1: infrastructure =========="
./scripts/audit/verify-audit-e2e-infrastructure.sh

if [[ "${INFRA_ONLY}" == "true" ]]; then
  exit 0
fi

if ! docker exec ranger-kafka timeout 2 bash -c 'echo >/dev/tcp/localhost/9092' 2>/dev/null; then
  echo ""
  echo "========== Phase 1b: repair Kafka broker + plugin =========="
  ./setup-audit-e2e.sh repair-kafka || true
fi

if ! docker ps --filter name=^ranger-hive$ --filter status=running -q | grep -q .; then
  echo ""
  echo "========== Phase 1c: restart ranger-hive =========="
  export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
  docker compose -f docker-compose.ranger-audit-e2e.yml start ranger-hive 2>/dev/null || docker start ranger-hive 2>/dev/null || true
  sleep 30
fi

if ! docker ps --filter name=^ranger-hbase$ --filter status=running -q | grep -q .; then
  echo ""
  echo "========== Phase 1d: start ranger-hbase =========="
  export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
  docker compose -f docker-compose.ranger-audit-e2e.yml up -d ranger-hbase 2>/dev/null || docker start ranger-hbase 2>/dev/null || true
  sleep 60
fi

echo ""
echo "========== Phase 2: runtime config =========="
./setup-audit-e2e.sh config

echo ""
echo "========== Phase 3: plugin audit pipelines =========="
fail=0

echo "--- HDFS ---"
./setup-audit-e2e.sh trigger-hdfs-audit || fail=1

echo "--- Ozone ---"
./setup-audit-e2e.sh trigger-ozone-audit || fail=1

echo "--- Hive ---"
./setup-audit-e2e.sh trigger-hive-audit || fail=1

echo "--- HBase ---"
if [[ -x ./scripts/hbase/verify-hbase-plugin-audit-e2e.sh ]]; then
  ./scripts/hbase/verify-hbase-plugin-audit-e2e.sh --full-e2e || fail=1
else
  ./setup-audit-e2e.sh trigger-hbase-audit || fail=1
fi

echo "--- Kafka ---"
if [[ -x ./scripts/kafka/verify-kafka-plugin-audit-e2e.sh ]]; then
  ./scripts/kafka/verify-kafka-plugin-audit-e2e.sh --full-e2e || fail=1
else
  ./setup-audit-e2e.sh trigger-kafka-audit || fail=1
fi

echo "--- Knox ---"
if docker ps --filter name=^ranger-knox$ --filter status=running -q | grep -q .; then
  if [[ -x ./scripts/knox/verify-knox-plugin-audit-e2e.sh ]]; then
    ./scripts/knox/verify-knox-plugin-audit-e2e.sh --full-e2e || fail=1
  else
    echo "SKIP: verify-knox-plugin-audit-e2e.sh not found" >&2
  fi
else
  echo "SKIP: ranger-knox not running (start docker-compose.ranger-knox.yml + ranger-hadoop)"
fi

echo "--- KMS ---"
if docker ps --filter name=^ranger-kms$ --filter status=running -q | grep -q .; then
  if [[ -x ./scripts/kms/verify-kms-plugin-audit-e2e.sh ]]; then
    ./scripts/kms/verify-kms-plugin-audit-e2e.sh --full-e2e || fail=1
  else
    echo "SKIP: verify-kms-plugin-audit-e2e.sh not found" >&2
  fi
else
  echo "SKIP: ranger-kms not running (start docker-compose.ranger-kms.yml)"
fi

if [[ "${WITH_DYNAMIC_PARTITION}" == "true" ]]; then
  echo ""
  echo "========== Phase 4: dynamic partition onboard + routing =========="
  if [[ -x ./scripts/audit/verify-dynamic-partition-plugin-e2e.sh ]]; then
    ./scripts/audit/verify-dynamic-partition-plugin-e2e.sh --with-harness-triggers || fail=1
  else
    echo "SKIP: verify-dynamic-partition-plugin-e2e.sh not found" >&2
    fail=1
  fi
fi

if [[ "${WITH_AUTH_ACCESS}" == "true" ]]; then
  echo ""
  echo "========== Phase 5: auth_to_local + ingestor allowlist =========="
  if [[ -x ./scripts/audit/verify-dynamic-auth-to-local-e2e.sh ]]; then
    ./scripts/audit/verify-dynamic-auth-to-local-e2e.sh || fail=1
  else
    echo "SKIP: verify-dynamic-auth-to-local-e2e.sh not found" >&2
    fail=1
  fi
fi

if [[ "${fail}" -eq 0 ]]; then
  echo ""
  echo "PASS: full audit E2E (infra + HDFS + Ozone + Hive + HBase + Kafka + optional Knox/KMS"
  if [[ "${WITH_DYNAMIC_PARTITION}" == "true" ]]; then
    echo "      + dynamic partition onboard/routing"
  fi
  if [[ "${WITH_AUTH_ACCESS}" == "true" ]]; then
    echo "      + auth_to_local / allowlist"
  fi
  echo ")"
else
  echo ""
  echo "FAIL: one or more audit E2E phases failed" >&2
fi
exit "${fail}"
