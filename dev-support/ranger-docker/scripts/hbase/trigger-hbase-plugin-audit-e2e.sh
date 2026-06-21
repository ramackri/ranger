#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Generate a DENIED HBase authorization event as testuser1 (dev_hbase audits).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/hbase/trigger-hbase-plugin-audit-e2e.sh

set -euo pipefail

CONTAINER="${RANGER_HBASE_CONTAINER:-ranger-hbase}"
TABLE="${HBASE_E2E_AUDIT_TABLE:-e2e_ranger_audit_deny}"
TEST_USER="${HBASE_E2E_TEST_USER:-testuser1}"
HBASE_PRINCIPAL="${HBASE_KERBEROS_PRINCIPAL:-hbase/ranger-hbase.rangernw@EXAMPLE.COM}"
TEST_PRINCIPAL="${TEST_USER}/ranger-hbase.rangernw@EXAMPLE.COM"

docker exec "${CONTAINER}" bash -c "
set -euo pipefail
export HBASE_HOME=/opt/hbase
export PATH=\${HBASE_HOME}/bin:\${PATH}

kinit -kt /etc/keytabs/hbase.keytab '${HBASE_PRINCIPAL}'
echo \"create '${TABLE}', 'cf'\" | hbase shell -n 2>&1 | tail -5 || true

kinit -kt /etc/keytabs/${TEST_USER}.keytab '${TEST_PRINCIPAL}'
set +e
OUT=\$(echo \"scan '${TABLE}'\" | hbase shell -n 2>&1)
rc=\$?
set -e
echo \"\${OUT}\" | tail -8
if echo \"\${OUT}\" | grep -qiE 'AccessDeniedException|Insufficient permissions|not authorized|denied'; then
  echo 'OK: triggered DENIED authorization as ${TEST_USER} on ${TABLE}'
  exit 0
fi
echo 'WARN: expected AccessDenied/denied from scan; review output above' >&2
exit 0
"
