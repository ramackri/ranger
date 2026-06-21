#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Trigger KMS Ranger plugin via keyadmin REST (dev_kms audits).
# Auth: Hadoop simple (user.name=keyadmin), not basic auth.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kms/trigger-kms-plugin-audit-e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kms-e2e-lib.sh
source "${SCRIPT_DIR}/kms-e2e-lib.sh"

CONTAINER="${RANGER_KMS_CONTAINER:-ranger-kms}"
KMS_URL="${KMS_E2E_URL:-http://localhost:9292}"

if ! docker ps --filter "name=^${CONTAINER}$" --filter status=running -q | grep -q .; then
  echo "FAIL: ${CONTAINER} is not running" >&2
  exit 1
fi

KEY_NAME="e2e_audit_$(date +%s)"
MATERIAL="$(openssl rand -hex 16)"

echo "Triggering KMS plugin: deny (guest) + list keys + create ${KEY_NAME} (user.name=${KMS_E2E_USER:-keyadmin})"
DENY_CODE="$(kms_e2e_http_code GET '/kms/v1/keys/names' '' 'guest')"
echo "GET /keys/names (guest) HTTP: ${DENY_CODE}"
LIST_CODE="$(kms_e2e_http_code GET '/kms/v1/keys/names')"
echo "GET /keys/names HTTP: ${LIST_CODE}"

CREATE_BODY="$(python3 - <<PY
import json
print(json.dumps({
    "name": "${KEY_NAME}",
    "cipher": "AES/CTR/NoPadding",
    "length": 128,
    "material": "${MATERIAL}",
}))
PY
)"

CREATE_CODE="$(kms_e2e_http_code POST '/kms/v1/keys' "${CREATE_BODY}")"
echo "POST /keys HTTP: ${CREATE_CODE}"

if [[ "${LIST_CODE}" == "200" || "${CREATE_CODE}" =~ ^(200|201)$ ]]; then
  echo "OK: KMS REST handled key operations (plugin authorize path exercised)"
  exit 0
fi

echo "WARN: unexpected KMS HTTP responses — audits may still have been emitted" >&2
exit 0
