#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create Ozone Kerberos principals/keytabs in the running KDC (idempotent).
# Required for Ozone plugin policy download and audit ingestor SPNEGO auth.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

KDC_CONTAINER="${KDC_CONTAINER:-ranger-kdc}"
CONTAINER_NAME="ranger-ozone"
REALM="${KERBEROS_REALM:-EXAMPLE.COM}"
KEYTAB_DIR="${SCRIPT_DIR}/dist/keytabs/${CONTAINER_NAME}"
PRINCIPALS=(ozone om scm dn HTTP)

if ! docker ps --filter "name=^${KDC_CONTAINER}$" --filter status=running --format '{{.Names}}' | grep -qx "${KDC_CONTAINER}"; then
  echo "ERROR: ${KDC_CONTAINER} is not running" >&2
  exit 1
fi

mkdir -p "${KEYTAB_DIR}"

for principal_name in "${PRINCIPALS[@]}"; do
  principal="${principal_name}/${CONTAINER_NAME}.rangernw"
  keytab="${KEYTAB_DIR}/${principal_name}.keytab"

  docker exec "${KDC_CONTAINER}" kadmin.local -q "addprinc -randkey ${principal}@${REALM}" >/dev/null 2>&1 || true
  docker exec "${KDC_CONTAINER}" bash -c "rm -f /etc/keytabs/${CONTAINER_NAME}/${principal_name}.keytab"
  docker exec "${KDC_CONTAINER}" kadmin.local -q "ktadd -k /etc/keytabs/${CONTAINER_NAME}/${principal_name}.keytab ${principal}@${REALM}"
  docker exec "${KDC_CONTAINER}" chmod 444 "/etc/keytabs/${CONTAINER_NAME}/${principal_name}.keytab"

  if [[ ! -s "${keytab}" ]]; then
    echo "ERROR: keytab not created: ${keytab}" >&2
    exit 1
  fi
  echo "OK: ${principal}@${REALM} -> ${keytab}"
done
