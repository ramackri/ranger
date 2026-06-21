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

# In-container: Kerberos bootstrap for Ozone OM (Ranger policy + audit ingestor SPNEGO).

set -euo pipefail

# RANGER_KERBEROS_ENABLED (not KERBEROS_ENABLED) avoids Ozone libexec entrypoint krb5:8081 wait.
if [[ "${RANGER_KERBEROS_ENABLED:-true}" != "true" ]]; then
  echo "Ranger Kerberos disabled; skipping Ozone krb bootstrap"
  exit 0
fi

KEYTAB_DIR="${KEYTAB_DIR:-/etc/keytabs}"
KEYTAB="${KEYTAB_DIR}/om.keytab"
PRINCIPAL="${OZONE_KERBEROS_PRINCIPAL:-om/ranger-ozone.rangernw}"
REALM="${KERBEROS_REALM:-EXAMPLE.COM}"
CONF_DIR="${OZONE_CONF_DIR:-/etc/hadoop}"

wait_for_keytab() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if [[ -s "${KEYTAB}" ]]; then
      return 0
    fi
    echo "Waiting for ${KEYTAB}..."
    sleep 2
  done
  echo "ERROR: timed out waiting for ${KEYTAB}" >&2
  exit 1
}

wait_for_keytab

mkdir -p "${CONF_DIR}"
if [[ -f /opt/hadoop/ranger-ozone-plugin/core-site.xml ]]; then
  cp -f /opt/hadoop/ranger-ozone-plugin/core-site.xml "${CONF_DIR}/core-site.xml"
elif [[ -f /opt/ozone-core-site.xml ]]; then
  cp -f /opt/ozone-core-site.xml "${CONF_DIR}/core-site.xml"
fi

export KRB5_CONFIG="${KRB5_CONFIG:-/etc/krb5.conf}"
kinit -kt "${KEYTAB}" "${PRINCIPAL}@${REALM}"
echo "Kerberos ticket acquired for ${PRINCIPAL}@${REALM}"
klist
