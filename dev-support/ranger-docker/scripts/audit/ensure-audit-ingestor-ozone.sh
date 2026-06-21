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

# Ensure audit ingestor accepts dev_ozone plugin audits (ozone,om users).
# Patches conf/ (honored via -Daudit.config) and WEB-INF classpath copy for older images.
# Idempotent; restarts ingestor only when site.xml was patched.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONTAINER="${AUDIT_INGESTOR_CONTAINER:-ranger-audit-ingestor}"
SITE_XMLS=(
  "/opt/ranger/audit-ingestor/conf/ranger-audit-ingestor-site.xml"
  "/opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf/ranger-audit-ingestor-site.xml"
)
PROP_NAME="ranger.audit.ingestor.service.dev_ozone.allowed.users"
SOURCE_SITE="${REPO_ROOT}/audit-server/audit-ingestor/src/main/resources/conf/ranger-audit-ingestor-site.xml"

if ! docker ps --filter "name=^${CONTAINER}$" --filter status=running --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "ERROR: ${CONTAINER} is not running" >&2
  exit 1
fi

site_has_ozone_users() {
  local path="$1"
  docker exec "${CONTAINER}" grep -A1 "${PROP_NAME}" "${path}" 2>/dev/null | grep -q 'ozone,om'
}

patched="no"
if [[ -f "${SOURCE_SITE}" ]] && grep -q "${PROP_NAME}" "${SOURCE_SITE}" && grep -A1 "${PROP_NAME}" "${SOURCE_SITE}" | grep -q 'ozone,om'; then
  for SITE_XML in "${SITE_XMLS[@]}"; do
    if ! docker exec "${CONTAINER}" test -f "${SITE_XML}" 2>/dev/null; then
      continue
    fi
    if site_has_ozone_users "${SITE_XML}"; then
      echo "Ingestor: ${PROP_NAME} already set in ${SITE_XML}"
      continue
    fi
    docker cp "${SOURCE_SITE}" "${CONTAINER}:${SITE_XML}"
    patched="yes"
    echo "Ingestor: copied source site.xml -> ${SITE_XML}"
  done
else
  for SITE_XML in "${SITE_XMLS[@]}"; do
    if ! docker exec "${CONTAINER}" test -f "${SITE_XML}" 2>/dev/null; then
      continue
    fi
    if site_has_ozone_users "${SITE_XML}"; then
      echo "Ingestor: ${PROP_NAME} already set in ${SITE_XML}"
      continue
    fi
    if docker exec "${CONTAINER}" grep -q "${PROP_NAME}" "${SITE_XML}" 2>/dev/null; then
      docker exec "${CONTAINER}" sed -i "/${PROP_NAME}/{n;s|<value>.*</value>|<value>ozone,om</value>|;}" "${SITE_XML}"
    else
      docker exec "${CONTAINER}" sed -i "s|</configuration>|    <property>\n        <name>${PROP_NAME}</name>\n        <value>ozone,om</value>\n        <description>Allowed users for dev_ozone audits</description>\n    </property>\n</configuration>|" "${SITE_XML}"
    fi
    patched="yes"
    echo "Ingestor: patched ${SITE_XML}"
  done
fi

if [[ "${patched}" == "yes" ]]; then
  echo "Ingestor: updated ${PROP_NAME}; restarting ${CONTAINER}..."
  docker restart "${CONTAINER}" >/dev/null
  sleep 30
fi

for SITE_XML in "${SITE_XMLS[@]}"; do
  if docker exec "${CONTAINER}" test -f "${SITE_XML}" 2>/dev/null && ! site_has_ozone_users "${SITE_XML}"; then
    echo "ERROR: ${PROP_NAME} missing in ${SITE_XML}" >&2
    exit 1
  fi
done

echo "Ingestor: ${PROP_NAME}=ozone,om verified"
