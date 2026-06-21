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

# Ensure ranger-audit-* JARs are present in the Ozone plugin impl directory.
# Idempotent fallback when the tarball was built without audit modules in the reactor.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

OZONE_PLUGIN_VERSION="${OZONE_PLUGIN_VERSION:-3.0.0-SNAPSHOT}"
IMPL="dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin/lib/libext/ranger-ozone-plugin-impl"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VERSION="${OZONE_PLUGIN_VERSION}"

need_jars=(
  "ranger-audit-core-${VERSION}.jar"
  "ranger-audit-dest-auditserver-${VERSION}.jar"
)

mkdir -p "${IMPL}"

# Ozone 2.x ships jersey-server on the app classpath; Ranger audit REST client uses
# jersey-hk2 in the plugin classloader. Copy jersey-server into impl/ so Jersey SPI
# (WadlAutoDiscoverable) resolves in one classloader.
OZONE_VERSION="${OZONE_VERSION:-2.1.0}"
JERSEY_SERVER="jersey-server-2.47.jar"
ozone_jersey="${SCRIPT_DIR}/downloads/ozone-${OZONE_VERSION}/share/ozone/lib/${JERSEY_SERVER}"
if [[ ! -s "${IMPL}/${JERSEY_SERVER}" ]] && [[ -s "${ozone_jersey}" ]]; then
  cp -f "${ozone_jersey}" "${IMPL}/${JERSEY_SERVER}"
  echo "Copied ${ozone_jersey} -> ${IMPL}/${JERSEY_SERVER}"
elif [[ -s "${IMPL}/${JERSEY_SERVER}" ]]; then
  echo "OK: ${IMPL}/${JERSEY_SERVER}"
fi

for jar in "${need_jars[@]}"; do
  if [[ -s "${IMPL}/${jar}" ]]; then
    echo "OK: ${IMPL}/${jar}"
    continue
  fi
  src=""
  case "${jar}" in
    ranger-audit-core-*)
      src="${REPO_ROOT}/agents-audit/core/target/${jar}"
      ;;
    ranger-audit-dest-auditserver-*)
      src="${REPO_ROOT}/agents-audit/dest-auditserver/target/${jar}"
      ;;
  esac
  if [[ ! -s "${src}" ]]; then
    echo "ERROR: missing ${jar}; build with:" >&2
    echo "  mvn package -Pranger-ozone-plugin -pl :ranger-audit-core,:ranger-audit-dest-auditserver -am -DskipTests" >&2
    exit 1
  fi
  cp -f "${src}" "${IMPL}/${jar}"
  echo "Copied ${src} -> ${IMPL}/${jar}"
done
