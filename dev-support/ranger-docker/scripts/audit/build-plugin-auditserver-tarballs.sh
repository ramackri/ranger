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

# Build Kafka and/or HBase plugin tarballs (RANGER-5642 / RANGER-5644) and verify JARs.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/audit/build-plugin-auditserver-tarballs.sh           # both
#   ./scripts/audit/build-plugin-auditserver-tarballs.sh kafka
#   ./scripts/audit/build-plugin-auditserver-tarballs.sh hbase
#   ./scripts/audit/build-plugin-auditserver-tarballs.sh both --no-verify

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${SCRIPT_DIR}"

RANGER_VERSION="${RANGER_VERSION:-3.0.0-SNAPSHOT}"
SCOPE="${1:-both}"
DO_VERIFY=true

if [[ "${SCOPE}" == "--no-verify" ]]; then
  SCOPE="both"
  DO_VERIFY=false
fi
if [[ "${2:-}" == "--no-verify" ]]; then
  DO_VERIFY=false
fi

MVN=(mvn -DskipTests -Dcheckstyle.skip=true -Dpmd.skip=true -Drat.skip=true -Dspotbugs.skip=true)
MIN_PLUGIN_BYTES=5000000

mvn_assembly() {
  local descriptor="$1"
  local artifact="$2"
  local profile="$3"
  echo "==> assembly ${descriptor} -> ${artifact} (profile ${profile})"
  (
    cd "${REPO_ROOT}/distro"
    mkdir -p target
    echo "${RANGER_VERSION}" > target/version
    "${MVN[@]}" -P"${profile}" org.apache.maven.plugins:maven-assembly-plugin:3.6.0:single \
      -DskipAssembly=false \
      -Ddescriptor="src/main/assembly/${descriptor}" \
      -DfinalName="ranger-${RANGER_VERSION}" \
      -DoutputDirectory=../target
  )
  local src="${REPO_ROOT}/target/ranger-${RANGER_VERSION}-${artifact}.tar.gz"
  local dst="${SCRIPT_DIR}/dist/ranger-${RANGER_VERSION}-${artifact}.tar.gz"
  [[ -f "${src}" ]] || { echo "ERROR: missing ${src}" >&2; return 1; }
  local size
  size="$(wc -c < "${src}" | tr -d ' ')"
  if [[ "${size}" -lt "${MIN_PLUGIN_BYTES}" ]]; then
    echo "ERROR: ${src} is only ${size} bytes — assembly likely skipped or empty" >&2
    return 1
  fi
  cp -f "${src}" "${dst}"
  echo "  copied ${dst} (${size} bytes)"
}

build_kafka() {
  echo "==> Build Kafka plugin tarball (RANGER-5642)"
  (cd "${REPO_ROOT}" && "${MVN[@]}" package -Pranger-kafka-plugin -pl :ranger-kafka-plugin,:ranger-distro -am)
  local src="${REPO_ROOT}/target/ranger-${RANGER_VERSION}-kafka-plugin.tar.gz"
  local dst="${SCRIPT_DIR}/dist/ranger-${RANGER_VERSION}-kafka-plugin.tar.gz"
  [[ -f "${src}" ]] || { echo "ERROR: missing ${src}" >&2; return 1; }
  local size
  size="$(wc -c < "${src}" | tr -d ' ')"
  if [[ "${size}" -lt "${MIN_PLUGIN_BYTES}" ]]; then
    echo "ERROR: ${src} is only ${size} bytes — assembly likely skipped or empty" >&2
    return 1
  fi
  cp -f "${src}" "${dst}"
  echo "  copied ${dst} (${size} bytes)"
}

build_hbase() {
  echo "==> Build HBase plugin tarball (RANGER-5644)"
  (cd "${REPO_ROOT}" && "${MVN[@]}" package -Pranger-hbase-plugin -pl :ranger-hbase-plugin -am)
  mvn_assembly "hbase-agent.xml" "hbase-plugin" "ranger-hbase-plugin"
}

usage() {
  cat <<'EOF'
Usage: ./scripts/audit/build-plugin-auditserver-tarballs.sh [kafka|hbase|both] [--no-verify]

Builds fat plugin tarballs with Jersey auditserver JARs and runs verify script.
Requires Maven + JDK from repo root (${REPO_ROOT}).

EOF
}

case "${SCOPE}" in
  kafka) build_kafka ;;
  hbase) build_hbase ;;
  both) build_kafka; echo ""; build_hbase ;;
  -h|--help) usage; exit 0 ;;
  *)
    echo "Unknown scope: ${SCOPE}" >&2
    usage >&2
    exit 1
    ;;
esac

if [[ "${DO_VERIFY}" == "true" ]]; then
  echo ""
  case "${SCOPE}" in
    kafka) ./scripts/audit/verify-plugin-auditserver-jars.sh --kafka-only ;;
    hbase) ./scripts/audit/verify-plugin-auditserver-jars.sh --hbase-only ;;
    both)  ./scripts/audit/verify-plugin-auditserver-jars.sh ;;
  esac
fi

echo "Done. Tarballs in dist/ — rebuild Docker images: docker compose ... build ranger-kafka ranger-hbase"
