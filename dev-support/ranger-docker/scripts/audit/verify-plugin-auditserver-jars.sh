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

# Verify auditserver REST client classpath in Kafka / HBase plugin packages.
# Covers RANGER-5642 (Kafka) and RANGER-5644 (HBase): Jersey JSON writer stack
# must be present in lib/*-plugin-impl/ to avoid:
#   MessageBodyWriter not found for media type=application/json
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/audit/verify-plugin-auditserver-jars.sh
#   ./scripts/audit/verify-plugin-auditserver-jars.sh --kafka-only
#   ./scripts/audit/verify-plugin-auditserver-jars.sh --container
#   ./scripts/audit/verify-plugin-auditserver-jars.sh --container --check-logs
#   ./scripts/audit/verify-plugin-auditserver-jars.sh --check-assembly
#
# Build fat tarballs first:
#   ./scripts/audit/build-plugin-auditserver-tarballs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

RANGER_VERSION="${RANGER_VERSION:-3.0.0-SNAPSHOT}"
KAFKA_PLUGIN_VERSION="${KAFKA_PLUGIN_VERSION:-${RANGER_VERSION}}"
HBASE_PLUGIN_VERSION="${HBASE_PLUGIN_VERSION:-${RANGER_VERSION}}"

SCOPE="both"
USE_CONTAINER=false
CHECK_LOGS=false
CHECK_ASSEMBLY=false

usage() {
  cat <<'EOF'
Usage: ./scripts/audit/verify-plugin-auditserver-jars.sh [OPTIONS]

Verify Jersey + ranger-audit-dest-auditserver JARs for Kafka/HBase plugins.

Options:
  --kafka-only       Check Kafka plugin only (RANGER-5642)
  --hbase-only       Check HBase plugin only (RANGER-5644)
  --container        Read JAR list from running Docker containers
  --check-logs       With --container, fail if MessageBodyWriter appears in logs
  --check-assembly   Static check: assembly XML includes required Jersey artifacts
  -h, --help         Show this help

Default: inspect dist/ tarballs (or extracted dist/ plugin trees).
Build tarballs: ./scripts/audit/build-plugin-auditserver-tarballs.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kafka-only) SCOPE="kafka"; shift ;;
    --hbase-only) SCOPE="hbase"; shift ;;
    --container) USE_CONTAINER=true; shift ;;
    --check-logs) CHECK_LOGS=true; shift ;;
    --check-assembly) CHECK_ASSEMBLY=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# JAR name prefixes required in Kafka plugin-impl (broker provides Jersey client/common/server)
KAFKA_JAR_PREFIXES=(
  jersey-entity-filtering-
  jersey-media-json-jackson-
  ranger-audit-dest-auditserver-
)

# Must NOT be packaged in plugin-impl when broker ships Jersey/Jackson (RANGER-5642)
KAFKA_FORBIDDEN_PREFIXES=(
  jersey-client-
  jersey-common-
  jersey-hk2-
  javax.inject-
  jackson-annotations-
  jackson-core-
  jackson-databind-
  jackson-jaxrs-json-provider-
)

HBASE_JAR_PREFIXES=(
  jersey-client-
  jersey-common-
  jersey-entity-filtering-
  jersey-media-json-jackson-
  jackson-jaxrs-json-provider-
  jakarta.ws.rs-api-
  ranger-audit-dest-auditserver-
)

list_jars_from_tarball() {
  local tarball="$1"
  local impl_glob="$2"
  tar tzf "${tarball}" 2>/dev/null | sed 's|.*/||' | grep '\.jar$' || true
}

list_jars_from_dir() {
  local impl_dir="$1"
  [[ -d "${impl_dir}" ]] || return 1
  find "${impl_dir}" -maxdepth 1 -name '*.jar' -exec basename {} \;
}

list_jars_from_container() {
  local container="$1"
  local impl_dir="$2"
  docker exec "${container}" find "${impl_dir}" -maxdepth 1 -name '*.jar' -printf '%f\n' 2>/dev/null \
    || docker exec "${container}" ls -1 "${impl_dir}" 2>/dev/null | grep '\.jar$' || true
}

jar_list_has_prefix() {
  local prefix="$1"
  grep -q "^${prefix}" <<< "${JAR_LIST}"
}

verify_prefixes() {
  local plugin_label="$1"
  local jira="$2"
  shift 2
  local -a prefixes=("$@")
  local missing=0
  local p

  echo "==> ${plugin_label} (${jira})"
  for p in "${prefixes[@]}"; do
    if jar_list_has_prefix "${p}"; then
      echo "  OK   ${p}*.jar"
    else
      echo "  FAIL missing ${p}*.jar" >&2
      missing=$((missing + 1))
    fi
  done

  if [[ "${missing}" -gt 0 ]]; then
    echo "  ${plugin_label}: ${missing} required JAR(s) missing — rebuild plugin tarball after assembly fix" >&2
    return 1
  fi
  echo "  ${plugin_label}: all required auditserver/Jersey JARs present"
  return 0
}

verify_forbidden_prefixes() {
  local plugin_label="$1"
  local jira="$2"
  shift 2
  local -a prefixes=("$@")
  local found=0
  local p

  for p in "${prefixes[@]}"; do
    if jar_list_has_prefix "${p}"; then
      echo "  FAIL unexpected ${p}*.jar in plugin-impl (${jira})" >&2
      found=$((found + 1))
    fi
  done

  if [[ "${found}" -gt 0 ]]; then
    echo "  ${plugin_label}: ${found} duplicate broker JAR(s) must be removed — use broker classpath" >&2
    return 1
  fi
  echo "  OK   no duplicate Jersey/HK2/Jackson JARs in plugin-impl"
  return 0
}

check_container_logs() {
  local container="$1"
  local jira="$2"
  if ! docker ps --filter "name=^${container}$" --filter status=running --format '{{.Names}}' | grep -qx "${container}"; then
    echo "  logs: skip (${container} not running)"
    return 0
  fi
  if docker logs "${container}" 2>&1 | grep -qi 'MessageBodyWriter not found'; then
    echo "  FAIL ${container} logs contain MessageBodyWriter error (${jira})" >&2
    docker logs "${container}" 2>&1 | grep -i MessageBodyWriter | tail -3 | sed 's/^/    /' >&2
    return 1
  fi
  if docker logs "${container}" 2>&1 | grep -qi 'WadlAutoDiscoverable'; then
    echo "  FAIL ${container} logs contain WadlAutoDiscoverable ClassCastException (${jira})" >&2
    docker logs "${container}" 2>&1 | grep -i WadlAutoDiscoverable | tail -3 | sed 's/^/    /' >&2
    return 1
  fi
  echo "  OK   ${container} logs: no MessageBodyWriter / WadlAutoDiscoverable errors"
  return 0
}

verify_kafka() {
  local jar_list_source=""
  JAR_LIST=""

  if [[ "${USE_CONTAINER}" == "true" ]]; then
    jar_list_source="container:ranger-kafka"
    JAR_LIST="$(list_jars_from_container ranger-kafka /opt/ranger/ranger-kafka-plugin/lib/ranger-kafka-plugin-impl)"
  else
    local tarball="dist/ranger-${KAFKA_PLUGIN_VERSION}-kafka-plugin.tar.gz"
    local impl_dir="dist/ranger-${KAFKA_PLUGIN_VERSION}-kafka-plugin/lib/ranger-kafka-plugin-impl"
    if [[ -f "${tarball}" ]]; then
      jar_list_source="${tarball}"
      JAR_LIST="$(list_jars_from_tarball "${tarball}" 'ranger-kafka-plugin-impl')"
    elif [[ -d "${impl_dir}" ]]; then
      jar_list_source="${impl_dir}"
      JAR_LIST="$(list_jars_from_dir "${impl_dir}")"
    else
      echo "ERROR: Kafka plugin not found: ${tarball} or ${impl_dir}" >&2
      return 1
    fi
  fi

  echo "  source: ${jar_list_source}"
  verify_prefixes "Kafka plugin" "RANGER-5642" "${KAFKA_JAR_PREFIXES[@]}"
  local rc=$?
  verify_forbidden_prefixes "Kafka plugin" "RANGER-5642" "${KAFKA_FORBIDDEN_PREFIXES[@]}" || rc=1
  if [[ "${CHECK_LOGS}" == "true" ]]; then
    check_container_logs ranger-kafka RANGER-5642 || rc=1
  fi
  return "${rc}"
}

verify_hbase() {
  local jar_list_source=""
  JAR_LIST=""

  if [[ "${USE_CONTAINER}" == "true" ]]; then
    jar_list_source="container:ranger-hbase"
    JAR_LIST="$(list_jars_from_container ranger-hbase /opt/ranger/ranger-hbase-plugin/lib/ranger-hbase-plugin-impl)"
  else
    local tarball="dist/ranger-${HBASE_PLUGIN_VERSION}-hbase-plugin.tar.gz"
    local impl_dir="dist/ranger-${HBASE_PLUGIN_VERSION}-hbase-plugin/lib/ranger-hbase-plugin-impl"
    if [[ -f "${tarball}" ]]; then
      jar_list_source="${tarball}"
      JAR_LIST="$(list_jars_from_tarball "${tarball}" 'ranger-hbase-plugin-impl')"
    elif [[ -d "${impl_dir}" ]]; then
      jar_list_source="${impl_dir}"
      JAR_LIST="$(list_jars_from_dir "${impl_dir}")"
    else
      echo "ERROR: HBase plugin not found: ${tarball} or ${impl_dir}" >&2
      return 1
    fi
  fi

  echo "  source: ${jar_list_source}"
  verify_prefixes "HBase plugin" "RANGER-5644" "${HBASE_JAR_PREFIXES[@]}"
  local rc=$?
  if [[ "${CHECK_LOGS}" == "true" ]]; then
    check_container_logs ranger-hbase RANGER-5644 || rc=1
  fi
  return "${rc}"
}

verify_assembly_xml() {
  local fail=0
  local repo_root
  repo_root="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  local kafka_xml="${repo_root}/distro/src/main/assembly/plugin-kafka.xml"
  local hbase_xml="${repo_root}/distro/src/main/assembly/hbase-agent.xml"

  echo "==> Assembly XML static check"
  if [[ "${SCOPE}" != "hbase" ]]; then
    echo "  plugin-kafka.xml (RANGER-5642):"
    for token in jersey-entity-filtering jersey-media-json-jackson ranger-audit-dest-auditserver; do
      if grep -q "${token}" "${kafka_xml}"; then
        echo "    OK   ${token}"
      else
        echo "    FAIL missing ${token}" >&2
        fail=1
      fi
    done
    for token in jersey-client jersey-common jersey-hk2 javax.inject; do
      case "${token}" in
        jersey-client)
          if grep -q 'org.glassfish.jersey.core:jersey-client' "${kafka_xml}"; then
            echo "    FAIL ${token} must not be whitelisted (duplicate broker Jersey)" >&2
            fail=1
          else
            echo "    OK   ${token} absent (expected)"
          fi
          ;;
        jersey-common)
          if grep -q 'org.glassfish.jersey.core:jersey-common' "${kafka_xml}"; then
            echo "    FAIL ${token} must not be whitelisted (duplicate broker Jersey)" >&2
            fail=1
          else
            echo "    OK   ${token} absent (expected)"
          fi
          ;;
        jersey-hk2)
          if grep -q 'jersey-hk2' "${kafka_xml}"; then
            echo "    FAIL ${token} must not be whitelisted (duplicate broker Jersey)" >&2
            fail=1
          else
            echo "    OK   ${token} absent (expected)"
          fi
          ;;
        javax.inject)
          if grep -q 'javax.inject' "${kafka_xml}"; then
            echo "    FAIL ${token} must not be whitelisted (duplicate broker Jersey)" >&2
            fail=1
          else
            echo "    OK   ${token} absent (expected)"
          fi
          ;;
      esac
    done
    for token in jackson-annotations jackson-core jackson-databind jackson-jaxrs-json-provider; do
      if grep -q "${token}" "${kafka_xml}"; then
        echo "    FAIL ${token} must not be whitelisted (duplicate broker Jackson)" >&2
        fail=1
      else
        echo "    OK   ${token} absent (expected)"
      fi
    done
  fi
  if [[ "${SCOPE}" != "kafka" ]]; then
    echo "  hbase-agent.xml (RANGER-5644):"
    for token in jersey-media-json-jackson jersey-entity-filtering; do
      if grep -q "${token}" "${hbase_xml}"; then
        echo "    OK   ${token}"
      else
        echo "    FAIL missing ${token}" >&2
        fail=1
      fi
    done
    if grep -q 'jersey-hk2' "${hbase_xml}"; then
      echo "    FAIL jersey-hk2 must not be whitelisted (crashes HMaster in plugins-docker-build)" >&2
      fail=1
    else
      echo "    OK   jersey-hk2 absent (expected)"
    fi
  fi
  return "${fail}"
}

main() {
  local fail=0
  echo "verify-plugin-auditserver-jars (RANGER-5642 / RANGER-5644)"
  echo "  RANGER_VERSION=${RANGER_VERSION}"

  if [[ "${CHECK_ASSEMBLY}" == "true" ]]; then
    verify_assembly_xml || fail=1
    if [[ "${fail}" -eq 0 ]]; then
      echo ""
      echo "PASS: assembly XML includes required Jersey artifacts (scope=${SCOPE})"
    else
      echo ""
      echo "FAIL: assembly XML missing required Jersey artifacts" >&2
      exit 1
    fi
    exit 0
  fi

  if [[ "${SCOPE}" != "hbase" ]]; then
    verify_kafka || fail=1
    echo ""
  fi
  if [[ "${SCOPE}" != "kafka" ]]; then
    verify_hbase || fail=1
  fi

  if [[ "${fail}" -eq 0 ]]; then
    echo ""
    echo "PASS: auditserver REST client classpath OK for scope=${SCOPE}"
  else
    echo ""
    echo "FAIL: missing JARs, assembly XML, or MessageBodyWriter in logs" >&2
    echo "  Fix: distro/src/main/assembly/plugin-kafka.xml + hbase-agent.xml" >&2
    echo "  Build: ./scripts/audit/build-plugin-auditserver-tarballs.sh" >&2
    exit 1
  fi
}

main
