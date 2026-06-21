#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# =============================================================================
# setup-audit-e2e.sh — single entry point for Ranger audit Docker (Tier 3/4)
# =============================================================================
#
# Brings up: Admin + Solr + Kafka + audit ingestor + dispatchers + plugins
# Default scope: HDFS + Ozone audits → Solr → Ranger Admin (Audit → Access).
#
# Pipeline:
#   ranger-hadoop (HDFS)  ──┐
#   ozone-om (Ozone)      ──┼──► ingestor :7081 ──► Kafka ──► dispatchers ──► Solr
#   ranger-hive (Hive)    ──┘
#   ranger-hbase (HBase)  ──┘
#   ranger-kafka plugin   ──┘  (RangerKafkaAuthorizer → dev_kafka)
#                           │                                              │
#                           └──────────────────────────────────► Admin :6080
#
# What this script does (action: up):
#   1. Build missing dist/ tarballs (admin, audit-*, hdfs/ozone plugins) via Maven
#   2. Download Hadoop/Ozone archives; patch Ozone plugin audit JARs if needed
#   3. docker compose build + up (scope selects services)
#   4. Runtime config (plugin audit XML, Admin Solr backend, Ozone policies)
#   5. Generate access logs and trace each hop to Ranger Admin
#   6. Optional smoke test (same pipeline verification)
#
# Quick start (from dev-support/ranger-docker):
#   export RANGER_DB_TYPE=postgres
#   chmod +x setup-audit-e2e.sh
#   ./setup-audit-e2e.sh                    # HDFS + Ozone (default)
#   ./setup-audit-e2e.sh up --with-hive    # HDFS + Ozone + Hive (default up includes Hive)
#   ./setup-audit-e2e.sh up --hdfs-only   # HDFS audits only
#   ./setup-audit-e2e.sh up --ozone-only  # Ozone audits only (no Hadoop)
#   ./setup-audit-e2e.sh up --no-hive     # HDFS + Ozone without ranger-hive
#
# Troubleshooting:
#   ./setup-audit-e2e.sh diagnose
#   ./setup-audit-e2e.sh fix
#   ./setup-audit-e2e.sh repair-hdfs
#   ./setup-audit-e2e.sh repair-ozone
#   ./setup-audit-e2e.sh repair-hive
#
# Full docs: README-AUDIT-E2E.md
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${SCRIPT_DIR}"

usage() {
  cat <<'EOF'
Usage: ./setup-audit-e2e.sh [ACTION] [OPTIONS]

Actions:
  up                 Build dist/ if needed, compose up, config, smoke (default)
  down               Tear down the stack
  verify             Re-run health + smoke for current scope
  restart            Compose up + runtime config (no full rebuild)
  config             Apply runtime plugin/Admin config only
  status             Container list + endpoint summary
  diagnose           Read-only health report (exit 1 if issues)
  fix                Diagnose, rebuild audit services if needed, config, smoke
  repair-hdfs        Fix HDFS plugin audit path + Admin Solr backend
  repair-ozone       Fix Ozone plugin, KDC keytabs, policies, ingestor users
  repair-hive        Redeploy Hive plugin tarball + audit ingestor URL + HS2 smoke
  repair-kafka       Kafka plugin JAR/repo/audit config + ingestor allowlist + broker restart
  repair-hbase       Redeploy HBase plugin tarball + audit ingestor URL + HBase smoke
  repair-knox        Apply Knox audit ingestor URL + dev_knox allowlist (optional compose)
  repair-kms         Apply KMS plugin-impl JARs + audit ingestor URL + dev_kms allowlist + KMS restart
  repair-solr        Rebuild Solr plugin tarball + recreate ranger-solr
  verify-full        Infrastructure check + all plugin pipelines (see verify-audit-e2e-full.sh)
  generate-access-logs  Generate HDFS/Ozone/Hive/Kafka plugin access logs; trace full pipeline to Admin
  trigger-hdfs-audit    Generate HDFS access log (alias for generate-access-logs --hdfs-only)
  trigger-ozone-audit   Generate Ozone access log (alias for generate-access-logs --ozone-only)
  trigger-hive-audit    Run beeline on ranger-hive and check dev_hive audit path
  trigger-kafka-audit   DENIED topic create on ranger-kafka; check dev_kafka audit path
  trigger-hbase-audit   DENIED scan on ranger-hbase; check dev_hbase audit path
  trigger-knox-audit    WebHDFS via Knox gateway; check dev_knox audit path
  trigger-kms-audit     KMS key ops on ranger-kms; check dev_kms audit path

Scope (default: HDFS + Ozone + Hive + HBase + Kafka plugin):
  --hdfs-only        Start/test HDFS plugin audits only (no Ozone containers)
  --ozone-only       Start/test Ozone plugin audits only (no Hadoop/HDFS dispatcher)
  --with-hive        Include ranger-hive (default for up/verify when scope is both)
  --no-hive          Exclude ranger-hive (HDFS + Ozone only)
  --no-kafka-plugin  Skip Kafka Ranger plugin E2E (Kafka still runs as audit bus)
  --no-hbase         Exclude ranger-hbase (HBase plugin audit E2E)

Build:
  --no-build         Require existing dist/ tarballs (no Maven)
  --docker-build     Full Ranger build inside docker-compose.ranger-build.yml

Runtime:
  --no-verify        Skip smoke tests after up/fix
  --no-recreate-ozone  Skip Ozone container recreate during config
  --timeout SECS     Health wait timeout (default 600)

Examples:
  ./setup-audit-e2e.sh
  ./setup-audit-e2e.sh up --hdfs-only
  ./setup-audit-e2e.sh up --ozone-only --no-verify
  ./setup-audit-e2e.sh fix --hdfs-only
  ./setup-audit-e2e.sh repair-ozone
  ./setup-audit-e2e.sh generate-access-logs
  ./setup-audit-e2e.sh generate-access-logs --hdfs-only
  ./setup-audit-e2e.sh diagnose && ./setup-audit-e2e.sh verify

Admin UI: http://localhost:6080  (admin / rangerR0cks!)
Pipeline: plugin → ingestor:7081 → Kafka:ranger_audits → dispatcher → Solr → Admin Audit tab
EOF
}

COMPOSE_FILE="docker-compose.ranger-audit-e2e.yml"
ACTION="${1:-up}"
if [[ "${ACTION}" == "-h" || "${ACTION}" == "--help" ]]; then
  usage
  exit 0
fi
shift || true

# Scope: both (default) | hdfs | ozone
SCOPE="both"
INCLUDE_HIVE=true
INCLUDE_HBASE=true
INCLUDE_KAFKA_PLUGIN=true
DO_BUILD=true
DOCKER_BUILD=false
DO_VERIFY=true
RECREATE_OZONE=true
HEALTH_TIMEOUT=600
SMOKE_TIMEOUT=120

# Tarball minimum sizes (bytes) — stubs from partial mvn -pl are ~12–19 KB
MIN_ADMIN_BYTES=1000000
MIN_PLUGIN_BYTES=5000000
MIN_AUDIT_BYTES=500000
MIN_OZONE_PLUGIN_BYTES=1000000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hdfs-only) SCOPE="hdfs"; shift ;;
    --ozone-only) SCOPE="ozone"; shift ;;
    --with-hive) INCLUDE_HIVE=true; shift ;;
    --no-hive) INCLUDE_HIVE=false; shift ;;
    --no-kafka-plugin) INCLUDE_KAFKA_PLUGIN=false; shift ;;
    --no-hbase) INCLUDE_HBASE=false; shift ;;
    --no-build) DO_BUILD=false; shift ;;
    --docker-build) DOCKER_BUILD=true; DO_BUILD=true; shift ;;
    --no-verify) DO_VERIFY=false; shift ;;
    --no-recreate-ozone) RECREATE_OZONE=false; shift ;;
    --timeout) HEALTH_TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# Safe .env load — unquoted JAVA_OPTS breaks `source` under set -e.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue
    [[ "${line}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
    export "${line}"
  done < "${SCRIPT_DIR}/.env"
fi
# shellcheck source=scripts/audit/audit-stack-lib.sh
source "${SCRIPT_DIR}/scripts/audit/audit-stack-lib.sh"

export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
export KERBEROS_ENABLED="${KERBEROS_ENABLED:-true}"
export RANGER_VERSION="${RANGER_VERSION:-3.0.0-SNAPSHOT}"
export OZONE_PLUGIN_VERSION="${OZONE_PLUGIN_VERSION:-${RANGER_VERSION}}"
export OZONE_VERSION="${OZONE_VERSION:-2.1.0}"
export HIVE_PLUGIN_VERSION="${HIVE_PLUGIN_VERSION:-${RANGER_VERSION}}"
export HIVE_VERSION="${HIVE_VERSION:-4.0.1}"
export SOLR_PLUGIN_VERSION="${SOLR_PLUGIN_VERSION:-${RANGER_VERSION}}"
export HIVE_HADOOP_VERSION="${HIVE_HADOOP_VERSION:-3.4.2}"
export TEZ_VERSION="${TEZ_VERSION:-0.10.4}"
export KAFKA_PLUGIN_VERSION="${KAFKA_PLUGIN_VERSION:-${RANGER_VERSION}}"
export HBASE_PLUGIN_VERSION="${HBASE_PLUGIN_VERSION:-${RANGER_VERSION}}"
export HBASE_VERSION="${HBASE_VERSION:-2.6.1}"

# Hive needs HDFS/Hadoop in this stack; ozone-only without hadoop cannot run HS2.
if [[ "${INCLUDE_HIVE}" == "true" && "${SCOPE}" == "ozone" ]]; then
  echo "NOTE: --with-hive requires ranger-hadoop; treating scope as both (hdfs+ozone+hive)" >&2
  SCOPE="both"
fi
if [[ "${SCOPE}" == "ozone" ]]; then
  INCLUDE_HIVE=false
  INCLUDE_HBASE=false
fi
if [[ "${SCOPE}" == "hdfs" && "${INCLUDE_HIVE}" == "true" ]]; then
  : # hdfs + hive is valid
fi

chmod +x download-archives.sh scripts/audit/*.sh scripts/**/*.sh 2>/dev/null || true

# ---------------------------------------------------------------------------
# dist/ helpers — build tarballs when missing (no separate prepare script)
# ---------------------------------------------------------------------------

min_tarball_bytes() {
  local f="$1"
  local min="${2:-500000}"
  [[ -f "${f}" ]] || return 1
  local size
  size="$(wc -c < "${f}" | tr -d ' ')"
  [[ "${size}" -ge "${min}" ]]
}

mvn_assembly() {
  local descriptor="$1"
  echo "  assembly: ${descriptor}"
  export MAVEN_OPTS="${MAVEN_OPTS:--Xmx8g -Xms2g}"
  (
    cd "${REPO_ROOT}/distro"
    mkdir -p target
    echo "${RANGER_VERSION}" > target/version
    mvn -P-all org.apache.maven.plugins:maven-assembly-plugin:3.6.0:single \
      -DskipAssembly=false \
      -Ddescriptor="src/main/assembly/${descriptor}" \
      -DfinalName="ranger-${RANGER_VERSION}" \
      -DoutputDirectory=../target \
      -Dcheckstyle.skip=true -Dpmd.skip=true -Drat.skip=true -q
  )
}

mvn_package_modules() {
  local profile="$1"
  local modules="$2"
  echo "  mvn package -P${profile} -pl ${modules} -am"
  export MAVEN_OPTS="${MAVEN_OPTS:--Xmx8g -Xms2g}"
  (
    cd "${REPO_ROOT}"
    mvn package -P"${profile}" -pl "${modules}" -am \
      -DskipTests -Dcheckstyle.skip=true -Dpmd.skip=true -Drat.skip=true -q
  )
}

copy_target_tarball() {
  local name="$1"
  local src="${REPO_ROOT}/target/ranger-${RANGER_VERSION}-${name}.tar.gz"
  local dst="${SCRIPT_DIR}/dist/ranger-${RANGER_VERSION}-${name}.tar.gz"
  if [[ ! -f "${src}" ]]; then
    echo "ERROR: expected ${src} after assembly" >&2
    return 1
  fi
  cp -f "${src}" "${dst}"
  echo "  copied ${dst} ($(wc -c < "${dst}" | tr -d ' ') bytes)"
}

ensure_audit_tarballs() {
  local ingestor="dist/ranger-${RANGER_VERSION}-audit-ingestor.tar.gz"
  local dispatcher="dist/ranger-${RANGER_VERSION}-audit-dispatcher.tar.gz"
  if min_tarball_bytes "${ingestor}" "${MIN_AUDIT_BYTES}" \
    && min_tarball_bytes "${dispatcher}" "${MIN_AUDIT_BYTES}"; then
    echo "Audit tarballs OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing audit tarballs; run without --no-build" >&2
    return 1
  }
  echo "Building audit-ingestor + audit-dispatcher..."
  mvn_package_modules "all" ":ranger-audit-ingestor,:ranger-audit-dispatcher"
  for desc in audit-ingestor audit-dispatcher; do
    mvn_assembly "${desc}.xml"
    copy_target_tarball "${desc}"
  done
}

ensure_admin_tarball() {
  local tarball="dist/ranger-${RANGER_VERSION}-admin.tar.gz"
  if min_tarball_bytes "${tarball}" "${MIN_ADMIN_BYTES}"; then
    echo "Admin tarball OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing admin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building admin tarball..."
  mvn_package_modules "ranger-admin" ":security-admin"
  mvn_assembly "admin-web.xml"
  copy_target_tarball "admin"
}

ensure_hdfs_plugin_tarball() {
  local tarball="dist/ranger-${RANGER_VERSION}-hdfs-plugin.tar.gz"
  if min_tarball_bytes "${tarball}" "${MIN_PLUGIN_BYTES}"; then
    echo "HDFS plugin tarball OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing HDFS plugin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building HDFS plugin tarball..."
  mvn_package_modules "ranger-hdfs-plugin" ":ranger-hdfs-plugin"
  mvn_assembly "hdfs-agent.xml"
  copy_target_tarball "hdfs-plugin"
}

ensure_hive_plugin_tarball() {
  local ver="${HIVE_PLUGIN_VERSION:-${RANGER_VERSION}}"
  local tarball="dist/ranger-${ver}-hive-plugin.tar.gz"
  if min_tarball_bytes "${tarball}" "${MIN_PLUGIN_BYTES}"; then
    echo "Hive plugin tarball OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing Hive plugin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building Hive plugin tarball..."
  if [[ -x "${SCRIPT_DIR}/scripts/hive/build-hive-plugin-tarball.sh" ]]; then
    "${SCRIPT_DIR}/scripts/hive/build-hive-plugin-tarball.sh"
  else
    mvn_package_modules "ranger-hive-plugin" \
      ":ranger-hive-plugin,:ranger-audit-core,:ranger-audit-dest-auditserver"
    mvn_assembly "hive-agent.xml"
    copy_target_tarball "hive-plugin"
  fi
}

ensure_kafka_plugin_tarball() {
  local ver="${KAFKA_PLUGIN_VERSION:-${RANGER_VERSION}}"
  local tarball="dist/ranger-${ver}-kafka-plugin.tar.gz"
  if min_tarball_bytes "${tarball}" "${MIN_PLUGIN_BYTES}"; then
    echo "Kafka plugin tarball OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing Kafka plugin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building Kafka plugin tarball..."
  mvn_package_modules "ranger-kafka-plugin" ":ranger-kafka-plugin,:ranger-distro"
  mvn_assembly "plugin-kafka.xml"
  local src="${REPO_ROOT}/target/ranger-${RANGER_VERSION}-kafka-plugin.tar.gz"
  local dst="${SCRIPT_DIR}/dist/ranger-${ver}-kafka-plugin.tar.gz"
  if [[ ! -f "${src}" ]]; then
    echo "ERROR: expected ${src} after assembly" >&2
    return 1
  fi
  cp -f "${src}" "${dst}"
  echo "  copied ${dst} ($(wc -c < "${dst}" | tr -d ' ') bytes)"
}

ensure_hbase_plugin_tarball() {
  local ver="${HBASE_PLUGIN_VERSION:-${RANGER_VERSION}}"
  local tarball="dist/ranger-${ver}-hbase-plugin.tar.gz"
  if min_tarball_bytes "${tarball}" "${MIN_PLUGIN_BYTES}"; then
    echo "HBase plugin tarball OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing HBase plugin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building HBase plugin tarball..."
  mvn_package_modules "ranger-hbase-plugin" ":ranger-hbase-plugin,:ranger-distro"
  mvn_assembly "hbase-agent.xml"
  local src="${REPO_ROOT}/target/ranger-${RANGER_VERSION}-hbase-plugin.tar.gz"
  local dst="${SCRIPT_DIR}/dist/ranger-${ver}-hbase-plugin.tar.gz"
  if [[ ! -f "${src}" ]]; then
    echo "ERROR: expected ${src} after assembly" >&2
    return 1
  fi
  cp -f "${src}" "${dst}"
  echo "  copied ${dst} ($(wc -c < "${dst}" | tr -d ' ') bytes)"
}

solr_plugin_tarball_ok() {
  local tarball="$1"
  local ver="${SOLR_PLUGIN_VERSION:-${RANGER_VERSION}}"
  [[ -f "${tarball}" ]] || return 1
  min_tarball_bytes "${tarball}" "${MIN_PLUGIN_BYTES}" || return 1
  tar -tzf "${tarball}" 2>/dev/null | grep -q 'enable-solr-plugin.sh' || return 1
  tar -tzf "${tarball}" 2>/dev/null | grep -q 'lib/ranger-solr-plugin-shim.*\.jar' || return 1
  tar -tzf "${tarball}" 2>/dev/null | grep -q "lib/ranger-solr-plugin-impl/ranger-solr-plugin-${ver}.jar" || return 1
  tar -tzf "${tarball}" 2>/dev/null | head -1 | grep -q "ranger-${ver}-solr-plugin/" || return 1
  local jar_count
  jar_count="$(tar -tzf "${tarball}" 2>/dev/null | grep -c '\.jar$' || true)"
  [[ "${jar_count}" -le 200 ]] || return 1
  return 0
}

ensure_solr_plugin_tarball() {
  local ver="${SOLR_PLUGIN_VERSION:-${RANGER_VERSION}}"
  local tarball="dist/ranger-${ver}-solr-plugin.tar.gz"
  if solr_plugin_tarball_ok "${tarball}"; then
    echo "Solr plugin tarball OK"
    return 0
  fi
  if [[ -f "${tarball}" ]]; then
    echo "WARN: ${tarball} is stub, over-packed, or missing enable/shim — rebuilding" >&2
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing or invalid Solr plugin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building Solr plugin tarball..."
  if [[ -x "${SCRIPT_DIR}/scripts/solr/build-solr-plugin-tarball.sh" ]]; then
    "${SCRIPT_DIR}/scripts/solr/build-solr-plugin-tarball.sh"
  else
    mvn_package_modules "ranger-solr-plugin" \
      ":ranger-solr-plugin,:ranger-audit-core,:ranger-audit-dest-auditserver"
    mvn_assembly "plugin-solr.xml"
    copy_target_tarball "solr-plugin"
  fi
  solr_plugin_tarball_ok "${tarball}" || {
    echo "ERROR: Solr plugin tarball still invalid after build" >&2
    return 1
  }
}

ensure_ozone_plugin_tarball() {
  local tarball="dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin.tar.gz"
  if min_tarball_bytes "${tarball}" "${MIN_OZONE_PLUGIN_BYTES}"; then
    echo "Ozone plugin tarball OK"
    return 0
  fi
  [[ "${DO_BUILD}" == "true" ]] || {
    echo "ERROR: missing Ozone plugin tarball; run without --no-build" >&2
    return 1
  }
  echo "Building Ozone plugin tarball (with audit modules)..."
  mvn_package_modules "ranger-ozone-plugin" \
    ":ranger-ozone-plugin,:ranger-audit-core,:ranger-audit-dest-auditserver"
  mvn_assembly "plugin-ozone.xml"
  local src="${REPO_ROOT}/target/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz"
  local dst="${SCRIPT_DIR}/dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin.tar.gz"
  cp -f "${src}" "${dst}"
  echo "  copied ${dst} ($(wc -c < "${dst}" | tr -d ' ') bytes)"
}

# Tier 4: copy audit + jersey JARs into extracted Ozone plugin tree when assembly missed them
ensure_ozone_plugin_audit_jars() {
  local impl="dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin/lib/libext/ranger-ozone-plugin-impl"
  local version="${OZONE_PLUGIN_VERSION}"
  mkdir -p "${impl}"

  local jersey="jersey-server-2.47.jar"
  local ozone_jersey="${SCRIPT_DIR}/downloads/ozone-${OZONE_VERSION}/share/ozone/lib/${jersey}"
  if [[ ! -s "${impl}/${jersey}" ]] && [[ -s "${ozone_jersey}" ]]; then
    cp -f "${ozone_jersey}" "${impl}/${jersey}"
    echo "  Ozone plugin: copied ${jersey}"
  fi

  local jar src
  for jar in "ranger-audit-core-${version}.jar" "ranger-audit-dest-auditserver-${version}.jar"; do
    if [[ -s "${impl}/${jar}" ]]; then
      continue
    fi
    case "${jar}" in
      ranger-audit-core-*)
        src="${REPO_ROOT}/agents-audit/core/target/${jar}"
        ;;
      ranger-audit-dest-auditserver-*)
        src="${REPO_ROOT}/agents-audit/dest-auditserver/target/${jar}"
        ;;
    esac
    if [[ ! -s "${src}" ]]; then
      echo "  Building audit JARs for Ozone plugin..."
      mvn_package_modules "ranger-ozone-plugin" \
        ":ranger-audit-core,:ranger-audit-dest-auditserver"
      [[ -s "${src}" ]] || {
        echo "ERROR: still missing ${src}" >&2
        return 1
      }
    fi
    cp -f "${src}" "${impl}/${jar}"
    echo "  Ozone plugin: copied ${jar}"
  done
}

prepare_ozone_plugin_dist() {
  local dir="dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin"
  local tarball="dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin.tar.gz"
  ensure_ozone_plugin_tarball
  if [[ ! -d "${dir}" ]]; then
    tar xvfz "${tarball}" --directory=dist/
  fi
  cp -f scripts/ozone/ranger-ozone-plugin-install.properties "${dir}/install.properties"
  cp -f scripts/ozone/ranger-ozone-setup.sh "${dir}/"
  cp -f scripts/ozone/enable-ozone-plugin.sh "${dir}/"
  chmod +x "${dir}/ranger-ozone-setup.sh" "${dir}/enable-ozone-plugin.sh"
  ensure_ozone_plugin_audit_jars
}

run_docker_full_build() {
  echo "==> Full Ranger build via docker-compose.ranger-build.yml"
  export RANGER_HOME="${REPO_ROOT}"
  docker compose -f docker-compose.ranger-build.yml down --remove-orphans 2>/dev/null || true
  docker compose -f docker-compose.ranger-build.yml up
}

prepare_dist() {
  mkdir -p dist
  echo "==> Preparing dist/ (scope=${SCOPE}, RANGER_VERSION=${RANGER_VERSION})"
  chmod +x ./download-archives.sh 2>/dev/null || true
  ./download-archives.sh hadoop kafka

  if [[ "${DOCKER_BUILD}" == "true" ]]; then
    run_docker_full_build
  elif [[ "${DO_BUILD}" != "true" ]]; then
    echo "==> --no-build: verifying dist/ tarballs"
    ensure_admin_tarball
    ensure_audit_tarballs
    ensure_solr_plugin_tarball
    [[ "${SCOPE}" != "ozone" ]] && ensure_hdfs_plugin_tarball
    [[ "${SCOPE}" != "hdfs" ]] && ensure_ozone_plugin_tarball
    [[ "${INCLUDE_HIVE}" == "true" ]] && ensure_hive_plugin_tarball
    [[ "${INCLUDE_HBASE}" == "true" ]] && ensure_hbase_plugin_tarball
    [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]] && ensure_kafka_plugin_tarball
    return 0
  fi

  ensure_admin_tarball
  ensure_audit_tarballs
  ensure_solr_plugin_tarball
  if [[ "${SCOPE}" != "ozone" ]]; then
    ensure_hdfs_plugin_tarball
  fi
  if [[ "${SCOPE}" != "hdfs" ]]; then
    prepare_ozone_plugin_dist
    ./download-archives.sh ozone
  fi
  if [[ "${INCLUDE_HIVE}" == "true" ]]; then
    ensure_hive_plugin_tarball
    ./download-archives.sh hive
  fi
  if [[ "${INCLUDE_HBASE}" == "true" && "${SCOPE}" != "ozone" ]]; then
    ensure_hbase_plugin_tarball
    ./download-archives.sh hbase
  fi
  if [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]]; then
    ensure_kafka_plugin_tarball
  fi
  echo "==> dist/ ready"
}

# ---------------------------------------------------------------------------
# Compose — service lists per scope
# ---------------------------------------------------------------------------

compose_service_list() {
  local -a services=(
    ranger ranger-kdc ranger-db ranger-zk ranger-solr ranger-kafka
    ranger-audit-ingestor ranger-audit-dispatcher-solr
  )
  case "${SCOPE}" in
    hdfs)
      services+=(ranger-hadoop ranger-audit-dispatcher-hdfs)
      ;;
    ozone)
      services+=(scm datanode om)
      ;;
    both)
      services+=(ranger-hadoop ranger-audit-dispatcher-hdfs scm datanode om)
      ;;
  esac
  if [[ "${INCLUDE_HIVE}" == "true" && "${SCOPE}" != "ozone" ]]; then
    services+=(ranger-hive)
  fi
  if [[ "${INCLUDE_HBASE}" == "true" && "${SCOPE}" != "ozone" ]]; then
    services+=(ranger-hbase)
  fi
  printf '%s\n' "${services[@]}"
}

ensure_compose_network() {
  # Pre-creating rangernw breaks compose labels; remove stale non-compose network only.
  if ! docker network inspect rangernw >/dev/null 2>&1; then
    return 0
  fi
  local label containers
  label="$(docker network inspect rangernw --format '{{index .Labels "com.docker.compose.network"}}' 2>/dev/null || true)"
  if [[ "${label}" == "ranger" ]]; then
    return 0
  fi
  containers="$(docker network inspect rangernw --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || true)"
  if [[ -n "${containers// }" ]]; then
    echo "ERROR: rangernw exists but is not owned by this compose project." >&2
    echo "       Run: ./setup-audit-e2e.sh down   then retry." >&2
    return 1
  fi
  docker network rm rangernw >/dev/null 2>&1 || true
}

ensure_kerberos_audit_services() {
  [[ "${KERBEROS_ENABLED}" != "true" ]] && return 0

  docker start ranger-kdc 2>/dev/null || true
  audit_stack_wait_container ranger-kdc "KDC" 120 || true

  if ! check_container ranger-zk; then
    echo "==> Restarting ranger-zk (Kerberos SASL)"
    docker compose -f "${COMPOSE_FILE}" up -d --no-deps --force-recreate ranger-zk
    sleep 20
  fi
  audit_stack_wait_container ranger-zk "ZooKeeper" 120 || true

  if ! check_container ranger-kafka; then
    echo "==> Restarting ranger-kafka (depends on ZK)"
    docker compose -f "${COMPOSE_FILE}" up -d --no-deps --force-recreate ranger-kafka
    sleep 30
  fi
  audit_stack_wait_container ranger-kafka "Kafka" 120 || true

  local solr_body
  solr_body="$(audit_stack_solr_curl '/solr/ranger_audits/admin/ping?wt=json' || true)"
  if ! grep -q '"status"[[:space:]]*:[[:space:]]*0' <<< "${solr_body}"; then
    echo "==> Restarting ranger-solr (core not ready)"
    docker compose -f "${COMPOSE_FILE}" up -d --no-deps --force-recreate ranger-solr
    sleep 45
  fi

  ensure_ingestor_kafka_ready 180
}

ensure_ingestor_kafka_ready() {
  local timeout="${1:-120}"
  local deadline=$((SECONDS + timeout))

  ingestor_kafka_producer_ready() {
    local recent
    recent="$(docker logs ranger-audit-ingestor 2>&1 | tail -300)"
    grep -q 'Kafka producer initialized and started' <<< "${recent}" \
      || grep -q 'Kafka producer available: true' <<< "${recent}"
  }

  echo -n "  Ingestor Kafka producer..."
  while (( SECONDS < deadline )); do
    if ingestor_kafka_producer_ready; then
      echo " OK"
      return 0
    fi
    sleep 5
  done

  if ! check_container ranger-audit-ingestor; then
    echo " MISSING"
    return 1
  fi

  echo " restarting ingestor"
  docker restart ranger-audit-ingestor >/dev/null 2>&1 || true
  sleep 45
  if ingestor_kafka_producer_ready; then
    echo "  Ingestor Kafka producer: OK (after restart)"
    return 0
  fi
  echo "  Ingestor Kafka producer: WARN (audits may spool to recovery)"
  return 1
}

compose_up() {
  ensure_compose_network
  local -a services=()
  while IFS= read -r svc; do
    [[ -n "${svc}" ]] && services+=("${svc}")
  done < <(compose_service_list)

  echo "==> docker compose up scope=${SCOPE} (${#services[@]} services)"
  docker compose -f "${COMPOSE_FILE}" up -d --build --remove-orphans "${services[@]}"
  ensure_kerberos_audit_services
  echo "==> Waiting for containers to settle (30s)"
  sleep 30
}

compose_restart() {
  local -a services=()
  while IFS= read -r svc; do
    [[ -n "${svc}" ]] && services+=("${svc}")
  done < <(compose_service_list)
  docker compose -f "${COMPOSE_FILE}" up -d --remove-orphans "${services[@]}"
  ensure_kerberos_audit_services
  echo "==> Waiting after restart (30s)"
  sleep 30
}

redeploy_audit_services() {
  echo "==> Rebuilding ingestor + dispatchers"
  local -a rebuild=(ranger-audit-ingestor ranger-audit-dispatcher-solr)
  [[ "${SCOPE}" != "ozone" ]] && rebuild+=(ranger-audit-dispatcher-hdfs)
  docker compose -f "${COMPOSE_FILE}" build "${rebuild[@]}"
  docker compose -f "${COMPOSE_FILE}" up -d --force-recreate --no-deps "${rebuild[@]}"
  sleep 90
}

# ---------------------------------------------------------------------------
# Health / audit XML checks
# ---------------------------------------------------------------------------

audit_xml_has_auditserver() {
  local container="$1"
  local path="$2"
  docker exec "${container}" test -f "${path}" || return 1
  docker exec -i "${container}" python3 - "${path}" <<'PY'
import sys
import xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
for prop in root.findall("property"):
    n = prop.find("name")
    if n is not None and (n.text or "").strip() == "xasecure.audit.destination.auditserver":
        v = prop.find("value")
        ok = v is not None and (v.text or "").strip().lower() == "true"
        sys.exit(0 if ok else 1)
sys.exit(1)
PY
}

wait_for_health() {
  local timeout="${1:-${HEALTH_TIMEOUT}}"
  local fail=0

  echo "==> Waiting for stack health (scope=${SCOPE}, timeout ${timeout}s)"
  audit_stack_wait_url "http://localhost:6080/login.jsp" "Admin UI (6080)" "${timeout}" || fail=1
  audit_stack_wait_url "http://localhost:7081/api/audit/health" "Ingestor (7081)" "${timeout}" || fail=1
  audit_stack_wait_url "http://localhost:7091/api/health/ping" "Solr dispatcher (7091)" "${timeout}" || fail=1
  audit_stack_wait_container "ranger-kafka" "Kafka" "${timeout}" || fail=1
  audit_stack_wait_solr "Solr collection" "${timeout}" || fail=1
  ensure_ingestor_kafka_ready 120 || fail=1

  if [[ "${SCOPE}" != "ozone" ]]; then
    audit_stack_wait_url "http://localhost:7092/api/health/ping" "HDFS dispatcher (7092)" "${timeout}" || fail=1
    audit_stack_wait_container "ranger-hadoop" "Hadoop" "${timeout}" || fail=1
    if docker exec ranger-hadoop test -f /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml 2>/dev/null; then
      echo -n "  HDFS auditserver config..."
      if audit_xml_has_auditserver ranger-hadoop /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml; then
        echo " OK"
      else
        echo " FAIL"
        fail=1
      fi
    else
      echo "  HDFS audit XML: missing"
      fail=1
    fi
  fi

  if [[ "${SCOPE}" != "hdfs" ]]; then
    audit_stack_wait_container "ozone-scm" "Ozone SCM" "${timeout}" || fail=1
    audit_stack_wait_container "ozone-datanode" "Ozone datanode" "${timeout}" || fail=1
    audit_stack_wait_container "ozone-om" "Ozone OM" "${timeout}" || fail=1
    if docker exec ozone-om test -f /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml 2>/dev/null; then
      echo -n "  Ozone auditserver config..."
      if audit_xml_has_auditserver ozone-om /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml; then
        echo " OK"
      else
        echo " FAIL"
        fail=1
      fi
    else
      echo "  Ozone audit XML: missing"
      fail=1
    fi
  fi

  if [[ "${INCLUDE_HIVE}" == "true" ]] && check_container ranger-hive; then
    audit_stack_wait_container "ranger-hive" "ranger-hive" "${timeout}" || fail=1
    if docker exec ranger-hive timeout 3 bash -c 'echo >/dev/tcp/localhost/10000' 2>/dev/null; then
      echo "  HiveServer2 port 10000: OK"
    else
      echo "  HiveServer2 port 10000: FAIL"
      fail=1
    fi
    if docker exec ranger-hive test -f /opt/hive/conf/ranger-hive-audit.xml 2>/dev/null; then
      echo -n "  Hive auditserver config..."
      if audit_xml_has_auditserver ranger-hive /opt/hive/conf/ranger-hive-audit.xml; then
        echo " OK"
      else
        echo " FAIL"
        fail=1
      fi
    else
      echo "  Hive audit XML: missing"
      fail=1
    fi
  fi

  return "${fail}"
}

# ---------------------------------------------------------------------------
# Runtime config (delegates to idempotent scripts under scripts/)
# ---------------------------------------------------------------------------

ensure_admin_solr_backend() {
  local script="${SCRIPT_DIR}/scripts/admin/ensure-admin-audit-solr.sh"
  if docker inspect ranger >/dev/null 2>&1; then
    if docker exec ranger test -f /home/ranger/scripts/admin/ensure-admin-audit-solr.sh 2>/dev/null; then
      docker exec ranger /home/ranger/scripts/admin/ensure-admin-audit-solr.sh
    else
      docker exec -i ranger bash -s < "${script}"
    fi
  elif [[ -f "${script}" ]]; then
    bash "${script}" 2>/dev/null || true
  fi
}

ensure_hdfs_audit_spool_dirs() {
  if ! check_container ranger-hadoop; then
    return 0
  fi
  echo "==> HDFS audit spool dirs"
  docker exec ranger-hadoop bash -c '
    mkdir -p /var/log/hadoop/hdfs/audit/audit-ingestor/spool \
             /var/log/hadoop/hdfs/audit/solr/spool \
             /var/log/hadoop/hdfs/audit/hdfs/spool \
             /var/log/hadoop/hdfs/audit/archive
    chown -R hdfs:hadoop /var/log/hadoop/hdfs/audit
    chmod -R g+w /var/log/hadoop/hdfs/audit
  ' 2>/dev/null || true
}

apply_hdfs_runtime_config() {
  echo "==> HDFS runtime config"
  ./scripts/hadoop/ensure-hdfs-plugin-audit-config.sh
  ./scripts/hadoop/ensure-dev-hdfs-e2e-audit-config.sh
  ./scripts/hadoop/ensure-hdfs-deny-traverse-policy.sh
  ensure_hdfs_audit_spool_dirs
  ensure_admin_solr_backend
}

apply_ozone_runtime_config() {
  local recreate="${1:-${RECREATE_OZONE}}"
  echo "==> Ozone runtime config"
  prepare_ozone_plugin_dist
  ./scripts/ozone/ensure-ozone-kdc-keytabs.sh
  ./scripts/ozone/ensure-dev-ozone-service-config.sh
  ./scripts/ozone/ensure-dev-ozone-e2e-audit-config.sh
  ./scripts/ozone/ensure-dev-ozone-om-policy.sh
  ./scripts/admin/ensure-ranger-admin-plugin-download-access.sh
  ./scripts/audit/ensure-audit-ingestor-ozone.sh
  ./scripts/ozone/apply-ozone-plugin-audit-config.sh

  if [[ "${recreate}" == "true" ]] && check_container ozone-om; then
    echo "==> Recreating Ozone containers"
    docker compose -f "${COMPOSE_FILE}" up -d --no-deps --force-recreate scm datanode om
    sleep 60
    ./scripts/audit/ensure-audit-ingestor-ozone.sh
  fi
}

apply_hive_runtime_config() {
  echo "==> Hive runtime config"
  if ! check_container ranger-hive; then
    echo "  ranger-hive not running — skip"
    return 0
  fi
  if [[ -x "${SCRIPT_DIR}/scripts/hive/deploy-hive-plugin-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/hive/deploy-hive-plugin-e2e.sh" || {
      echo "  WARN: deploy-hive-plugin-e2e failed; waiting for HS2 recovery" >&2
      docker start ranger-hive 2>/dev/null || true
      audit_stack_wait_container ranger-hive "ranger-hive" 180 || true
      local hs2_deadline=$((SECONDS + 360))
      while (( SECONDS < hs2_deadline )); do
        if docker exec ranger-hive timeout 2 bash -c 'echo >/dev/tcp/localhost/10000' 2>/dev/null; then
          echo "  HS2 recovered on port 10000"
          break
        fi
        sleep 5
      done
      "${SCRIPT_DIR}/scripts/hive/apply-hive-plugin-audit-config.sh" || true
    }
  else
    ./scripts/hive/apply-hive-plugin-audit-config.sh || true
  fi
}

apply_kafka_runtime_config() {
  local restart_broker="${1:-false}"
  if [[ "${INCLUDE_KAFKA_PLUGIN}" != "true" ]]; then
    return 0
  fi
  echo "==> Kafka plugin runtime config"
  if ! check_container ranger-kafka; then
    echo "  ranger-kafka not running — skip"
    return 0
  fi
  ensure_kafka_plugin_tarball || true
  "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" || true
  if ! docker exec ranger-kafka test -d /opt/ranger/ranger-kafka-plugin/lib/ranger-kafka-plugin-impl 2>/dev/null; then
    echo "  Kafka plugin not enabled — deploying..."
    if [[ -x "${SCRIPT_DIR}/scripts/kafka/deploy-kafka-plugin-e2e.sh" ]]; then
      "${SCRIPT_DIR}/scripts/kafka/deploy-kafka-plugin-e2e.sh" --no-restart || true
    fi
  else
    docker exec ranger-kafka bash "${RANGER_SCRIPTS:-/home/ranger/scripts}/ensure-kafka-plugin-audit-jars.sh" || true
    docker exec ranger-kafka bash "${RANGER_SCRIPTS:-/home/ranger/scripts}/apply-kafka-plugin-repo-config.sh" || true
    docker exec ranger-kafka bash "${RANGER_SCRIPTS:-/home/ranger/scripts}/apply-kafka-plugin-audit-config.sh" --no-restart || true
    "${SCRIPT_DIR}/scripts/kafka/ensure-kafka-audit-bus-acls.sh" || true
    if [[ "${restart_broker}" == "true" ]]; then
      "${SCRIPT_DIR}/scripts/kafka/restart-kafka-broker-docker.sh" || true
    fi
  fi
  if [[ "${restart_broker}" == "true" ]] \
    && docker exec ranger-kafka test -d /opt/ranger/ranger-kafka-plugin/lib/ranger-kafka-plugin-impl 2>/dev/null; then
    if ! docker exec ranger-kafka timeout 2 bash -c 'echo >/dev/tcp/localhost/9092' 2>/dev/null; then
      "${SCRIPT_DIR}/scripts/kafka/restart-kafka-broker-docker.sh" || true
    fi
  fi
}

apply_hbase_runtime_config() {
  if [[ "${INCLUDE_HBASE}" != "true" ]]; then
    return 0
  fi
  echo "==> HBase plugin runtime config"
  if ! check_container ranger-hbase; then
    echo "  ranger-hbase not running — skip"
    return 0
  fi
  ensure_hbase_plugin_tarball || true
  if ! docker exec ranger-hbase test -d /opt/ranger/ranger-hbase-plugin/lib/ranger-hbase-plugin-impl 2>/dev/null; then
    echo "  HBase plugin not enabled — deploying..."
    if [[ -x "${SCRIPT_DIR}/scripts/hbase/deploy-hbase-plugin-e2e.sh" ]]; then
      "${SCRIPT_DIR}/scripts/hbase/deploy-hbase-plugin-e2e.sh" || true
    fi
  else
    "${SCRIPT_DIR}/scripts/hbase/apply-hbase-plugin-audit-config.sh" || true
  fi
}

apply_knox_runtime_config() {
  echo "==> Knox plugin runtime config"
  if ! check_container ranger-knox; then
    echo "  ranger-knox not running — skip"
    return 0
  fi
  "${SCRIPT_DIR}/scripts/knox/apply-knox-plugin-audit-config.sh" --no-restart || true
  "${SCRIPT_DIR}/scripts/knox/ensure-knox-ingestor-allowlist.sh" || true
}

apply_kms_runtime_config() {
  echo "==> KMS plugin runtime config"
  if ! check_container ranger-kms; then
    echo "  ranger-kms not running — skip"
    return 0
  fi
  docker network connect rangernw ranger 2>/dev/null || true
  "${SCRIPT_DIR}/scripts/kms/ensure-kms-plugin-audit-jars.sh" || true
  "${SCRIPT_DIR}/scripts/kms/clear-dev-kms-audit-exclude.sh" || true
  "${SCRIPT_DIR}/scripts/kms/ensure-dev-kms-e2e-policy.sh" || true
  "${SCRIPT_DIR}/scripts/kms/apply-kms-plugin-audit-config.sh" || true
  "${SCRIPT_DIR}/scripts/kms/ensure-kms-ingestor-allowlist.sh" || true
}

apply_runtime_config() {
  local recreate="${1:-${RECREATE_OZONE}}"
  echo "==> Applying runtime config (scope=${SCOPE}, hive=${INCLUDE_HIVE}, hbase=${INCLUDE_HBASE}, kafka_plugin=${INCLUDE_KAFKA_PLUGIN})"
  "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" || true
  if [[ "${SCOPE}" != "ozone" ]]; then
    apply_hdfs_runtime_config
  fi
  if [[ "${SCOPE}" != "hdfs" ]]; then
    apply_ozone_runtime_config "${recreate}"
  fi
  if [[ "${INCLUDE_HIVE}" == "true" ]]; then
    apply_hive_runtime_config
  fi
  if [[ "${INCLUDE_HBASE}" == "true" ]]; then
    apply_hbase_runtime_config
  fi
  if [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]]; then
    apply_kafka_runtime_config false
  fi
  apply_knox_runtime_config
  apply_kms_runtime_config
  if [[ "${SCOPE}" == "ozone" ]]; then
    ensure_admin_solr_backend
  fi
}

repair_hdfs() {
  echo "==> repair-hdfs"
  apply_hdfs_runtime_config
  echo "==> Restarting ranger-hadoop (pick up audit config; do not stop-dfs in-place — exits container)"
  docker start ranger-hadoop 2>/dev/null || true
  docker restart ranger-hadoop 2>/dev/null || true
  sleep 45
  audit_stack_wait_container ranger-hadoop "Hadoop" 120 || true
  redeploy_audit_services
  ensure_ingestor_kafka_ready 120 || true
  echo "HDFS repair done — run: ./setup-audit-e2e.sh trigger-hdfs-audit"
}

repair_ozone() {
  echo "==> repair-ozone"
  prepare_ozone_plugin_dist
  apply_ozone_runtime_config "${RECREATE_OZONE}"
  echo "==> Recreating ozone-om (keytab mount + audit JAAS config)"
  docker compose -f "${COMPOSE_FILE}" up -d --no-deps om
  sleep 45
  audit_stack_wait_container ozone-om "Ozone OM" 120 || true
  ./scripts/audit/ensure-audit-ingestor-ozone.sh
  echo "Ozone repair done — run: ./setup-audit-e2e.sh trigger-ozone-audit"
}

repair_hive() {
  echo "==> repair-hive"
  ensure_hive_plugin_tarball
  apply_hive_runtime_config
  if [[ -x "${SCRIPT_DIR}/scripts/hive/verify-hive-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/hive/verify-hive-plugin-audit-e2e.sh" || true
  fi
  echo "Hive repair done — run: ./setup-audit-e2e.sh trigger-hive-audit"
}

repair_kafka() {
  echo "==> repair-kafka"
  ensure_kafka_plugin_tarball
  if [[ -x "${SCRIPT_DIR}/scripts/kafka/deploy-kafka-plugin-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/kafka/deploy-kafka-plugin-e2e.sh"
  else
    apply_kafka_runtime_config true
  fi
  if [[ -x "${SCRIPT_DIR}/scripts/kafka/verify-kafka-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/kafka/verify-kafka-plugin-audit-e2e.sh" || true
  fi
  echo "Kafka plugin repair done — run: ./setup-audit-e2e.sh trigger-kafka-audit"
}

repair_hbase() {
  echo "==> repair-hbase"
  ensure_hbase_plugin_tarball
  apply_hbase_runtime_config
  if [[ -x "${SCRIPT_DIR}/scripts/hbase/verify-hbase-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/hbase/verify-hbase-plugin-audit-e2e.sh" || true
  fi
  echo "HBase plugin repair done — run: ./setup-audit-e2e.sh trigger-hbase-audit"
}

repair_knox() {
  echo "==> repair-knox"
  if ! check_container ranger-knox; then
    echo "ERROR: ranger-knox not running — start docker-compose.ranger-knox.yml (+ ranger-hadoop for WebHDFS)" >&2
    return 1
  fi
  "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" || true
  "${SCRIPT_DIR}/scripts/knox/apply-knox-plugin-audit-config.sh" --no-restart || true
  "${SCRIPT_DIR}/scripts/knox/ensure-knox-ingestor-allowlist.sh" || true
  if [[ -x "${SCRIPT_DIR}/scripts/knox/verify-knox-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/knox/verify-knox-plugin-audit-e2e.sh" || true
  fi
  echo "Knox plugin repair done — run: ./setup-audit-e2e.sh trigger-knox-audit"
}

repair_kms() {
  echo "==> repair-kms"
  if ! check_container ranger-kms; then
    echo "ERROR: ranger-kms not running — start docker-compose.ranger-kms.yml" >&2
    return 1
  fi
  "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" || true
  docker network connect rangernw ranger 2>/dev/null || true
  "${SCRIPT_DIR}/scripts/kms/ensure-kms-plugin-audit-jars.sh" || true
  "${SCRIPT_DIR}/scripts/kms/clear-dev-kms-audit-exclude.sh" || true
  "${SCRIPT_DIR}/scripts/kms/ensure-dev-kms-e2e-policy.sh" || true
  "${SCRIPT_DIR}/scripts/kms/apply-kms-plugin-audit-config.sh" --no-restart || true
  "${SCRIPT_DIR}/scripts/kms/ensure-kms-ingestor-allowlist.sh" || true
  echo "Restarting ranger-kms (plugin-impl classpath + audit XML)..."
  docker restart ranger-kms >/dev/null
  for _ in $(seq 1 30); do
    if curl -sf -o /dev/null \
      "http://localhost:9292/kms/v1/keys/names?user.name=${KMS_E2E_USER:-keyadmin}" 2>/dev/null; then
      echo "OK: ranger-kms responding on 9292"
      break
    fi
    sleep 5
  done
  if [[ -x "${SCRIPT_DIR}/scripts/kms/verify-kms-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/kms/verify-kms-plugin-audit-e2e.sh" || true
  fi
  echo "KMS plugin repair done — run: ./setup-audit-e2e.sh trigger-kms-audit"
}

repair_solr() {
  echo "==> repair-solr"
  ensure_solr_plugin_tarball
  echo "==> Rebuilding ranger-solr image (plugin tarball baked into image)"
  docker compose -f "${COMPOSE_FILE}" build ranger-solr
  docker compose -f "${COMPOSE_FILE}" up -d --no-deps --force-recreate ranger-solr
  sleep 45
  audit_stack_wait_solr "Solr (ranger_audits)" 180 || true
  local solr_ping
  solr_ping="$(audit_stack_solr_curl '/solr/ranger_audits/admin/ping?wt=json' || true)"
  if grep -q '"status"[[:space:]]*:[[:space:]]*0' <<< "${solr_ping}"; then
    echo "Solr repair done — ranger_audits ping OK"
  else
    echo "WARN: Solr ping failed after repair — check: docker logs ranger-solr --tail 80" >&2
    return 1
  fi
}

# ---------------------------------------------------------------------------
# Access log generation — plugin → ingestor → Kafka → dispatcher → Solr → Admin
# ---------------------------------------------------------------------------

PIPELINE_HOPS=6

pipeline_banner() {
  echo ""
  echo "========================================================================"
  echo " $1"
  echo "========================================================================"
}

pipeline_step() {
  echo ""
  echo "── [$1/${PIPELINE_HOPS}] $2 ──"
}

pipeline_ok() {
  echo "  ✓ $1"
}

pipeline_warn() {
  echo "  ~ $1"
}

pipeline_fail() {
  echo "  ✗ $1" >&2
}

solr_count() {
  local query="$1"
  audit_stack_solr_count "${query}"
}

solr_print_latest() {
  local query="$1"
  audit_stack_solr_curl "/solr/ranger_audits/select?q=${query}&rows=1&sort=evtTime+desc&wt=json" 2>/dev/null \
    | python3 -c "
import sys, json
doc = json.load(sys.stdin).get('response', {}).get('docs', [{}])[0]
if not doc:
    sys.exit(0)
for k in ('repo', 'reqUser', 'evtTime', 'resource', 'access', 'action', 'agent', 'cliIP', 'resourceType'):
    if k in doc and doc[k]:
        print(f'    {k}: {doc[k]}')
" 2>/dev/null || true
}

admin_access_audit_total() {
  curl -sf -u 'admin:rangerR0cks!' \
    'http://localhost:6080/service/assets/accessAudit?pageSize=1&startIndex=0' 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('totalCount', len(d.get('vXAccessAudits',[]))))" 2>/dev/null || echo 0
}

admin_access_audit_has_user() {
  local user="$1"
  curl -sf -u 'admin:rangerR0cks!' \
    "http://localhost:6080/service/assets/accessAudit?pageSize=50&startIndex=0" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); audits=d.get('vXAccessAudits',[]); u=sys.argv[1]; print('yes' if any(a.get('requestUser')==u or a.get('reqUser')==u for a in audits) else 'no')" \
    "${user}" 2>/dev/null || echo "no"
}

admin_print_latest_audit() {
  curl -sf -u 'admin:rangerR0cks!' \
    'http://localhost:6080/service/assets/accessAudit?pageSize=1&startIndex=0' 2>/dev/null \
    | python3 -c "
import sys, json
audits = json.load(sys.stdin).get('vXAccessAudits', [])
if not audits:
    sys.exit(0)
a = audits[0]
for k in ('repo', 'reqUser', 'evtTime', 'resource', 'access', 'action', 'agent'):
    if k in a and a[k] is not None:
        print(f'    {k}: {a[k]}')
" 2>/dev/null || true
}

kafka_ranger_audits_end_offset() {
  # Kafka 3.x CLI needs KafkaClient JAAS + --command-config (server JAAS / GetOffsetShell fail on SASL).
  docker exec ranger-kafka bash -c '
    cat > /tmp/e2e-kafka-client.properties <<'\''EOF'\''
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
EOF
    cat > /tmp/e2e-kafka-client-jaas.conf <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab="/etc/keytabs/kafka.keytab"
  principal="kafka/ranger-kafka.rangernw@EXAMPLE.COM";
};
EOF
    export KAFKA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=/tmp/e2e-kafka-client-jaas.conf"
    /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server ranger-kafka.rangernw:9092 \
      --topic ranger_audits \
      --command-config /tmp/e2e-kafka-client.properties 2>/dev/null \
      | awk -F: "{s+=\$3} END{print s+0}"
  ' 2>/dev/null || echo ""
}

container_log_hint() {
  local container="$1"
  local pattern="$2"
  local since="${3:-3m}"
  docker logs "${container}" --since "${since}" 2>&1 | grep -iE "${pattern}" | tail -3 || true
}

wait_for_solr_increase() {
  local query="$1"
  local before="$2"
  local deadline=$((SECONDS + SMOKE_TIMEOUT))
  local found="${before}"
  while (( SECONDS < deadline )); do
    sleep 10
    found="$(solr_count "${query}")"
    if [[ "${found}" -gt "${before}" ]]; then
      echo "${found}"
      return 0
    fi
  done
  echo "${found}"
  return 1
}

print_pipeline_summary() {
  local source="$1"
  local p_plugin="$2"
  local p_ingestor="$3"
  local p_kafka="$4"
  local p_dispatcher="$5"
  local p_solr="$6"
  local p_admin="$7"
  echo ""
  echo "  Pipeline summary (${source}):"
  printf "    %-22s %s\n" "1. Plugin" "${p_plugin}"
  printf "    %-22s %s\n" "2. Ingestor (:7081)" "${p_ingestor}"
  printf "    %-22s %s\n" "3. Kafka (ranger_audits)" "${p_kafka}"
  printf "    %-22s %s\n" "4. Dispatcher" "${p_dispatcher}"
  printf "    %-22s %s\n" "5. Solr (ranger_audits)" "${p_solr}"
  printf "    %-22s %s\n" "6. Ranger Admin API" "${p_admin}"
  echo ""
  echo "  View in UI: http://localhost:6080 -> Audit -> Access"
}

trace_hdfs_access_pipeline() {
  local trace_id="hdfs-$(date +%s)-$$"
  local deny_path="/tmp/e2e-audit-deny-traverse"
  local solr_q="reqUser:testuser1"
  local fail=0
  local st_plugin="FAIL"
  local st_ingestor="FAIL"
  local st_kafka="SKIP"
  local st_disp="FAIL"
  local st_solr="FAIL"
  local st_admin="FAIL"
  local solr_before solr_after kafka_before kafka_after admin_before admin_after
  local ls_out=""

  pipeline_banner "HDFS access log -> ingestor -> Kafka -> dispatcher -> Solr -> Admin  [${trace_id}]"

  ensure_ingestor_kafka_ready 90 || pipeline_warn "ingestor Kafka producer not ready — audits may spool"
  ./scripts/hadoop/ensure-dev-hdfs-e2e-audit-config.sh
  ./scripts/hadoop/ensure-hdfs-deny-traverse-policy.sh

  solr_before="$(solr_count "${solr_q}")"
  kafka_before="$(kafka_ranger_audits_end_offset)"
  admin_before="$(admin_access_audit_total)"

  pipeline_step 1 "HDFS plugin — generate audited access event (testuser1)"
  # RangerHdfsAuditHandler is skipped when Hadoop passes a single inode (chmod, rm, touchz).
  # Allowed traverse-only ls skips audit (auditOnlyIfDenied). Use multi-component path
  # (/tmp/...) + explicit Ranger EXECUTE DENY so traverse check creates auditHandler and logs DENY.
  if ! docker exec ranger-hadoop bash -c \
    "export KRB5CCNAME=FILE:/tmp/krb5cc_hdfs_e2e_${trace_id} && \
     kinit -kt /etc/keytabs/hdfs.keytab hdfs/ranger-hadoop.rangernw@EXAMPLE.COM && \
     hdfs dfs -mkdir -p ${deny_path}"; then
    pipeline_fail "HDFS prep failed (hdfs keytab / mkdir) — run: ./setup-audit-e2e.sh repair-hdfs"
    print_pipeline_summary "HDFS" "FAIL" "SKIP" "SKIP" "SKIP" "SKIP" "SKIP"
    return 1
  fi
  echo "  waiting 35s for HDFS plugin policy refresh (deny-traverse policy)..."
  sleep 35
  ls_out="$(docker exec ranger-hadoop bash -c \
    "export KRB5CCNAME=FILE:/tmp/krb5cc_t1_e2e_${trace_id} && \
     kinit -kt /etc/keytabs/testuser1.keytab testuser1/ranger-hadoop.rangernw@EXAMPLE.COM && \
     hdfs dfs -ls ${deny_path}" 2>&1)" || true
  if echo "${ls_out}" | grep -qE 'Permission denied.*user=testuser1'; then
    pipeline_ok "Ranger DENY traverse on ${deny_path} (multi-component ls, audited denial)"
    st_plugin="PASS"
    echo "  waiting 65s for audit batch flush / ingestor recovery retry..."
    sleep 65
  else
    pipeline_fail "HDFS audit trigger failed — expected Ranger DENY on ls ${deny_path}"
    echo "${ls_out}" | sed 's/^/    /'
    print_pipeline_summary "HDFS" "${st_plugin}" "${st_ingestor}" "${st_kafka}" "${st_disp}" "${st_solr}" "${st_admin}"
    return 1
  fi

  pipeline_step 2 "Audit ingestor — receive plugin POST and produce to Kafka"
  if check_url "http://localhost:7081/api/audit/health"; then
    pipeline_ok "ingestor health http://localhost:7081/api/audit/health"
    st_ingestor="PASS"
    if lines="$(container_log_hint ranger-audit-ingestor 'audit|kafka|produced|POST' 2m)"; then
      [[ -n "${lines}" ]] && echo "${lines}" | sed 's/^/    log: /'
    else
      pipeline_warn "no recent ingestor log lines (audit may still be in flight)"
    fi
  else
    pipeline_fail "ingestor unhealthy"
    fail=1
  fi

  pipeline_step 3 "Kafka — topic ranger_audits"
  if [[ -n "${kafka_before}" ]]; then
    kafka_after="$(kafka_ranger_audits_end_offset)"
    if [[ -n "${kafka_after}" ]] && [[ "${kafka_after}" -gt "${kafka_before}" ]]; then
      pipeline_ok "end offset ${kafka_before} → ${kafka_after}"
      st_kafka="PASS"
    else
      pipeline_warn "offset unchanged (${kafka_before} → ${kafka_after:-?}); checking downstream hops"
      st_kafka="WARN"
    fi
  else
    pipeline_warn "Kafka offset check skipped (Kerberos); infer from ingestor + dispatcher logs"
    st_kafka="SKIP"
    container_log_hint ranger-kafka 'ranger_audits' 2m | sed 's/^/    log: /' || true
  fi

  pipeline_step 4 "Solr dispatcher — consume Kafka and index to Solr"
  if check_url "http://localhost:7092/api/health/ping"; then
    pipeline_ok "HDFS dispatcher health http://localhost:7092/api/health/ping"
  else
    pipeline_fail "HDFS dispatcher unhealthy"
    fail=1
  fi
  if check_url "http://localhost:7091/api/health/ping"; then
    pipeline_ok "Solr dispatcher health http://localhost:7091/api/health/ping"
    st_disp="PASS"
    container_log_hint ranger-audit-dispatcher-solr 'solr|index|consume|audit' 2m | sed 's/^/    log: /' || true
  else
    pipeline_fail "Solr dispatcher unhealthy"
    st_disp="FAIL"
    fail=1
  fi

  pipeline_step 5 "Solr — ranger_audits collection"
  echo "  waiting for Solr docs (${solr_q})..."
  if solr_after="$(wait_for_solr_increase "${solr_q}" "${solr_before}")"; then
    pipeline_ok "Solr numFound ${solr_before} → ${solr_after}"
    st_solr="PASS"
    echo "  latest Solr access log:"
    solr_print_latest "${solr_q}"
  else
    pipeline_fail "Solr count did not increase (before=${solr_before}, after=${solr_after})"
    st_solr="FAIL"
    fail=1
    echo "  hint: docker logs ranger-audit-ingestor --tail 50" >&2
    echo "  hint: docker logs ranger-audit-dispatcher-solr --tail 50" >&2
  fi

  pipeline_step 6 "Ranger Admin — Audit → Access (Solr backend)"
  admin_after="$(admin_access_audit_total)"
  if curl -sf -u 'admin:rangerR0cks!' \
    'http://localhost:6080/service/assets/accessAudit?pageSize=1&startIndex=0' >/dev/null; then
    pipeline_ok "accessAudit API reachable (totalCount ${admin_before} → ${admin_after})"
    if [[ "$(admin_access_audit_has_user testuser1)" == "yes" ]] || [[ "${admin_after}" -gt "${admin_before}" ]]; then
      pipeline_ok "Admin exposes access audits for testuser1"
      st_admin="PASS"
      echo "  latest Admin access log:"
      admin_print_latest_audit
    else
      pipeline_warn "Admin API OK but testuser1 not in first page — check Solr-backed UI tab"
      st_admin="WARN"
    fi
  else
    pipeline_fail "accessAudit API failed — run: ./setup-audit-e2e.sh repair-hdfs"
    st_admin="FAIL"
    fail=1
  fi

  print_pipeline_summary "HDFS" "${st_plugin}" "${st_ingestor}" "${st_kafka}" "${st_disp}" "${st_solr}" "${st_admin}"
  return "${fail}"
}

trace_ozone_access_pipeline() {
  local trace_id="ozone-$(date +%s)-$$"
  local vol="e2e${trace_id//-}"
  local solr_q="repo:dev_ozone"
  local fail=0
  local st_plugin="FAIL"
  local st_ingestor="FAIL"
  local st_kafka="SKIP"
  local st_disp="FAIL"
  local st_solr="FAIL"
  local st_admin="FAIL"
  local solr_before solr_after kafka_before kafka_after admin_before admin_after

  pipeline_banner "Ozone access log -> ingestor -> Kafka -> dispatcher -> Solr -> Admin  [${trace_id}]"

  ensure_ingestor_kafka_ready 90 || pipeline_warn "ingestor Kafka producer not ready"

  solr_before="$(solr_count "${solr_q}")"
  kafka_before="$(kafka_ranger_audits_end_offset)"
  admin_before="$(admin_access_audit_total)"

  ./scripts/admin/ensure-ranger-admin-plugin-download-access.sh
  ./scripts/ozone/ensure-dev-ozone-service-config.sh
  ./scripts/ozone/ensure-dev-ozone-e2e-audit-config.sh
  ./scripts/ozone/ensure-dev-ozone-om-policy.sh
  ./scripts/ozone/apply-ozone-plugin-audit-config.sh
  if check_container ozone-om; then
    echo "  restarting ozone-om (pick up Admin policies + om CLI policy)..."
    docker restart ozone-om 2>/dev/null || true
    sleep 45
    audit_stack_wait_container ozone-om "Ozone OM" 120 || true
  fi
  pipeline_step 1 "Ozone plugin — generate access event (om user)"
  echo "  waiting 35s for Ozone plugin policy + audit-filter refresh..."
  sleep 35
  docker exec -u om ozone-om /opt/hadoop/bin/ozone sh volume list >/dev/null 2>&1 || true
  if docker exec -u om ozone-om /opt/hadoop/bin/ozone sh volume create "/${vol}" >/dev/null 2>&1 \
    && docker exec -u om ozone-om /opt/hadoop/bin/ozone sh bucket create "/${vol}/bkt1" >/dev/null 2>&1; then
    pipeline_ok "ozone volume /${vol} + bucket bkt1 (audited by Ranger Ozone plugin, repo dev_ozone)"
    st_plugin="PASS"
    echo "  waiting 65s for audit batch flush / ingestor recovery retry..."
    sleep 65
  else
    pipeline_fail "ozone volume/bucket create failed — run: ./setup-audit-e2e.sh repair-ozone"
    print_pipeline_summary "Ozone" "${st_plugin}" "${st_ingestor}" "${st_kafka}" "${st_disp}" "${st_solr}" "${st_admin}"
    return 1
  fi

  pipeline_step 2 "Audit ingestor — receive Ozone plugin audits"
  if check_url "http://localhost:7081/api/audit/health"; then
    pipeline_ok "ingestor health http://localhost:7081/api/audit/health"
    st_ingestor="PASS"
    container_log_hint ranger-audit-ingestor 'ozone|dev_ozone|audit|kafka' 2m | sed 's/^/    log: /' || true
  else
    pipeline_fail "ingestor unhealthy"
    fail=1
  fi

  pipeline_step 3 "Kafka — topic ranger_audits"
  if [[ -n "${kafka_before}" ]]; then
    kafka_after="$(kafka_ranger_audits_end_offset)"
    if [[ -n "${kafka_after}" ]] && [[ "${kafka_after}" -gt "${kafka_before}" ]]; then
      pipeline_ok "end offset ${kafka_before} → ${kafka_after}"
      st_kafka="PASS"
    else
      pipeline_warn "offset unchanged (${kafka_before} → ${kafka_after:-?})"
      st_kafka="WARN"
    fi
  else
    pipeline_warn "Kafka offset check skipped (Kerberos)"
    st_kafka="SKIP"
  fi

  pipeline_step 4 "Solr dispatcher — index to ranger_audits"
  if check_url "http://localhost:7091/api/health/ping"; then
    pipeline_ok "Solr dispatcher health http://localhost:7091/api/health/ping"
    st_disp="PASS"
    container_log_hint ranger-audit-dispatcher-solr 'solr|dev_ozone|consume' 2m | sed 's/^/    log: /' || true
  else
    pipeline_fail "Solr dispatcher unhealthy"
    st_disp="FAIL"
    fail=1
  fi

  pipeline_step 5 "Solr — ranger_audits collection"
  echo "  waiting for Solr docs (${solr_q})..."
  if solr_after="$(wait_for_solr_increase "${solr_q}" "${solr_before}")"; then
    pipeline_ok "Solr numFound ${solr_before} → ${solr_after}"
    st_solr="PASS"
    echo "  latest Solr access log:"
    solr_print_latest "${solr_q}"
  else
    pipeline_fail "Solr count did not increase (before=${solr_before}, after=${solr_after})"
    st_solr="FAIL"
    fail=1
  fi

  pipeline_step 6 "Ranger Admin — Audit → Access"
  ensure_admin_solr_backend
  sleep 5
  admin_after="$(admin_access_audit_total)"
  if curl -sf -u 'admin:rangerR0cks!' \
    'http://localhost:6080/service/assets/accessAudit?pageSize=5&startIndex=0' >/dev/null; then
    pipeline_ok "accessAudit API (totalCount ${admin_before} → ${admin_after})"
    if [[ "$(admin_access_audit_has_user om)" == "yes" ]] || [[ "${admin_after}" -gt "${admin_before}" ]]; then
      pipeline_ok "Admin exposes Ozone access audits (om / dev_ozone)"
      st_admin="PASS"
      echo "  latest Admin access log:"
      admin_print_latest_audit
    else
      pipeline_warn "Admin totalCount unchanged — Solr may be ahead of UI cache; refresh Audit tab"
      st_admin="WARN"
    fi
  else
    pipeline_fail "accessAudit API failed — run: ./setup-audit-e2e.sh fix (ensure-admin-audit-solr)"
    st_admin="FAIL"
    fail=1
  fi

  print_pipeline_summary "Ozone" "${st_plugin}" "${st_ingestor}" "${st_kafka}" "${st_disp}" "${st_solr}" "${st_admin}"
  return "${fail}"
}

trace_hive_access_pipeline() {
  local fail=0
  local solr_q="repo:dev_hive"
  local solr_before solr_after
  local principal="${HIVE_KERBEROS_PRINCIPAL:-hive/ranger-hive.rangernw@EXAMPLE.COM}"

  pipeline_banner "Hive (dev_hive) access log pipeline"
  echo "  Trace: ranger-hive → ingestor:7081 → Kafka → Solr dispatcher → Admin"

  if ! check_container ranger-hive; then
    pipeline_fail "ranger-hive not running"
    return 1
  fi

  solr_before="$(solr_count "${solr_q}")"

  pipeline_step 1 "Hive plugin — HS2 + beeline (table SELECT)"
  if docker exec ranger-hive timeout 3 bash -c 'echo >/dev/tcp/localhost/10000' 2>/dev/null; then
    pipeline_ok "HiveServer2 port 10000"
  else
    pipeline_fail "HiveServer2 not listening — run: ./setup-audit-e2e.sh repair-hive"
    return 1
  fi

  # SHOW DATABASES often skips Ranger access audit; SELECT on a table generates dev_hive events.
  if docker exec ranger-hive bash -c "
    kinit -kt /etc/keytabs/hive.keytab '${principal}' &&
    beeline -u 'jdbc:hive2://localhost:10000/default;principal=${principal}' --silent=true -e '
      CREATE DATABASE IF NOT EXISTS rbac_demo;
      USE rbac_demo;
      CREATE TABLE IF NOT EXISTS sales (id INT, amount DOUBLE, region STRING);
      SELECT id, region FROM sales LIMIT 1;
    '
  " >/dev/null 2>&1; then
    pipeline_ok "beeline SELECT on rbac_demo.sales (hive user)"
  else
    pipeline_fail "beeline query failed — run: ./setup-audit-e2e.sh repair-hive"
    fail=1
  fi

  pipeline_step 2 "Ingestor — dev_hive"
  if check_url "http://localhost:7081/api/audit/health"; then
    pipeline_ok "ingestor health"
  else
    pipeline_fail "ingestor unhealthy"
    fail=1
  fi

  sleep 20

  pipeline_step 3 "Solr — repo:dev_hive"
  if solr_after="$(wait_for_solr_increase "${solr_q}" "${solr_before}")"; then
    pipeline_ok "Solr numFound ${solr_before} → ${solr_after}"
    solr_print_latest "${solr_q}"
  else
    pipeline_warn "Solr dev_hive count did not increase (before=${solr_before}, after=${solr_after:-?})"
    container_log_hint ranger-hive '401|audit|Authentication' 5m | sed 's/^/    hs2: /' || true
    fail=1
  fi

  if [[ "${fail}" -eq 0 ]]; then
    pipeline_ok "Hive audit pipeline hops succeeded"
  fi
  return "${fail}"
}

trace_kafka_access_pipeline() {
  local fail=0
  local solr_q="repo:dev_kafka"
  local solr_before solr_after

  pipeline_banner "Kafka plugin (dev_kafka) access log pipeline"
  echo "  Trace: ranger-kafka → ingestor:7081 → Kafka → Solr dispatcher → Admin"

  if ! check_container ranger-kafka; then
    pipeline_fail "ranger-kafka not running"
    return 1
  fi

  apply_kafka_runtime_config false || true
  solr_before="$(solr_count "${solr_q}")"

  pipeline_step 1 "Kafka plugin — DENIED topic create (testuser1)"
  if [[ -x "${SCRIPT_DIR}/scripts/kafka/trigger-kafka-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/kafka/trigger-kafka-plugin-audit-e2e.sh" || fail=1
  else
    pipeline_fail "missing trigger-kafka-plugin-audit-e2e.sh"
    fail=1
  fi

  pipeline_step 2 "Ingestor — dev_kafka"
  if check_url "http://localhost:7081/api/audit/health"; then
    pipeline_ok "ingestor health"
  else
    pipeline_fail "ingestor unhealthy"
    fail=1
  fi

  sleep 45

  pipeline_step 3 "Solr — repo:dev_kafka"
  if solr_after="$(wait_for_solr_increase "${solr_q}" "${solr_before}")"; then
    pipeline_ok "Solr numFound ${solr_before} → ${solr_after}"
    solr_print_latest "${solr_q}"
  else
    pipeline_warn "Solr dev_kafka count did not increase (before=${solr_before}, after=${solr_after:-?})"
    container_log_hint ranger-kafka '401|403|MessageBodyWriter|WadlAutoDiscoverable|Failed to send audit batch' 5m | sed 's/^/    kafka: /' || true
    fail=1
  fi

  if [[ "${fail}" -eq 0 ]]; then
    pipeline_ok "Kafka plugin audit pipeline hops succeeded"
  fi
  return "${fail}"
}

trace_hbase_access_pipeline() {
  local fail=0
  local solr_q="repo:dev_hbase"

  pipeline_banner "HBase (dev_hbase) access log pipeline"
  echo "  Trace: ranger-hbase → ingestor:7081 → Kafka → Solr dispatcher → Admin"

  if ! check_container ranger-hbase; then
    pipeline_fail "ranger-hbase not running"
    return 1
  fi

  apply_hbase_runtime_config || true

  pipeline_step 1 "HBase plugin — DENIED scan (testuser1)"
  if ! docker exec ranger-hbase timeout 3 bash -c 'echo >/dev/tcp/localhost/16000' 2>/dev/null; then
    pipeline_fail "HBase Master not listening — run: ./setup-audit-e2e.sh repair-hbase"
    return 1
  fi

  local solr_before
  solr_before="$(audit_stack_solr_count "${solr_q}" || echo 0)"

  if [[ -x "${SCRIPT_DIR}/scripts/hbase/trigger-hbase-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/hbase/trigger-hbase-plugin-audit-e2e.sh" || fail=1
  else
    pipeline_fail "missing trigger-hbase-plugin-audit-e2e.sh"
    fail=1
  fi

  sleep 45

  pipeline_step 2 "Ingestor — dev_hbase"
  if lines="$(container_log_hint ranger-audit-ingestor 'dev_hbase|service=dev_hbase' 2m)"; then
    pipeline_ok "ingestor log lines"
    echo "${lines}" | sed 's/^/    /'
  else
    pipeline_warn "no recent dev_hbase lines in ingestor log"
  fi

  pipeline_step 3 "Solr — repo:dev_hbase"
  local solr_after
  solr_after="$(audit_stack_solr_count "${solr_q}" || echo 0)"
  if [[ "${solr_after}" -gt "${solr_before}" ]]; then
    pipeline_ok "Solr count ${solr_before} → ${solr_after}"
  else
    pipeline_warn "Solr dev_hbase count did not increase (before=${solr_before}, after=${solr_after:-?})"
    container_log_hint ranger-hbase '401|MessageBodyWriter|Failed to send audit batch' 5m | sed 's/^/    hbase: /' || true
    fail=1
  fi

  if [[ "${fail}" -eq 0 ]]; then
    pipeline_ok "HBase plugin audit pipeline hops succeeded"
  fi
  return "${fail}"
}

trace_knox_access_pipeline() {
  local fail=0
  local solr_q="repo:dev_knox"

  pipeline_banner "Knox (dev_knox) access log pipeline"
  echo "  Trace: ranger-knox → ingestor:7081 → Kafka → Solr dispatcher → Admin"

  if ! check_container ranger-knox; then
    pipeline_fail "ranger-knox not running — start docker-compose.ranger-knox.yml"
    return 1
  fi

  apply_knox_runtime_config || true

  pipeline_step 1 "Knox plugin — WebHDFS via gateway"
  local solr_before
  solr_before="$(audit_stack_solr_count "${solr_q}" || echo 0)"

  if [[ -x "${SCRIPT_DIR}/scripts/knox/trigger-knox-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/knox/trigger-knox-plugin-audit-e2e.sh" || fail=1
  else
    pipeline_fail "missing trigger-knox-plugin-audit-e2e.sh"
    fail=1
  fi

  sleep 45

  pipeline_step 2 "Ingestor — dev_knox"
  if lines="$(container_log_hint ranger-audit-ingestor 'dev_knox|service=dev_knox' 2m)"; then
    pipeline_ok "ingestor log lines"
    echo "${lines}" | sed 's/^/    /'
  else
    pipeline_warn "no recent dev_knox lines in ingestor log"
  fi

  pipeline_step 3 "Solr — repo:dev_knox"
  local solr_after
  solr_after="$(audit_stack_solr_count "${solr_q}" || echo 0)"
  if [[ "${solr_after}" -gt "${solr_before}" ]]; then
    pipeline_ok "Solr count ${solr_before} → ${solr_after}"
  else
    pipeline_warn "Solr dev_knox count did not increase (before=${solr_before}, after=${solr_after:-?})"
    container_log_hint ranger-knox '401|403|Failed to send audit batch' 5m | sed 's/^/    knox: /' || true
    fail=1
  fi

  if [[ "${fail}" -eq 0 ]]; then
    pipeline_ok "Knox plugin audit pipeline hops succeeded"
  fi
  return "${fail}"
}

trace_kms_access_pipeline() {
  local fail=0
  local solr_q="repo:dev_kms"

  pipeline_banner "KMS (dev_kms) access log pipeline"
  echo "  Trace: ranger-kms → ingestor:7081 → Kafka → Solr dispatcher → Admin"

  if ! check_container ranger-kms; then
    pipeline_fail "ranger-kms not running — start docker-compose.ranger-kms.yml"
    return 1
  fi

  apply_kms_runtime_config || true

  pipeline_step 1 "KMS plugin — key list/create"
  local solr_before
  solr_before="$(audit_stack_solr_count "${solr_q}" || echo 0)"

  if [[ -x "${SCRIPT_DIR}/scripts/kms/trigger-kms-plugin-audit-e2e.sh" ]]; then
    "${SCRIPT_DIR}/scripts/kms/trigger-kms-plugin-audit-e2e.sh" || fail=1
  else
    pipeline_fail "missing trigger-kms-plugin-audit-e2e.sh"
    fail=1
  fi

  sleep 45

  pipeline_step 2 "Ingestor — dev_kms"
  if lines="$(container_log_hint ranger-audit-ingestor 'dev_kms|service=dev_kms' 2m)"; then
    pipeline_ok "ingestor log lines"
    echo "${lines}" | sed 's/^/    /'
  else
    pipeline_warn "no recent dev_kms lines in ingestor log"
  fi

  pipeline_step 3 "Solr — repo:dev_kms"
  local solr_after
  solr_after="$(audit_stack_solr_count "${solr_q}" || echo 0)"
  if [[ "${solr_after}" -gt "${solr_before}" ]]; then
    pipeline_ok "Solr count ${solr_before} → ${solr_after}"
  else
    pipeline_warn "Solr dev_kms count did not increase (before=${solr_before}, after=${solr_after:-?})"
    container_log_hint ranger-kms '401|403|Failed to send audit batch|audit.exclude' 5m | sed 's/^/    kms: /' || true
    fail=1
  fi

  if [[ "${fail}" -eq 0 ]]; then
    pipeline_ok "KMS plugin audit pipeline hops succeeded"
  fi
  return "${fail}"
}

generate_access_logs() {
  local fail=0
  pipeline_banner "Access log generation (scope=${SCOPE}, hive=${INCLUDE_HIVE}, hbase=${INCLUDE_HBASE}, kafka_plugin=${INCLUDE_KAFKA_PLUGIN})"
  echo "  Trace: plugin → ingestor:7081 → Kafka:ranger_audits → dispatcher → Solr → Admin:6080"
  if [[ "${SCOPE}" != "ozone" ]]; then
    trace_hdfs_access_pipeline || fail=1
  fi
  if [[ "${SCOPE}" != "hdfs" ]]; then
    trace_ozone_access_pipeline || fail=1
  fi
  if [[ "${INCLUDE_HIVE}" == "true" ]]; then
    trace_hive_access_pipeline || fail=1
  fi
  if [[ "${INCLUDE_HBASE}" == "true" ]]; then
    trace_hbase_access_pipeline || fail=1
  fi
  if [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]]; then
    trace_kafka_access_pipeline || fail=1
  fi
  if [[ "${fail}" -eq 0 ]]; then
    pipeline_ok "All access log pipeline hops succeeded for scope=${SCOPE}"
  else
    pipeline_fail "One or more pipeline hops failed — ./setup-audit-e2e.sh fix"
  fi
  return "${fail}"
}

run_smoke() {
  echo "==> Smoke / access-log pipeline verification (scope=${SCOPE})"
  generate_access_logs
}

# ---------------------------------------------------------------------------
# Diagnose / fix
# ---------------------------------------------------------------------------

check_url() {
  curl -sf -o /dev/null "$1" 2>/dev/null
}

check_container() {
  docker ps --filter "name=^${1}$" --filter status=running --format '{{.Names}}' | grep -qx "$1"
}

diagnose_stack() {
  local issues=0
  echo "==> Diagnose (scope=${SCOPE})"

  local -a containers=(
    ranger "ranger-${RANGER_DB_TYPE}" ranger-kdc ranger-zk ranger-solr ranger-kafka
    ranger-audit-ingestor ranger-audit-dispatcher-solr
  )
  [[ "${SCOPE}" != "ozone" ]] && containers+=(ranger-hadoop ranger-audit-dispatcher-hdfs)
  [[ "${SCOPE}" != "hdfs" ]] && containers+=(ozone-scm ozone-datanode ozone-om)
  [[ "${INCLUDE_HIVE}" == "true" ]] && containers+=(ranger-hive)
  [[ "${INCLUDE_HBASE}" == "true" ]] && containers+=(ranger-hbase)

  local c
  for c in "${containers[@]}"; do
    if check_container "${c}"; then
      echo "  container ${c}: OK"
    else
      echo "  container ${c}: NOT RUNNING"
      issues=$((issues + 1))
    fi
  done

  local -a urls=(
    "http://localhost:6080/login.jsp|Admin UI"
    "http://localhost:7081/api/audit/health|Ingestor"
    "http://localhost:7091/api/health/ping|Solr dispatcher"
  )
  [[ "${SCOPE}" != "ozone" ]] && urls+=("http://localhost:7092/api/health/ping|HDFS dispatcher")

  local entry url label
  for entry in "${urls[@]}"; do
    url="${entry%%|*}"
    label="${entry##*|}"
    if check_url "${url}"; then
      echo "  ${label}: OK"
    else
      echo "  ${label}: FAIL"
      issues=$((issues + 1))
    fi
  done

  local solr_ping
  solr_ping="$(audit_stack_solr_curl '/solr/ranger_audits/admin/ping?wt=json' || true)"
  if grep -q '"status"[[:space:]]*:[[:space:]]*0' <<< "${solr_ping}"; then
    echo "  Solr: OK"
  else
    echo "  Solr: FAIL"
    issues=$((issues + 1))
  fi

  local ingestor_recent
  ingestor_recent="$(docker logs ranger-audit-ingestor 2>&1 | tail -300)"
  if ! grep -q 'Kafka producer initialized and started' <<< "${ingestor_recent}" \
    && ! grep -q 'Kafka producer available: true' <<< "${ingestor_recent}"; then
    echo "  Ingestor Kafka producer: FAIL → restart ingestor after Kafka is up"
    issues=$((issues + 1))
  else
    echo "  Ingestor Kafka producer: OK"
  fi

  if [[ "${SCOPE}" != "ozone" ]]; then
    if ! docker exec ranger-hadoop test -f /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml 2>/dev/null; then
      echo "  HDFS audit config: missing → ./setup-audit-e2e.sh repair-hdfs"
      issues=$((issues + 1))
    elif ! audit_xml_has_auditserver ranger-hadoop /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml 2>/dev/null; then
      echo "  HDFS auditserver: disabled → ./setup-audit-e2e.sh repair-hdfs"
      issues=$((issues + 1))
    elif ! docker exec ranger-hadoop test -d /var/log/hadoop/hdfs/audit/audit-ingestor/spool 2>/dev/null; then
      echo "  HDFS audit spool dir: missing → ./setup-audit-e2e.sh repair-hdfs"
      issues=$((issues + 1))
    fi
  fi

  if [[ "${SCOPE}" != "hdfs" ]]; then
    if ! docker exec ozone-om test -f /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml 2>/dev/null; then
      echo "  Ozone audit config: missing → ./setup-audit-e2e.sh repair-ozone"
      issues=$((issues + 1))
    elif ! audit_xml_has_auditserver ozone-om /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml 2>/dev/null; then
      echo "  Ozone auditserver: disabled → ./setup-audit-e2e.sh repair-ozone"
      issues=$((issues + 1))
    fi
  fi

  if [[ "${issues}" -eq 0 ]]; then
    echo "Diagnose: no issues found"
  else
    echo "Diagnose: ${issues} issue(s) — try: ./setup-audit-e2e.sh fix"
  fi
  return "${issues}"
}

fix_stack() {
  local had_issues=0
  diagnose_stack || had_issues=1

  echo ""
  echo "==> Fix: ensure stack is up (scope=${SCOPE})"
  ensure_compose_network
  compose_restart

  ensure_kerberos_audit_services

  if ! check_url "http://localhost:7081/api/audit/health" \
    || ! check_url "http://localhost:7091/api/health/ping"; then
    redeploy_audit_services
    ensure_ingestor_kafka_ready 180 || true
  fi

  apply_runtime_config "${RECREATE_OZONE}"

  if ! wait_for_health "${HEALTH_TIMEOUT}"; then
    echo "==> Fix: health still failing — full restart"
    compose_restart
    apply_runtime_config false
    wait_for_health "${HEALTH_TIMEOUT}" || true
  fi

  echo ""
  diagnose_stack || true

  if [[ "${DO_VERIFY}" == "true" ]]; then
    echo ""
    run_smoke || {
      echo "Smoke failed — try scope-specific repair:" >&2
      echo "  ./setup-audit-e2e.sh repair-hdfs   # HDFS plugin + Admin Solr" >&2
      echo "  ./setup-audit-e2e.sh repair-ozone  # Ozone plugin + policies" >&2
      return 1
    }
  fi

  if [[ "${had_issues}" -eq 1 ]]; then
    echo "Fix completed (issues were remediated)"
  else
    echo "Fix completed (stack was already healthy)"
  fi
}

print_status() {
  echo ""
  echo "Audit E2E stack (scope=${SCOPE}, hive=${INCLUDE_HIVE}, hbase=${INCLUDE_HBASE}) — endpoints:"
  echo "  Admin UI:          http://localhost:6080  (admin / rangerR0cks!)"
  echo "  Audit -> Access:   same UI (Solr backend)"
  echo "  Ingestor:          http://localhost:7081/api/audit/health"
  echo "  Solr dispatcher:   http://localhost:7091/api/health/ping"
  [[ "${SCOPE}" != "ozone" ]] && echo "  HDFS dispatcher:   http://localhost:7092/api/health/ping"
  echo "  Solr collection:   http://localhost:8983/solr/ranger_audits"
  [[ "${SCOPE}" != "hdfs" ]] && echo "  Ozone OM UI:       http://localhost:9874"
  [[ "${INCLUDE_HIVE}" == "true" ]] && echo "  HiveServer2:       jdbc:hive2://localhost:10000/default"
  [[ "${INCLUDE_HBASE}" == "true" ]] && echo "  HBase Master:      localhost:16010 (plugin repo dev_hbase)"
  [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]] && echo "  Kafka broker:      localhost:9092 (plugin repo dev_kafka)"
  check_container ranger-knox && echo "  Knox gateway:      https://localhost:8443 (plugin repo dev_knox)"
  check_container ranger-kms && echo "  Ranger KMS:        http://localhost:9292 (plugin repo dev_kms)"
  echo ""
  echo "Commands:"
  echo "  ./scripts/audit/verify-audit-e2e-full.sh   # infra + all plugin pipelines"
  echo "  ./setup-audit-e2e.sh generate-access-logs"
  echo "  ./setup-audit-e2e.sh verify"
  echo "  ./setup-audit-e2e.sh diagnose"
  echo "  ./setup-audit-e2e.sh fix"
  echo "  ./setup-audit-e2e.sh repair-hdfs"
  echo "  ./setup-audit-e2e.sh repair-ozone"
  [[ "${INCLUDE_HIVE}" == "true" ]] && echo "  ./setup-audit-e2e.sh repair-hive"
  [[ "${INCLUDE_HIVE}" == "true" ]] && echo "  ./setup-audit-e2e.sh trigger-hive-audit"
  [[ "${INCLUDE_HBASE}" == "true" ]] && echo "  ./setup-audit-e2e.sh repair-hbase"
  [[ "${INCLUDE_HBASE}" == "true" ]] && echo "  ./setup-audit-e2e.sh trigger-hbase-audit"
  [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]] && echo "  ./setup-audit-e2e.sh repair-kafka"
  [[ "${INCLUDE_KAFKA_PLUGIN}" == "true" ]] && echo "  ./setup-audit-e2e.sh trigger-kafka-audit"
  check_container ranger-knox && echo "  ./setup-audit-e2e.sh repair-knox"
  check_container ranger-knox && echo "  ./setup-audit-e2e.sh trigger-knox-audit"
  check_container ranger-kms && echo "  ./setup-audit-e2e.sh repair-kms"
  check_container ranger-kms && echo "  ./setup-audit-e2e.sh trigger-kms-audit"
  echo "  ./setup-audit-e2e.sh down"
  echo "  README-AUDIT-E2E.md"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${ACTION}" in
  up)
    prepare_dist
    compose_up
    wait_for_health "${HEALTH_TIMEOUT}" || {
      echo "WARN: health check failed before config — continuing with apply_runtime_config" >&2
    }
    apply_runtime_config "${RECREATE_OZONE}"
    if ! wait_for_health "${HEALTH_TIMEOUT}"; then
      echo "ERROR: stack unhealthy after config — run: ./setup-audit-e2e.sh fix" >&2
      exit 1
    fi
    if [[ "${DO_VERIFY}" == "true" ]]; then
      run_smoke || {
        echo "ERROR: smoke failed — run: ./setup-audit-e2e.sh fix" >&2
        exit 1
      }
    fi
    print_status
    ;;
  down)
    docker compose -f "${COMPOSE_FILE}" down --remove-orphans
    ;;
  verify)
    wait_for_health "${HEALTH_TIMEOUT}"
    run_smoke
    ;;
  restart)
    compose_restart
    wait_for_health "${HEALTH_TIMEOUT}" || true
    apply_runtime_config false
    ;;
  config)
    apply_runtime_config "${RECREATE_OZONE}"
    ;;
  status)
    docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E 'ranger|ozone|NAMES' || true
    print_status
    ;;
  diagnose)
    diagnose_stack || exit 1
    ;;
  fix)
    fix_stack
    print_status
    ;;
  repair-hdfs)
    SCOPE="hdfs"
    repair_hdfs
    ;;
  repair-ozone)
    SCOPE="ozone"
    repair_ozone
    ;;
  repair-hive)
    INCLUDE_HIVE=true
    repair_hive
    ;;
  repair-kafka)
    INCLUDE_KAFKA_PLUGIN=true
    repair_kafka
    ;;
  repair-hbase)
    INCLUDE_HBASE=true
    repair_hbase
    ;;
  repair-knox)
    repair_knox
    ;;
  repair-kms)
    repair_kms
    ;;
  repair-solr)
    repair_solr
    ;;
  generate-access-logs)
    wait_for_health "${HEALTH_TIMEOUT}" || {
      echo "WARN: stack not fully healthy — attempting access log generation anyway" >&2
    }
    generate_access_logs
    ;;
  trigger-hdfs-audit)
    SCOPE="hdfs"
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_hdfs_access_pipeline
    ;;
  trigger-ozone-audit)
    SCOPE="ozone"
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_ozone_access_pipeline
    ;;
  trigger-hive-audit)
    INCLUDE_HIVE=true
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_hive_access_pipeline
    ;;
  trigger-kafka-audit)
    INCLUDE_KAFKA_PLUGIN=true
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_kafka_access_pipeline
    ;;
  trigger-hbase-audit)
    INCLUDE_HBASE=true
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_hbase_access_pipeline
    ;;
  trigger-knox-audit)
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_knox_access_pipeline
    ;;
  trigger-kms-audit)
    wait_for_health "${HEALTH_TIMEOUT}" 2>/dev/null || true
    trace_kms_access_pipeline
    ;;
  verify-full)
    "${SCRIPT_DIR}/scripts/audit/verify-audit-e2e-full.sh"
    ;;
  *)
    echo "ERROR: unknown action '${ACTION}'" >&2
    usage >&2
    exit 1
    ;;
esac
