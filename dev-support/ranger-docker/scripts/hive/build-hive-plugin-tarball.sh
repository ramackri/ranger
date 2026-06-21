#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Build ranger-*-hive-plugin.tar.gz via full Maven reactor (not stub assembly).
# See dev-support/RANGER-HIVE-PLUGIN-RBAC-E2E.md

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${DOCKER_DIR}/../.." && pwd)"
VERSION="${RANGER_VERSION:-3.0.0-SNAPSHOT}"
TARBALL="${REPO_ROOT}/target/ranger-${VERSION}-hive-plugin.tar.gz"
DIST="${DOCKER_DIR}/dist/ranger-${VERSION}-hive-plugin.tar.gz"
# Script-only stub assemblies are ~19 KB; real tarballs are tens of MB.
MIN_TARBALL_BYTES="${MIN_TARBALL_BYTES:-5000000}"

HIVE_REACTOR_MODULES="agents-audit,agents-common,agents-cred,agents-installer,common-utils,credentialbuilder,hive-agent,ranger-hive-plugin-shim,ranger-plugin-classloader,ranger-util"
MVN_COMMON=( -DskipTests -Drat.skip=true -DskipDocs -Dcheckstyle.skip=true -Dpmd.skip=true )

echo "Building hive plugin (RANGER_VERSION=${VERSION})..."
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx4g}"

cd "${REPO_ROOT}"
# -P-all disables the default "all" profile (every assembly descriptor on a partial reactor → stub).
# distro/authz-api/ugsync-util are not in the ranger-hive-plugin root reactor; build them separately.
mvn install -P-all,ranger-hive-plugin \
  -pl "${HIVE_REACTOR_MODULES}" \
  -am \
  "${MVN_COMMON[@]}"

for standalone_module in authz-api ugsync-util; do
  echo "Installing ${standalone_module}..."
  (cd "${REPO_ROOT}/${standalone_module}" && mvn install "${MVN_COMMON[@]}")
done

echo "Running distro hive-agent assembly..."
(cd "${REPO_ROOT}/distro" && mvn install -P-all,ranger-hive-plugin "${MVN_COMMON[@]}")

pick_tarball() {
  local target_size=0 dist_size=0
  [[ -f "${TARBALL}" ]] && target_size="$(wc -c < "${TARBALL}" | tr -d ' ')"
  [[ -f "${DIST}" ]] && dist_size="$(wc -c < "${DIST}" | tr -d ' ')"
  if [[ "${dist_size}" -gt "${target_size}" ]]; then
    echo "${DIST}"
  elif [[ "${target_size}" -gt 0 ]]; then
    echo "${TARBALL}"
  elif [[ "${dist_size}" -gt 0 ]]; then
    echo "${DIST}"
  else
    return 1
  fi
}

maybe_pack_fallback() {
  local reason="$1"
  echo "WARN: ${reason} — falling back to pack-plugin-tarball.sh hive" >&2
  if ! (cd "${DOCKER_DIR}" && ./scripts/pack-plugin-tarball.sh hive); then
    echo "ERROR: pack-plugin-tarball.sh hive failed after Maven stub assembly" >&2
    exit 1
  fi
}

ASSEMBLY_STUB=false
if [[ ! -f "${TARBALL}" ]]; then
  ASSEMBLY_STUB=true
  maybe_pack_fallback "missing ${TARBALL}"
elif [[ "$(wc -c < "${TARBALL}" | tr -d ' ')" -lt "${MIN_TARBALL_BYTES}" ]]; then
  ASSEMBLY_STUB=true
  maybe_pack_fallback "tarball too small ($(wc -c < "${TARBALL}" | tr -d ' ') bytes) — likely stub assembly"
fi

TARBALL="$(pick_tarball || true)"
if [[ -z "${TARBALL:-}" ]]; then
  echo "ERROR: no hive plugin tarball after Maven build and pack fallback" >&2
  exit 1
fi

SIZE="$(wc -c < "${TARBALL}" | tr -d ' ')"
if [[ "${SIZE}" -lt "${MIN_TARBALL_BYTES}" ]]; then
  echo "ERROR: tarball still too small (${SIZE} bytes) after pack fallback" >&2
  exit 1
fi

mkdir -p "${DOCKER_DIR}/dist"
cp -f "${TARBALL}" "${DIST}"

IMPL_COUNT="$(tar -tzf "${TARBALL}" | grep -c 'ranger-hive-plugin-impl/.*\.jar$' || true)"
if [[ "${ASSEMBLY_STUB}" == true ]]; then
  echo "NOTE: Maven hive-agent assembly was stub or missing; used pack-plugin-tarball.sh hive" >&2
else
  echo "NOTE: Maven hive-agent assembly produced full tarball (no pack fallback)" >&2
fi
echo "OK: ${DIST} ($(ls -lh "${DIST}" | awk '{print $5}'), ${IMPL_COUNT} plugin-impl jars)"

if [[ "${IMPL_COUNT}" -gt 80 ]]; then
  echo "ERROR: assembly over-packed plugin-impl (${IMPL_COUNT} jars); expected ~20-50" >&2
  exit 1
fi

if tar -tzf "${TARBALL}" | grep 'ranger-hive-plugin-impl' | grep -qE 'hppc|httpclient|jackson-core-2\.17|commons-collections-3\.2\.2'; then
  echo "WARN: excluded RANGER-5646 jars still present — check distro/src/main/assembly/hive-agent.xml" >&2
fi

if ! tar -tzf "${TARBALL}" | grep -q 'ranger-hive-plugin-impl/.*jersey-client'; then
  echo "WARN: jersey-client missing from plugin-impl" >&2
fi
