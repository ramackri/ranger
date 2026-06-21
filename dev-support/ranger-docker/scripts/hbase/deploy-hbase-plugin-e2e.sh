#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Deploy/re-enable Ranger HBase plugin inside ranger-hbase (audit E2E).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/hbase/deploy-hbase-plugin-e2e.sh
#   ./scripts/hbase/deploy-hbase-plugin-e2e.sh --no-restart

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${DOCKER_DIR}/../.." && pwd)"
VERSION="${HBASE_PLUGIN_VERSION:-${RANGER_VERSION:-3.0.0-SNAPSHOT}}"
CONTAINER="${RANGER_HBASE_CONTAINER:-ranger-hbase}"
NO_RESTART=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-restart) NO_RESTART=true; shift ;;
    -h|--help)
      sed -n '9,12p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

pick_tarball() {
  local target="${REPO_ROOT}/target/ranger-${VERSION}-hbase-plugin.tar.gz"
  local dist="${DOCKER_DIR}/dist/ranger-${VERSION}-hbase-plugin.tar.gz"
  local target_size=0 dist_size=0
  [[ -f "${target}" ]] && target_size="$(wc -c < "${target}" | tr -d ' ')"
  [[ -f "${dist}" ]] && dist_size="$(wc -c < "${dist}" | tr -d ' ')"
  if [[ "${dist_size}" -gt "${target_size}" ]]; then
    echo "${dist}"
  elif [[ "${target_size}" -gt 0 ]]; then
    echo "${target}"
  elif [[ "${dist_size}" -gt 0 ]]; then
    echo "${dist}"
  else
    return 1
  fi
}

cd "${DOCKER_DIR}"

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "Starting ${CONTAINER}..."
  export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
  docker compose -f docker-compose.ranger-audit-e2e.yml start "${CONTAINER}" >/dev/null 2>&1 || \
    docker start "${CONTAINER}" >/dev/null
  sleep 10
fi

TARBALL="$(pick_tarball || true)"
if [[ -z "${TARBALL:-}" ]]; then
  echo "Missing HBase plugin tarball; run: ./scripts/audit/build-plugin-auditserver-tarballs.sh hbase" >&2
  exit 1
fi

echo "=== Deploy HBase plugin (${VERSION}) to ${CONTAINER} ==="
echo "Using tarball: ${TARBALL} ($(ls -lh "${TARBALL}" | awk '{print $5}'))"

docker cp "${TARBALL}" "${CONTAINER}:/tmp/ranger-hbase-plugin.tar.gz"
docker exec "${CONTAINER}" bash -c "
set -euo pipefail
VERSION='${VERSION}'
PLUGIN_ROOT=/opt/ranger/ranger-\${VERSION}-hbase-plugin
SYMLINK=/opt/ranger/ranger-hbase-plugin
EXTRACT=/tmp/ranger-hbase-plugin-extract

rm -rf \"\${EXTRACT}\"
mkdir -p \"\${EXTRACT}\"
tar xzf /tmp/ranger-hbase-plugin.tar.gz -C \"\${EXTRACT}\" --strip-components=1
rm -f /tmp/ranger-hbase-plugin.tar.gz

mkdir -p \"\${PLUGIN_ROOT}\"
find \"\${PLUGIN_ROOT}\" -mindepth 1 -maxdepth 1 ! -name install.properties -exec rm -rf {} +
for item in \"\${EXTRACT}\"/*; do
  base=\$(basename \"\${item}\")
  [[ \"\${base}\" == install.properties ]] && continue
  rm -rf \"\${PLUGIN_ROOT}/\${base}\"
  cp -a \"\${item}\" \"\${PLUGIN_ROOT}/\"
done
rm -rf \"\${EXTRACT}\"

ln -sfn \"\${PLUGIN_ROOT}\" \"\${SYMLINK}\"

export JAVA_HOME=\${JAVA_HOME:-/opt/java/openjdk}
export PATH=\"\${JAVA_HOME}/bin:\${PATH}\"
cd \"\${PLUGIN_ROOT}\"
chmod +x enable-hbase-plugin.sh
./enable-hbase-plugin.sh

if [[ ! -d \"\${PLUGIN_ROOT}/lib/ranger-hbase-plugin-impl\" ]]; then
  echo \"ERROR: enable-hbase-plugin did not create lib/ranger-hbase-plugin-impl\" >&2
  exit 1
fi
echo \"OK: plugin impl at \${PLUGIN_ROOT}/lib/ranger-hbase-plugin-impl\"
"

docker exec "${CONTAINER}" bash -c "HBASE_HOME=/opt/hbase ${RANGER_SCRIPTS:-/home/ranger/scripts}/apply-hbase-plugin-audit-config.sh --no-restart"

if [[ "${NO_RESTART}" == "true" ]]; then
  echo "Plugin deployed; restart ${CONTAINER} to load config."
  exit 0
fi

echo "Restarting HBase..."
docker exec "${CONTAINER}" bash -c '
  pkill -9 -f org.apache.hadoop.hbase.master.HMaster 2>/dev/null || true
  pkill -9 -f org.apache.hadoop.hbase.regionserver.HRegionServer 2>/dev/null || true
  sleep 3
  su -c "/opt/hbase/bin/start-hbase.sh" hbase
' 2>/dev/null || true
for _ in $(seq 1 36); do
  if docker exec "${CONTAINER}" timeout 2 bash -c 'echo >/dev/tcp/localhost/16000' 2>/dev/null; then
    echo "HBase Master is up."
    exit 0
  fi
  sleep 5
done
echo "WARN: HBase Master may not be ready" >&2
exit 0
