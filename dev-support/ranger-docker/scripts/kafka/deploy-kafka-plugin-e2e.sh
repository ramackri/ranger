#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Deploy/re-enable Ranger Kafka plugin inside ranger-kafka (audit E2E).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kafka/deploy-kafka-plugin-e2e.sh
#   ./scripts/kafka/deploy-kafka-plugin-e2e.sh --no-restart

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${DOCKER_DIR}/../.." && pwd)"
VERSION="${KAFKA_PLUGIN_VERSION:-${RANGER_VERSION:-3.0.0-SNAPSHOT}}"
CONTAINER="${RANGER_KAFKA_CONTAINER:-ranger-kafka}"
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
  local target="${REPO_ROOT}/target/ranger-${VERSION}-kafka-plugin.tar.gz"
  local dist="${DOCKER_DIR}/dist/ranger-${VERSION}-kafka-plugin.tar.gz"
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
  sleep 5
fi

TARBALL="$(pick_tarball || true)"
if [[ -z "${TARBALL:-}" ]]; then
  echo "Missing Kafka plugin tarball; run: ./setup-audit-e2e.sh config (builds dist/)" >&2
  exit 1
fi

echo "=== Deploy Kafka plugin (${VERSION}) to ${CONTAINER} ==="
echo "Using tarball: ${TARBALL} ($(ls -lh "${TARBALL}" | awk '{print $5}'))"

docker cp "${TARBALL}" "${CONTAINER}:/tmp/ranger-kafka-plugin.tar.gz"
docker exec "${CONTAINER}" bash -c "
set -euo pipefail
VERSION='${VERSION}'
PLUGIN_ROOT=/opt/ranger/ranger-\${VERSION}-kafka-plugin
SYMLINK=/opt/ranger/ranger-kafka-plugin
EXTRACT=/tmp/ranger-kafka-plugin-extract
KAFKA_LIB=\${KAFKA_HOME:-/opt/kafka}/libs

rm -rf \"\${EXTRACT}\"
mkdir -p \"\${EXTRACT}\"
tar xzf /tmp/ranger-kafka-plugin.tar.gz -C \"\${EXTRACT}\" --strip-components=1

mkdir -p \"\${PLUGIN_ROOT}\"
find \"\${PLUGIN_ROOT}\" -mindepth 1 -maxdepth 1 ! -name install.properties -exec rm -rf {} +
for item in \"\${EXTRACT}\"/*; do
  base=\$(basename \"\${item}\")
  [[ \"\${base}\" == install.properties ]] && continue
  rm -rf \"\${PLUGIN_ROOT}/\${base}\"
  cp -a \"\${item}\" \"\${PLUGIN_ROOT}/\"
done
rm -rf \"\${EXTRACT}\" /tmp/ranger-kafka-plugin.tar.gz

ln -sfn \"\${PLUGIN_ROOT}\" \"\${SYMLINK}\"

if [[ ! -e \"\${PLUGIN_ROOT}/install.properties\" && -f /opt/ranger/ranger-kafka-plugin/install.properties ]]; then
  true
elif [[ ! -e \"\${PLUGIN_ROOT}/install.properties\" && -f ${RANGER_SCRIPTS:-/home/ranger/scripts}/ranger-kafka-plugin-install.properties ]]; then
  cp ${RANGER_SCRIPTS:-/home/ranger/scripts}/ranger-kafka-plugin-install.properties \"\${PLUGIN_ROOT}/install.properties\"
fi

export JAVA_HOME=\${JAVA_HOME:-/opt/java/openjdk}
export PATH=\"\${JAVA_HOME}/bin:\${PATH}\"
cd \"\${PLUGIN_ROOT}\"
chmod +x enable-kafka-plugin.sh
./enable-kafka-plugin.sh

if [[ ! -d \"\${PLUGIN_ROOT}/lib/ranger-kafka-plugin-impl\" ]]; then
  echo \"ERROR: enable-kafka-plugin did not create lib/ranger-kafka-plugin-impl\" >&2
  exit 1
fi
echo \"OK: plugin impl at \${PLUGIN_ROOT}/lib/ranger-kafka-plugin-impl\"
"

"${SCRIPT_DIR}/ensure-kafka-plugin-audit-jars.sh"
"${SCRIPT_DIR}/apply-kafka-plugin-repo-config.sh"
"${SCRIPT_DIR}/apply-kafka-plugin-audit-config.sh" --no-restart
if ! "${SCRIPT_DIR}/enable-kafka-authorizer-docker.sh" --check-only >/dev/null 2>&1; then
  "${SCRIPT_DIR}/enable-kafka-authorizer-docker.sh" --no-restart
fi
"${SCRIPT_DIR}/ensure-kafka-audit-bus-acls.sh" --no-restart-dispatchers

if [[ "${NO_RESTART}" != "true" ]]; then
  "${SCRIPT_DIR}/restart-kafka-broker-docker.sh"
fi

echo "Kafka plugin deploy complete"
