#!/usr/bin/env bash
# Deploy rebuilt Ranger Hive plugin and validate HS2 + testuser2 RBAC deny + Solr audit.
#
# Ranger 3.0 hive-agent source compiles against Hive 4.x (parent 3.0.0-SNAPSHOT in ~/.m2).
# HS2 must run the same Hive major version as the plugin or HS2 fails with VerifyError.
# For apache/hive:3.1.3 containers use HIVE_OFFICIAL_IMAGE=apache/hive:4.0.1 and rebuild the image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${DOCKER_DIR}/../.." && pwd)"
VERSION="${RANGER_VERSION:-3.0.0-SNAPSHOT}"
HIVE_VERSION="${HIVE_VERSION:-4.0.1}"
TARGET_TARBALL="${REPO_ROOT}/target/ranger-${VERSION}-hive-plugin.tar.gz"
DIST_TARBALL="${DOCKER_DIR}/dist/ranger-${VERSION}-hive-plugin.tar.gz"

pick_tarball() {
  local target_size=0 dist_size=0
  [[ -f "${TARGET_TARBALL}" ]] && target_size="$(wc -c < "${TARGET_TARBALL}" | tr -d ' ')"
  [[ -f "${DIST_TARBALL}" ]] && dist_size="$(wc -c < "${DIST_TARBALL}" | tr -d ' ')"
  if [[ "${dist_size}" -gt "${target_size}" ]]; then
    echo "${DIST_TARBALL}"
  elif [[ "${target_size}" -gt 0 ]]; then
    echo "${TARGET_TARBALL}"
  elif [[ "${dist_size}" -gt 0 ]]; then
    echo "${DIST_TARBALL}"
  else
    return 1
  fi
}

cd "${DOCKER_DIR}"

# Detect runtime Hive version inside container (e.g. 3.1.3 or 4.0.1).
RUNTIME_HIVE_VER="$(docker exec ranger-hive bash -c 'ls -d /opt/hive/lib/hive-exec-*.jar /opt/apache-hive-*/lib/hive-exec-*.jar 2>/dev/null | head -1 | sed "s/.*hive-exec-//;s/.jar//"' 2>/dev/null || true)"
if [[ -n "${RUNTIME_HIVE_VER}" && "${RUNTIME_HIVE_VER}" != "${HIVE_VERSION}" ]]; then
  echo "WARN: plugin packed for Hive ${HIVE_VERSION} but container runs Hive ${RUNTIME_HIVE_VER}" >&2
  echo "      Re-pack: HIVE_VERSION=${RUNTIME_HIVE_VER} ./scripts/pack-plugin-tarball.sh hive" >&2
  echo "      Or upgrade image: HIVE_OFFICIAL_IMAGE=apache/hive:4.0.1 ./setup-audit-stack.sh --tier 4 --hive-official" >&2
fi

echo "=== Deploy Hive plugin (${HIVE_VERSION}) to ranger-hive ==="
TARBALL="$(pick_tarball || true)"
if [[ -z "${TARBALL:-}" ]]; then
  echo "Missing hive plugin tarball; run build-hive-plugin-tarball.sh or pack-plugin-tarball.sh hive first" >&2
  exit 1
fi
echo "Using tarball: ${TARBALL} ($(ls -lh "${TARBALL}" | awk '{print $5}'))"

for script in apply-hive-plugin-audit-config.sh enable-hive-plugin-docker.sh; do
  if docker exec ranger-hive test -f "/home/ranger/scripts/hive/${script}" 2>/dev/null; then
    echo "Hive script already present (bind-mount): ${script}"
  else
    docker cp "${SCRIPT_DIR}/${script}" "ranger-hive:/home/ranger/scripts/hive/${script}"
  fi
done
docker cp "${TARBALL}" ranger-hive:/tmp/ranger-hive-plugin.tar.gz
docker exec ranger-hive bash -c "
  set -euo pipefail
  VERSION='${VERSION}'
  PLUGIN_ROOT=/opt/ranger/ranger-\${VERSION}-hive-plugin
  SYMLINK=/opt/ranger/ranger-hive-plugin
  EXTRACT=/tmp/ranger-hive-plugin-extract

  rm -rf \"\${EXTRACT}\"
  mkdir -p \"\${EXTRACT}\"
  tar xzf /tmp/ranger-hive-plugin.tar.gz -C \"\${EXTRACT}\" --strip-components=1 2>/dev/null || \
    tar xzf /tmp/ranger-hive-plugin.tar.gz -C \"\${EXTRACT}\" --strip-components=1

  mkdir -p \"\${PLUGIN_ROOT}\"
  find \"\${PLUGIN_ROOT}\" -mindepth 1 -maxdepth 1 ! -name install.properties -exec rm -rf {} +
  for item in \"\${EXTRACT}\"/*; do
    base=\$(basename \"\${item}\")
    [[ \"\${base}\" == install.properties ]] && continue
    rm -rf \"\${PLUGIN_ROOT}/\${base}\"
    cp -a \"\${item}\" \"\${PLUGIN_ROOT}/\"
  done
  rm -rf \"\${EXTRACT}\"

  if [[ ! -e \"\${PLUGIN_ROOT}/install.properties\" ]]; then
    echo \"WARN: install.properties missing under \${PLUGIN_ROOT}\" >&2
  fi

  if [[ -d \"\${SYMLINK}\" && ! -L \"\${SYMLINK}\" ]]; then
    if mountpoint -q \"\${SYMLINK}/install.properties\" 2>/dev/null; then
      find \"\${SYMLINK}\" -mindepth 1 -maxdepth 1 ! -name install.properties -exec rm -rf {} +
    else
      rm -rf \"\${SYMLINK}\"
    fi
  fi
  ln -sfn \"\${PLUGIN_ROOT}\" \"\${SYMLINK}\"

  rm -f /tmp/ranger-hive-plugin.tar.gz
  export HIVE_VERSION=${HIVE_VERSION}
  export HIVE_HOME=\${HIVE_HOME:-/opt/hive}
  /home/ranger/scripts/hive/enable-hive-plugin-docker.sh \"\${PLUGIN_ROOT}\"
  /home/ranger/scripts/hive/apply-hive-plugin-audit-config.sh --url http://ranger-audit-ingestor.rangernw:7081
"

wait_container_running() {
  local name="$1"
  local max_wait_s="${2:-180}"
  local elapsed=0
  while (( elapsed < max_wait_s )); do
    if docker inspect -f '{{.State.Running}}' "${name}" 2>/dev/null | grep -q true; then
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "TIMEOUT: ${name} not running after ${max_wait_s}s" >&2
  return 1
}

wait_hs2_port() {
  local max_wait_s="${1:-360}"
  local elapsed=0
  while (( elapsed < max_wait_s )); do
    if ! docker inspect -f '{{.State.Running}}' ranger-hive 2>/dev/null | grep -q true; then
      sleep 5
      elapsed=$((elapsed + 5))
      continue
    fi
    if docker exec ranger-hive timeout 2 bash -c 'echo >/dev/tcp/localhost/10000' 2>/dev/null; then
      echo "HS2 listening after ${elapsed}s (container up)"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "TIMEOUT waiting for HS2 (${max_wait_s}s)" >&2
  if docker inspect -f '{{.State.Running}}' ranger-hive 2>/dev/null | grep -q true; then
    docker exec ranger-hive bash -c 'grep -iE "VerifyError|ClassCast|ERROR|Started|Thrift" ${HIVE_HOME:-/opt/hive}/logs/hiveserver2.log 2>/dev/null || grep -iE "VerifyError|ClassCast|ERROR" /opt/apache-hive-*/logs/hiveserver2.log 2>/dev/null' | tail -20 || true
  fi
  return 1
}

echo "=== Restart ranger-hive ==="
docker restart ranger-hive

echo "=== Wait for ranger-hive container ==="
wait_container_running ranger-hive 180

echo "=== Wait for HS2 on port 10000 ==="
wait_hs2_port 360

echo "=== Bootstrap rbac_demo.sales (hive user) ==="
docker exec ranger-hive bash -c '
  kinit -kt /etc/keytabs/hive.keytab hive/ranger-hive.rangernw@EXAMPLE.COM 2>/dev/null || true
  beeline -u "jdbc:hive2://localhost:10000/default" --silent=true -e "
    CREATE DATABASE IF NOT EXISTS rbac_demo;
    USE rbac_demo;
    CREATE TABLE IF NOT EXISTS sales (id INT, amount DOUBLE, region STRING);
    INSERT INTO sales VALUES (1, 100.0, '\''north'\'') ;
  " 2>&1 | tail -5
'

echo "=== testuser2 deny flow ==="
docker exec ranger-hive bash -c '
  kinit -kt /etc/keytabs/testuser2.keytab testuser2/ranger-hive.rangernw@EXAMPLE.COM && \
  beeline -u "jdbc:hive2://localhost:10000/default" --silent=true -e "
    USE rbac_demo;
    SELECT * FROM sales LIMIT 1;
  " 2>&1
' || BEELINE_RC=$?
BEELINE_RC=${BEELINE_RC:-0}
echo "beeline exit code: ${BEELINE_RC}"

echo "=== Wait 45s for audit pipeline ==="
sleep 45

echo "=== Solr audit query ==="
curl -s 'http://localhost:8983/solr/ranger_audits/select?q=repo:dev_hive+AND+reqUser:testuser2&rows=5&sort=evtTime+desc&wt=json' | python3 -m json.tool 2>/dev/null || \
  curl -s 'http://localhost:8983/solr/ranger_audits/select?q=repo:dev_hive+AND+reqUser:testuser2&rows=5&sort=evtTime+desc&wt=json'

echo "=== Ingestor recent logs ==="
docker logs ranger-audit-ingestor 2>&1 | grep -iE 'testuser2|dev_hive' | tail -5 || true
