#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Point Hive ranger-hive-audit.xml at the audit ingestor (Tier 3/4 pipeline).
# enable-hive-plugin.sh may leave localhost:7081 when it exits early on SSL/creds.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/hive/apply-hive-plugin-audit-config.sh
#   ./scripts/hive/apply-hive-plugin-audit-config.sh --url http://ranger-audit-ingestor.rangernw:7081
#   ./scripts/hive/apply-hive-plugin-audit-config.sh --check-only
#   ./scripts/hive/apply-hive-plugin-audit-config.sh --no-restart
#
# Also runs inside ranger-hive during ranger-hive-setup.sh / ranger-hive.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${RANGER_HIVE_CONTAINER:-ranger-hive}"
CHECK_ONLY=false
NO_RESTART=false
AUDIT_URL_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    --no-restart) NO_RESTART=true; shift ;;
    --url)
      [[ $# -ge 2 ]] || { echo "--url requires a value" >&2; exit 1; }
      AUDIT_URL_OVERRIDE="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '10,17p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# In-container: install.properties is mounted at plugin dir; on host: scripts/hive/
if [[ -f /opt/ranger/ranger-hive-plugin/install.properties ]]; then
  INSTALL_PROPS=/opt/ranger/ranger-hive-plugin/install.properties
  IN_CONTAINER=true
else
  INSTALL_PROPS="${SCRIPT_DIR}/ranger-hive-plugin-install.properties"
  IN_CONTAINER=false
fi

AUDIT_URL="${AUDIT_URL_OVERRIDE:-$(grep -E '^XAAUDIT\.AUDITSERVER\.URL=' "${INSTALL_PROPS}" | cut -d= -f2-)}"
AUDIT_SPOOL="$(grep -E '^XAAUDIT\.AUDITSERVER\.FILE_SPOOL_DIR=' "${INSTALL_PROPS}" | cut -d= -f2-)"
AUDIT_URL="${AUDIT_URL:-http://ranger-audit-ingestor.rangernw:7081}"
AUDIT_SPOOL="${AUDIT_SPOOL:-/var/log/hive/audit/audit-ingestor/spool}"

apply_audit_xml() {
  local audit_file="$1"
  [[ -f "${audit_file}" ]] || return 1
  python3 - "${audit_file}" "${AUDIT_URL}" "${AUDIT_SPOOL}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, url, spool = sys.argv[1:4]
if not url or not spool:
    print("audit url/spool must be non-empty", file=sys.stderr)
    sys.exit(1)
updates = {
    'xasecure.audit.destination.auditserver': 'true',
    'xasecure.audit.destination.auditserver.url': url,
    'xasecure.audit.destination.auditserver.batch.filespool.dir': spool,
    'xasecure.audit.destination.auditserver.authn.type': 'kerberos',
    'xasecure.audit.destination.auditserver.batch.batch.interval.ms': '3000',
    'xasecure.audit.is.enabled': 'true',
    'xasecure.audit.solr.is.enabled': 'false',
    'xasecure.audit.hdfs.is.enabled': 'false',
}
root = ET.parse(path).getroot()
names = {n.text: p for p in root.findall('property') for n in [p.find('name')] if n is not None and n.text}
for name, value in updates.items():
    prop = names.get(name)
    if prop is None:
        prop = ET.SubElement(root, 'property')
        ET.SubElement(prop, 'name').text = name
        ET.SubElement(prop, 'value').text = value
    else:
        v = prop.find('value')
        if v is None:
            v = ET.SubElement(prop, 'value')
        v.text = value
tree = ET.ElementTree(root)
tree.write(path, encoding='unicode', xml_declaration=False)
PY
  mkdir -p "${AUDIT_SPOOL}"
  chown -R hive:hadoop /var/log/hive/audit 2>/dev/null || true
}

apply_audit_xml_remote() {
  docker exec -i "${CONTAINER}" python3 - /opt/hive/conf/ranger-hive-audit.xml "${AUDIT_URL}" "${AUDIT_SPOOL}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, url, spool = sys.argv[1:4]
if not url or not spool:
    print("audit url/spool must be non-empty", file=sys.stderr)
    sys.exit(1)
updates = {
    'xasecure.audit.destination.auditserver': 'true',
    'xasecure.audit.destination.auditserver.url': url,
    'xasecure.audit.destination.auditserver.batch.filespool.dir': spool,
    'xasecure.audit.destination.auditserver.authn.type': 'kerberos',
    'xasecure.audit.destination.auditserver.batch.batch.interval.ms': '3000',
    'xasecure.audit.is.enabled': 'true',
    'xasecure.audit.solr.is.enabled': 'false',
    'xasecure.audit.hdfs.is.enabled': 'false',
}
root = ET.parse(path).getroot()
names = {n.text: p for p in root.findall('property') for n in [p.find('name')] if n is not None and n.text}
for name, value in updates.items():
    prop = names.get(name)
    if prop is None:
        prop = ET.SubElement(root, 'property')
        ET.SubElement(prop, 'name').text = name
        ET.SubElement(prop, 'value').text = value
    else:
        v = prop.find('value')
        if v is None:
            v = ET.SubElement(prop, 'value')
        v.text = value
tree = ET.ElementTree(root)
tree.write(path, encoding='unicode', xml_declaration=False)
PY
  docker exec "${CONTAINER}" bash -c "mkdir -p '${AUDIT_SPOOL}' && chown -R hive:hadoop /var/log/hive/audit 2>/dev/null || true"
}

audit_config_ok() {
  local audit_file="$1"
  [[ -f "${audit_file}" ]] || return 1
  python3 - "${audit_file}" "${AUDIT_URL}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, url = sys.argv[1:3]
root = ET.parse(path).getroot()

def val(name):
    for prop in root.findall("property"):
        n = prop.find("name")
        if n is not None and (n.text or "").strip() == name:
            v = prop.find("value")
            return (v.text or "").strip() if v is not None else ""
    return ""

ok = (
    val("xasecure.audit.destination.auditserver").lower() == "true"
    and val("xasecure.audit.destination.auditserver.url") == url
    and val("xasecure.audit.destination.auditserver.authn.type").lower() == "kerberos"
)
sys.exit(0 if ok else 1)
PY
}

audit_config_ok_remote() {
  docker exec -i "${CONTAINER}" python3 - /opt/hive/conf/ranger-hive-audit.xml "${AUDIT_URL}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, url = sys.argv[1:3]
root = ET.parse(path).getroot()

def val(name):
    for prop in root.findall("property"):
        n = prop.find("name")
        if n is not None and (n.text or "").strip() == name:
            v = prop.find("value")
            return (v.text or "").strip() if v is not None else ""
    return ""

ok = (
    val("xasecure.audit.destination.auditserver").lower() == "true"
    and val("xasecure.audit.destination.auditserver.url") == url
    and val("xasecure.audit.destination.auditserver.authn.type").lower() == "kerberos"
)
sys.exit(0 if ok else 1)
PY
}

if [[ "${IN_CONTAINER}" == "true" ]]; then
  AUDIT_FILE=/opt/hive/conf/ranger-hive-audit.xml
  if audit_config_ok "${AUDIT_FILE}"; then
    [[ "${CHECK_ONLY}" == "true" ]] && exit 0
    [[ "${NO_RESTART}" == "true" ]] && exit 0
    echo "Hive plugin audit ingestor URL already set."
    exit 0
  fi
  if [[ "${CHECK_ONLY}" == "true" ]]; then
    echo "Hive plugin audit URL is not ${AUDIT_URL}." >&2
    exit 1
  fi
  echo "Applying Hive plugin audit config (url=${AUDIT_URL})..."
  apply_audit_xml "${AUDIT_FILE}"
  [[ "${NO_RESTART}" == "true" ]] && exit 0
  exit 0
fi

if ! docker inspect "${CONTAINER}" >/dev/null 2>&1; then
  echo "Container '${CONTAINER}' not found." >&2
  exit 1
fi

if ! docker inspect -f '{{.State.Running}}' "${CONTAINER}" 2>/dev/null | grep -q true; then
  echo "Container '${CONTAINER}' is not running." >&2
  exit 1
fi

if audit_config_ok_remote 2>/dev/null; then
  if [[ "${CHECK_ONLY}" == "true" ]]; then
    exit 0
  fi
  echo "Hive plugin audit ingestor URL already set in ${CONTAINER}."
  exit 0
fi

if [[ "${CHECK_ONLY}" == "true" ]]; then
  echo "Hive plugin audit URL is not ${AUDIT_URL} in ${CONTAINER}." >&2
  exit 1
fi

echo "Applying Hive plugin audit config in ${CONTAINER} (url=${AUDIT_URL})..."
apply_audit_xml_remote

if [[ "${NO_RESTART}" == "true" ]]; then
  echo "Config applied. Restart ${CONTAINER} to load ranger-hive-audit.xml."
  exit 0
fi

echo "Restarting ${CONTAINER} (HiveServer2 must reload ranger-hive-audit.xml)..."
docker restart "${CONTAINER}" >/dev/null

echo "Waiting for HiveServer2 (up to 180s)..."
for _ in $(seq 1 36); do
  if docker exec "${CONTAINER}" bash -c 'timeout 2 bash -c "echo > /dev/tcp/localhost/10000"' 2>/dev/null; then
    echo "HiveServer2 is up."
    exit 0
  fi
  sleep 5
done

echo "WARN: HiveServer2 may not be ready; check: docker logs ${CONTAINER}" >&2
exit 0
