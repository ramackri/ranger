#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Point Knox ranger-knox-audit.xml at the audit ingestor (Kerberos).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/knox/apply-knox-plugin-audit-config.sh
#   ./scripts/knox/apply-knox-plugin-audit-config.sh --check-only
#   ./scripts/knox/apply-knox-plugin-audit-config.sh --no-restart

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${RANGER_KNOX_CONTAINER:-ranger-knox}"
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
      sed -n '10,14p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -f /opt/ranger/ranger-knox-plugin/install.properties ]]; then
  INSTALL_PROPS=/opt/ranger/ranger-knox-plugin/install.properties
  IN_CONTAINER=true
else
  INSTALL_PROPS="${SCRIPT_DIR}/ranger-knox-plugin-install.properties"
  IN_CONTAINER=false
fi

AUDIT_URL="${AUDIT_URL_OVERRIDE:-$(grep -E '^XAAUDIT\.AUDITSERVER\.URL=' "${INSTALL_PROPS}" | cut -d= -f2-)}"
AUDIT_SPOOL="$(grep -E '^XAAUDIT\.AUDITSERVER\.FILE_SPOOL_DIR=' "${INSTALL_PROPS}" | cut -d= -f2-)"
AUDIT_URL="${AUDIT_URL:-http://ranger-audit-ingestor.rangernw:7081}"
AUDIT_SPOOL="${AUDIT_SPOOL:-/var/log/knox/audit/audit-ingestor/spool}"

apply_audit_xml() {
  local audit_file="$1"
  [[ -f "${audit_file}" ]] || return 1
  python3 - "${audit_file}" "${AUDIT_URL}" "${AUDIT_SPOOL}" "${INSTALL_PROPS}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, url, spool, install_props = sys.argv[1:5]

def prop(key, default=""):
    val = default
    try:
        with open(install_props, encoding="utf-8") as f:
            for line in f:
                if line.startswith(key + "="):
                    val = line.split("=", 1)[1].strip()
                    break
    except OSError:
        pass
    return val

updates = {
    'xasecure.audit.destination.auditserver': 'true',
    'xasecure.audit.destination.auditserver.url': url,
    'xasecure.audit.destination.auditserver.batch.filespool.dir': spool,
    'xasecure.audit.destination.auditserver.authn.type': 'kerberos',
    'xasecure.audit.destination.auditserver.batch.batch.interval.ms': '3000',
    'xasecure.audit.is.enabled': 'true',
    'xasecure.audit.solr.is.enabled': 'false',
    'xasecure.audit.hdfs.is.enabled': 'false',
    'xasecure.audit.jaas.Client.loginModuleName': prop('XAAUDIT.JAAS.CLIENT.LOGIN_MODULE_NAME', 'com.sun.security.auth.module.Krb5LoginModule'),
    'xasecure.audit.jaas.Client.loginModuleControlFlag': prop('XAAUDIT.JAAS.CLIENT.LOGIN_MODULE_CONTROL_FLAG', 'required'),
    'xasecure.audit.jaas.Client.option.useKeyTab': prop('XAAUDIT.JAAS.CLIENT.OPTION.USE_KEY_TAB', 'true'),
    'xasecure.audit.jaas.Client.option.storeKey': prop('XAAUDIT.JAAS.CLIENT.OPTION.STORE_KEY', 'true'),
    'xasecure.audit.jaas.Client.option.useTicketCache': 'false',
    'xasecure.audit.jaas.Client.option.keyTab': prop('XAAUDIT.JAAS.CLIENT.OPTION.KEY_TAB', '/etc/keytabs/knox.keytab'),
    'xasecure.audit.jaas.Client.option.principal': prop('XAAUDIT.JAAS.CLIENT.OPTION.PRINCIPAL', 'knox/ranger-knox.rangernw@EXAMPLE.COM'),
}
root = ET.parse(path).getroot()
names = {n.text: p for p in root.findall('property') for n in [p.find('name')] if n is not None and n.text}
for name, value in updates.items():
    prop_el = names.get(name)
    if prop_el is None:
        prop_el = ET.SubElement(root, 'property')
        ET.SubElement(prop_el, 'name').text = name
        ET.SubElement(prop_el, 'value').text = value
    else:
        v = prop_el.find('value')
        if v is None:
            v = ET.SubElement(prop_el, 'value')
        v.text = value
ET.ElementTree(root).write(path, encoding='unicode', xml_declaration=False)
PY
  mkdir -p "${AUDIT_SPOOL}"
  chown -R knox:knox /var/log/knox/audit 2>/dev/null || true
}

audit_ok() {
  local audit_file="$1"
  python3 - "${audit_file}" "${AUDIT_URL}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, url = sys.argv[1:3]
root = ET.parse(path).getroot()
vals = {}
for p in root.findall('property'):
    n, v = p.find('name'), p.find('value')
    if n is not None and n.text and v is not None:
        vals[n.text] = (v.text or '').strip()
ok = (
    vals.get('xasecure.audit.destination.auditserver', '').lower() == 'true'
    and vals.get('xasecure.audit.destination.auditserver.url') == url
    and vals.get('xasecure.audit.destination.auditserver.authn.type', '').lower() == 'kerberos'
)
sys.exit(0 if ok else 1)
PY
}

run_in_container() {
  local audit_file="${KNOX_HOME:-/opt/knox}/conf/ranger-knox-audit.xml"
  if [[ ! -f "${audit_file}" ]]; then
    echo "WARN: ${audit_file} not found; enable-knox-plugin may not have run yet" >&2
    return 1
  fi
  if [[ "${CHECK_ONLY}" == "true" ]]; then
    audit_ok "${audit_file}"
    return $?
  fi
  apply_audit_xml "${audit_file}"
  echo "Applied auditserver config to ${audit_file}"
}

if [[ "${IN_CONTAINER}" == "true" ]]; then
  run_in_container
  exit $?
fi

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "ERROR: container ${CONTAINER} is not running" >&2
  exit 1
fi

if [[ "${CHECK_ONLY}" == "true" ]]; then
  docker exec "${CONTAINER}" bash -c "KNOX_HOME=/opt/knox ${RANGER_SCRIPTS:-/home/ranger/scripts}/apply-knox-plugin-audit-config.sh --check-only"
  exit $?
fi

docker exec "${CONTAINER}" bash -c "KNOX_HOME=/opt/knox ${RANGER_SCRIPTS:-/home/ranger/scripts}/apply-knox-plugin-audit-config.sh"
if [[ "${NO_RESTART}" != "true" ]]; then
  echo "Restarting ${CONTAINER} (gateway entrypoint re-attaches after gateway start)..."
  docker restart "${CONTAINER}" >/dev/null
  for _ in $(seq 1 36); do
    if curl -sk -o /dev/null -w '%{http_code}' \
      -u "${KNOX_GATEWAY_USER:-guest}:${KNOX_GATEWAY_PASSWORD:-guest-password}" \
      "${KNOX_GATEWAY_URL:-https://localhost:8443}${KNOX_E2E_WEBHDFS_PATH:-/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS}" 2>/dev/null \
      | grep -qE '200|401|403'; then
      echo "Knox gateway is responding on WebHDFS path."
      exit 0
    fi
    sleep 5
  done
  echo "WARN: Knox gateway may not be ready; check: docker logs ${CONTAINER}" >&2
fi
