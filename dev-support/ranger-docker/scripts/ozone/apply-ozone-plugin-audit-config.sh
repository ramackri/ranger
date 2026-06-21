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

# Host-side: refresh Ozone plugin install.properties and re-apply Ranger config
# inside ozone-om (policy URL, auditserver destination). Idempotent.
#
# Usage:
#   ./scripts/ozone/apply-ozone-plugin-audit-config.sh
#   ./scripts/ozone/apply-ozone-plugin-audit-config.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

CHECK_ONLY=false
OZONE_PLUGIN_VERSION="${OZONE_PLUGIN_VERSION:-3.0.0-SNAPSHOT}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    -h|--help)
      sed -n '19,22p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

ozone_audit_patch_extras() {
  local audit_xml="/opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml"
  docker exec ozone-om test -f "${audit_xml}" || return 1
  docker exec -i ozone-om python3 - "${audit_xml}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path = sys.argv[1]
extras = {
    "xasecure.audit.destination.auditserver.authn.type": "kerberos",
    "xasecure.audit.destination.auditserver.batch.batch.interval.ms": "3000",
}
root = ET.parse(path).getroot()
names = {n.text: p for p in root.findall("property") for n in [p.find("name")] if n is not None and n.text}
for name, value in extras.items():
    prop = names.get(name)
    if prop is None:
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = name
        ET.SubElement(prop, "value").text = value
    else:
        v = prop.find("value")
        if v is None:
            v = ET.SubElement(prop, "value")
        v.text = value
ET.ElementTree(root).write(path, encoding="unicode", xml_declaration=False)
PY
}

ozone_audit_ok() {
  docker exec ozone-om test -f /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml || return 1
  docker exec -i ozone-om python3 - /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-audit.xml <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()

def val(name):
    for prop in root.findall("property"):
        n = prop.find("name")
        if n is not None and (n.text or "").strip() == name:
            v = prop.find("value")
            return (v.text or "").strip() if v is not None else ""
    return ""

jaas_principal = val("xasecure.audit.jaas.Client.option.principal")
ok = (
    val("xasecure.audit.destination.auditserver").lower() == "true"
    and "ranger-audit-ingestor" in val("xasecure.audit.destination.auditserver.url")
    and val("xasecure.audit.destination.auditserver.authn.type").lower() == "kerberos"
    and "ozone/ranger-ozone" in jaas_principal
    and val("xasecure.audit.jaas.Client.option.keyTab") == "/etc/keytabs/ozone.keytab"
)
sys.exit(0 if ok else 1)
PY
}

ozone_policy_ok() {
  docker exec ozone-om test -f /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-security.xml || return 1
  docker exec -i ozone-om python3 - /opt/hadoop/ranger-ozone-plugin/conf/ranger-ozone-security.xml <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()

def val(name):
    for prop in root.findall("property"):
        n = prop.find("name")
        if n is not None and (n.text or "").strip() == name:
            v = prop.find("value")
            return (v.text or "").strip() if v is not None else ""
    return ""

policy_url = val("ranger.plugin.ozone.policy.rest.url")
ok = (
    val("ranger.plugin.ozone.service.name") == "dev_ozone"
    and policy_url.startswith("http://")
    and ":6080" in policy_url
    and ("ranger" in policy_url or "ranger.rangernw" in policy_url)
)
sys.exit(0 if ok else 1)
PY
}

if [[ "${CHECK_ONLY}" == "true" ]]; then
  ozone_audit_ok && ozone_policy_ok
  exit $?
fi

chmod +x scripts/ozone/ozone-plugin-docker-setup.sh
./scripts/ozone/ozone-plugin-docker-setup.sh

need_refresh=false
if ! ozone_audit_ok || ! ozone_policy_ok; then
  need_refresh=true
  echo "Current Ozone plugin config needs refresh (policy URL or auditserver destination)"
  echo "Re-applying Ozone Ranger plugin config in ozone-om..."
  docker exec ozone-om bash -c '
    set -e
    export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/jre/}"
    export OZONE_HOME=/opt/hadoop
    cd /opt/hadoop/ranger-ozone-plugin
    if [[ ! -d conf ]]; then
      mkdir -p conf
      echo "export JAVA_HOME=${JAVA_HOME}" >> conf/ozone-env.sh
    fi
    rm -f /opt/hadoop/.setupDone
    /opt/hadoop/ranger-ozone-plugin/ranger-ozone-setup.sh
  '
fi

ozone_audit_patch_extras || true

if ozone_audit_ok && ozone_policy_ok; then
  if [[ "${need_refresh}" == "true" ]]; then
    echo "Ozone plugin config applied; restart ozone-om to load policies if OM was already running"
  else
    echo "Ozone plugin audit + policy config already OK (authn.type kerberos patched)"
  fi
else
  echo "ERROR: Ozone plugin config still incomplete after enable-ozone-plugin.sh" >&2
  exit 1
fi
