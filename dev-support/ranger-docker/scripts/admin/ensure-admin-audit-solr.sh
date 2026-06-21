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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# In-container: ensure Admin Audit -> Access reads Solr ranger_audits (audit_store=solr).
# Installs Jetty client jars if missing. Idempotent.

set -euo pipefail

# FQDN required for SPNEGO to Solr (HTTP/ranger-solr.rangernw@REALM)
SOLR_URLS="${AUDIT_SOLR_URLS:-http://ranger-solr.rangernw:8983/solr/ranger_audits}"
JETTY_VERSION="${JETTY_CLIENT_VERSION:-9.4.56.v20240826}"
SITE_XML="/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml"
LIB_DIR="/opt/ranger/admin/ews/webapp/WEB-INF/lib"
PATCH_SCRIPT="${RANGER_SCRIPTS}/patch-audit-solr-site-xml.py"

needs_site_patch() {
  python3 - "${SITE_XML}" "${SOLR_URLS}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, want_url = sys.argv[1], sys.argv[2]
root = ET.parse(path).getroot()

def val(name):
    for prop in root.findall("property"):
        n = prop.find("name")
        if n is not None and (n.text or "").strip() == name:
            v = prop.find("value")
            return (v.text or "").strip() if v is not None else ""
    return ""

ok = val("ranger.audit.source.type").lower() == "solr" and val("ranger.audit.solr.urls") == want_url
sys.exit(0 if ok else 1)
PY
}

needs_jetty() {
  ! test -f "${LIB_DIR}/jetty-client-${JETTY_VERSION}.jar"
}

install_jetty() {
  echo "Admin audit: installing Jetty ${JETTY_VERSION} jars for Solr client..."
  for j in jetty-client jetty-http jetty-io jetty-util; do
    jar="${LIB_DIR}/${j}-${JETTY_VERSION}.jar"
    if [[ -f "${jar}" ]]; then
      continue
    fi
    url="https://repo1.maven.org/maven2/org/eclipse/jetty/${j}/${JETTY_VERSION}/${j}-${JETTY_VERSION}.jar"
    if ! curl -fsSL "${url}" -o "${jar}"; then
      echo "WARN: could not download ${url}" >&2
      return 1
    fi
    chown ranger:ranger "${jar}" 2>/dev/null || true
  done
}

CHANGED=false

if needs_site_patch; then
  :
else
  echo "Admin audit: patching ranger-admin-site.xml for audit_store=solr..."
  if [[ -f "${PATCH_SCRIPT}" ]]; then
    python3 "${PATCH_SCRIPT}" "${SOLR_URLS}"
  else
    python3 - "${SOLR_URLS}" <<'PY'
import shutil
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

path = "/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml"
solr_urls = sys.argv[1]
backup = f"{path}.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.bak"
shutil.copy2(path, backup)
root = ET.parse(path).getroot()

def set_prop(name, value):
    for prop in root.findall("property"):
        n = prop.find("name")
        if n is not None and (n.text or "").strip() == name:
            v = prop.find("value")
            if v is None:
                v = ET.SubElement(prop, "value")
            v.text = value
            return
    prop = ET.SubElement(root, "property")
    ET.SubElement(prop, "name").text = name
    ET.SubElement(prop, "value").text = value

set_prop("ranger.audit.source.type", "solr")
set_prop("ranger.audit.solr.urls", solr_urls)
ET.indent(root, space="    ")
ET.ElementTree(root).write(path, encoding="unicode", xml_declaration=False)
print(f"Admin audit: patched {path}")
PY
  fi
  CHANGED=true
fi

if needs_jetty; then
  install_jetty || true
  CHANGED=true
fi

if [[ "${CHANGED}" == "true" ]]; then
  touch "${RANGER_HOME}/.auditSolrPatched" 2>/dev/null || true
  echo "Admin audit: restarting ranger to pick up Solr audit backend..."
  if command -v ranger-admin-restart.sh >/dev/null 2>&1; then
    ranger-admin-restart.sh 2>/dev/null || true
  fi
  sleep 30
fi
