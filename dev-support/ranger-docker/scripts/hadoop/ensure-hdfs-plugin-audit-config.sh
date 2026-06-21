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

# Ensure ranger-hdfs-audit.xml sends audits to the audit ingestor.
# Idempotent — safe from host (docker exec) or inside ranger-hadoop.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

hdfs_security_patch_e2e() {
  local security_xml="$1"
  [[ -f "${security_xml}" ]] || return 1
  python3 - "${security_xml}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path = sys.argv[1]
props = {
    "ranger.hdfs.authz.enable.optimization": "false",
}
root = ET.parse(path).getroot()
names = {n.text: p for p in root.findall("property") for n in [p.find("name")] if n is not None and n.text}
for name, value in props.items():
    prop = names.get(name)
    if prop is None:
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = name
        ET.SubElement(prop, "value").text = value
    else:
        v = prop.find("value")
        if v is None:
            v = ET.SubElement(prop, "value")
        if (v.text or "").strip().lower() != value:
            v.text = value
ET.ElementTree(root).write(path, encoding="unicode", xml_declaration=False)
PY
}

hdfs_audit_patch_extras() {
  local audit_xml="$1"
  [[ -f "${audit_xml}" ]] || return 1
  python3 - "${audit_xml}" <<'PY'
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

hdfs_audit_ok_in() {
  local audit_xml="$1"
  [[ -f "${audit_xml}" ]] || return 1
  python3 - "${audit_xml}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path = sys.argv[1]
root = ET.parse(path).getroot()
for prop in root.findall("property"):
    n = prop.find("name")
    if n is not None and (n.text or "").strip() == "xasecure.audit.destination.auditserver":
        v = prop.find("value")
        ok = v is not None and (v.text or "").strip().lower() == "true"
        sys.exit(0 if ok else 1)
sys.exit(1)
PY
}

# Host-side: delegate to ranger-hadoop when not running inside the container.
if [[ ! -d "${HADOOP_HOME:-/opt/hadoop}/etc/hadoop" ]] || [[ ! -f "${RANGER_HOME:-/opt/ranger}/ranger-hdfs-plugin/enable-hdfs-plugin.sh" ]]; then
  if ! docker inspect ranger-hadoop >/dev/null 2>&1; then
    echo "ERROR: ranger-hadoop container not found" >&2
    exit 1
  fi
  if docker exec ranger-hadoop test -f /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml \
    && docker exec -i ranger-hadoop python3 - /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml <<'PY'
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
  then
    echo "HDFS plugin: auditserver already enabled in ranger-hdfs-audit.xml"
    docker exec -i ranger-hadoop python3 - /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml <<'PY'
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
    echo "HDFS plugin: auditserver kerberos auth + 3s batch interval applied"
    docker exec -i ranger-hadoop python3 - /opt/hadoop/etc/hadoop/ranger-hdfs-security.xml <<'PY'
import sys
import xml.etree.ElementTree as ET

path = sys.argv[1]
props = {"ranger.hdfs.authz.enable.optimization": "false"}
root = ET.parse(path).getroot()
names = {n.text: p for p in root.findall("property") for n in [p.find("name")] if n is not None and n.text}
for name, value in props.items():
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
    echo "HDFS plugin: authz optimization disabled for E2E audit emission"
    exit 0
  fi
  echo "HDFS plugin: applying audit-server destination in ranger-hadoop..."
  docker exec ranger-hadoop bash -c '
    export RANGER_SCRIPTS=/home/ranger/scripts
    export RANGER_HOME=/opt/ranger
    export HADOOP_HOME=/opt/hadoop
    export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"
    /home/ranger/scripts/enable-ranger-plugin-docker.sh /opt/ranger/ranger-hdfs-plugin
  '
  if docker exec -i ranger-hadoop python3 - /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml <<'PY'
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
  then
    docker exec -i ranger-hadoop python3 - /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml <<'PY'
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
    echo "HDFS plugin: auditserver enabled in ranger-hdfs-audit.xml (kerberos + 3s batch)"
  else
    echo "WARN: ranger-hdfs-audit.xml still missing auditserver=true" >&2
    exit 1
  fi
  exit 0
fi

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
[[ -d "${HADOOP_HOME}/etc/hadoop" ]] || export HADOOP_HOME=/opt/hadoop-3.4.2
export RANGER_HOME="${RANGER_HOME:-/opt/ranger}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"
export RANGER_SCRIPTS="${RANGER_SCRIPTS:-/home/ranger/scripts}"

if hdfs_audit_ok_in "${HADOOP_HOME}/etc/hadoop/ranger-hdfs-audit.xml"; then
  hdfs_audit_patch_extras "${HADOOP_HOME}/etc/hadoop/ranger-hdfs-audit.xml"
  hdfs_security_patch_e2e "${HADOOP_HOME}/etc/hadoop/ranger-hdfs-security.xml"
  exit 0
fi

echo "HDFS plugin: applying audit-server destination (XmlConfigChanger)..."
"${RANGER_SCRIPTS}/enable-ranger-plugin-docker.sh" "${RANGER_HOME}/ranger-hdfs-plugin"

if hdfs_audit_ok_in "${HADOOP_HOME}/etc/hadoop/ranger-hdfs-audit.xml"; then
  hdfs_audit_patch_extras "${HADOOP_HOME}/etc/hadoop/ranger-hdfs-audit.xml"
  hdfs_security_patch_e2e "${HADOOP_HOME}/etc/hadoop/ranger-hdfs-security.xml"
  echo "HDFS plugin: auditserver enabled in ranger-hdfs-audit.xml (kerberos + 3s batch)"
else
  echo "WARN: ranger-hdfs-audit.xml still missing auditserver=true" >&2
  exit 1
fi

if ps -ef | grep -v grep | grep -q 'org.apache.hadoop.hdfs.server.namenode.NameNode'; then
  echo "HDFS plugin: reloading DFS to pick up audit config..."
  su -c "${HADOOP_HOME}/sbin/stop-dfs.sh" hdfs || true
  sleep 3
  su -c "${HADOOP_HOME}/sbin/start-dfs.sh" hdfs
fi
