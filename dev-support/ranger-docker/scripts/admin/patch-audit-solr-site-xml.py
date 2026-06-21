#!/usr/bin/env python3
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

"""Set ranger.audit.source.type=solr and ranger.audit.solr.urls in ranger-admin-site.xml."""

import shutil
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

SITE_XML = "/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml"


def set_property(root, name, value):
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


def main():
    solr_urls = sys.argv[1] if len(sys.argv) > 1 else "http://ranger-solr.rangernw:8983/solr/ranger_audits"
    path = SITE_XML
    backup = f"{path}.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.bak"
    shutil.copy2(path, backup)
    root = ET.parse(path).getroot()
    set_property(root, "ranger.audit.source.type", "solr")
    set_property(root, "ranger.audit.solr.urls", solr_urls)
    ET.indent(root, space="    ")
    tree = ET.ElementTree(root)
    tree.write(path, encoding="unicode", xml_declaration=False)
    print(f"Admin audit: patched {path} (backup {backup})")


if __name__ == "__main__":
    main()
