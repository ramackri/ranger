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

# E2E-only: widen dev_hdfs audit filters so smoke triggers produce audits.
# Default dev_hdfs filters audit DENIED, delete, and rename only — allowed
# hdfs dfs -ls is skipped (traverse-only + auditOnlyIfDenied). Idempotent.

set -euo pipefail

ADMIN_URL="${RANGER_ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${RANGER_ADMIN_USER:-admin}"
ADMIN_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
SERVICE="dev_hdfs"
MARKER="e2e-audit-all-access"

python3 <<PY
import json
import re
import sys
import urllib.request
import base64

admin_url = "${ADMIN_URL}"
service = "${SERVICE}"
marker = "${MARKER}"
auth = base64.b64encode(b"${ADMIN_USER}:${ADMIN_PASS}").decode()

def api(method, path, body=None):
    req = urllib.request.Request(
        admin_url + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
    )
    req.add_header("Authorization", "Basic " + auth)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)

svc = api("GET", f"/service/public/v2/api/service/name/{service}")
raw = svc.get("configs", {}).get("ranger.plugin.audit.filters", "[]")
if isinstance(raw, list):
    filters = raw
else:
    fixed = re.sub(r"\btrue\b", "True", str(raw))
    fixed = re.sub(r"\bfalse\b", "False", fixed)
    filters = eval(fixed)  # Ranger stores Python-ish literals

catch = {"isAudited": True, "description": marker}
if any(f.get("description") == marker for f in filters):
    print(f"dev_hdfs: audit filter '{marker}' already present")
    sys.exit(0)

filters = [catch] + filters
svc["configs"]["ranger.plugin.audit.filters"] = json.dumps(filters)
api("PUT", f"/service/public/v2/api/service/name/{service}", svc)
print(f"dev_hdfs: prepended E2E audit-all filter ({len(filters)} filters total)")
PY
