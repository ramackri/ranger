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

# Grant OM shell user (om) access on dev_ozone. Default install policies only include hdfs.
# Creates om in Ranger Admin xusers if missing (policies require known users).

set -euo pipefail

ADMIN_URL="${RANGER_ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${RANGER_ADMIN_USER:-admin}"
ADMIN_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
SERVICE="dev_ozone"
POLICY_NAME="tier4-om-ozone-cli-access"
OM_USER="om"

python3 <<PY
import json
import sys
import urllib.request
import base64

admin_url = "${ADMIN_URL}"
service = "${SERVICE}"
policy_name = "${POLICY_NAME}"
om_user = "${OM_USER}"
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

users = api("GET", f"/service/xusers/users?name={om_user}")
if not users.get("vXUsers"):
    api(
        "POST",
        "/service/xusers/users",
        {
            "name": om_user,
            "firstName": "ozone",
            "lastName": "om",
            "password": "OmUser1!",
            "userRoleList": ["ROLE_USER"],
            "status": 1,
        },
    )
    print(f"Ranger Admin: created xuser '{om_user}' (required for Ozone CLI policy)")
else:
    print(f"Ranger Admin: xuser '{om_user}' already present")

policies = api("GET", f"/service/public/v2/api/policy?serviceName={service}")
existing = next((p for p in policies if p.get("name") == policy_name), None)

payload = {
    "service": service,
    "name": policy_name,
    "policyType": 0,
    "isEnabled": True,
    "isAuditEnabled": True,
    "description": "Allow OM shell user (om) for audit E2E volume/bucket smoke",
    "resources": {
        "volume": {"values": ["*"], "isExcludes": False, "isRecursive": False},
        "bucket": {"values": ["*"], "isExcludes": False, "isRecursive": False},
        "key": {"values": ["*"], "isExcludes": False, "isRecursive": False},
    },
    "policyItems": [{
        "users": [om_user],
        "groups": [],
        "roles": [],
        "conditions": [],
        "delegateAdmin": False,
        "accesses": [
            {"type": "all", "isAllowed": True},
            {"type": "read", "isAllowed": True},
            {"type": "write", "isAllowed": True},
            {"type": "create", "isAllowed": True},
            {"type": "list", "isAllowed": True},
            {"type": "delete", "isAllowed": True},
            {"type": "read_acl", "isAllowed": True},
            {"type": "write_acl", "isAllowed": True},
        ],
    }],
}

if existing:
    payload["id"] = existing["id"]
    payload["version"] = existing.get("version", 1)
    api("PUT", f"/service/public/v2/api/policy/{existing['id']}", payload)
    print(f"dev_ozone: updated policy {policy_name} for user {om_user}")
else:
    api("POST", "/service/public/v2/api/policy", payload)
    print(f"dev_ozone: created policy {policy_name} for user {om_user}")
PY
