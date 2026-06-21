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

# E2E-only: deny testuser1 EXECUTE on /tmp/e2e-audit-deny-traverse so
# `hdfs dfs -ls` hits a RangerAccessControlException (multi-component /tmp/... path).
# Creates testuser1 in Ranger Admin if missing (deny policies require known users).

set -euo pipefail

ADMIN_URL="${RANGER_ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${RANGER_ADMIN_USER:-admin}"
ADMIN_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
SERVICE="dev_hdfs"
POLICY_NAME="e2e-audit-deny-traverse"
DENY_PATH="/tmp/e2e-audit-deny-traverse"
DENY_USER="testuser1"

python3 <<PY
import json
import sys
import urllib.request
import base64

admin_url = "${ADMIN_URL}"
service = "${SERVICE}"
policy_name = "${POLICY_NAME}"
deny_path = "${DENY_PATH}"
deny_user = "${DENY_USER}"
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

users = api("GET", f"/service/xusers/users?name={deny_user}")
if not users.get("vXUsers"):
    api(
        "POST",
        "/service/xusers/users",
        {
            "name": deny_user,
            "firstName": "test",
            "lastName": "user1",
            "password": "Testuser1!",
            "userRoleList": ["ROLE_USER"],
            "status": 1,
        },
    )
    print(f"Ranger Admin: created xuser '{deny_user}' (required for deny policy)")
else:
    print(f"Ranger Admin: xuser '{deny_user}' already present")

policies = api("GET", f"/service/public/v2/api/policy?serviceName={service}")
existing = next((p for p in policies if p.get("name") == policy_name), None)

deny_item = {
    "users": [deny_user],
    "groups": [],
    "roles": [],
    "accesses": [{"type": "execute", "isAllowed": True}],
    "conditions": [],
    "delegateAdmin": False,
}

body = {
    "service": service,
    "name": policy_name,
    "policyType": 0,
    "policyPriority": 1,
    "description": "E2E HDFS audit trigger: deny traverse for testuser1",
    "isAuditEnabled": True,
    "resources": {"path": {"values": [deny_path], "isExcludes": False, "isRecursive": True}},
    "policyItems": [],
    "denyPolicyItems": [deny_item],
    "allowExceptions": [],
    "denyExceptions": [],
    "isEnabled": True,
}

if existing:
    body["id"] = existing["id"]
    body["guid"] = existing.get("guid")
    body["version"] = existing.get("version")
    api("PUT", f"/service/public/v2/api/policy/{existing['id']}", body)
    print(f"dev_hdfs: updated policy '{policy_name}' (deny execute for {deny_user} on {deny_path})")
else:
    api("POST", "/service/public/v2/api/policy", body)
    print(f"dev_hdfs: created policy '{policy_name}' (deny execute for {deny_user} on {deny_path})")
PY
