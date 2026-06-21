#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Ensure dev_kms has a keyadmin allow policy (GET_KEYS, create, etc.) for E2E.
# Idempotent — skips when a policy already grants keyadmin getkeys on *.
#
# KMS policies are only manageable by the keyadmin portal role (not admin).

set -euo pipefail

ADMIN_URL="${RANGER_ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${RANGER_KMS_POLICY_USER:-keyadmin}"
ADMIN_PASS="${RANGER_KMS_POLICY_PASSWORD:-rangerR0cks!}"
SERVICE="dev_kms"
POLICY_NAME="e2e-keyadmin-all-keys"

python3 <<PY
import base64
import json
import urllib.error
import urllib.request

admin_url = "${ADMIN_URL}"
service = "${SERVICE}"
policy_name = "${POLICY_NAME}"
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

try:
    policies = api("GET", f"/service/public/v2/api/policy?serviceName={service}")
except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", errors="replace")
    print(f"FAIL: could not list policies for {service}: HTTP {e.code} {body[:200]}", file=__import__('sys').stderr)
    raise SystemExit(1)

for p in policies:
    for item in p.get("policyItems", []):
        users = item.get("users") or []
        accesses = {a.get("type", "").lower() for a in item.get("accesses", []) if a.get("isAllowed")}
        if "keyadmin" in users and "getkeys" in accesses:
            print(f"{service}: keyadmin getkeys policy already present ({p.get('name')})")
            raise SystemExit(0)

body = {
    "service": service,
    "name": policy_name,
    "description": "E2E: keyadmin KMS key operations",
    "isEnabled": True,
    "resources": {"keyname": {"values": ["*"], "isExcludes": False, "isRecursive": False}},
    "policyItems": [{
        "accesses": [
            {"type": "create", "isAllowed": True},
            {"type": "delete", "isAllowed": True},
            {"type": "rollover", "isAllowed": True},
            {"type": "setkeymaterial", "isAllowed": True},
            {"type": "get", "isAllowed": True},
            {"type": "getkeys", "isAllowed": True},
            {"type": "getmetadata", "isAllowed": True},
            {"type": "generateeek", "isAllowed": True},
            {"type": "decrypteek", "isAllowed": True},
        ],
        "users": ["keyadmin"],
        "groups": [],
        "conditions": [],
        "delegateAdmin": True,
    }],
}
try:
    api("POST", "/service/public/v2/api/policy", body)
    print(f"{service}: created policy {policy_name} for keyadmin")
except urllib.error.HTTPError as e:
    err = e.read().decode("utf-8", errors="replace")
    if e.code == 400 and "already exists" in err.lower():
        print(f"{service}: policy name {policy_name} exists but lacks keyadmin getkeys — check Admin UI", file=__import__('sys').stderr)
    raise SystemExit(1)
PY
