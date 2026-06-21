#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Clear dev_kms audit exclude lists so KMS access events are emitted (E2E).
# Idempotent — no-op when exclude configs are absent.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kms/clear-dev-kms-audit-exclude.sh
#   ./scripts/kms/clear-dev-kms-audit-exclude.sh --check-only

set -euo pipefail

ADMIN_URL="${RANGER_ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${RANGER_ADMIN_USER:-admin}"
ADMIN_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
SERVICE="dev_kms"
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    -h|--help)
      sed -n '11,14p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

python3 <<PY
import base64
import json
import sys
import urllib.error
import urllib.request

admin_url = "${ADMIN_URL}"
service = "${SERVICE}"
check_only = "${CHECK_ONLY}" == "true"
auth = base64.b64encode(b"${ADMIN_USER}:${ADMIN_PASS}").decode()
exclude_keys = (
    "ranger.plugin.audit.exclude.users",
    "ranger.plugin.audit.exclude.groups",
    "ranger.plugin.audit.exclude.roles",
)

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
    svc = api("GET", f"/service/public/v2/api/service/name/{service}")
except urllib.error.HTTPError as e:
    print(f"FAIL: could not load service {service}: HTTP {e.code}", file=sys.stderr)
    sys.exit(1)

configs = svc.get("configs") or {}
present = [k for k in exclude_keys if configs.get(k)]
if not present:
    print(f"{service}: no audit exclude configs (OK for E2E)")
    sys.exit(0)

if check_only:
    print(f"FAIL: {service} still has audit exclude: {', '.join(present)}", file=sys.stderr)
    sys.exit(1)

for key in exclude_keys:
    configs.pop(key, None)
configs["ranger.plugin.kms.policy.refresh.synchronous"] = "true"
svc["configs"] = configs
api("PUT", f"/service/public/v2/api/service/name/{service}", svc)
print(f"{service}: cleared audit exclude configs ({', '.join(present)})")
PY
