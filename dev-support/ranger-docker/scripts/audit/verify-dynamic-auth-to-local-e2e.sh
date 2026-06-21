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

# E2E: dynamic service allowlist + auth_to_local + POST /api/audit/access per plugin.
#
# For each configured plugin repo:
#   1. POST /access with plugin Kerberos principal (SPNEGO) → 200 + authenticatedUser
#   2. Toggle allowlist via PUT partition-plan → 403 then 200
#   3. Cross-repo denial (hdfs principal → dev_kms) → 403
#
# Writes a curl cookbook: dist/audit-e2e/access-ingestor-curl-cookbook.sh
#
# Prerequisites: Tier 3 Docker (ingestor + Kafka + plugin containers with keytabs).
#
# Usage:
#   cd dev-support/ranger-docker
#   ./scripts/audit/verify-dynamic-auth-to-local-e2e.sh
#   ./scripts/audit/verify-dynamic-auth-to-local-e2e.sh --no-enable --plugins hdfs,hive
#   ./scripts/audit/verify-dynamic-auth-to-local-e2e.sh --generate-curl-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"
# shellcheck source=dynamic-auth-to-local-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/dynamic-auth-to-local-e2e-lib.sh"

DO_ENABLE=true
GENERATE_ONLY=false
PLUGIN_FILTER=""
TIMEOUT=300
COOKBOOK="${SCRIPT_DIR}/dist/audit-e2e/access-ingestor-curl-cookbook.sh"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-enable) DO_ENABLE=false; shift ;;
    --generate-curl-only) GENERATE_ONLY=true; shift ;;
    --plugins) PLUGIN_FILTER="${2:?}"; shift 2 ;;
    --cookbook) COOKBOOK="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '19,33p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$(dirname "${COOKBOOK}")"
dael_write_curl_cookbook "${COOKBOOK}"
chmod +x "${COOKBOOK}" 2>/dev/null || true

if [[ "${GENERATE_ONLY}" == "true" ]]; then
  echo "Curl cookbook only (no Docker tests): ${COOKBOOK}"
  exit 0
fi

echo "=== Dynamic auth_to_local + allowlist E2E ==="
pp_preflight_tier3 "${TIMEOUT}"

PLAN_URL="$(pp_ingestor_plan_url "${CONTAINER}")"

if [[ "${DO_ENABLE}" == "true" ]]; then
  if ! pp_dynamic_enabled "${CONTAINER}" "${SITE_XMLS[0]}"; then
    echo "Enabling dynamic partition plan..."
    pp_set_dynamic_enabled "${CONTAINER}" "true" || { pp_record_fail "enable dynamic"; pp_print_results; exit 1; }
    pp_wait_health 7081 "Ingestor after enable" "${TIMEOUT}" || { pp_record_fail "health"; pp_print_results; exit 1; }
  fi
fi

if ! pp_dynamic_enabled "${CONTAINER}" "${SITE_XMLS[0]}"; then
  pp_record_fail "dynamic.enabled must be true (use default or drop --no-enable)"
  pp_print_results
  exit 1
fi

if ! pp_wait_watcher "${CONTAINER}" "${TIMEOUT}"; then
  pp_record_fail "PartitionPlanWatcher not ready"
  pp_print_results
  exit 1
fi
pp_record_pass "PartitionPlanWatcher ready"

pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
if [[ "${HTTP_CODE}" == "200" ]]; then
  pp_record_pass "GET partition-plan returns 200"
else
  pp_record_fail "GET partition-plan expected 200, got ${HTTP_CODE}"
  pp_print_results
  exit 1
fi

echo ""
echo "=== Per-plugin POST /access (auth_to_local + allowlist) ==="
profile=""
while IFS= read -r profile; do
  [[ -n "${profile}" ]] || continue
  dael_test_plugin_access "${profile}" || true
done < <(dael_filter_profiles "${PLUGIN_FILTER}")

echo ""
echo "=== Dynamic allowlist toggle (dev_kms / rangerkms) ==="
if dael_container_running "ranger-kms"; then
  dael_test_dynamic_user_toggle \
    "dev_kms" "rangerkms" "ranger-kms" \
    "/etc/keytabs/rangerkms.keytab" "rangerkms/ranger-kms.rangernw@EXAMPLE.COM" "kms" || true
else
  echo "  SKIP dev_kms toggle: ranger-kms not running"
fi

echo ""
echo "=== Cross-repo denial (hdfs principal → dev_kms) ==="
if dael_container_running "ranger-hadoop" && dael_container_running "ranger-kms"; then
  dael_test_cross_repo_denied \
    "ranger-hadoop" "/etc/keytabs/hdfs.keytab" "hdfs/ranger-hadoop.rangernw@EXAMPLE.COM" \
    "dev_kms" "kms" "hdfs principal rejected for dev_kms" || true
else
  echo "  SKIP cross-repo: ranger-hadoop or ranger-kms not running"
fi

echo ""
echo "=== Onboard new repo dynamically (buffer plugin) ==="
pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
version="$(pp_json_field "${HTTP_BODY}" version)"
new_repo="e2e_auth_$(date +%s)"
new_plugin="e2eAuth$(date +%s)"
if dael_onboard_repo "${new_repo}" "${new_plugin}" "hdfs" 2 "${version}"; then
  pp_record_pass "onboard-repo ${new_repo} plugin=${new_plugin}"
  if dael_wait_auth_to_local_applied "${CONTAINER}" 30; then
    pp_record_pass "auth_to_local recomposed after onboard-repo"
  else
    pp_record_fail "no auth_to_local log after onboard-repo"
  fi
  if dael_container_running "ranger-hadoop"; then
    dael_access_post_from_container \
      "ranger-hadoop" "/etc/keytabs/hdfs.keytab" "hdfs/ranger-hadoop.rangernw@EXAMPLE.COM" \
      "${new_repo}" "${new_plugin}"
    dael_expect_access "POST /access to onboarded ${new_repo}" "200" || true
  fi
else
  pp_record_fail "onboard-repo failed: HTTP ${HTTP_CODE} ${HTTP_BODY}"
fi

echo ""
echo "Curl cookbook: ${COOKBOOK}"
echo "See: audit-server/README-AUDIT-INGESTOR-ACCESS-CURL-E2E.md"

pp_print_results
