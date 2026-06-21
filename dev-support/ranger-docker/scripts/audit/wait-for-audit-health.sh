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

# Wait for audit ingestor (and optional Tier 3 dependencies) to become healthy.
#
# Usage:
#   ./scripts/audit/wait-for-audit-health.sh
#   ./scripts/audit/wait-for-audit-health.sh --tier 3 --timeout 180

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=audit-stack-lib.sh
source "${SCRIPT_DIR}/scripts/audit/audit-stack-lib.sh"

TIER=3
TIMEOUT=120
INGESTOR_PORT=7081

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier) TIER="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    --port) INGESTOR_PORT="${2:?}"; shift 2 ;;
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

audit_stack_wait_url "http://localhost:${INGESTOR_PORT}/api/audit/health" "Audit ingestor" "${TIMEOUT}"
audit_stack_require_container "${AUDIT_INGESTOR_CONTAINER}"

if [[ "${TIER}" -ge 3 ]]; then
  audit_stack_require_container "${KAFKA_CONTAINER}"
fi

echo "Audit stack health check passed (tier=${TIER})."
