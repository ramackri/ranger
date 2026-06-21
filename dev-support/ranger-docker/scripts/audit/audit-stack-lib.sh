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

# Shared helpers for Tier 3 audit Docker E2E scripts.

AUDIT_INGESTOR_CONTAINER="${AUDIT_INGESTOR_CONTAINER:-ranger-audit-ingestor}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ranger-kafka}"
RANGER_SOLR_CONTAINER="${RANGER_SOLR_CONTAINER:-ranger-solr}"

audit_stack_container_running() {
  local name="$1"
  docker ps --filter "name=^${name}$" --filter status=running --format '{{.Names}}' | grep -qx "${name}"
}

audit_stack_wait_url() {
  local url="$1"
  local label="${2:-Service}"
  local timeout="${3:-120}"
  local deadline=$((SECONDS + timeout))

  while (( SECONDS < deadline )); do
    if curl -sf "${url}" >/dev/null 2>&1; then
      echo "  ${label} ready: ${url}"
      return 0
    fi
    sleep 2
  done

  echo "ERROR: ${label} not ready within ${timeout}s: ${url}" >&2
  return 1
}

audit_stack_require_container() {
  local name="$1"
  if ! audit_stack_container_running "${name}"; then
    echo "ERROR: container ${name} is not running" >&2
    return 1
  fi
  return 0
}
