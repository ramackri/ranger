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

# Shared helpers for audit-stack.sh and setup-audit-e2e.sh.

audit_stack_script_dir() {
  cd "$(dirname "${BASH_SOURCE[1]}")/../.." && pwd
}

audit_stack_compose_files() {
  local tier="$1"
  local tier2_full="${2:-false}"
  local with_usersync="${3:-false}"
  local -a files=(
    -f docker-compose.ranger.yml
    -f docker-compose.ranger-kafka.yml
    -f docker-compose.ranger-audit-ingestor.yml
  )

  case "${tier}" in
    1) ;;
    2)
      files+=(-f docker-compose.ranger-audit-dispatcher-solr.yml)
      if [[ "${tier2_full}" == "true" ]]; then
        files+=(
          -f docker-compose.ranger-hadoop.yml
          -f docker-compose.ranger-audit-server.yml
        )
      fi
      ;;
    3)
      files+=(
        -f docker-compose.ranger-hadoop.yml
        -f docker-compose.ranger-audit-dispatcher-solr.yml
      )
      if [[ "${with_usersync}" == "true" ]]; then
        files+=(-f docker-compose.ranger-usersync.yml)
      fi
      ;;
    4)
      files+=(
        -f docker-compose.ranger-hadoop.yml
        -f docker-compose.ranger-audit-dispatcher-solr.yml
        -f docker-compose.ranger-ozone.yml
      )
      if [[ "${with_usersync}" == "true" ]]; then
        files+=(-f docker-compose.ranger-usersync.yml)
      fi
      ;;
    *)
      echo "ERROR: invalid tier ${tier}" >&2
      return 1
      ;;
  esac

  printf '%s\n' "${files[@]}"
}

audit_stack_wait_url() {
  local url="$1"
  local label="$2"
  local timeout="${3:-300}"
  local deadline=$((SECONDS + timeout))

  echo -n "  ${label}..."
  while (( SECONDS < deadline )); do
    if curl -sf -o /dev/null "${url}" 2>/dev/null; then
      echo " OK"
      return 0
    fi
    sleep 5
  done
  echo " TIMEOUT (${timeout}s)"
  return 1
}

audit_stack_wait_container() {
  local name="$1"
  local label="$2"
  local timeout="${3:-300}"
  local deadline=$((SECONDS + timeout))

  echo -n "  ${label}..."
  while (( SECONDS < deadline )); do
    if docker ps --filter "name=^${name}$" --filter status=running --format '{{.Names}}' | grep -qx "${name}"; then
      echo " OK"
      return 0
    fi
    sleep 5
  done
  echo " TIMEOUT (${timeout}s)"
  return 1
}

# Solr is Kerberos-protected in Docker; host curl gets 401. Use dispatcher container.
audit_stack_solr_curl() {
  local path="$1"
  if [[ "${KERBEROS_ENABLED:-true}" == "true" ]] \
    && docker inspect ranger-audit-dispatcher-solr >/dev/null 2>&1 \
    && docker ps --filter name=^ranger-audit-dispatcher-solr$ --filter status=running -q | grep -q .; then
    docker exec ranger-audit-dispatcher-solr bash -c "
      kinit -kt /etc/keytabs/rangerauditserver.keytab \
        rangerauditserver/ranger-audit-dispatcher-solr.rangernw@EXAMPLE.COM 2>/dev/null
      curl -sf --negotiate -u : 'http://ranger-solr.rangernw:8983${path}'
    " 2>/dev/null
  else
    curl -sf "http://localhost:8983${path}" 2>/dev/null
  fi
}

audit_stack_wait_solr() {
  local label="${1:-Solr collection}"
  local timeout="${2:-300}"
  local deadline=$((SECONDS + timeout))

  echo -n "  ${label}..."
  while (( SECONDS < deadline )); do
    local body
    body="$(audit_stack_solr_curl '/solr/ranger_audits/admin/ping?wt=json' || true)"
    if grep -q '"status"[[:space:]]*:[[:space:]]*0' <<< "${body}"; then
      echo " OK"
      return 0
    fi
    sleep 5
  done
  echo " TIMEOUT (${timeout}s)"
  return 1
}

audit_stack_solr_count() {
  local query="$1"
  audit_stack_solr_curl "/solr/ranger_audits/select?q=${query}&rows=0&wt=json" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('response',{}).get('numFound',0))" 2>/dev/null \
    || echo 0
}

AUDIT_INGESTOR_CONTAINER="${AUDIT_INGESTOR_CONTAINER:-ranger-audit-ingestor}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ranger-kafka}"
RANGER_SOLR_CONTAINER="${RANGER_SOLR_CONTAINER:-ranger-solr}"

audit_stack_container_running() {
  local name="$1"
  docker ps --filter "name=^${name}$" --filter status=running --format '{{.Names}}' | grep -qx "${name}"
}

audit_stack_require_container() {
  local name="$1"
  if ! audit_stack_container_running "${name}"; then
    echo "ERROR: container ${name} is not running" >&2
    return 1
  fi
  return 0
}
