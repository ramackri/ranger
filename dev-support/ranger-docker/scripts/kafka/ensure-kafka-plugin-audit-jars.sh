#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# RANGER-5642: Kafka broker ships Jersey + Jackson on the application classpath.
# Duplicate JARs in ranger-kafka-plugin-impl cause audit ingestor POST failures
# (WadlAutoDiscoverable ClassCastException, Jackson LinkageError / MessageBodyWriter).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kafka/ensure-kafka-plugin-audit-jars.sh
#   ./scripts/kafka/ensure-kafka-plugin-audit-jars.sh --check-only
#
# Also runs inside ranger-kafka during ranger-kafka-setup.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${RANGER_KAFKA_CONTAINER:-ranger-kafka}"
CHECK_ONLY=false
IN_CONTAINER=false
[[ -d /opt/kafka/libs ]] && IN_CONTAINER=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    -h|--help)
      sed -n '11,16p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "${IN_CONTAINER}" != "true" ]]; then
  if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
    echo "ERROR: container ${CONTAINER} is not running" >&2
    exit 1
  fi
  args=()
  [[ "${CHECK_ONLY}" == "true" ]] && args+=(--check-only)
  docker exec "${CONTAINER}" bash -c \
    "${RANGER_SCRIPTS:-/home/ranger/scripts}/ensure-kafka-plugin-audit-jars.sh ${args[*]:-}"
  exit $?
fi

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
PLUGIN_HOME="${RANGER_KAFKA_PLUGIN_HOME:-/opt/ranger/ranger-kafka-plugin}"
IMPL="${PLUGIN_HOME}/lib/ranger-kafka-plugin-impl"
BROKER_LIB="${KAFKA_HOME}/libs"

if [[ ! -d "${IMPL}" ]]; then
  echo "ERROR: missing plugin impl dir ${IMPL}" >&2
  exit 1
fi

# Prefixes the broker already provides — must not also live in plugin-impl.
JERSEY_DUP_PREFIXES=(
  jersey-client-
  jersey-common-
  jersey-server-
  jersey-hk2-
  hk2-api-
  hk2-locator-
  hk2-utils-
  hk2-
  javax.inject-
  jakarta.ws.rs-api-
)

# Jackson JAX-RS must match broker version when Jersey client loads from broker classpath.
JACKSON_DUP_GLOBS=(
  jackson-annotations-*.jar
  jackson-core-*.jar
  jackson-databind-*.jar
  jackson-jaxrs-base-*.jar
  jackson-jaxrs-json-provider-*.jar
  jackson-module-jaxb-annotations-*.jar
)

JACKSON_COPY_PATTERNS=(
  jackson-annotations-*.jar
  jackson-core-*.jar
  jackson-databind-*.jar
  jackson-jaxrs-base-*.jar
  jackson-jaxrs-json-provider-*.jar
  jackson-module-jaxb-annotations-*.jar
)

broker_has() {
  local prefix="$1"
  compgen -G "${BROKER_LIB}/${prefix}"*.jar >/dev/null 2>&1
}

broker_has_glob() {
  local pattern="$1"
  compgen -G "${BROKER_LIB}/${pattern}" >/dev/null 2>&1
}

list_jersey_dupes() {
  local prefix f
  shopt -s nullglob
  for prefix in "${JERSEY_DUP_PREFIXES[@]}"; do
    broker_has "${prefix}" || continue
    for f in "${IMPL}/${prefix}"*.jar; do
      [[ -f "${f}" ]] || continue
      echo "${f}"
    done
  done
}

list_jackson_dupes() {
  local pattern f bn
  shopt -s nullglob
  for pattern in "${JACKSON_DUP_GLOBS[@]}"; do
    broker_has_glob "${pattern}" || continue
    for f in "${IMPL}"/${pattern}; do
      [[ -f "${f}" ]] || continue
      bn="$(basename "${f}")"
      if compgen -G "${BROKER_LIB}/${bn}" >/dev/null 2>&1; then
        continue
      fi
      echo "${f}"
    done
  done
}

missing_broker_jackson() {
  local pattern src bn
  shopt -s nullglob
  for pattern in "${JACKSON_COPY_PATTERNS[@]}"; do
    broker_has_glob "${pattern}" || continue
    for src in "${BROKER_LIB}"/${pattern}; do
      [[ -f "${src}" ]] || continue
      bn="$(basename "${src}")"
      [[ -f "${IMPL}/${bn}" ]] || echo "${bn}"
    done
  done
}

jersey_dupes="$(list_jersey_dupes || true)"
jackson_dupes="$(list_jackson_dupes || true)"
missing_jackson="$(missing_broker_jackson || true)"

if [[ -z "${jersey_dupes}" && -z "${jackson_dupes}" && -z "${missing_jackson}" ]]; then
  echo "OK: plugin-impl aligned with broker Jersey/Jackson (${BROKER_LIB})"
  exit 0
fi

if [[ "${CHECK_ONLY}" == "true" ]]; then
  [[ -z "${jersey_dupes}" ]] || {
    echo "FAIL: duplicate Jersey/HK2 JARs in plugin-impl (RANGER-5642):" >&2
    echo "${jersey_dupes}" | sed 's/^/  /' >&2
  }
  [[ -z "${jackson_dupes}" ]] || {
    echo "FAIL: Jackson version skew in plugin-impl (align with broker):" >&2
    echo "${jackson_dupes}" | sed 's/^/  /' >&2
  }
  [[ -z "${missing_jackson}" ]] || {
    echo "FAIL: missing broker-aligned Jackson JARs in plugin-impl:" >&2
    echo "${missing_jackson}" | sed 's/^/  /' >&2
  }
  exit 1
fi

removed=0
while IFS= read -r f; do
  [[ -n "${f}" ]] || continue
  rm -f "${f}"
  echo "Removed duplicate $(basename "${f}") from plugin-impl"
  removed=$((removed + 1))
done <<< "$(printf '%s\n%s\n' "${jersey_dupes}" "${jackson_dupes}" | sed '/^$/d')"

copied=0
shopt -s nullglob
for pattern in "${JACKSON_COPY_PATTERNS[@]}"; do
  for src in "${BROKER_LIB}"/${pattern}; do
    [[ -f "${src}" ]] || continue
    bn="$(basename "${src}")"
    rm -f "${IMPL}/${bn}"
    cp -f "${src}" "${IMPL}/${bn}"
    echo "Copied broker-aligned ${bn} into plugin-impl"
    copied=$((copied + 1))
  done
done

echo "Stripped ${removed} duplicate JAR(s); copied ${copied} broker Jackson JAR(s) (RANGER-5642)"
