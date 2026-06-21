#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# KMS embeds the Ranger plugin in ranger-kms-plugin-impl behind RangerPluginClassLoader.
# The KMS WAR ships Jersey + Jackson for REST; the isolated plugin classloader needs its
# own aligned copy in plugin-impl (HBase RANGER-5644 / #1015 pattern — not Kafka #1020).
# Copy from WEB-INF/lib; include jersey-server + hk2 so audit batch POST does not hit
# WadlAutoDiscoverable ClassCastException. Do NOT flatten plugin-impl into WEB-INF/lib.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kms/ensure-kms-plugin-audit-jars.sh
#   ./scripts/kms/ensure-kms-plugin-audit-jars.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${RANGER_KMS_CONTAINER:-ranger-kms}"
CHECK_ONLY=false
IN_CONTAINER=false
[[ -d /opt/ranger/kms/ews/webapp/WEB-INF/lib ]] && IN_CONTAINER=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=true; shift ;;
    -h|--help)
      sed -n '16,19p' "$0"
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
    "${RANGER_SCRIPTS:-/home/ranger/scripts}/ensure-kms-plugin-audit-jars.sh ${args[*]:-}"
  exit $?
fi

WEBINF_LIB="${RANGER_HOME:-/opt/ranger}/kms/ews/webapp/WEB-INF/lib"
IMPL="${WEBINF_LIB}/ranger-kms-plugin-impl"

if [[ ! -d "${IMPL}" ]]; then
  echo "ERROR: missing ${IMPL}" >&2
  exit 1
fi

# jersey-server must be in plugin-impl (not just WEB-INF/lib) so Jersey ServiceLoader
# does not mix WadlAutoDiscoverable from the webapp classloader with plugin client SPI.
FORBIDDEN_GLOBS=(
  jersey-container-*.jar
)

COPY_GLOBS=(
  jackson-annotations-*.jar
  jackson-core-*.jar
  jackson-databind-*.jar
  jackson-jaxrs-base-*.jar
  jackson-jaxrs-json-provider-*.jar
  jackson-module-jaxb-annotations-*.jar
  jersey-client-*.jar
  jersey-common-*.jar
  jersey-entity-filtering-*.jar
  jersey-media-json-jackson-*.jar
  jersey-server-*.jar
  # hk2-* belong in tarball assembly only; copying from WEB-INF/lib causes LinkageError
  jakarta.ws.rs-api-*.jar
  javax.inject-*.jar
)

shopt -s nullglob
for pattern in "${FORBIDDEN_GLOBS[@]}"; do
  for f in "${IMPL}"/${pattern}; do
  [[ -f "${f}" ]] || continue
  if [[ "${CHECK_ONLY}" == "true" ]]; then
    echo "FAIL: forbidden in plugin-impl: $(basename "${f}")" >&2
    exit 1
  fi
  rm -f "${f}"
  echo "Removed $(basename "${f}") from plugin-impl"
  done
done

missing=()
for pattern in "${COPY_GLOBS[@]}"; do
  found=false
  for _ in "${IMPL}"/${pattern}; do
    found=true
    break
  done
  if [[ "${found}" != "true" ]]; then
    for src in "${WEBINF_LIB}"/${pattern}; do
      [[ -f "${src}" ]] || continue
      missing+=("$(basename "${src}")")
      break
    done
  fi
done
shopt -u nullglob

if [[ ${#missing[@]} -eq 0 ]]; then
  echo "OK: KMS plugin-impl has HBase-style Jackson/Jersey client for audit"
  exit 0
fi

if [[ "${CHECK_ONLY}" == "true" ]]; then
  echo "FAIL: missing from plugin-impl:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

copied=0
for bn in "${missing[@]}"; do
  src="${WEBINF_LIB}/${bn}"
  [[ -f "${src}" ]] || continue
  cp -f "${src}" "${IMPL}/${bn}"
  echo "Copied ${bn} into plugin-impl"
  copied=$((copied + 1))
done

echo "KMS plugin-impl: copied ${copied} WAR-aligned JAR(s) (client stack only)"
