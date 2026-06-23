#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Minimal Ranger Hive plugin enable for ranger-docker (dev). Upstream
# enable-hive-plugin.sh exits when SSL credential creation fails.

set -euo pipefail

PLUGIN_HOME="${1:-/opt/ranger/ranger-hive-plugin}"
HIVE_CONF="${HIVE_HOME:-/opt/hive}/conf"
HIVE_LIB="${HIVE_HOME:-/opt/hive}/lib"
ENABLE_SCRIPT="${PLUGIN_HOME}/enable-hive-plugin.sh"

if [[ ! -x "${ENABLE_SCRIPT}" ]]; then
  echo "ERROR: missing ${ENABLE_SCRIPT}" >&2
  exit 1
fi

ENABLE_JAVA_HOME="${RANGER_ENABLE_JAVA_HOME:-}"
if [[ -z "${ENABLE_JAVA_HOME}" ]] && [[ -x /opt/java17/openjdk/bin/java ]]; then
  ENABLE_JAVA_HOME=/opt/java17/openjdk
fi
if [[ -z "${ENABLE_JAVA_HOME}" ]] && docker inspect ranger >/dev/null 2>&1; then
  docker exec ranger tar -C /opt/java -cf - openjdk 2>/dev/null | tar -C /opt/java17 -xf - 2>/dev/null || true
  [[ -x /opt/java17/openjdk/bin/java ]] && ENABLE_JAVA_HOME=/opt/java17/openjdk
fi
ENABLE_JAVA_HOME="${ENABLE_JAVA_HOME:-/opt/java/openjdk}"

# Upstream enable may exit early on SSL — continue with jars + XmlConfigChanger.
(
  export JAVA_HOME="${ENABLE_JAVA_HOME}"
  export PATH="${JAVA_HOME}/bin:${PATH}"
  set +e
  "${ENABLE_SCRIPT}"
  set -e
) || true

mkdir -p "${HIVE_LIB}"
# Match HDFS docker enable: shim + classloader jars only; impl stays in a subdirectory.
for f in "${PLUGIN_HOME}"/lib/*.jar; do
  bn="$(basename "${f}")"
  ln -sf "${f}" "${HIVE_LIB}/${bn}"
done
if [[ -d "${PLUGIN_HOME}/lib/ranger-hive-plugin-impl" ]]; then
  ln -sfn "${PLUGIN_HOME}/lib/ranger-hive-plugin-impl" "${HIVE_LIB}/ranger-hive-plugin-impl"
fi

# Plugin impl must not ship hive-* APIs: HS2 loads hive-exec from ${HIVE_LIB}.
# Duplicate HiveAuthorizerFactory across plugin CL and app CL causes ClassCastException on HS2 start.
hive_ver="${HIVE_VERSION:-3.1.3}"
if [[ -d "${HIVE_LIB}/ranger-hive-plugin-impl" ]]; then
  removed=0
  for stale in "${HIVE_LIB}/ranger-hive-plugin-impl"/hive-*-*.jar; do
    [[ -f "${stale}" ]] || continue
    rm -f "${stale}"
    removed=$((removed + 1))
  done
  if [[ "${removed}" -gt 0 ]]; then
    echo "Removed ${removed} Hive jar(s) from ranger-hive-plugin-impl (use runtime Hive ${hive_ver})"
  fi
fi

# RANGER-5646: drop plugin-shipped copies that duplicate HiveServer2 lib (version skew → HTTP 401).
IMPL_DIR="${PLUGIN_HOME}/lib/ranger-hive-plugin-impl"
if [[ -d "${IMPL_DIR}" ]]; then
  dup_removed=0
  for stale in \
    "${IMPL_DIR}"/commons-collections-3.2.2.jar \
    "${IMPL_DIR}"/hppc-*.jar \
    "${IMPL_DIR}"/httpclient-*.jar \
    "${IMPL_DIR}"/httpcore-*.jar \
    "${IMPL_DIR}"/httpcore-nio-*.jar \
    "${IMPL_DIR}"/jackson-annotations-2.17*.jar \
    "${IMPL_DIR}"/jackson-core-2.17*.jar \
    "${IMPL_DIR}"/jackson-databind-2.17*.jar \
    "${IMPL_DIR}"/jackson-jaxrs-base-2.17*.jar \
    "${IMPL_DIR}"/jackson-jaxrs-json-provider-2.17*.jar \
    "${IMPL_DIR}"/jackson-module-jaxb-annotations-2.17*.jar \
    "${IMPL_DIR}"/javax.annotation-api-*.jar \
    "${IMPL_DIR}"/joda-time-2.10*.jar \
    "${IMPL_DIR}"/hadoop-common-*.jar; do
    [[ -f "${stale}" ]] || continue
    rm -f "${stale}"
    dup_removed=$((dup_removed + 1))
  done
  if [[ "${dup_removed}" -gt 0 ]]; then
    echo "Removed ${dup_removed} duplicate jar(s) from ranger-hive-plugin-impl (RANGER-5646)"
  fi
  # Jersey audit client needs Jackson JAX-RS + HTTP client on the plugin classloader.
  # Copy Hive/Hadoop versions (2.16.x / 4.5.x) — do not ship Ranger 2.17 duplicates (RANGER-5646 HTTP 401).
  jackson_copied=0
  for pattern in jackson-core-*.jar jackson-databind-*.jar jackson-annotations-*.jar \
                 jackson-jaxrs-base-*.jar jackson-jaxrs-json-provider-*.jar \
                 jackson-module-jaxb-annotations-*.jar; do
    for src in "${HIVE_LIB}"/${pattern}; do
      [[ -f "${src}" ]] || continue
      bn="$(basename "${src}")"
      rm -f "${IMPL_DIR}/${bn}"
      cp -f "${src}" "${IMPL_DIR}/${bn}"
      jackson_copied=$((jackson_copied + 1))
    done
  done
  for pattern in httpclient-*.jar httpcore-*.jar; do
    for src in "${HIVE_LIB}"/${pattern} "${HADOOP_HOME:-/opt/hadoop}/share/hadoop/common/lib"/${pattern}; do
      [[ -f "${src}" ]] || continue
      bn="$(basename "${src}")"
      [[ -f "${IMPL_DIR}/${bn}" ]] && continue
      cp -f "${src}" "${IMPL_DIR}/${bn}"
      jackson_copied=$((jackson_copied + 1))
    done
  done
  if [[ "${jackson_copied}" -gt 0 ]]; then
    echo "Copied ${jackson_copied} Jackson/HTTP jar(s) from Hive/Hadoop lib for audit-server REST client"
  fi
  if [[ ! -f "${IMPL_DIR}/jackson-module-jaxb-annotations-"*.jar ]]; then
    for src in "${PLUGIN_HOME}"/lib/ranger-hive-plugin-impl/jackson-module-jaxb-annotations-*.jar; do
      [[ -f "${src}" ]] || continue
      cp -f "${src}" "${IMPL_DIR}/"
      echo "Copied $(basename "${src}") from plugin tarball for Jersey JAXB support"
      break
    done
  fi
fi

RANGER_COLL="${PLUGIN_HOME}/lib/ranger-hive-plugin-impl/commons-collections-3.2.2.jar"
if [[ -f "${RANGER_COLL}" ]] && [[ ! -f "${HIVE_LIB}/commons-collections-3.2.2.jar" ]]; then
  cp "${RANGER_COLL}" "${HIVE_LIB}/"
fi

ENABLE_TEMPLATE_DIR="${PLUGIN_HOME}/install/conf.templates/enable"
INSTALL_PROPS="${PLUGIN_HOME}/install.properties"
INSTALL_CP="${PLUGIN_HOME}/install/lib/*"
JAVA_BIN="${ENABLE_JAVA_HOME}/bin/java"
dt="$(date '+%Y%m%d%H%M%S')"

if [[ -d "${ENABLE_TEMPLATE_DIR}" ]]; then
  cp -f "${ENABLE_TEMPLATE_DIR}"/*.xml "${HIVE_CONF}/" 2>/dev/null || true
  chmod a+r "${HIVE_CONF}"/ranger-*.xml 2>/dev/null || true

  echo "<ranger><enabled>$(date)</enabled></ranger>" >"${HIVE_CONF}/ranger-security.xml"
  chmod a+r "${HIVE_CONF}/ranger-security.xml"

  repo_name="$(grep -E '^REPOSITORY_NAME=' "${INSTALL_PROPS}" | cut -d= -f2- | head -1)"
  if [[ -n "${repo_name}" ]]; then
    export POLICY_CACHE_FILE_PATH="/etc/ranger/${repo_name}/policycache"
    export CREDENTIAL_PROVIDER_FILE="/etc/ranger/${repo_name}/cred.jceks"
    mkdir -p "${POLICY_CACHE_FILE_PATH}"
  fi

  for cfg in "${ENABLE_TEMPLATE_DIR}"/*-changes.cfg; do
    [[ -f "${cfg}" ]] || continue
    orgfn="$(basename "${cfg}" | sed 's:-changes.cfg:.xml:')"
    fullpath="${HIVE_CONF}/${orgfn}"
    if [[ ! -f "${fullpath}" ]] && [[ -f "${ENABLE_TEMPLATE_DIR}/${orgfn}" ]]; then
      cp "${ENABLE_TEMPLATE_DIR}/${orgfn}" "${fullpath}"
    fi
    if [[ ! -f "${fullpath}" ]]; then
      echo "WARN: missing ${fullpath}; skipping ${cfg}" >&2
      continue
    fi
    archive="${HIVE_CONF}/.${orgfn}.${dt}"
    newfn="${HIVE_CONF}/.${orgfn}-new.${dt}"
    cp "${fullpath}" "${archive}"
    if ! "${JAVA_BIN}" -cp "${INSTALL_CP}" org.apache.ranger.utils.install.XmlConfigChanger \
      -i "${archive}" -o "${newfn}" -c "${cfg}" -p "${INSTALL_PROPS}"; then
      echo "ERROR: XmlConfigChanger failed for ${cfg}" >&2
      exit 1
    fi
    if ! diff -w "${newfn}" "${fullpath}" >/dev/null 2>&1; then
      cp "${newfn}" "${fullpath}"
    fi
    chmod a+r "${fullpath}"
  done

  spool_dir="$(grep -E '^XAAUDIT\.AUDITSERVER\.FILE_SPOOL_DIR=' "${INSTALL_PROPS}" 2>/dev/null \
    | cut -d= -f2- | head -1)"
  if [[ -n "${spool_dir}" ]]; then
    mkdir -p "${spool_dir}"
    chown hive:hadoop "${spool_dir}" 2>/dev/null || true
  fi
fi

echo "Ranger Hive plugin enabled (docker) under ${PLUGIN_HOME} -> ${HIVE_LIB}"
