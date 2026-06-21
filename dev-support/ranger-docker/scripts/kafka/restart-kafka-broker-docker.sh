#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Safe broker restart for audit E2E (avoids ZK NodeExists on docker restart).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kafka/restart-kafka-broker-docker.sh

set -euo pipefail

CONTAINER="${RANGER_KAFKA_CONTAINER:-ranger-kafka}"
ZK_CONTAINER="${RANGER_ZK_CONTAINER:-ranger-zk}"

zk_cli_path() {
  docker exec "${ZK_CONTAINER}" bash -c '
    for p in /apache-zookeeper-*/bin/zkCli.sh /opt/zookeeper/bin/zkCli.sh; do
      [[ -x "${p}" ]] && { echo "${p}"; exit 0; }
    done
    command -v zkCli.sh 2>/dev/null || true
  ' 2>/dev/null | head -1
}

delete_broker_zk_node() {
  local zk_cli
  zk_cli="$(zk_cli_path)"
  if [[ -z "${zk_cli}" ]]; then
    echo "WARN: zkCli.sh not found in ${ZK_CONTAINER}" >&2
    return 1
  fi
  docker exec "${ZK_CONTAINER}" bash -c \
    "\"${zk_cli}\" -server localhost:2181 delete /brokers/ids/0 2>/dev/null || true" \
    >/dev/null 2>&1 || true
}

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  docker start "${CONTAINER}" >/dev/null 2>&1 || true
fi

docker stop "${CONTAINER}" >/dev/null 2>&1 || true
for _ in $(seq 1 12); do
  docker ps --filter "name=^${CONTAINER}$" --filter status=running -q | grep -q . || break
  sleep 2
done

if docker ps --format '{{.Names}}' | grep -qx "${ZK_CONTAINER}"; then
  delete_broker_zk_node
  sleep 3
fi

docker start "${CONTAINER}" >/dev/null
echo "Started ${CONTAINER}; waiting for broker on 9092..."
for _ in $(seq 1 36); do
  if docker exec "${CONTAINER}" timeout 2 bash -c 'echo >/dev/tcp/localhost/9092' 2>/dev/null; then
    echo "OK: Kafka broker ready"
    exit 0
  fi
  sleep 5
done

echo "FAIL: broker did not become ready on 9092" >&2
exit 1
