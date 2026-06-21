#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Generate a DENIED Kafka authorization event as testuser1 (dev_kafka audits).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kafka/trigger-kafka-plugin-audit-e2e.sh

set -euo pipefail

CONTAINER="${RANGER_KAFKA_CONTAINER:-ranger-kafka}"
TOPIC="${KAFKA_E2E_AUDIT_TOPIC:-e2e-kafka-audit-deny}"
TEST_USER="${KAFKA_E2E_TEST_USER:-testuser1}"

docker exec "${CONTAINER}" bash -c "
set -e
cat > /tmp/kafka-client-jaas.conf <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab=\"/etc/keytabs/${TEST_USER}.keytab\"
  principal=\"${TEST_USER}/ranger-kafka.rangernw@EXAMPLE.COM\";
};
EOF
cat > /tmp/kafka-client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
EOF
export KAFKA_OPTS=\"-Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=/tmp/kafka-client-jaas.conf\"
set +e
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server ranger-kafka.rangernw:9092 \
  --command-config /tmp/kafka-client.properties \
  --topic ${TOPIC} \
  --partitions 1 \
  --replication-factor 1 2>&1
rc=\$?
set -e
if grep -qi 'Authorization failed' /dev/stderr 2>/dev/null; then
  :
fi
exit 0
" 2>&1 | tee /tmp/kafka-trigger.log || true

if grep -qi 'Authorization failed' /tmp/kafka-trigger.log; then
  echo "OK: triggered DENIED authorization as ${TEST_USER} on topic ${TOPIC}"
else
  echo "WARN: expected Authorization failed from topic create; got:" >&2
  tail -3 /tmp/kafka-trigger.log >&2
fi
