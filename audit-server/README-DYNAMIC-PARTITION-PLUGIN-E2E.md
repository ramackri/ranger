# Dynamic partition plugin E2E

End-to-end validation for **dynamic partition allocation**: onboard plugins via **`POST /api/audit/partition-plan/services`**, then prove audits land on **assigned Kafka partitions** and pass **auth_to_local + allowlist** checks.

Built on:

| Layer | Branch / path |
|-------|----------------|
| Plugin audit pipelines (Hive, Ozone, Kafka, HDFS, HBase, Knox, KMS) | `dev-support/ranger-docker/setup-audit-e2e.sh` + `scripts/{hive,kafka,...}/` — [README-AUDIT-E2E.md](../dev-support/ranger-docker/README-AUDIT-E2E.md) |
| Partition-plan REST + watcher | `ranger-kafka-dynamic-partition-plan` — `scripts/audit/partition-plan-e2e-lib.sh` |
| Access + allowlist curl | `scripts/audit/dynamic-auth-to-local-e2e-lib.sh` |

## What this tests

For each plugin that has a **running** Docker container:

1. **Onboard** — `POST /partition-plan/services` with:
   - `repo` (Policy Manager service name, e.g. `dev_kms`)
   - `pluginId` (Kafka record key / agent id, e.g. `kms`, `hiveServer2`)
   - `partitionCount` (dedicated partitions carved from buffer)
   - `allowedUsers` (Kerberos short names after `auth_to_local`, from XML catalog rules)
   - `expectedVersion` (optimistic concurrency)
2. **Auth** — `POST /api/audit/access` with plugin SPNEGO → **200/202**
3. **Routing** — consume `ranger_audits`, verify record **partition ∈ plan.plugins[pluginId].partitions**
4. **auth_to_local** — recomposed after onboard (same as access E2E)

`HTTP.keytab` on the ingestor is **unchanged** when onboarding plugins.

## Plugin matrix (default specs)

| Repo | pluginId | allowedUsers (short names) | Container |
|------|----------|----------------------------|-----------|
| `dev_hdfs` | `hdfs` | `hdfs` | `ranger-hadoop` |
| `dev_hive` | `hiveServer2` | `hive` | `ranger-hive` |
| `dev_hbase` | `hbaseMaster` | `hbase` | `ranger-hbase` |
| `dev_kafka` | `kafka` | `kafka` | `ranger-kafka` |
| `dev_knox` | `knox` | `knox` | `ranger-knox` |
| `dev_kms` | `kms` | `rangerkms` | `ranger-kms` |
| `dev_ozone` | `ozone` | `om`, `ozone` | `ozone-om` |

Specs live in `dynamic-partition-plugin-e2e-lib.sh` (`DPP_PLUGIN_ONBOARD_SPECS`). Align with `ranger-audit-ingestor-site.xml` `auth.to.local` catalog and harness allowlists.

## Quick start (Tier 3 Docker)

```bash
cd dev-support/ranger-docker

# Stack: ingestor + Kafka + plugins (same compose as partition-plan E2E)
docker compose -f docker-compose.ranger.yml \
  -f docker-compose.ranger-kafka.yml \
  -f docker-compose.ranger-audit-server.yml \
  -f docker-compose.ranger-hadoop.yml \
  -f docker-compose.ranger-hive.yml \
  -f docker-compose.ranger-kms.yml up -d

chmod +x scripts/audit/verify-dynamic-partition-plugin-e2e.sh
./scripts/audit/verify-dynamic-partition-plugin-e2e.sh
```

Subset of plugins:

```bash
./scripts/audit/verify-dynamic-partition-plugin-e2e.sh --plugins kms,hiveServer2
```

With plugin trigger scripts from the audit E2E harness:

```bash
./scripts/audit/verify-dynamic-partition-plugin-e2e.sh --with-harness-triggers
# or after full plugin → Solr pipelines:
./scripts/audit/verify-audit-e2e-full.sh --with-dynamic-partition --with-auth-access
```

Full suite (core partition-plan + auth + plugin onboard):

```bash
./scripts/audit/verify-partition-plan-e2e-all.sh --with-auth-access --with-plugin-onboard
```

## Greenfield layout (empty `configured.plugins`)

Sample site XML leaves `kafka.configured.plugins` **empty**:

- First bootstrap → **buffer-only** plan sized by `kafka.topic.partitions` (default **10**)
- Each `POST /services` promotes a plugin from buffer with `partitionCount` (default **2** in E2E specs)
- `auth_to_local` rules compose from union of `services[].allowedUsers` (XML catalog + generated rules)

## Onboard service example (manual)

```bash
# Inside ingestor container or any host with HTTP.keytab + FQDN
kinit -kt /etc/keytabs/HTTP.keytab HTTP/ranger-audit-ingestor.rangernw@EXAMPLE.COM

curl -s --negotiate -u : -X POST \
  -H 'Content-Type: application/json' \
  'http://ranger-audit-ingestor.rangernw:7081/api/audit/partition-plan/services' \
  -d '{
    "serviceName": "dev_trino",
    "pluginId": "trino",
    "partitionCount": 2,
    "allowedUsers": ["trino"],
    "expectedVersion": 1
  }'
```

Then POST audits from the Trino container using `serviceName=dev_trino&appId=trino`.

## Related docs

- [README-AUDIT-INGESTOR-ACCESS-CURL-E2E.md](README-AUDIT-INGESTOR-ACCESS-CURL-E2E.md) — SPNEGO, three identifiers, curl cookbook
- [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md) — operator cutover and REST API
- `dev-support/ranger-docker/README-AUDIT-E2E.md` — full plugin → Solr pipelines
