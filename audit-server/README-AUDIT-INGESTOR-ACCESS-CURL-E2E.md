# Audit ingestor access E2E — curl and dynamic allowlist guide

End-to-end validation for **dynamic `auth_to_local` rule composition** and **per-plugin service allowlists** on the Ranger audit ingestor.

## What this tests

1. **Kerberos SPNEGO** on `POST /api/audit/access` from each plugin container
2. **`auth_to_local`** maps the plugin principal → short name (`hdfs`, `hive`, `rangerkms`, …)
3. **`services[repo].allowedUsers`** in the partition-plan registry authorizes the short name
4. **Dynamic updates** via delta `PATCH /partition-plan` (services allowlist) or `POST /partition-plan/services` refresh composed rules without restart

## Quick start (Docker Tier 3)

```bash
cd dev-support/ranger-docker

# Bring up audit stack (ingestor + Kafka + plugins you need)
docker compose -f docker-compose.ranger.yml \
  -f docker-compose.ranger-kafka.yml \
  -f docker-compose.ranger-audit-server.yml \
  -f docker-compose.ranger-hadoop.yml \
  -f docker-compose.ranger-hive.yml \
  -f docker-compose.ranger-kms.yml up -d

# Run E2E (enables dynamic mode if needed)
chmod +x scripts/audit/verify-dynamic-auth-to-local-e2e.sh
./scripts/audit/verify-dynamic-auth-to-local-e2e.sh
```

Generate curl cookbook only (no Docker required):

```bash
./scripts/audit/verify-dynamic-auth-to-local-e2e.sh --generate-curl-only
# → dist/audit-e2e/access-ingestor-curl-cookbook.sh
```

## Base URLs

| Endpoint | URL | Auth |
|----------|-----|------|
| Health | `http://localhost:7081/api/audit/health` | None |
| Partition plan | `http://ranger-audit-ingestor.rangernw:7081/api/audit/partition-plan` | SPNEGO (HTTP service keytab) |
| Onboard service | `…/api/audit/partition-plan/services` | SPNEGO |
| **Post audits** | `…/api/audit/access?serviceName=<repo>&appId=<pluginId>` | SPNEGO (plugin keytab) |

**Important:** SPNEGO curl must use the **FQDN** host (`ranger-audit-ingestor.rangernw`), not `localhost`, when run inside Docker.

## Three identifiers (do not conflate)

| Field | Example | Role |
|-------|---------|------|
| **Repo / `serviceName`** | `dev_hdfs`, `dev_kms` | Query param on POST; key in `services[]` |
| **Kerberos short name** | `hdfs`, `rangerkms` | Output of `auth_to_local`; checked in `allowedUsers` |
| **Plugin ID / `appId`** | `hdfs`, `hiveServer2`, `kms` | Kafka partition routing; audit `agentId` |

HDFS `auth_to_local` maps `nn/dn/jn/hdfs/*` → short name **`hdfs`**. That is **not** the repo name (`dev_hdfs`); it is the allowlist entry.

## POST /access — curl template

Run **inside the plugin container** (has keytab + krb5):

```bash
export KRB5CCNAME=/tmp/krb-$$
kinit -kt /etc/keytabs/hdfs.keytab hdfs/ranger-hadoop.rangernw@EXAMPLE.COM

curl -s -w '\nHTTP %{http_code}\n' --negotiate -u : -X POST \
  -H 'Content-Type: application/json' \
  'http://ranger-audit-ingestor.rangernw:7081/api/audit/access?serviceName=dev_hdfs&appId=hdfs' \
  -d '[{
    "repo": "dev_hdfs",
    "reqUser": "e2e-audit-user",
    "evtTime": 1700000000000,
    "access": "read",
    "resource": "/e2e/path",
    "result": 1,
    "agent": "hdfs"
  }]'
```

### Expected success response (HTTP 200)

```json
{
  "total": 1,
  "timestamp": 1717500000000,
  "serviceName": "dev_hdfs",
  "appId": "hdfs",
  "authenticatedUser": "hdfs"
}
```

`authenticatedUser` is the **short name** after `auth_to_local`, not the full Kerberos principal.

### Error responses

| HTTP | Meaning |
|------|---------|
| **401** | No SPNEGO / authentication failed |
| **403** | Authenticated, but short name not in `services[serviceName].allowedUsers` |
| **400** | Missing `serviceName` or empty audit batch |

## Per-plugin matrix (Docker Tier 3)

| Repo | appId | auth_to_local short name | Container | kinit principal |
|------|-------|--------------------------|-----------|-----------------|
| `dev_hdfs` | `hdfs` | `hdfs` | `ranger-hadoop` | `hdfs/ranger-hadoop.rangernw@EXAMPLE.COM` |
| `dev_yarn` | `yarn` | `yarn` | `ranger-hadoop` | `yarn/ranger-hadoop.rangernw@EXAMPLE.COM` |
| `dev_hive` | `hiveServer2` | `hive` | `ranger-hive` | `hive/ranger-hive.rangernw@EXAMPLE.COM` |
| `dev_hbase` | `hbaseMaster` | `hbase` | `ranger-hbase` | `hbase/ranger-hbase.rangernw@EXAMPLE.COM` |
| `dev_kafka` | `kafka` | `kafka` | `ranger-kafka` | `kafka/ranger-kafka.rangernw@EXAMPLE.COM` |
| `dev_knox` | `knox` | `knox` | `ranger-knox` | `knox/ranger-knox.rangernw@EXAMPLE.COM` |
| `dev_kms` | `kms` | `rangerkms` | `ranger-kms` | `rangerkms/ranger-kms.rangernw@EXAMPLE.COM` |
| `dev_trino` | `trino` | `trino` | `ranger-trino` | `trino/ranger-trino.rangernw@EXAMPLE.COM` |
| `dev_solr` | `solr` | `solr` | `ranger-solr` | `solr/ranger-solr.rangernw@EXAMPLE.COM` |
| `dev_ozone` | `ozone` | `om` | `ozone-om` | `om/om.rangernw@EXAMPLE.COM` |

## Dynamically add allowlist users

### Option A — onboard new repo + plugin (recommended)

```bash
kinit -kt /etc/keytabs/HTTP.keytab HTTP/ranger-audit-ingestor.rangernw@EXAMPLE.COM

curl -s --negotiate -u : -X POST -H 'Content-Type: application/json' \
  'http://ranger-audit-ingestor.rangernw:7081/api/audit/partition-plan/services' \
  -d '{
    "serviceName": "dev_trino",
    "pluginId": "trino",
    "partitionCount": 3,
    "allowedUsers": ["trino"],
    "expectedVersion": 1
  }'
```

After install, ingestor logs:

```text
Applied composed auth_to_local rules for plan version N (M active short names)
```

### Option B — update allowlist only (PATCH partial plan)

```bash
# GET current plan version, then PATCH only the services entry to change
curl -s --negotiate -u : -X PATCH -H 'Content-Type: application/json' \
  'http://ranger-audit-ingestor.rangernw:7081/api/audit/partition-plan' \
  -d '{
    "expectedVersion": 2,
    "services": {
      "dev_kms": { "allowedUsers": ["rangerkms"] }
    }
  }'
```

## E2E script scenarios

`verify-dynamic-auth-to-local-e2e.sh` runs:

1. **Per-plugin POST** — each running plugin container posts audits → 200 + correct `authenticatedUser`
2. **Allowlist toggle** — clear `dev_kms` users → 403; restore → 200
3. **Cross-repo denial** — HDFS principal posting to `dev_kms` → 403
4. **POST /services** — new synthetic repo + buffer plugin; HDFS principal → 200

Options:

```bash
./scripts/audit/verify-dynamic-auth-to-local-e2e.sh --plugins hdfs,hive,kms
./scripts/audit/verify-dynamic-auth-to-local-e2e.sh --no-enable   # assume dynamic already on
```

## Access log (ingestor)

Successful POSTs are logged at DEBUG/INFO in the ingestor container:

```bash
docker logs ranger-audit-ingestor 2>&1 | grep -E 'logAccessAudit|authenticatedUser|Applied composed auth_to_local'
```

Example log lines:

```text
Authenticated user: Principal: hdfs/ranger-hadoop.rangernw@EXAMPLE.COM, shortName: hdfs
Applied composed auth_to_local rules for plan version 3 (12 active short names)
```

Audits accepted on `/access` are produced to Kafka topic **`ranger_audits`** (partition keyed by `appId`).

## Related docs

- `README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md` — unified partition plan + allowlist
- `README-KAFKA-PARTITION-PLAN-E2E-TEST-PLAN.md` — partition routing E2E
- `dev-support/ranger-docker/scripts/audit/verify-partition-plan-e2e-all.sh` — routing-only suite

## Unit tests (no Docker)

```bash
mvn test -pl audit-server/audit-ingestor -am \
  -Dtest=AuthToLocalRuleComposerTest -Dsurefire.failIfNoSpecifiedTests=false
```
