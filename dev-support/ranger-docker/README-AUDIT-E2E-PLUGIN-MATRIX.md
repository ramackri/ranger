# Audit E2E — plugin coverage matrix

**Purpose:** Clarify which Docker Compose plugins are on the **auditserver path** (plugin → ingestor `:7081` → Kafka `ranger_audits` → dispatcher → Solr → Admin), what `setup-audit-e2e.sh` already proves, and what to add next.

**Related:** [README-AUDIT-E2E.md](README-AUDIT-E2E.md) (harness), [README-KAFKA-BROKER-TUNING.md](../../audit-server/README-KAFKA-BROKER-TUNING.md) (broker sizing).

---

## 1. The six-hop pipeline (what “audit E2E” means)

```text
Plugin access  →  audit-ingestor :7081  →  Kafka ranger_audits  →  dispatcher  →  Solr  →  Admin Audit tab
                      (repo dev_*)
```

**Not the same pipeline:** usersync, tagsync, PDP (admin/sync audits — separate validation).

**Mega-compose alone is not audit E2E** — it proves images build and services start; end-to-end proof needs a **per-plugin trigger** + Solr trace for `repo:dev_*`.

---

## 2. Plugin-by-plugin status

| Compose service | On auditserver path today? | In `setup-audit-e2e.sh`? | Priority / take |
|-----------------|----------------------------|---------------------------|-----------------|
| **HDFS** (`ranger-hadoop`) | Yes | **Done** — full harness | Ignore for new work |
| **Ozone** (`ozone-om`) | Yes | **Done** — full harness | Ignore for new work |
| **Hive** (`ranger-hive`) | Yes | **Done** — full harness | Ignore for new work |
| **HBase** (`ranger-hbase`) | Yes — `install.properties` → ingestor; ingestor allows `dev_hbase` | **Yes** — `verify-hbase-plugin-audit-e2e.sh --full-e2e` | RANGER-5644 (#1015): plugin-impl **must** ship Jersey JSON writers (opposite of Kafka #1020) |
| **Knox** (`ranger-knox`) | Yes — install props + `dev_knox` on ingestor | **Yes** — `verify-knox-plugin-audit-e2e.sh --full-e2e` (**proven**) | **Done** — harness only; [RANGER-KNOX-PLUGIN-AUDIT-E2E.md](../RANGER-KNOX-PLUGIN-AUDIT-E2E.md) |
| **KMS** (`ranger-kms`) | Yes — `plugin-kms.xml` + runtime JAR script + `dev_kms` | **Yes** — `verify-kms-plugin-audit-e2e.sh --full-e2e` | **Proven** — [RANGER-KMS-PLUGIN-AUDIT-E2E.md](../RANGER-KMS-PLUGIN-AUDIT-E2E.md) |
| **Kafka plugin** (`ranger-kafka`) | Config yes; **packaging smoke PASS**; **full `dev_kafka` E2E harness** (`--full-e2e`) | **Partial** — `./scripts/kafka/verify-kafka-plugin-audit-e2e.sh` | **Tier C** — see [README-RANGER-5642-KAFKA-AUDITSERVER-JERSEY.md](../README-RANGER-5642-KAFKA-AUDITSERVER-JERSEY.md) |
| **Trino** (`ranger-trino`) | **Product:** auditserver supported but **off** by default; **Docker:** still **direct Solr** | N/A | **Tier D** — add `apply-trino-audit-config.sh` + enable auditserver (see §9) |
| **usersync / tagsync / pdp** | Different audit model | Not same 6-hop access pipeline | Separate validation if needed |

---

## 3. Kafka: two roles (do not conflate)

| Role | What it is | Exercised by audit E2E today? |
|------|------------|-------------------------------|
| **Kafka as audit bus** | `ranger_audits` topic; ingestor produce / dispatcher consume | **Yes** — HDFS/Ozone/Hive E2E use this |
| **Kafka Ranger plugin** | `RangerKafkaAuthorizer` → would emit `repo:dev_kafka` access audits | **No** — disabled in Docker |

In the audit E2E stack, `ranger-kafka-setup.sh` disables the authorizer:

```properties
# Ranger authorization disabled in Docker (audit E2E uses Kafka as a bus only).
# authorizer.class.name=org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer
```

| Goal | What to do |
|------|------------|
| Validate audit **pipeline** | Already covered by HDFS/Ozone/Hive triggers |
| Validate **Kafka plugin audits** | `./scripts/kafka/verify-kafka-plugin-audit-e2e.sh --full-e2e` |
| #1015 packaging (JARs) | `verify-plugin-auditserver-jars.sh` + CI `plugins-docker-build` + log grep |
| **RANGER-5642 fix** (#1020) | Smoke **PASS**; full Solr via `--full-e2e` + Docker script chain — see [README-RANGER-5642-KAFKA-AUDITSERVER-JERSEY.md](../README-RANGER-5642-KAFKA-AUDITSERVER-JERSEY.md) |

---

## 4. Priority tiers

| Tier | Plugins | Work type |
|------|---------|-----------|
| **A — done** | HDFS, Ozone, Hive | Full harness in `setup-audit-e2e.sh` |
| **B — next** | Trino | On ingestor path; add Hive-shaped trigger + repair + trace |
| **C — special** | Kafka plugin (authorizer) | Extra setup before same 6-hop proof |
| **D — config gap** | Trino | Point audit XML at auditserver first |
| **Separate** | usersync, tagsync, pdp | Not data-plane access pipeline |

---

## 5. Four pieces per new plugin (reuse pattern, not clone whole script)

For each Tier B/C plugin, add the same shape HDFS/Ozone/Hive use:

| Piece | Purpose |
|-------|---------|
| `ensure_*_plugin_tarball` | Fat tarball in `dist/` |
| `apply-*-audit-config.sh` | Audit URL, Kerberos, batch interval (Hive showed `authn.type=kerberos` is not automatic from `install.properties` alone) |
| `repair-*` + `trigger-*` | Runtime fix + generate access event |
| `trace_*_access_pipeline` | Solr `repo:dev_*` + optional Admin API |

---

## 6. Cheap vs expensive validation

| Check | Command / scope | Proves |
|-------|-----------------|--------|
| **Packaging** | `./scripts/audit/verify-plugin-auditserver-jars.sh --kafka-only\|--hbase-only` | Audit JARs on classpath / in tarball |
| **Assembly** | `verify-plugin-auditserver-jars.sh --check-assembly` | Build output |
| **Container + logs** | `verify-plugin-auditserver-jars.sh --container --check-logs` | Service starts; no `MessageBodyWriter` / HK2 errors |
| **Log grep** | `docker logs ranger-hbase 2>&1 \| grep -i MessageBodyWriter` | Same for kafka/hbase |
| **Full E2E** | `setup-audit-e2e.sh` trigger + verify | 6-hop pipeline to Solr/Admin |

**Mega-compose value:** regression that images build and HBase HMaster does not crash (#1015 HK2 lesson). **Necessary, not sufficient** for audit E2E.

---

## 7. Practical recommendations

| Goal | Approach |
|------|----------|
| “Every plugin in mega-compose sends audits via auditserver to Solr” | **Not true today** — only HDFS/Ozone/Hive are proven end-to-end |
| Minimum for #1015 (Kafka/HBase packaging) | `verify-plugin-auditserver-jars.sh` + CI `plugins-docker-build` + log grep |
| Next harness extensions | **Trino** — same shape as Hive |
| Kafka **plugin** audits | Enable authorizer; produce/consume; check `repo:dev_kafka` |
| Trino | Fix audit XML → auditserver; then add trigger |
| Knox / KMS | `./scripts/knox/verify-knox-plugin-audit-e2e.sh --full-e2e` / `./scripts/kms/verify-kms-plugin-audit-e2e.sh --full-e2e` (optional compose services) |

---

## 8. Bottom line

- Do **not** clone the entire `setup-audit-e2e.sh` stack for every compose service.
- Use a **plugin matrix**: **cheap packaging verify** + **optional per-plugin `trigger-*`** (expensive).
- **Knox** and **KMS** have Hive-shaped verify scripts; Knox needs `ranger-hadoop` for sandbox WebHDFS.
- **Kafka plugin** and **Trino** need extra setup before they fit the same pipeline.
- The **mega-compose** validates build/runtime health; **`setup-audit-e2e.sh`** validates the audit pipeline.

---

## 9. Trino — is auditserver fixed on master?

**Short answer:** **Partially in the product; not fixed for Docker audit E2E.**

| Layer | Status on master | Detail |
|-------|------------------|--------|
| **Plugin code / JAR** | **Yes** | `plugin-trino/pom.xml` depends on `ranger-audit-dest-auditserver` |
| **Install template** | **Supported, disabled** | `plugin-trino/scripts/install.properties`: `XAAUDIT.AUDITSERVER.ENABLE=false`, URL `http://ranger-audit:7081`; `ranger-trino-audit-changes.cfg` has `xasecure.audit.destination.auditserver.*` |
| **Shipped `ranger-trino-audit.xml`** | Solr/Kafka off | Base XML has `xasecure.audit.solr.is.enabled=false` — destination chosen at install time via `install.properties` |
| **Docker `scripts/trino/ranger-trino-audit.xml`** | **Still direct Solr** | `xasecure.audit.destination.solr=true` → `http://ranger-solr.rangernw:8983/solr/ranger_audits` — **bypasses ingestor `:7081`** |
| **Docker install props** | **Missing** | No `ranger-trino-plugin-install.properties` with `XAAUDIT.AUDITSERVER.ENABLE=true` (unlike HDFS/Hive/Knox/HBase in `scripts/*/ranger-*-plugin-install.properties`) |
| **Harness** | **None** | No `apply-trino-audit-config.sh` / `trigger-trino-audit` (unlike Hive/Ozone/HDFS) |

So master **has the auditserver destination plumbing** for Trino, but **Docker still ships Solr-direct XML** and **does not** apply the same auditserver pattern as Hive. Trino remains **Tier D** for audit E2E until someone adds Docker install props + apply script (or replaces `ranger-trino-audit.xml` with auditserver + Kerberos like Hive).
