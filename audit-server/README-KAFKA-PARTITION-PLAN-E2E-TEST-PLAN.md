<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Dynamic partition plan — E2E test plan

End-to-end validation for **Phases 1–5 and 8** (dynamic Kafka partition plan). Phase 6 (admin allow-list + `/status` metadata) is **deferred**.

**Automated subset:** [dev-support/ranger-docker/scripts/audit/verify-partition-plan-e2e.sh](../dev-support/ranger-docker/scripts/audit/verify-partition-plan-e2e.sh)

**Related:**

| Doc | Use |
|-----|-----|
| [README-KAFKA-PARTITION-PLAN-E2E-VALIDATION.md](README-KAFKA-PARTITION-PLAN-E2E-VALIDATION.md) | **What was tested** — plain-language validation report (start here if you do not have the codebase) |
| [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md) | Static→dynamic preservation, bootstrap, multi-pod races (§3.1) |
| [README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md](README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md) | REST operations |
| [README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md](README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md) | Cutover scenarios |
| [dev-support/RANGER-AUDIT-TIER3-VALIDATION-PLAN.md](../dev-support/RANGER-AUDIT-TIER3-VALIDATION-PLAN.md) | Full audit pipeline |
| [.cursor/skills/ranger-audit-tier3-e2e/SKILL.md](../.cursor/skills/ranger-audit-tier3-e2e/SKILL.md) | Docker bring-up |

---

## 1. Scope

| In scope | Out of scope |
|----------|--------------|
| Plan topic, watcher, partitioner, REST API | Phase 6 plugin-user **403** on partition-plan |
| Multi-ingestor consistency (manual / 2nd replica) | Shrinking `ranger_audits` partitions |
| Audit pipeline after promote/scale | Admin `audit_store=db` |

---

## 2. Environments

| Layer | Command / stack |
|-------|-----------------|
| **Unit** | `mvn test -pl audit-server/audit-ingestor -Dtest=PartitionPlan*Test,AuditPartitionerDynamicTest -Drat.skip=true` |
| **Tier 3 Docker** | `./dev-support/ranger-docker/setup-audit-e2e.sh up` or `audit-stack.sh up --tier 3` |
| **Partition plan E2E** | `./scripts/audit/verify-partition-plan-e2e.sh` (from `ranger-docker/`) |

**Pre-flight (every E2E run):**

1. Tier 3 healthy: `./scripts/audit/wait-for-audit-health.sh --tier 3`
2. Baseline audit path (optional): `./scripts/audit/verify-audit-tier3-e2e.sh`
3. Record: `kafka-topics --describe ranger_audits`; ingestor `AuditPartitioner Configuration` log

---

## 3. Automated scripts

| Script | Covers |
|--------|--------|
| `verify-partition-plan-e2e.sh` | POS-A*, POS-C*, NEG-1–8 (core REST) |
| `verify-partition-plan-multipod-e2e.sh` | POS-D1 (replica on :7082, watcher convergence) |
| `verify-partition-plan-brownfield-e2e.sh` | POS-F1, POS-F2 (pre-seed + rollback) |
| `verify-partition-plan-kafka-down-e2e.sh` | NEG-9 (startup with Kafka down) |
| `verify-partition-plan-e2e-all.sh` | Runs all of the above |

```bash
cd dev-support/ranger-docker
chmod +x scripts/audit/verify-partition-plan-*.sh

# Full suite (recommended after Tier 3 is up)
./scripts/audit/verify-partition-plan-e2e-all.sh

# Or run individually:
./scripts/audit/verify-partition-plan-e2e.sh --static-only
./scripts/audit/verify-partition-plan-e2e.sh --dynamic --restore-static
./scripts/audit/verify-partition-plan-multipod-e2e.sh
./scripts/audit/verify-partition-plan-brownfield-e2e.sh --restore-static
./scripts/audit/verify-partition-plan-kafka-down-e2e.sh

# With audit pipeline smoke after REST mutations
./scripts/audit/verify-partition-plan-e2e-all.sh --with-audit-smoke
```

---

## 4. Positive scenarios

### A. Static mode regression

| ID | Scenario | Expected |
|----|----------|----------|
| POS-A1 | `dynamic.enabled` false / absent | No `PartitionPlanWatcher` in logs |
| POS-A2 | `GET /api/audit/partition-plan` | **503** feature disabled |
| POS-A3 | `GET /api/audit/health` | **200** |
| POS-A4 | HDFS plugin audits | Solr count increases (Tier 3 smoke) |

### B. Greenfield dynamic enable

| ID | Scenario | Expected |
|----|----------|----------|
| POS-B1 | Enable `dynamic.enabled=true`; restart | Ingestor starts; watcher ready |
| POS-B2 | Empty plan topic → Race B | v1 in `ranger_audit_partition_plan` |
| POS-B3 | `GET /partition-plan` | JSON v1; `topicPartitionCount` matches `describe` |
| POS-B4 | Ingestor log | `Mode=dynamic`, plan version shown |
| POS-B5 | HDFS audits after enable | Pipeline still OK |

### C. REST (no ingestor restart)

| ID | Scenario | Expected |
|----|----------|----------|
| POS-C1 | `POST .../plugins` new plugin (e.g. `storm`) | **200**; version+1; plugin in `plugins` |
| POS-C2 | `PATCH .../plugins/{pluginId}` existing plugin | **200**; tail IDs appended; topic grown if needed |
| POS-C3 | `PATCH` partial plan + valid `expectedVersion` | **200** |
| POS-C4 | Wait ≤ refresh interval | Same plan on repeated `GET` without restart |
| POS-C5 | 409 retry | Re-`GET` version; retry succeeds |

### D. Multi-pod

| ID | Scenario | Expected | Script |
|----|----------|----------|--------|
| POS-D1 | `GET` from 2 ingestor pods | Same `version` within 30s after promote | `verify-partition-plan-multipod-e2e.sh` |
| POS-D2 | Parallel cold start on empty plan topic | Single v1; both pods aligned | Manual (scale 2 replicas on empty topic) |

### E. Audit pipeline

| ID | Scenario | Expected |
|----|----------|----------|
| POS-E1 | HDFS audits after promote/scale | Solr + HDFS dispatchers healthy |
| POS-E2 | Dispatcher rebalance after scale | Consumer groups rebalance; no sustained lag |

### F. Brownfield

| ID | Scenario | Expected | Script |
|----|----------|----------|--------|
| POS-F1 | Pre-seed v1 in Kafka; then enable | `updatedBy` preserved; no XML bootstrap | `verify-partition-plan-brownfield-e2e.sh` |
| POS-F2 | Rollback to static | **503** on partition-plan API | `--restore-static` on brownfield script |

---

## 5. Negative scenarios

### REST / validation

| ID | Scenario | Expected |
|----|----------|----------|
| NEG-1 | Partition-plan API with feature off | **503** |
| NEG-2 | Unauthenticated `GET` partition-plan | **401** |
| NEG-3 | Stale `expectedVersion` | **409** + current plan body |
| NEG-4 | Promote plugin already in plan (e.g. `hdfs`) | **400** |
| NEG-5 | Scale plugin only in buffer | **400** |
| NEG-6 | `PATCH` reshuffles existing plugin IDs | **400** |
| NEG-7 | Invalid / overlapping partition lists | **400** |
| NEG-8 | `partitionCount: 0` | **400** |

### Infrastructure

| ID | Scenario | Expected |
|----|----------|----------|
| NEG-9 | Kafka down at startup with dynamic on | Health fail or plan error in logs | `verify-partition-plan-kafka-down-e2e.sh` |
| NEG-10 | Cannot ALTER `ranger_audits` | Scale/promote needing growth → **503** |
| NEG-11 | Corrupt JSON on plan topic | Watcher ignores; last good plan kept |

### Concurrency

| ID | Scenario | Expected |
|----|----------|----------|
| NEG-12 | Two promotes same `expectedVersion` | One **200**, one **409** |

### Deferred (Phase 6)

| ID | Scenario | When Phase 6 lands |
|----|----------|-------------------|
| NEG-13 | Plugin service user calls partition-plan | **403** |
| NEG-14 | `/status` includes `partitionPlan` block | JSON metadata present |

---

## 6. Release gate

| Gate | Criteria |
|------|----------|
| G1 | All unit tests pass |
| G2 | `verify-partition-plan-e2e.sh --static-only` pass |
| G3 | `verify-partition-plan-e2e.sh --dynamic` pass on Tier 3 |
| G4 | `verify-audit-tier3-e2e.sh` pass after dynamic promote |
| G5 | Manual POS-D1 if multi-ingestor available |

---

## 7. Suggested execution order

```text
Unit tests → Tier 3 up → static-only script → enable dynamic → dynamic script
  → optional verify-audit-tier3-e2e → optional brownfield / rollback
```

---

## 8. Test layouts (XML)

| Layout | `configured.plugins` | Buffer | Total partitions |
|--------|---------------------|--------|------------------|
| **L0 empty (site XML default)** | *(empty)* | N/A — dynamic bootstrap uses all of `kafka.topic.partitions` as buffer | **`kafka.topic.partitions`** (default **10**) |
| L1 minimal | `hdfs` | 3 | 6 |
| L2 full lab / brownfield | `hdfs,yarn,knox,hiveServer2,hiveMetastore,kafka,hbaseRegional,hbaseMaster,solr,trino,ozone,kudu,nifi` (13 plugins) | 9 | **48** (13×3+9) |
| L3 hot | + per-plugin overrides | 9 | varies |

Greenfield E2E uses **L0** unless site XML is patched. Promote tests pick a buffer plugin such as **`storm`** (not in `configured.plugins`). For **L2** layout validation, set the 13-plugin list in site XML before enabling dynamic mode.
