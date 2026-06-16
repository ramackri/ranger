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

# Dynamic partition plan — what we validated (plain-language report)

This document explains **what was tested and what passed**, for people who do not have the Ranger source tree open. It is a validation report, not a runbook.

**Want the product story first?** Read [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md) §3.1 (static → dynamic cutover).

---

## In 60 seconds

Ranger audit-ingestor can route each plugin’s audits to specific Kafka partitions. **Dynamic mode** stores that routing map in Kafka so you can change it at runtime (promote a new plugin, add partitions) **without restarting** ingestor.

We validated on a **full test lab** (Docker, Kerberos, Kafka, Solr, HDFS, Hadoop):

| Area | Plain question | Answer |
|------|----------------|--------|
| **Off by default** | Does old behavior still work when dynamic mode is off? | **Yes** — health OK; admin partition API returns “not enabled”. |
| **Turn dynamic on** | Does first startup build a correct routing map? | **Yes** — 13 plugins, 48 partitions, matches Kafka. |
| **Change routing live** | Can an admin promote/scale via REST without restart? | **Yes** — version bumps; bad requests rejected (409/400). |
| **Several ingestors** | Do two ingestor pods see the same map after a change? | **Yes** — second pod catches up within ~35 seconds. |
| **Existing clusters** | If we pre-load the map in Kafka before cutover, is it respected? | **Yes** — ingestor does not overwrite it from XML. |
| **Kafka down** | If Kafka is down at startup with dynamic on, do we fail safely? | **Yes** — ingestor does not come up healthy; logs show Kafka/plan errors. |

**Overall: 31 end-to-end checks — all passed.**

---

## Words you will see in the tables

| Term | Meaning (no code required) |
|------|----------------------------|
| **Ingestor** | Ranger service that receives audits from plugins and writes them to Kafka. |
| **Plugin** | Source of audits (e.g. HDFS, Hive). Identified by a short name like `hdfs`, `storm`. |
| **Partition** | A numbered “lane” inside the Kafka topic `ranger_audits`. More lanes = more parallel consumption. |
| **Partition plan** | A JSON document: which plugin may use which partition numbers, plus a **buffer** pool for unknown plugins. |
| **Plan topic** | Kafka topic `ranger_audit_partition_plan` — durable copy of the plan all ingestors read. |
| **Dynamic mode** | Feature on when `kafka.partition.plan.dynamic.enabled=true`. Live routing comes from the plan topic, not from editing XML on disk. |
| **Static mode** | Default. Routing fixed at startup from XML config. Plan topic is not used. |
| **Buffer** | Shared partitions for plugins that do not have dedicated slots yet (e.g. a new `storm` plugin). |
| **Promote** | Admin action: move a plugin from buffer to its own dedicated partitions. |
| **Scale** | Admin action: add more partitions to a plugin that already has dedicated slots. |
| **Version** | Integer on the plan; every successful change increments it (like optimistic locking). |
| **expectedVersion** | Client sends “I think the plan is version N”; server rejects with **409** if someone else already published N+1. |
| **Watcher** | Background process on each ingestor that reloads the plan from Kafka every ~30 seconds. |
| **Greenfield** | Brand-new setup: empty plan topic, first ingestor creates version 1 from XML layout. |
| **Brownfield / pre-seed** | Existing cluster: operator writes version 1 into Kafka **before** enabling dynamic mode so cutover does not guess wrong. |

---

## Where testing happened

| Item | Detail |
|------|--------|
| **Environment** | Ranger audit “Tier 3” lab stack in Docker (Kerberos, ZooKeeper, Kafka, Solr + HDFS dispatchers, Hadoop, Ranger Admin). |
| **Main ingestor** | Port **7081**. |
| **Second ingestor** (multi-pod only) | Port **7082**, separate hostname and Kerberos identity. |
| **Audit Kafka topic** | `ranger_audits` — actual audit events. |
| **Plan Kafka topic** | `ranger_audit_partition_plan` — routing configuration only. |
| **Standard plugin layout** | 13 plugins in config, 3 partitions each, plus 9 buffer partitions → **48** partitions total on a clean topic. |

Before each block of tests, the lab was checked: Admin UI up, ingestor health up, Solr/HDFS path healthy.

---

## 1. Static mode still works (2 checks)

**Situation:** Dynamic feature is **off** (normal default for existing deployments).

| # | Scenario | What we did | Success means | Result |
|---|----------|-------------|---------------|--------|
| 1 | Admin API disabled | Called read partition-plan API | Response **503** — feature not enabled | **Pass** |
| 2 | Audits still served | Called ingestor health API | Response **200** — service healthy | **Pass** |

**Takeaway:** Turning the feature off does not break the ingestor. The partition-plan admin API is simply unavailable.

---

## 2. Enabling dynamic mode on a clean cluster (6 checks)

**Situation:** Fresh plan topic (no prior routing map). Audit topic reset to **48** partitions to match config. Dynamic mode turned **on**, ingestor restarted.

| # | Scenario | What we did | Success means | Result |
|---|----------|-------------|---------------|--------|
| 1 | Service starts | Wait for ingestor to finish startup | Healthy; background sync reports “plan ready” | **Pass** |
| 2 | First plan published | Inspect Kafka plan topic | Topic exists; plan **version 1** (or higher) stored | **Pass** |
| 3 | Correct plugin list | Read plan via admin API | **13** configured plugins listed (hdfs, yarn, hive, … per site config) | **Pass** |
| 4 | Partition count honest | Compare plan’s total vs Kafka topic size | Numbers **match** (48 on clean run) | **Pass** |
| 5 | Plan topic created | List Kafka topics | `ranger_audit_partition_plan` present | **Pass** |

---

## 3. Changing the plan without restart (6 checks)

**Situation:** Dynamic mode on; plan already loaded. Changes made only through the **admin REST API** (authenticated).

| # | Scenario | What we did | Success means | Result |
|---|----------|-------------|---------------|--------|
| 1 | Onboard new plugin | **Promote** a plugin that only used buffer (e.g. `storm`) — give it 2 dedicated partitions | **200 OK**; version increases; plugin appears in plan | **Pass** |
| 2 | Stale edit rejected | Promote again but claim plan is still **version 1** (wrong) | **409 Conflict** — forces admin to refresh | **Pass** |
| 3 | No double-promote | Try to promote **hdfs** (already has dedicated partitions) | **400 Bad Request** | **Pass** |
| 4 | Grow hot plugin | **Scale** the newly promoted plugin (+1 partition) | **200 OK**; version increases again | **Pass** |
| 5 | Plan stays in memory | Read plan again **without** restarting ingestor | Same version as last change; data consistent | **Pass** |

**Takeaway:** Operators can change routing live. The API blocks obvious mistakes (stale version, promoting twice).

---

## 4. Two ingestor pods stay in sync (7 checks)

**Situation:** Primary ingestor on 7081 (dynamic on). Second ingestor started on 7082 with its own Kerberos identity but the **same** Kafka plan topic.

| # | Scenario | What we did | Success means | Result |
|---|----------|-------------|---------------|--------|
| 1 | Second pod starts | Bring up replica ingestor | Replica healthy; plan sync ready | **Pass** |
| 2 | Same map at start | Read plan version on **both** pods | **Identical version number** | **Pass** |
| 3 | Change on one pod only | **Promote** a buffer plugin on **primary only** | Primary returns new version | **Pass** |
| 4 | Other pod catches up | Wait up to **35 seconds** (one sync cycle) | Replica shows **same new version** without anyone calling API on replica | **Pass** |
| 5 | Primary unchanged | Read plan on primary again | Still on new version (no drift) | **Pass** |

**Takeaway:** In a real cluster with multiple ingestors behind a load balancer, a routing change on any pod propagates to all pods automatically.

---

## 5. Brownfield cutover — pre-load plan in Kafka (8 checks)

**Situation:** Mimics a **production cluster** moving from static to dynamic. Operator puts the routing map into Kafka **first**, then enables dynamic mode — ingestor must **not** replace it with a fresh guess from XML.

| Step | Scenario | What we did | Success means | Result |
|------|----------|-------------|---------------|--------|
| 1 | Save real layout | Read current plan while dynamic is briefly on | Valid JSON captured | **Pass** |
| 2 | Simulate “still on static” | Turn dynamic off; delete plan topic | Ingestor healthy; plan topic gone | **Pass** |
| 3 | Operator pre-seed | Write plan to Kafka with marker `updatedBy=brownfield-e2e-seed`, version **1** | Message on plan topic | **Pass** |
| 4 | Enable dynamic | Turn dynamic on; restart ingestor | Plan still shows **brownfield-e2e-seed** (not auto-`bootstrap`) | **Pass** |
| 5 | Version respected | Read plan | **version = 1** | **Pass** |
| 6 | Kafka alignment | Compare plan total partitions vs `ranger_audits` | **Match** | **Pass** |
| 7 | Rollback API | Turn dynamic off again | Partition-plan API returns **503** | **Pass** |
| 8 | Rollback health | Health check | **200** — audits path still OK in static mode | **Pass** |

**Takeaway:** Pre-seeding (Path A in the migration guide) works. Rollback to static mode is safe.

---

## 6. Kafka unavailable when dynamic starts (3 checks)

**Situation:** Dynamic mode enabled in config, but **Kafka broker stopped**, then ingestor restarted. This must **not** silently pretend everything is fine.

| # | Scenario | What we did | Success means | Result |
|---|----------|-------------|---------------|--------|
| 1 | Unhealthy startup | Restart ingestor with Kafka down | Health endpoint **not** OK | **Pass** |
| 2 | Clear failure in logs | Read ingestor logs from that restart | Errors mention Kafka connection failure and/or inability to start plan sync | **Pass** |
| 3 | Recovery | Start Kafka; disable dynamic; restart ingestor | Health **200** in static mode | **Pass** |

**Takeaway:** Dynamic mode depends on Kafka at startup. Fix Kafka (or leave dynamic off) before expecting a healthy ingestor.

---

## 7. Automated unit tests (companion, not Tier 3 Docker)

These run in CI / developer machines — no full cluster. They prove individual pieces before the lab tests above.

| Area | What was proven |
|------|-----------------|
| **First-time plan layout** | Version 1 from XML matches the same partition ranges static mode would use. |
| **Multi-pod race (logic)** | If two startups see an empty plan topic, only one writes; the other adopts the winner’s plan. |
| **Plan JSON** | Serialize/deserialize; reject garbage input. |
| **Validation rules** | No duplicate partition IDs; total must match Kafka; changes cannot reshuffle existing plugin slots (append-only). |
| **Promote / scale math** | Buffer → dedicated; grow topic tail when needed; reject illegal promote/scale. |
| **REST service** | Feature flag; reads from memory; **409** when two writers conflict. |
| **Audit routing** | With dynamic on, partitioner uses in-memory plan; unknown plugins → buffer. |

---

## 8. Not tested in this pass

| Topic | Why |
|-------|-----|
| Two pods starting **at the same time** on an empty plan topic | Needs choreographed parallel rollout; logic covered in unit tests |
| End-to-end “HDFS audit → Solr” after every promote | Optional smoke; lab path validated separately |
| Admin **403** for non-admin users | Planned for a later phase |
| Shrinking Kafka partition count | Not supported by Kafka |

---

## 9. Quick reference — numbers used in the lab

| Setting | Value |
|---------|--------|
| Configured plugins (13) | hdfs, yarn, knox, hiveServer2, hiveMetastore, kafka, hbaseRegional, hbaseMaster, solr, trino, ozone, kudu, nifi |
| Partitions per configured plugin | 3 |
| Buffer partitions | 9 |
| **Total on clean topic** | **48** |
| Plan sync interval | 30 seconds (default) |
| Promote examples in tests | Plugins **not** in the list above (e.g. storm, ambari, impala) |

---

## Related documents

| Document | Audience |
|----------|----------|
| [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md) | Everyone — concepts and diagrams |
| [README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md](README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md) | Operators cutting over live clusters |
| [README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md](README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md) | Day-2 promote/scale procedures |
| [README-KAFKA-PARTITION-PLAN-E2E-TEST-PLAN.md](README-KAFKA-PARTITION-PLAN-E2E-TEST-PLAN.md) | QA — full scenario catalog with IDs (POS-*, NEG-*) |
