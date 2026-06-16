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

# Dynamic Kafka partitioning for Ranger audit plugins — guide for everyone

This guide explains **why** and **how** Ranger can move from **static** plugin-to-partition mapping (set once at startup) to **dynamic** mapping (change at runtime without restarting the audit ingestor).

It is written for operators, architects, and reviewers who need a shared mental model — **without reading the codebase**.

---

## 1. What problem are we solving?

Ranger plugins (HDFS, Hive, Trino, etc.) send audit events to **audit-ingestor**, which writes them to a Kafka topic (default: `ranger_audits`).

To keep high-volume plugins from starving others, ingestor can assign **dedicated Kafka partitions** per plugin. Unlisted plugins share a **buffer** pool of partitions.

**Today (static mode):**

- Which plugin uses which partitions is defined in **XML config** at startup.
- Adding a plugin or giving a hot plugin more partitions usually means: edit config → increase topic partitions → **restart ingestor**.

**Goal (dynamic mode):**

- Change partition assignments **while ingestor is running**.
- Onboard a new plugin without reshuffling partitions already used by other plugins.
- Grow capacity by adding **new partitions at the end** of the topic (append-only), not by recomputing everyone’s ranges.

---

## 2. Core ideas (plain language)

### Kafka topic partitions

Think of a Kafka topic as a queue split into numbered lanes: partition `0`, `1`, `2`, …

- Each audit event is routed to **one** partition based on the plugin id (Kafka record **key**).
- More partitions → more parallel consumption downstream (Solr/HDFS dispatchers).

Kafka allows **increasing** partition count; it does **not** support shrinking.

### Plugin id

The plugin id (also called agent id / app id) identifies the source of the audit — e.g. `hdfs`, `hiveServer2`, `trino`.

### Partition plan

A **partition plan** is a versioned JSON document that answers:

- For each known plugin: **which partition numbers** may receive its audits?
- Which partition numbers are reserved for **unknown / new** plugins (the buffer)?
- What is the current **`topicPartitionCount`** (must match Kafka)?

Example (simplified):

```json
{
  "topic": "ranger_audits",
  "version": 12,
  "topicPartitionCount": 48,
  "plugins": {
    "hdfs":        { "partitions": [0, 1, 2, 3, 4, 5] },
    "hiveServer2": { "partitions": [6, 7, 8, 9, 10, 11] }
  },
  "buffer": { "partitions": [12, 13, 14, "... through 47 ..."] }
}
```

The `version` field increments on every successful admin change (optimistic locking).

Every ingestor pod uses the **same** plan so routing stays consistent.

### Partition registry (source of truth)

The live plan lives in **durable shared storage** that all ingestor replicas read — a Kafka **compacted** topic (`ranger_audit_partition_plan`).

- No new database or ZooKeeper dependency.
- Survives pod restarts.
- All replicas see the same latest plan.

### Append-only growth

When a plugin needs more capacity:

1. Increase the audit topic’s partition count (add lanes at the **tail**).
2. Assign **only the new** partition numbers to that plugin.
3. **Do not** move partitions away from other plugins.

### Buffer partitions

Plugins not yet in the plan (or newly appearing in the fleet) go to **buffer** partitions until an operator **promotes** them to dedicated partitions.

---

## 3. Today vs proposed (at a glance)

| | Static (today) | Dynamic (proposed) |
|---|----------------|---------------------|
| **Where mapping lives** | XML on each pod at startup | Shared plan in Kafka compacted topic |
| **Change mapping** | Edit XML + restart | REST API; no restart |
| **Add new plugin** | Edit `configured.plugins`, often reshuffles ranges | Promote from buffer; allocate from tail only |
| **Scale hot plugin** | Edit overrides + restart | Add tail partitions + update plan |
| **Multi-replica ingestor** | Same XML if synced via ConfigMap | All pods watch same Kafka plan |
| **Feature flag** | Default behavior | `ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled=true` |

---

## 4. Static → dynamic cutover — direct answers

**Goal:** Turn on dynamic mode so each plugin keeps sending audits to the **same Kafka partitions** it uses today — no surprise rerouting on cutover day.

### When dynamic mode is off

| Question | Answer |
|----------|--------|
| Is `ranger_audit_partition_plan` created? | **No** — the plan topic is not created or used. |
| How is routing decided? | From XML at startup (static mode), same as today. |
| Is there a background plan sync? | **No**. |

### When dynamic mode is on

| Question | Answer |
|----------|--------|
| Is the plan topic created? | **Yes** — on first startup that needs the registry. |
| Where does every ingestor get the plan? | From `ranger_audit_partition_plan` (compacted topic), kept in memory on each pod. |
| Can XML edits change live routing? | **No** (once a plan message exists in Kafka). Runtime changes go through REST. |

### How “same partitions per plugin” is achieved on cutover

Ingestor does **not** read Kafka to discover which plugin owns which partition. Kafka stores audit **records**, not plugin ownership.

Preservation works because the **first published plan** uses the **same layout rules as static mode**:

| Piece | Static today | Dynamic (first plan from XML) |
|-------|--------------|-------------------------------|
| Plugin order | `configured.plugins` list | Same list |
| Partitions per plugin | Default + per-plugin overrides | Same |
| Layout | Contiguous ranges (hdfs → 0–2, next → 3–5, … buffer tail) | Same ranges as explicit partition ID lists |
| Within-plugin pick | Round-robin per plugin id | Same round-robin |
| Unknown plugin | Hash into buffer | Same buffer pool |

**What Kafka is used for at bootstrap:**

| Step | Reads from | Purpose |
|------|------------|---------|
| Build first plan (empty registry) | **XML only** | Plugin list, overrides, buffer → partition lists |
| Install plan (every pod) | **Kafka AdminClient** on `ranger_audits` | **Total** partition count only — must match `topicPartitionCount` in plan |
| Route each audit | **In-memory plan** | No per-event Kafka read |

**Brownfield (existing production cluster):** Auto-bootstrap from XML is safe only when **XML, ingestor startup logs, and `kafka-topics --describe ranger_audits` all agree**. If they differ, **pre-load the plan into Kafka before enabling dynamic** (operator publishes JSON to the plan topic while dynamic is still off).

### Plan already in Kafka vs empty registry

```mermaid
sequenceDiagram
  participant Pod as Ingestor pod
  participant Plan as ranger_audit_partition_plan
  participant Audits as ranger_audits
  participant XML as site.xml

  Pod->>Plan: create plan topic if missing (Race A)
  Pod->>Plan: read plan for key ranger_audits
  alt plan message exists
    Plan-->>Pod: use stored plan — skip XML bootstrap
  else registry empty
    Pod->>XML: build first plan from XML layout
    Pod->>Plan: re-read (Race B — peer may have published)
    Pod->>Plan: publish first plan if still empty
    Pod->>Plan: mandatory read-back
  end
  Pod->>Audits: describe topic — partition count
  Pod->>Pod: validate count matches plan → load into memory
```

| Situation | What each pod does |
|-----------|-------------------|
| Plan **message** already in `ranger_audit_partition_plan` | Read and use it; **do not** publish a new plan from XML |
| Plan topic exists but **no message** yet | One pod publishes the first plan; others re-read and adopt the same plan (**Race B**) |
| Several pods create the plan **topic** at once | Idempotent create — **Race A**; “already exists” is success |

**Bootstrap logic (summary):** read plan → if missing, build from XML → re-read → publish if still missing → mandatory read-back → validate partition count → install in memory.

After the first plan is stored in Kafka, **Kafka is the source of truth** for routing.

### Multi-pod race — is it implemented?

| Race | Scenario | Resolution |
|------|----------|------------|
| **A** | Several pods create `ranger_audit_partition_plan` topic together | Idempotent topic create |
| **B** | Several pods publish the **first** plan when registry is empty | Re-read before and after publish; all pods install the same plan from Kafka |

**Your rule:** *If the plan topic exists but no plan message → add the plan; otherwise use the plan from the topic* — **Yes, implemented** via the bootstrap flow above.

---

## 5. How dynamic mode works (end-to-end)

| Plane | Kafka topic | Traffic | Who reads/writes |
|-------|-------------|---------|------------------|
| **Data** | `ranger_audits` | High — every audit event | Plugins → ingestor → dispatchers |
| **Control** | `ranger_audit_partition_plan` (compacted) | Low — rare plan changes | Admin REST + background sync on ingestor pods |

The partition plan is **configuration**, not audit data. Ingestor keeps the current plan **in memory**; only the background sync thread and REST handlers touch the plan topic.

### Architecture (control plane vs data plane)

```mermaid
flowchart TB
  subgraph plugins [Ranger plugins]
    PluginIn[HDFS / Hive / new plugins]
  end

  subgraph ops [Ops or automation]
    Admin[Admin REST client]
  end

  subgraph ingestor [Each audit-ingestor pod]
    REST[Partition-plan REST API]
    Svc[Plan update service]
    Watcher[Background plan sync]
    Mem[(In-memory plan)]
    Part[Partition router]
    Queue[Audit queue]

    REST --> Svc
    Svc --> PlanTopic
    Watcher -->|poll / consume| PlanTopic
    Watcher -->|atomic swap| Mem
    Mem --> Part
    PluginIn -->|POST /access| Queue
    Queue --> Part
    Part -->|Kafka produce key=plugin id| AuditTopic
  end

  subgraph kafka [Kafka]
    PlanTopic[(ranger_audit_partition_plan<br/>compacted, low volume)]
    AuditTopic[(ranger_audits<br/>high volume)]
  end

  subgraph consumers [Downstream — unchanged]
    Solr[Solr dispatcher]
    HDFSdisp[HDFS dispatcher]
  end

  Admin -->|GET / PUT via load balancer| REST
  Svc -->|createPartitions if needed| AuditTopic
  AuditTopic --> Solr
  AuditTopic --> HDFSdisp
```

### First startup — seeding the plan (once per cluster)

When dynamic mode starts and the plan registry is **empty**:

1. Ingestor enables dynamic mode.
2. Background sync finds no plan in `ranger_audit_partition_plan`.
3. Ingestor reads XML (`configured.plugins`, overrides, buffer).
4. Ingestor builds and publishes the **first plan** to the compacted topic.
5. Ingestor loads that plan into memory and begins routing audits.

**After that:** additional pods and restarts **read Kafka only** — they do not re-build from XML.

### Every audit — the hot path

The plan topic is **not** read on this path.

1. Plugin POSTs audit to ingestor.
2. Partition router reads the **in-memory** plan.
3. Known plugin → round-robin within its partition list.
4. Unknown plugin → buffer partition.
5. Record written to `ranger_audits`.

### Changing the plan — admin or automation

**On the pod that receives the REST call:**

1. Read current plan from Kafka.
2. Validate append-only rules.
3. Grow `ranger_audits` tail if needed (Kafka AdminClient).
4. Write new plan **version** to compacted topic.
5. Return **200 OK** or **409 Conflict** (stale `expectedVersion`).

**On every ingestor pod (~30s sync interval):**

1. Background sync picks up new plan version.
2. Validates against live `ranger_audits` partition count.
3. Swaps plan in memory — **no restart**.

```mermaid
sequenceDiagram
  participant Admin as Admin / automation
  participant REST as Ingestor REST (one pod)
  participant Plan as ranger_audit_partition_plan
  participant Audit as ranger_audits
  participant W as Background sync (each pod)
  participant Mem as In-memory plan
  participant Part as Partition router

  Admin->>REST: promote / scale / PUT plan
  REST->>Plan: read current plan version
  REST->>Audit: createPartitions (if needed)
  REST->>Plan: write new plan version
  REST-->>Admin: 200 OK

  loop Every ingestor pod
    W->>Plan: poll latest plan
    W->>Mem: atomic swap
    Note over Part,Mem: Hot path reads memory only
    Part->>Audit: produce audit (key = plugin id)
  end
```

### Rules to remember

- **Two topics, two jobs** — plan topic = config; audit topic = data.
- **Memory on the hot path** — no per-audit read of the plan topic.
- **Kafka is the source of truth** after the first plan is published.
- **Append-only growth** — new partitions only at the tail of `ranger_audits`.
- **All pods must agree** — every ingestor syncs from the same compacted topic.

---

## 6. Admin REST API (control plane)

When dynamic mode is on, operators change routing through the **ingestor admin API** on **any** pod (usually via load balancer). Mutations are written to `ranger_audit_partition_plan`; every pod picks up changes through background sync (~30s).

**Auth:** Kerberos or JWT (same ingestor admin pattern as other audit APIs). Dynamic mode off → all partition-plan calls return **503**.

### Endpoints

| Method | Path | Use when |
|--------|------|----------|
| `GET` | `/api/audit/partition-plan` | Read current plan (from in-memory copy on that pod) |
| `PUT` | `/api/audit/partition-plan` | Replace the full plan (advanced / corrections) |
| `POST` | `/api/audit/partition-plan/promote` | Give a **new** plugin dedicated partitions (from buffer) |
| `POST` | `/api/audit/partition-plan/scale` | Add more partitions to a plugin **already** in the plan |

Base URL example: `https://<ingestor-host>:7081/api/audit/partition-plan`

### `expectedVersion` (all writes)

Every `PUT` / `POST` must include the plan **version** you read from the last `GET`. If another admin changed the plan first, the server returns **409 Conflict** with the current plan — refresh and retry.

### Common operations

**Read plan**

```http
GET /api/audit/partition-plan
→ 200 + JSON plan (note the "version" field)
```

**Promote** — e.g. onboard `trino` from buffer with 3 dedicated partitions:

```json
POST /api/audit/partition-plan/promote
{
  "pluginId": "trino",
  "partitionCount": 3,
  "expectedVersion": 4
}
```

**Scale** — e.g. add 2 tail partitions to `hiveServer2`:

```json
POST /api/audit/partition-plan/scale
{
  "pluginId": "hiveServer2",
  "additionalPartitions": 2,
  "expectedVersion": 5
}
```

Success → **200** + updated plan JSON (version incremented).  
Invalid request (e.g. promote a plugin already dedicated) → **400**.  
Stale version → **409** + current plan in body.

### What happens inside one REST call

```mermaid
flowchart LR
  Admin[Admin or script] --> REST[Ingestor REST]
  REST --> Read[Read plan from Kafka]
  Read --> Check{expectedVersion OK?}
  Check -->|No| R409[409 + current plan]
  Check -->|Yes| Valid[Validate append-only change]
  Valid --> Grow{Need more audit partitions?}
  Grow -->|Yes| Topic[Grow ranger_audits tail]
  Grow -->|No| Write[Write new plan to Kafka]
  Topic --> Write
  Write --> OK[200 + new plan]
  OK --> Sync[All pods sync within ~30s]
```

| Step | What the ingestor does |
|------|------------------------|
| 1 | Authenticate the caller |
| 2 | Read current plan from `ranger_audit_partition_plan` |
| 3 | Reject if `expectedVersion` does not match |
| 4 | Compute new plugin lists (append-only — no reshuffling existing slots) |
| 5 | If new partition IDs are needed → grow `ranger_audits` **first** |
| 6 | Publish new plan version to Kafka |
| 7 | Return updated plan JSON |

**GET** is cheap (memory). **PUT / POST** always goes through Kafka so all pods converge on the same plan.

---

## 7. Operator workflow: onboarding a plugin

### Stage 0 — Plugin appears (unknown)

- Audits use **buffer** partitions.
- Monitor volume per plugin id.

### Stage 1 — Promote plugin

- Call `POST /api/audit/partition-plan/promote` (see [§6](#6-admin-rest-api-control-plane)).
- All ingestors apply within ~30s. **No restart** required.

### Stage 2 — Scale a hot plugin

- Call `POST /api/audit/partition-plan/scale`.
- Dispatchers rebalance automatically when the audit topic grows.

**Do not** edit `ranger-audit-ingestor-site.xml` on one pod for runtime changes. XML is only for **initial bootstrap** when the plan registry is empty.

---

## 8. Configuration (dynamic mode)

| Property | Purpose | Example |
|----------|---------|---------|
| `ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled` | Turn dynamic mode on/off | `false` (default) = static XML |
| `ranger.audit.ingestor.kafka.partition.plan.topic` | Compacted plan topic name | `ranger_audit_partition_plan` |
| `ranger.audit.ingestor.kafka.partition.plan.refresh.interval.ms` | How often pods reload plan | `30000` |

When dynamic is **off**, routing is fixed from XML at startup and the plan topic is not used.

When dynamic is **on** and the registry is **empty**, the first ingestor pod seeds the plan from XML. Later pods and restarts read **Kafka only**.

---

## 9. FAQ

### Basics

**Why plugin-based partitioning?**  
Hot plugins (HDFS, Hive) can get dedicated Kafka lanes so they do not starve others. Unknown plugins share a buffer until you promote them.

**What is the difference between static and dynamic mode?**  
Static: routing is computed once from XML at startup; changes need restart. Dynamic: routing lives in Kafka; admins change it via REST while ingestor keeps running.

**Why not store the plan in Postgres or ZooKeeper?**  
Ingestor already requires Kafka. A compacted plan topic adds no new infrastructure.

**Why not edit XML on a running pod?**  
Each pod has its own copy; edits are not shared, not durable, and are lost on restart. Runtime changes belong in the plan topic via REST.

**Can I change routing by editing XML while dynamic mode is on?**  
No for live routing. Ingestor uses the Kafka plan in memory, not XML edits on disk. Update XML only when preparing static rollback or documenting the intended layout.

**Can I decrease `ranger_audits` partition count?**  
No. Kafka does not support shrinking partitions. You can only add partitions at the tail.

### Two topics and sync

**What are the two Kafka topics?**  
`ranger_audits` = audit data (high volume). `ranger_audit_partition_plan` = routing config (low volume, compacted).

**Does every audit POST read the plan topic?**  
No. Each audit uses the plan already in **memory** on that pod. Only background sync and REST mutations touch the plan topic.

**How do all ingestor pods stay in sync?**  
Every pod watches the same compacted plan topic (default every 30s) and swaps the new plan into memory.

**What happens when a pod restarts?**  
It reads the latest plan from Kafka (if dynamic is on). The plan survives in Kafka across crashes.

**Do Solr and HDFS dispatchers need the partition plan?**  
No. They consume **all** partitions of `ranger_audits`. Only ingestor uses the plan to **choose** which partition to write to.

**Will changing the plan break consumers?**  
Adding partitions triggers normal consumer rebalance. Existing plugin slots keep the same partition numbers if you follow append-only promote/scale rules.

### Plan content and routing

**What is the buffer?**  
Partitions reserved for plugins that are not yet promoted (or newly seen plugin ids). Promote moves a plugin from buffer to dedicated slots.

**What does append-only mean?**  
When scaling, only **new** partition numbers at the end of `ranger_audits` are assigned. Existing plugins keep the same partition IDs in the same order.

**How is a partition chosen inside a plugin’s list?**  
Round-robin per plugin id — same behavior as static mode.

**Where does an unknown plugin send audits?**  
To the buffer partition pool (hash-based pick within buffer list in dynamic mode).

### REST and concurrency

**Do I need to restart ingestor after promote or scale?**  
No. Background sync applies the new plan within about one refresh interval (~30s).

**What is `expectedVersion`?**  
The plan `version` you believe is current when you write. If someone else published first, your version is stale and you get **409**.

**What should I do on HTTP 409?**  
Another writer published a newer plan. Use the plan in the 409 response (or `GET` again), note the new `version`, and retry your change with that `expectedVersion`.

**Why grow `ranger_audits` before publishing a new plan?**  
The plan must not reference partition IDs that do not exist yet. The server grows the audit topic tail first, then writes the plan.

**Why does promote return 400?**  
Common cases: plugin already has dedicated partitions, invalid partition count, or plugin id missing. Scale returns 400 if the plugin is not in the plan yet (promote it first).

**What do the HTTP status codes mean?**  
**503** — dynamic mode is off, or the server could not grow `ranger_audits` (Kafka admin failure). **400** — validation failed (bad shape, append-only violation, `topicPartitionCount` ≠ Kafka). **409** — version conflict; retry with the plan body returned in the response.

**Do dispatchers need reconfiguration after scale?**  
No. Consumer groups rebalance automatically when partition count increases. Tune consumer threads only if you see sustained lag.

### Cutover and bootstrap

**When is `ranger_audit_partition_plan` created?**  
Only when dynamic mode is enabled. With dynamic off, the topic is not created or used.

**Does bootstrap read Kafka to learn plugin → partition mapping?**  
No. The first plan is built from XML (same layout as static mode). Kafka is used for **total** partition count validation and as the durable registry.

**Can I publish the plan to Kafka before enabling dynamic mode?**  
Yes — recommended for brownfield clusters. Ingestor will read your pre-loaded plan and will not replace it with a fresh XML bootstrap.

**Greenfield vs brownfield cutover — what is different?**  
Greenfield: enable dynamic on an empty registry; first pod seeds from XML. Brownfield: export static layout, pre-seed the plan topic (or rely on bootstrap matching XML), then enable dynamic and verify every pod shows the same plan.

**Does the `configured.plugins` XML list still matter in dynamic mode?**  
Yes for **bootstrap only** when the registry is empty. After the first plan exists, runtime routing changes go through REST, not by editing that list.

**How do I verify cutover succeeded?**  
`GET /api/audit/partition-plan` on every pod (same `version`); plugin lists match your saved static logs; `topicPartitionCount` matches `kafka-topics --describe ranger_audits`.

**How do I roll back to static mode?**  
Export current plan via `GET`, align XML to that layout if needed, set `dynamic.enabled=false`, rolling restart. Plan topic remains but is ignored.

**What if multiple pods start together with an empty registry?**  
Race A: idempotent plan-topic create. Race B: re-read before/after first publish; all pods install the same plan. See [§4](#4-static--dynamic-cutover--direct-answers).

### Troubleshooting

**Ingestor fails startup after enabling dynamic — what now?**  
Often Kafka unreachable or cannot create/read the plan topic. Fix Kafka connectivity; keep dynamic off until Kafka is healthy. With Kafka down at startup, ingestor should **not** come up healthy in dynamic mode.

**Pods show different plan versions — what now?**  
Wait one refresh interval after a change. If still mismatched, check plan topic readability and watcher logs on the lagging pod.

**Does audit recovery / local spool behavior change?**  
No. Per-pod spool and retry when Kafka is briefly unavailable works as today.

**Does this replace producer throughput tuning?**  
No. Batch size, linger, and compression are separate settings.

**Who can call partition-plan REST?**  
Authenticated admin callers on ingestor (Kerberos/JWT). Plugin audit POST users are a different path — they send audits, not plan changes.

---

## Docs

| Doc | Purpose |
|-----|---------|
| [DESIGN-KAFKA-DYNAMIC-PARTITIONING.md](DESIGN-KAFKA-DYNAMIC-PARTITIONING.md) | Partition plan architecture |
| [README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md) | Partition-plan Kafka topic + REST |
| [README-KAFKA-PARTITION-PLAN-IMPLEMENTATION.md](README-KAFKA-PARTITION-PLAN-IMPLEMENTATION.md) | Partition-plan implementation phases |
| [README-DYNAMIC-SERVICE-ALLOWLIST-GUIDE.md](README-DYNAMIC-SERVICE-ALLOWLIST-GUIDE.md) | **Service allowlist** operator guide (Surface 1 — orthogonal to partition routing) |
| [README-DYNAMIC-SERVICE-ALLOWLIST-DESIGN.md](README-DYNAMIC-SERVICE-ALLOWLIST-DESIGN.md) | Service allowlist design proposal |
| [DESIGN-KAFKA-AUDIT-SERVER.md](DESIGN-KAFKA-AUDIT-SERVER.md) | End-to-end audit pipeline |
