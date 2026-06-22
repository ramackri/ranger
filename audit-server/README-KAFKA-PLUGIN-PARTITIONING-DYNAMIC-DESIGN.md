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

# Dynamic plugin onboarding + dynamic partition scaling (design notes)

> **Consolidated design (start here for review):** [DESIGN-KAFKA-DYNAMIC-PARTITIONING.md](DESIGN-KAFKA-DYNAMIC-PARTITIONING.md) — architecture, flows, diagrams, brownfield migration, and Q&A in one document. This README adds engineering checklist and implementation detail.

**Plain-language overview (operators, architects, new readers):** [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md)

This document proposes **code/engineering changes** to evolve `audit-ingestor` plugin-based Kafka partitioning from **static-at-startup** to **runtime dynamic**:

- onboard new plugins without restart
- increase hot-plugin capacity without reshuffling existing plugins
- (optionally) auto-increase Kafka topic partitions

Current behavior (static config-based mapping) is documented in:

- `audit-server/README-KAFKA-PLUGIN-PARTITIONING.md`

**Detailed design (Kafka registry + REST):** `audit-server/README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md`  
**Phased implementation plan (code + tests by phase):** [README-KAFKA-PARTITION-PLAN-IMPLEMENTATION.md](README-KAFKA-PARTITION-PLAN-IMPLEMENTATION.md)  
**Producer performance (separate topic):** `audit-server/README-KAFKA-PRODUCER-PERFORMANCE.md`

---

## Current limitation (why changes are needed)

Today `AuditPartitioner` builds plugin → partition-range mapping once in `configure()`. There is:

- no runtime reload of `configured.plugins` / overrides
- no durable record of “which plugin owns which partitions”
- no safe way to add a new plugin without changing ranges for other plugins (contiguous-range algorithm depends on ordering)

So “dynamic” today requires: config update + topic partition update + **restart**.

---

## Goals and non-goals

### Goals

- **G1: Stable allocations**: onboarding/scaling a plugin should not move other plugins’ traffic unnecessarily.
- **G2: Runtime updates**: update mapping without restarting `audit-ingestor`.
- **G3: Append-only growth**: only allocate from newly added tail partitions (Kafka can increase partitions, not decrease).
- **G4: Backward compatibility**: existing config (`configured.plugins`, overrides) can still be used as a bootstrap/default.

### Non-goals

- Decreasing topic partition counts (Kafka does not support shrinking partitions).
- Perfect global ordering guarantees across plugins (already not guaranteed when plugins use multiple partitions).

---

## Recommended architecture (pragmatic)

### 1) Introduce a Partition Registry (source of truth)

Define a small interface (conceptually):

- **Inputs**: topic name
- **Outputs**:
  - plugin → assigned partitions (range or explicit list)
  - buffer partition set (for unconfigured/unonboarded plugins)
  - topic partition count the plan was built for (for safety)

Backend choices (pick one based on your environment):

| Backend | New infra? | Multi-pod sync | Survives pod restart | Notes |
|---------|------------|----------------|----------------------|-------|
| **Kafka compacted config topic** (recommended) | No — uses existing Kafka | Yes — all ingestors consume same topic | Yes — plan lives in Kafka | Best fit when ingestor already depends on Kafka |
| **File-based JSON** (ConfigMap + volume) | Minimal | Requires shared read + rollout or poll | Only if volume/ConfigMap is durable | Good for single replica or GitOps rollout |
| **DB (Postgres)** | Yes | Yes | Yes | Auditable; **not** a current ingestor dependency |
| **ZooKeeper** | Yes | Yes | Yes | Kafka-era pattern; **not** a current ingestor dependency |

**Operational preference:** avoid Postgres/ZooKeeper for partition plan storage. Use **Kafka as the source of truth** plus **Kafka AdminClient** for partition increases — both are already required for audit-ingestor.

See [Alternative: Kafka-backed registry + REST (no Postgres/ZK)](#alternative-kafka-backed-registry--rest-no-postgreszk) below.

### 2) Make `AuditPartitioner` consume a Partition Plan

Replace “compute ranges from configured.plugins at startup” with:

- `AtomicReference<PartitionPlan>` inside partitioner
- `partition()` consults the current plan

Plan contents:

- `Map<String, IntRange>` OR `Map<String, int[]>` (explicit partitions)
- `int[] bufferPartitions` OR `(bufferStart, bufferCount)` if contiguous
- metadata:
  - `planVersion`
  - `topicPartitionCount`
  - timestamps

### 3) Add runtime refresh (no restart)

In `AuditPartitioner.configure()`:

- build initial plan (from registry or fallback config)
- start a lightweight scheduled refresh (every N seconds) that:
  - reads registry
  - validates it against current Kafka topic partitions (from `Cluster.partitionsForTopic`)
  - atomically swaps the plan

Refresh should be:

- safe under concurrent calls to `partition()`
- resilient to registry failures (keep last known good plan)

### 4) Adopt an append-only allocation rule (critical)

To avoid reshuffling existing plugins:

- never recompute ranges based on `configured.plugins` ordering
- when a plugin needs more capacity or a new plugin is promoted:
  - **increase topic partitions** (if needed)
  - allocate only from the **new tail partitions** (previously unassigned)

This preserves:

- existing plugin partition ownership
- consumer-side locality and throughput behavior

---

## Optional: Auto-increase topic partitions (AdminClient)

If you want `audit-ingestor` to scale partitions automatically:

- add a component using Kafka `AdminClient` to:
  - fetch current partition count
  - increase partitions when requested by the control plane / policy

Important considerations:

- permissions: `CreatePartitions` / topic admin rights required
- coordination: avoid multiple ingestors racing (use registry/DB lock or single leader)
- safety: only increase; never attempt decrease

---

## Onboarding workflow (suggested)

### Stage 0: Unknown plugin arrives

- plugin id not in registry
- route to **buffer partitions**
- monitor per-plugin volume (metrics/logging)

### Stage 1: Promote plugin

- update registry to assign dedicated partitions (initial allocation)
- plan refresh applies it without restart

### Stage 2: Scale hot plugin

When metrics indicate sustained load:

- add more partitions to the topic (if needed)
- allocate additional tail partitions to that plugin in the registry
- refresh applies change

---

## Suggested registry data model (example JSON)

This is an example shape (not a committed schema):

```json
{
  "topic": "ranger_audits",
  "version": 12,
  "topicPartitionCount": 48,
  "plugins": {
    "hdfs":        { "partitions": [0,1,2,3,4,5] },
    "hiveServer2": { "partitions": [6,7,8,9,10,11] },
    "trino":       { "partitions": [12,13,14,15,16,17,18,19,20] }
  },
  "buffer": { "partitions": [21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47] }
}
```

If you prefer ranges:

- `{"range": {"start": 0, "end": 5}}` etc.

---

## Key trade-offs and decisions

- **Explicit partition lists vs ranges**
  - lists give maximum flexibility (non-contiguous tail allocations)
  - ranges are simpler but can constrain append-only growth if you want to keep partitions contiguous

- **Who writes the registry**
  - manual ops (initially)
  - future: automated controller (Kubernetes job / service) that watches metrics and updates registry

- **Ordering guarantees**
  - allocating multiple partitions per plugin increases throughput but reduces strict per-plugin ordering across partitions
  - ensure consumers don’t rely on strict order across partitions for correctness

---

## Implementation checklist (code vs deployment)

This section is a concrete “what to change” checklist to implement dynamic onboarding/scaling and to run multiple ingestor replicas safely.

### Code changes (dynamic partitioning)

- **Partition plan model**
  - Add a `PartitionPlan` representation (plugin → partitions/ranges + buffer partitions + metadata: version, topic partition count).
- **Partition registry**
  - Add a `PartitionRegistry` interface; **required impl for dynamic mode:** Kafka compacted topic (`ranger_audit_partition_plan`).
  - XML / `configured.plugins` seeds the **initial** plan only when the compacted topic is empty (first pod bootstrap); avoid Postgres/ZK unless already in your stack.
- **`AuditPartitioner` runtime plan**
  - Replace one-time contiguous-range calculation with `AtomicReference<PartitionPlan>`.
  - `partition()` consults the current plan loaded from the registry (in memory on the hot path).
  - Do not use a config-only plan without the registry for runtime updates or multi-pod consistency.
- **Refresh loop (no restart)**
  - Add a scheduled refresh (every N seconds) to reload the registry and atomically swap the plan.
  - Validate the plan against `Cluster.partitionsForTopic(topic)` before applying.
- **Append-only allocation rule**
  - Add an allocation strategy/tooling so new plugins and “add capacity” operations allocate only from **new tail partitions** (no reshuffle of existing plugin allocations).
- **Optional: Kafka AdminClient**
  - Add an AdminClient helper to increase topic partitions when the control plane requests it (increase-only).
- **Testing**
  - Concurrency tests: plan swap while `partition()` is hot.
  - Behavior tests: configured plugin routing vs buffer routing; promotion/scaling does not move existing allocations.

### Code changes (audit recovery)

- **No code changes required** for the current recovery flow (per-pod spool + per-pod retry).
- Code changes are only needed if you want shared/centralized retry across replicas (leader election/locking or external queue), which is a separate design.

### Deployment/config changes (dynamic mapping control plane)

- **Kafka compacted plan topic**
  - Create `ranger_audit_partition_plan` (1 partition, cleanup.policy=compact).
  - Grant ingestor principal permission to produce to plan topic and `createPartitions` on audit topic.
- **Bootstrap XML**
  - Keep `configured.plugins` / overrides as defaults when plan topic is empty.
  - First pod with `dynamic.enabled=true` publishes v1 from XML to `ranger_audit_partition_plan` (see [First pod bootstrap](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#first-pod-xml-populates-the-plan-topic)).
- **Refresh interval**
  - `ranger.audit.ingestor.kafka.partition.plan.refresh.interval.ms`
- **REST admin auth**
  - Restrict partition-plan endpoints to admin roles (same pattern as audit `/access` authorization).

---

See [Detailed elaboration: Kafka-backed registry + REST](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md) for the full end-to-end design (architecture, REST API, multi-pod sync, examples).

### What not to do: edit XML inside the pod filesystem

Properties like these are **bootstrap defaults**, not a good runtime store for a multi-replica deployment:

- `ranger.audit.ingestor.kafka.configured.plugins`
- `ranger.audit.ingestor.kafka.partitioner.class`
- `ranger.audit.ingestor.kafka.topic.partitions.per.configured.plugin`
- `ranger.audit.ingestor.kafka.plugin.partition.overrides.<plugin>`

If a REST handler only writes `ranger-audit-ingestor-site.xml` on **one pod’s local disk**:

- **Other pods never see the change** (each pod has its own filesystem).
- **Restart/reschedule loses the change** (ephemeral container filesystem).
- **Rolling update from Git/ConfigMap** can overwrite in-memory or local edits.

**Runtime partition plan must live in durable shared storage** — recommended: a **Kafka compacted topic**, not pod-local XML.

Keep XML/ConfigMap for **initial bootstrap** (first startup when no plan exists yet). See [First pod: XML populates the plan topic](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#first-pod-xml-populates-the-plan-topic).

#### First pod bootstrap (XML → `ranger_audit_partition_plan`)

Requires `ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled=true`.

| Step | What happens |
|------|----------------|
| 1 | First ingestor pod starts; plan topic is empty |
| 2 | Watcher reads XML: `configured.plugins`, overrides, buffer, default per-plugin count |
| 3 | Builds plan **v1** (contiguous ranges → explicit partition lists) |
| 4 | **Publishes v1** to `ranger_audit_partition_plan` (key `ranger_audits`) |
| 5 | Loads v1 into memory; serves audits |

Later pods and restarts **read Kafka only** — they do not re-seed from XML.

| Condition | Plan topic populated from XML? |
|-----------|-------------------------------|
| `dynamic.enabled=true`, empty plan topic | **Yes** (first pod) |
| `dynamic.enabled=true`, plan exists | **No** |
| `dynamic.enabled=false` or absent | **No** (legacy mode; plan topic unused) |

**Example v1** and full property list: [README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md → First pod: XML populates the plan topic](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#first-pod-xml-populates-the-plan-topic).

### Recommended design: Kafka compacted topic as partition registry

Introduce a dedicated Kafka topic, e.g. `ranger_audit_partition_plan`:

| Property | Suggested value |
|----------|-----------------|
| Partitions | 1 (single key = audit topic name) |
| Cleanup policy | `compact` (keeps latest plan per key) |
| Retention | compacted — survives broker retention if configured appropriately |

**Message key:** audit topic name (e.g. `ranger_audits`)  
**Message value:** JSON `PartitionPlan` (version, explicit partition lists per plugin, buffer list, `topicPartitionCount`)

**Append vs update, compaction, and performance:** each plan change appends a new record (not an in-place edit); compaction retains the latest value per key. This does not affect audit ingest hot path. Details: [README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md → Compacted topic semantics](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#compacted-topic-semantics-append-retention-and-performance).

**Why this works without new dependencies:**

- Ingestor already connects to Kafka (producer + existing `AdminClient` for topic create).
- Plan is **durable in Kafka** → survives ingestor pod crash/restart.
- All ingestor replicas **read the same plan** → consistent routing.

### Kafka AdminClient: increase audit topic partitions

When the plan needs more capacity (new plugin, scale hot plugin, grow buffer):

1. Compute required `topicPartitionCount` (append-only: only add **tail** partitions).
2. Call `AdminClient.createPartitions()` on `ranger_audits` (increase only).
3. Publish updated `PartitionPlan` JSON to `ranger_audit_partition_plan`.

Reuse/extend existing logic in `AuditMessageQueueUtils` (already increases partitions on startup for static config).

**Permissions:** ingestor service principal needs `ALTER` / create-partitions on the audit topic.

### New REST endpoint on `AuditREST` (control plane)

Add an admin-only API (authenticated, not public like `/health`), e.g.:

```text
PATCH /api/audit/partition-plan
GET /api/audit/partition-plan
POST /api/audit/partition-plan/plugins
PATCH /api/audit/partition-plan/plugins/{pluginId}
POST /api/audit/partition-plan/services
```

**Handler flow (single transactional intent):**

1. Validate request (plugin id, desired partition count, append-only rules).
2. Load current plan from Kafka compacted topic (or bootstrap from XML if empty).
3. Allocate new partitions from **tail only** (do not reshuffle existing plugin lists).
4. If `requiredPartitions > currentTopicPartitions` → `AdminClient.createPartitions`.
5. Write new plan to `ranger_audit_partition_plan` (bump `version`).
6. Return new plan + version.

**Do not** rewrite `ranger-audit-ingestor-site.xml` on the pod for runtime updates.

Optional: after successful Kafka write, ops may update Git/ConfigMap **as documentation** — not required for runtime.

### How ingestor pods apply changes (no restart)

Each ingestor pod runs a **`PartitionPlanWatcher`** background thread only:

```text
Startup (first pod, empty topic):
  1. Build v1 from XML → publish to compacted topic → install in AtomicReference

Startup (later pods or restart):
  1. Read latest plan from compacted topic → install in AtomicReference
  2. Do NOT re-bootstrap from XML if plan exists

Runtime:
  3. Watcher consumes/polls plan topic (interval refresh)
  4. On new plan version → validate → atomic swap in AuditPartitioner

Audit hot path:
  5. AuditPartitioner.partition() → partitionPlanRef.get() only (no Kafka)
```

**Multi-pod:** pod 1 seeds Kafka from XML once; pod 2+ always use Kafka. REST and watcher are the **only** components that read/write the plan topic.

**Crash / restart:** pod reconnects to Kafka, reads latest compacted plan → **same routing as before crash**. No dependency on pod-local files.

**Other pods:** each pod’s watcher sees the same compacted message → **all ingestors route consistently** within seconds (bounded by poll interval).

See `README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md` → **Bootstrap and multi-pod startup** and **Kafka read load**.

### Multi-pod REST: who handles the PUT?

Any ingestor pod can serve REST behind the load balancer. **Writes are coordinated** without a leader by default:

| Approach | Complexity | Recommendation |
|----------|------------|----------------|
| **Optimistic concurrency + compare-and-swap** (`expectedVersion`, re-read before produce, read-back) | Low | **Default** — client retries on 409 |
| **Single leader** (only one pod executes PUT) | Medium | Optional — K8s Lease if zero 409 retries is required |
| **External operator** (CI job calls REST once) | Low | Good for manual onboarding |

**Full flow, sequence diagram, and 409 retry pattern:** [README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md → Concurrent updates from multiple ingestor pods](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#concurrent-updates-from-multiple-ingestor-pods)

Avoid two concurrent PUTs allocating the same tail partitions — server always re-reads before produce; loser gets 409 and must allocate from the new tail on retry.

### Mapping from today’s XML properties to the plan

| Today (XML) | Dynamic model |
|-------------|---------------|
| `configured.plugins` | Keys in `plan.plugins` |
| `plugin.partition.overrides.<plugin>` | Length of `plan.plugins[plugin].partitions` (explicit list, not contiguous ranges) |
| `topic.partitions.per.configured.plugin` | Default count when **promoting** a new plugin |
| `topic.partitions.buffer` | `plan.buffer.partitions` list (grow tail when buffer shrinks) |
| `partitioner.class` | Still set once at bootstrap (`AuditPartitioner`); runtime behavior from plan |

**Append-only example:** hdfs has `[0,1,2]`; scale hdfs to 4 → allocate partition `3` from new tail (after topic increase), new list `[0,1,2,3]` — hive/trino lists **unchanged**.

This avoids the reshuffle problem documented in `README-KAFKA-PLUGIN-PARTITIONING.md` when changing early plugins in contiguous-range mode.

### Impact on Solr/HDFS dispatchers

**No change required** for dispatchers when the plan updates:

- They consume **all** partitions of `ranger_audits` via their consumer groups (`README-KAFKA-DISPATCHERS.md`).
- More partitions → rebalance → more consumer parallelism (up to partition count).
- They do **not** read `configured.plugins` or partition overrides.

Tune dispatchers separately (`CooperativeStickyAssignor`, `max.poll.interval.ms`, etc.) if hot plugins increase partition traffic.

### Impact on recovery (`AuditRecoveryManager` / `AuditRecoveryWriter` / `AuditRecoveryRetry`)

No code change required for recovery. Retries resend `(topic, key=agentId, value)`; partition may differ if plan changed since original send — acceptable for audit pipelines.

### Phased implementation (Kafka-centric)

Dynamic partitioning is **not** viable without a shared registry (Kafka compacted plan topic). Do not ship an in-memory-only or config-only intermediate — it is not durable or safe across pods/restarts.

1. **`PartitionPlan` + append-only allocator** (unit tests).
2. **Kafka registry**: compacted topic create + read/write helpers.
3. **`AuditPartitioner`**: `AtomicReference` + plan-based routing (XML bootstrap only when plan topic is empty).
4. **`PartitionPlanWatcher`**: background refresh on all ingestor pods.
5. **`AuditREST` admin endpoints**: POST/PATCH plugins + POST services → AdminClient + registry write.
6. **AuthZ** on admin endpoints (same pattern as `/access` allowed-users).
7. **Metrics/logging**: expose current `plan.version` on `/status`.
8. **Ops runbook**: [README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md](README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md) — REST workflow, 409 retry, sample XML.
9. **Brownfield migration**: [README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md](README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md) — pre-seed plan v1, cutover checklist, rollback.

### Configuration additions (bootstrap / feature flag)

```xml
<!-- Optional: only needed when enabling dynamic mode -->
<property>
  <name>ranger.audit.ingestor.kafka.partition.plan.topic</name>
  <value>ranger_audit_partition_plan</value>
</property>
<property>
  <name>ranger.audit.ingestor.kafka.partition.plan.refresh.interval.ms</name>
  <value>30000</value>
</property>
<property>
  <name>ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled</name>
  <value>false</value>
  <description>
    false or absent = legacy XML AuditPartitioner at startup (today's behavior).
    true = Kafka registry + REST + PartitionPlanWatcher.
  </description>
</property>
```

When **`dynamic.enabled` is `false` or not set**: use existing `configured.plugins` / overrides / restart workflow (`README-KAFKA-PLUGIN-PARTITIONING.md`).

When **`dynamic.enabled` is `true`**: `configured.plugins` / overrides seed the initial plan only if the compacted topic is empty. **`partition.plan.topic` is optional** — defaults to `ranger_audit_partition_plan`.

See `README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md` → **Feature flag: backward compatibility** and **Property defaults when dynamic.enabled=true**.

**Operators:** step-by-step enablement and REST examples → [README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md](README-KAFKA-PARTITION-PLAN-OPS-RUNBOOK.md). Live-cluster cutover → [README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md](README-KAFKA-PARTITION-PLAN-BROWNFIELD-MIGRATION.md).

### Design assessment

The recommended approach (Kafka compacted plan topic + REST + background watcher, in-memory partitioner on the audit hot path) is **appropriate for Ranger**: Kafka is already required, plan changes are rare, and multi-pod consistency does not depend on pod-local files. Tradeoffs (startup dependency on Kafka when dynamic=true, ~30s watcher convergence, bootstrap race handling) are acceptable for plugin onboarding workflows.

**Full rationale, alternatives, and when *not* to use this pattern:** [README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md → Design rationale and tradeoffs](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#design-rationale-and-tradeoffs)

### Summary

| Question | Answer |
|----------|--------|
| Postgres/ZK needed? | **No** — use Kafka compacted topic + AdminClient |
| Update XML in pod? | **No** for runtime — XML is bootstrap only |
| REST endpoint? | **Yes** — writes plan to Kafka + increases partitions via AdminClient |
| Pod crash/restart? | Re-read plan from Kafka compacted topic |
| Other pods notified? | All pods watch same topic / poll same plan |
| Dispatchers affected? | Rebalance only when partition count grows; no plan API needed |

**Full design:** [README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md)

