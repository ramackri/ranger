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

# Kafka producer partitioning: plugin-based allocation (audit-ingestor)

**Design review summary:** [DESIGN-KAFKA-AUDIT-SERVER.md](DESIGN-KAFKA-AUDIT-SERVER.md) (short, non-duplicative).

This document explains how **audit-ingestor** (Kafka producer) allocates Kafka partitions per Ranger plugin (appId/agentId), and how to plan onboarding and scaling as new plugins are added over time.

Dynamic partitioning (plain-language guide): [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md)  
Engineering design notes: [README-KAFKA-PLUGIN-PARTITIONING-DYNAMIC-DESIGN.md](README-KAFKA-PLUGIN-PARTITIONING-DYNAMIC-DESIGN.md)  
**Detailed Kafka registry + REST design:** `README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md`  
**Producer throughput / tuning gaps:** `README-KAFKA-PRODUCER-PERFORMANCE.md`

---

## How partitions get allocated per plugin (current audit-ingestor code)

### Key idea: Kafka record key = plugin id

`audit-ingestor` publishes audit events to Kafka with:

- **topic**: `ranger.audit.ingestor.kafka.topic.name` (default `ranger_audits`)
- **key**: `AuthzAuditEvent.getAgentId()` (plugin id / agentId / appId)
- **value**: serialized audit JSON

This matters because Kafka partitioning is **key-based**.

### Two producer modes

`AuditProducer` chooses the partitioning strategy based on:

- `ranger.audit.ingestor.kafka.configured.plugins`

#### Mode A: Plugin-based partitioning (custom partitioner)

If `ranger.audit.ingestor.kafka.configured.plugins` is **non-empty**, the producer uses a custom partitioner:

- `ranger.audit.ingestor.kafka.partitioner.class` (default `org.apache.ranger.audit.producer.kafka.AuditPartitioner`)

Allocation algorithm (`AuditPartitioner`):

1. Parse `configured.plugins` list.
2. For each configured plugin, assign a **contiguous range** of partitions.
   - Default partitions per plugin:
     - `ranger.audit.ingestor.kafka.topic.partitions.per.configured.plugin` (default `3`)
   - Optional per-plugin override:
     - `ranger.audit.ingestor.kafka.plugin.partition.overrides.<plugin>=<N>`
3. The remaining partitions become the **buffer** range used for any plugin not listed in `configured.plugins`.

Routing behavior:

- **Configured plugin**: round-robin within its allocated range (load-balances that plugin across multiple partitions).
- **Unconfigured plugin**: hash within the buffer range (shares buffer partitions with other unconfigured plugins).

#### Mode B: Hash-based (Kafka default partitioner)

If `configured.plugins` is **empty/not set**, the producer uses Kafka’s default hashing partitioner.

- In this mode, the relevant partition count is:
  - `ranger.audit.ingestor.kafka.topic.partitions`

All plugins share all partitions based on hash(key).

---

## By default: how many Kafka partitions are created?

### Plugin-based mode (`configured.plugins` is set)

Topic creation (`AuditMessageQueueUtils.getPartitions`) **auto-calculates** partition count:

```text
topic partitions = Σ (partitions per plugin in configured.plugins) + buffer partitions
```

| Property | Default (shipped sample) | Role |
|----------|--------------------------|------|
| `ranger.audit.ingestor.kafka.topic.partitions.per.configured.plugin` | **3** | Per plugin unless overridden |
| `ranger.audit.ingestor.kafka.topic.partitions.buffer` | **9** | Reserved for plugins **not** in the list |
| `ranger.audit.ingestor.kafka.plugin.partition.overrides.<plugin>` | (none) | Optional per-plugin count |

`ranger.audit.ingestor.kafka.topic.partitions` (default **10** in XML) is used for **hash-based mode only**. In plugin-based mode, topic create/update uses the sum formula above, not `10`.

**Default site XML** leaves `configured.plugins` **empty** (hash-based static mode; dynamic greenfield bootstraps a **buffer-only** plan using `kafka.topic.partitions`). To pre-assign dedicated ranges at bootstrap, set an explicit comma-separated list (example plugin IDs: hdfs, yarn, knox, hiveServer2, hiveMetastore, kafka, hbaseRegional, hbaseMaster, solr, trino, ozone, kudu, nifi). A full 13-plugin layout yields a large topic (e.g. 13 × 3 + 9 buffer = **48** partitions).

### Hash-based mode (`configured.plugins` empty)

Topic uses `ranger.audit.ingestor.kafka.topic.partitions` → default **10**.

### Operational note (`topic.partitions` vs partitioner)

`AuditPartitioner` also reads `topic.partitions` for buffer sizing at startup, while topic creation uses the calculated sum. For consistent layout, align `topic.partitions` with the calculated total or rely on Kafka cluster metadata at produce time (partitioner uses actual topic size when routing).

---

## Walkthrough: hdfs + hiveServer2, then trino, then hot-plugin overrides

Examples use a **minimal** plugin list (not the full default list in sample XML):

```properties
ranger.audit.ingestor.kafka.topic.partitions.per.configured.plugin=3
ranger.audit.ingestor.kafka.topic.partitions.buffer=9
```

### Phase 1: Start with HDFS + Hive only

```properties
ranger.audit.ingestor.kafka.configured.plugins=hdfs,hiveServer2
```

| Plugin | Partitions | Partition IDs |
|--------|------------|---------------|
| hdfs | 3 (default) | **0–2** |
| hiveServer2 | 3 (default) | **3–5** |
| buffer (unconfigured / future plugins) | 9 | **6–14** |

**Topic created:** `3 + 3 + 9 = **15 partitions**` (0..14)

Trino (or storm/knox/kms, etc.) before onboarding → **buffer** (6–14), hash-distributed within buffer.

### Phase 2: Add Trino later

1. Append `trino` to `configured.plugins` (do **not** reorder existing entries).
2. Increase topic to **18** partitions (ingestor can increase existing topic on startup).
3. **Restart audit-ingestor** (ranges computed at producer startup).

```properties
ranger.audit.ingestor.kafka.configured.plugins=hdfs,hiveServer2,trino
```

| Plugin | Partitions | Partition IDs |
|--------|------------|---------------|
| hdfs | 3 | **0–2** |
| hiveServer2 | 3 | **3–5** |
| trino | 3 | **6–8** |
| buffer | 9 | **9–17** |

**Topic:** `3 + 3 + 3 + 9 = **18 partitions**`

### Phase 3: Hot plugins — more partitions (overrides)

In plugin-based mode, a hot plugin can only use **its own** partition range. Increase throughput with overrides:

```properties
ranger.audit.ingestor.kafka.plugin.partition.overrides.hiveServer2=6
ranger.audit.ingestor.kafka.plugin.partition.overrides.trino=9
# hdfs stays default 3
ranger.audit.ingestor.kafka.configured.plugins=hdfs,hiveServer2,trino
ranger.audit.ingestor.kafka.topic.partitions.buffer=9
```

| Plugin | Partitions | Partition IDs |
|--------|------------|---------------|
| hdfs | 3 | **0–2** |
| hiveServer2 | **6** | **3–8** |
| trino | **9** | **9–17** |
| buffer | 9 | **18–26** |

**Topic:** `3 + 6 + 9 + 9 = **27 partitions**` (0..26)

**Steps:**

1. Set overrides in `ranger-audit-ingestor-site.xml`.
2. Ensure Kafka topic has **≥ 27** partitions.
3. Restart **audit-ingestor**.
4. Scale **Solr/HDFS dispatchers** (consumer parallelism per group is capped by topic partition count).

### Quick reference table

| Stage | `configured.plugins` | Overrides | Topic partitions |
|-------|----------------------|-----------|------------------|
| Start | `hdfs,hiveServer2` | none | **15** |
| + trino | `hdfs,hiveServer2,trino` | none | **18** |
| Hot hive + trino | `hdfs,hiveServer2,trino` | hiveServer2=6, trino=9 (hdfs=3) | **27** |
| Hot hive + trino + hdfs | same | hiveServer2=6, trino=9, **hdfs=4** | **28** |

### Later change: add `plugin.partition.overrides.hdfs=4`

If you already run Phase 3 (hdfs=3, hiveServer2=6, trino=9, topic **27** partitions) and later set:

```properties
ranger.audit.ingestor.kafka.plugin.partition.overrides.hdfs=4
```

Allocation is still **contiguous in list order** (`hdfs`, then `hiveServer2`, then `trino`). Only hdfs’s block grows; every later plugin’s partition **numbers shift**.

| Plugin | Partitions | Before (hdfs=3) | After (hdfs=4) |
|--------|------------|-----------------|----------------|
| hdfs | **4** | 0–2 | **0–3** |
| hiveServer2 | 6 | 3–8 | **4–9** (+1 shift) |
| trino | 9 | 9–17 | **10–18** (+1 shift) |
| buffer | 9 | 18–26 | **19–27** (+1 shift) |

**Topic:** `4 + 6 + 9 + 9 = **28 partitions**` (0..27) — one more than the 27-partition layout.

**Operational steps:**

1. Add `ranger.audit.ingestor.kafka.plugin.partition.overrides.hdfs=4`.
2. Increase Kafka topic partitions **27 → 28** (ingestor can increase on startup).
3. Restart **audit-ingestor**.
4. Solr/HDFS dispatchers will **rebalance**; hive/trino traffic moves to new partition IDs.

**Important:** Changing an early plugin’s partition count reshuffles ranges for all plugins listed after it. To avoid that in production, prefer **append-only** growth (allocate new capacity from new tail partitions only) — see `README-KAFKA-PLUGIN-PARTITIONING-DYNAMIC-DESIGN.md`.

### Phase 4: Add more plugins over months (storm/knox/kms, …)

- New plugin can send audits immediately → lands in **buffer** (no config change).
- To dedicate capacity: append to `configured.plugins`, add override if high volume, increase topic partitions, restart ingestor.

---

## How to scale plugin partitions dynamically (what’s possible today vs what needs changes)

### What’s possible today (no code changes)

You can scale with **configuration + operational actions**:

1. **Increase Kafka topic partitions** (Kafka supports increase only).
2. Adjust `configured.plugins` and per-plugin overrides.
3. Restart audit-ingestor to apply a new partition layout.

This is “dynamic” in the sense that you can evolve the system without code, but it is not runtime-reconfigurable.

### What is not dynamic today (current limitations)

With current code:

- `AuditPartitioner` computes plugin → range mapping once during producer startup (`configure()`).
- There is no built-in watcher to reload `configured.plugins` or overrides.
- There is no control-plane that allocates new partition ranges for new plugins without restart.

So you cannot:

- automatically “promote” a plugin from buffer → dedicated partitions without a rollout/restart
- automatically increase a specific plugin’s partition allocation at runtime based on observed load

### What would need changes for true dynamic onboarding + scaling

If you need runtime automation (no restart, self-scaling), you’d typically introduce:

- **A persistent “partition registry”** (plugin → assigned partitions or weights), stored outside the process.
- **Dynamic refresh**: audit-ingestor periodically pulls the registry and updates routing.
- **Kafka Admin integration**: increase topic partitions when required (still increase-only).
- **A safe growth rule**: avoid reshuffling existing plugins’ ranges; prefer allocating new plugin ranges from newly added tail partitions.

These are product/code changes (not just configuration).

---

## Suggested changes to implement for your environment (practical plan)

This section lists the changes you can do **today** vs changes that are **architectural**.

### Recommended changes you can do today (config + ops)

- **Use plugin-based mode** (set `ranger.audit.ingestor.kafka.configured.plugins`) if you want isolation and predictable per-plugin throughput.
- **Reserve a real buffer** (enough partitions so new plugins don’t fight over 1–2 partitions).
- **Append-only discipline**:
  - never reorder `configured.plugins`
  - always append new plugins at the end
- **Per-plugin overrides for hot plugins**:
  - allocate more partitions to high-volume plugins (Trino/Hive)
- **Capacity planning**:
  - increase Kafka topic partitions ahead of onboarding waves
  - keep some slack so you aren’t resizing frequently
- **Restart audit-ingestor after allocation changes**:
  - required to apply new plugin partition ranges

### Recommended changes that require engineering work (if you truly need “dynamic”)

- **Dynamic mapping reload**: enable live reconfiguration of plugin allocations in `AuditPartitioner`.
- **Registry/control-plane**: a single source of truth for allocations (and change audit trail).
- **Auto partition expansion**: Kafka AdminClient to increase partitions when buffer is low.
- **Explicit onboarding workflow**:
  - “new plugin starts in buffer”
  - “promote plugin to dedicated partitions” (allocates new tail partitions, does not reshuffle)

**Proposed design (documented):** Kafka compacted topic `ranger_audit_partition_plan` + REST + background watcher. With `partition.plan.dynamic.enabled=true`, the **first ingestor pod** seeds that topic from the same XML properties above when the plan topic is empty; later pods read Kafka only. See `README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md` → [First pod: XML populates the plan topic](README-KAFKA-PARTITION-PLAN-REGISTRY-REST.md#first-pod-xml-populates-the-plan-topic).

---

## Key configuration reference (audit-ingestor)

| Property | Purpose |
|----------|---------|
| `ranger.audit.ingestor.kafka.topic.name` | Kafka topic name |
| `ranger.audit.ingestor.kafka.configured.plugins` | Enables plugin-based partitioning and lists onboarded plugins |
| `ranger.audit.ingestor.kafka.topic.partitions.per.configured.plugin` | Default partitions per configured plugin (default **3**) |
| `ranger.audit.ingestor.kafka.topic.partitions.buffer` | Buffer partitions for unlisted plugins (default **9**) |
| `ranger.audit.ingestor.kafka.plugin.partition.overrides.<plugin>` | Per-plugin partition override |
| `ranger.audit.ingestor.kafka.topic.partitions` | Topic size in **hash-based** mode (default **10**); also read by partitioner for buffer math in plugin-based mode |

