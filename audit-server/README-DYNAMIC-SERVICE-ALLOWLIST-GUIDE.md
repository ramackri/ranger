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

# Dynamic service allowlist — merged into unified ingestor registry guide

**This guide has been merged** into the unified operator guide:

- **Repo:** [README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md](README-KAFKA-DYNAMIC-PARTITIONING-GUIDE.md) — *Dynamic ingestor registry for Ranger audit-ingestor*
- **Confluence:** [Dynamic Ingestor Registry Guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12043681813) (page `12043681813`)

Service allowlist (`services` map) and Kafka partition routing (`plugins` / `buffer`) now share one document on **`ranger_audit_partition_plan`** and one REST surface: **`/api/audit/partition-plan`** (including `POST .../services`).

**Design detail (allowlist authorization layers, security, API):** [README-DYNAMIC-SERVICE-ALLOWLIST-DESIGN.md](README-DYNAMIC-SERVICE-ALLOWLIST-DESIGN.md)

**Former Confluence page** (redirect stub): [Dynamic Service Allowlist Guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12055576591) → merged into page `12043681813`.
