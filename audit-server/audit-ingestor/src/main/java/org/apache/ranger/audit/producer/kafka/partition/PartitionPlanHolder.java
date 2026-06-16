/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;

import java.util.concurrent.atomic.AtomicReference;

/** Hot-path in-memory plan for {@code AuditPartitioner} and the background watcher. */
public final class PartitionPlanHolder {
    private static final PartitionPlanHolder INSTANCE = new PartitionPlanHolder();

    private final AtomicReference<PartitionPlan> planRef = new AtomicReference<>();
    private volatile int lastInstalledVersion;

    private PartitionPlanHolder() {
    }

    public static PartitionPlanHolder getInstance() {
        return INSTANCE;
    }

    public PartitionPlan getPlan() {
        return planRef.get();
    }

    public int getLastInstalledVersion() {
        return lastInstalledVersion;
    }

    /** Validates and atomically installs the plan used by the Kafka partitioner. */
    public void install(PartitionPlan plan, Integer kafkaPartitionCount) {
        PartitionPlanValidator.validate(plan, kafkaPartitionCount);
        planRef.set(plan);
        lastInstalledVersion = plan.getVersion();
    }

    /** Clears holder state between unit tests. */
    public void resetForTests() {
        planRef.set(null);
        lastInstalledVersion = 0;
    }
}
