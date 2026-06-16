/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionPlanAllocatorTest {
    private PartitionPlan initialPlan;

    @BeforeEach
    public void setUp() {
        initialPlan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create("ranger_audits", new String[] {"hdfs", "hiveServer2"}, 3, 9));
    }

    @Test
    public void testPromotePluginFromBuffer() {
        PartitionPlan next = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops");

        assertEquals(2, next.getVersion());
        assertEquals(15, next.getTopicPartitionCount());
        assertIterableEquals(List.of(6, 7, 8), next.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of(9, 10, 11, 12, 13, 14), next.getBuffer().getPartitions());
        assertIterableEquals(List.of(0, 1, 2), next.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(3, 4, 5), next.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testPromotePluginGrowsTopicWhenBufferInsufficient() {
        PartitionPlan next = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 12, "ops");

        assertEquals(18, next.getTopicPartitionCount());
        assertIterableEquals(List.of(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17), next.getPlugins().get("trino").getPartitions());
        assertEquals(0, next.getBuffer().size());
    }

    @Test
    public void testScalePluginAppendsTailOnly() {
        PartitionPlan promoted = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops");
        PartitionPlan scaled   = PartitionPlanAllocator.scalePlugin(promoted, "hiveServer2", 3, "ops");

        assertEquals(3, scaled.getVersion());
        assertEquals(18, scaled.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16, 17), scaled.getPlugins().get("hiveServer2").getPartitions());
        assertIterableEquals(List.of(0, 1, 2), scaled.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(6, 7, 8), scaled.getPlugins().get("trino").getPartitions());
    }

    @Test
    public void testPromoteAlreadyConfiguredPluginFails() {
        assertThrows(PartitionPlanException.class, () -> PartitionPlanAllocator.promotePlugin(initialPlan, "hdfs", 1, "ops"));
    }

    @Test
    public void testScaleUnknownPluginFails() {
        assertThrows(PartitionPlanException.class, () -> PartitionPlanAllocator.scalePlugin(initialPlan, "trino", 2, "ops"));
    }
}
