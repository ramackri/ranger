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

import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanConflictException;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlanReplaceRequest;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.producer.kafka.partition.model.PromotePluginRequest;
import org.apache.ranger.audit.producer.kafka.partition.model.ScalePluginRequest;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanServiceMutationTest {
    private static final String TOPIC = "ranger_audits";

    private PartitionPlan initialPlan;

    @BeforeEach
    public void setUp() {
        initialPlan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs", "hiveServer2"}, 3, 9));
    }

    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testPromotePluginPublishesNextVersion() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.promotePlugin(new PromotePluginRequest("trino", 3, 1), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(2, registry.getPlan().getVersion());
        assertIterableEquals(List.of(6, 7, 8), result.getPlugins().get("trino").getPartitions());
        assertEquals(1, registry.getWriteCount());
        assertEquals(result, PartitionPlanHolder.getInstance().getPlan());
    }

    @Test
    public void testScalePluginAppendsTailPartitions() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.scalePlugin(new ScalePluginRequest("hiveServer2", 2, 1), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(17, result.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16), result.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testReplacePlanHonorsAppendOnly() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>(initialPlan.getPlugins());
        plugins.put("hiveServer2", PluginPartitionAssignment.of(3, 4, 5, 15, 16, 17));
        PartitionPlanReplaceRequest request = new PartitionPlanReplaceRequest(1, 18, plugins, initialPlan.getBuffer());

        PartitionPlan result = service.replacePartitionPlan(request, "ops");

        assertEquals(2, result.getVersion());
        assertEquals(18, result.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16, 17), result.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testStaleExpectedVersionReturnsConflict() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanConflictException conflict = assertThrows(PartitionPlanConflictException.class,
                () -> service.promotePlugin(new PromotePluginRequest("trino", 3, 99), "ops"));

        assertEquals(initialPlan, conflict.getCurrentPlan());
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testConflictWhenPeerPublishedBeforeWrite() {
        PartitionPlan peerPlan = initialPlan.toBuilder().version(2).updatedBy("peer").build();
        MutableRegistry registry = new MutableRegistry(initialPlan) {
            private final AtomicInteger reads = new AtomicInteger();

            @Override
            public PartitionPlan readPlan(String auditTopicKey) {
                if (reads.incrementAndGet() >= 2) {
                    return peerPlan;
                }
                return super.readPlan(auditTopicKey);
            }
        };
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanConflictException conflict = assertThrows(PartitionPlanConflictException.class,
                () -> service.promotePlugin(new PromotePluginRequest("trino", 3, 1), "ops"));

        assertEquals(peerPlan, conflict.getCurrentPlan());
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testTopicGrowFailureSurfacesAsPlanException() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new FailingAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.promotePlugin(new PromotePluginRequest("trino", 12, 1), "ops"));

        assertTrue(error.getMessage().contains("grow audit topic"));
        assertEquals(0, registry.getWriteCount());
    }

    private static PartitionPlanService service(MutableRegistry registry, KafkaAuditTopicPartitionGrower topicGrower) {
        return new PartitionPlanService(enabledConfig(), PartitionPlanHolder.getInstance(), new FixedPartitionPlanRegistryFactory(registry), topicGrower);
    }

    private static Properties enabledConfig() {
        Properties props = new Properties();
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_NAME, TOPIC);
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, "true");
        return props;
    }

    private static final class FixedPartitionPlanRegistryFactory extends PartitionPlanRegistryFactory {
        private final PartitionPlanRegistry registry;

        private FixedPartitionPlanRegistryFactory(PartitionPlanRegistry registry) {
            this.registry = registry;
        }

        @Override
        public PartitionPlanRegistry open(Properties props, String propPrefix) {
            return registry;
        }
    }

    private static final class NoOpAuditTopicPartitionGrower extends KafkaAuditTopicPartitionGrower {
        @Override
        public void growAuditTopicToRequiredPartitionCount(Properties props, String propPrefix, String auditTopicName, int requiredPartitionCount) {
        }
    }

    private static final class FailingAuditTopicPartitionGrower extends KafkaAuditTopicPartitionGrower {
        @Override
        public void growAuditTopicToRequiredPartitionCount(Properties props, String propPrefix, String auditTopicName, int requiredPartitionCount) {
            throw new RuntimeException("kafka down");
        }
    }

    private static class MutableRegistry implements PartitionPlanRegistry {
        private PartitionPlan plan;
        private int writeCount;

        private MutableRegistry(PartitionPlan plan) {
            this.plan = plan;
        }

        @Override
        public String getPlanTopicName() {
            return "ranger_audit_partition_plan";
        }

        @Override
        public PartitionPlan readPlan(String auditTopicKey) {
            return plan;
        }

        @Override
        public void writePlan(String auditTopicKey, PartitionPlan newPlan) {
            plan = newPlan;
            writeCount++;
        }

        @Override
        public void close() {
        }

        private PartitionPlan getPlan() {
            return plan;
        }

        private int getWriteCount() {
            return writeCount;
        }
    }
}
