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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Append-only plan updates: promote unknown plugins and scale hot plugins without reshuffling. */
public final class PartitionPlanAllocator {
    private PartitionPlanAllocator() {
    }

    /**
     * Give a plugin its own partitions. Uses buffer IDs first; adds new tail IDs when buffer is too small.
     */
    public static PartitionPlan promotePlugin(PartitionPlan current, String pluginId, int partitionCount, String updatedBy) {
        requireMutationInputs(current, pluginId, partitionCount, updatedBy);
        if (current.getPlugins().containsKey(pluginId)) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has dedicated partitions");
        }

        List<Integer> remainingBuffer = new ArrayList<>(current.getBuffer().getPartitions());
        List<Integer> newPluginIds    = takeFromBuffer(remainingBuffer, partitionCount);
        int topicPartitionCount       = appendTailPartitions(newPluginIds, current.getTopicPartitionCount(), partitionCount - newPluginIds.size());

        return commitPlanUpdate(current, updatedBy, topicPartitionCount, addPluginAssignment(current, pluginId, newPluginIds), remainingBuffer);
    }

    /**
     * Add more partitions to an existing plugin by appending new tail IDs only.
     */
    public static PartitionPlan scalePlugin(PartitionPlan current, String pluginId, int additionalPartitions, String updatedBy) {
        requireMutationInputs(current, pluginId, additionalPartitions, updatedBy);
        if (!current.getPlugins().containsKey(pluginId)) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' is not configured; promote it first");
        }

        List<Integer> pluginIds = new ArrayList<>(current.getPlugins().get(pluginId).getPartitions());
        int topicPartitionCount = appendTailPartitions(pluginIds, current.getTopicPartitionCount(), additionalPartitions);

        return commitPlanUpdate(current, updatedBy, topicPartitionCount, addPluginAssignment(current, pluginId, pluginIds), current.getBuffer().getPartitions());
    }

    /** Full plan replace (REST PUT) with append-only checks against the current plan. */
    public static PartitionPlan replacePlan(PartitionPlan current, PartitionPlan proposed) {
        if (current == null || proposed == null) {
            throw new PartitionPlanException("Current and proposed plans are required");
        }
        if (!StringUtils.equals(current.getTopic(), proposed.getTopic())) {
            throw new PartitionPlanException("Proposed topic must match current topic");
        }
        PartitionPlan next = proposed.toBuilder().version(current.getVersion() + 1).build();
        PartitionPlanValidator.validate(next);
        PartitionPlanValidator.validateAppendOnly(current, next);
        return next;
    }

    /** Pull up to count partition IDs from the front of the buffer list. */
    private static List<Integer> takeFromBuffer(List<Integer> bufferIds, int count) {
        List<Integer> taken = new ArrayList<>(Math.min(count, bufferIds.size()));
        while (taken.size() < count && !bufferIds.isEmpty()) {
            taken.add(bufferIds.remove(0));
        }
        return taken;
    }

    /** Append new tail partition IDs and return the new topic partition count. */
    private static int appendTailPartitions(List<Integer> target, int topicPartitionCount, int count) {
        for (int i = 0; i < count; i++) {
            target.add(topicPartitionCount++);
        }
        return topicPartitionCount;
    }

    private static Map<String, PluginPartitionAssignment> addPluginAssignment(PartitionPlan current, String pluginId, List<Integer> partitionIds) {
        Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>(current.getPlugins());
        plugins.put(pluginId, new PluginPartitionAssignment(partitionIds));
        return plugins;
    }

    private static PartitionPlan commitPlanUpdate(PartitionPlan current, String updatedBy, int topicPartitionCount, Map<String, PluginPartitionAssignment> plugins, List<Integer> bufferIds) {
        PartitionPlan next = current.toBuilder()
                .version(current.getVersion() + 1)
                .topicPartitionCount(topicPartitionCount)
                .plugins(plugins)
                .buffer(new PluginPartitionAssignment(bufferIds))
                .updatedAt(Instant.now().toString())
                .updatedBy(updatedBy)
                .build();
        PartitionPlanValidator.validate(next);
        PartitionPlanValidator.validateAppendOnly(current, next);
        return next;
    }

    private static void requireMutationInputs(PartitionPlan current, String pluginId, int partitionCount, String updatedBy) {
        if (current == null) {
            throw new PartitionPlanException("Current plan is required");
        }
        PartitionPlanValidator.validate(current);
        if (StringUtils.isBlank(pluginId) || partitionCount < 1 || StringUtils.isBlank(updatedBy)) {
            throw new PartitionPlanException("pluginId, partitionCount, and updatedBy are required");
        }
    }
}
