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

package org.apache.ranger.audit.producer.kafka.partition.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** PUT /api/audit/partition-plan body (server assigns version). */
public final class PartitionPlanReplaceRequest {
    private final int expectedVersion;
    private final int topicPartitionCount;
    private final Map<String, PluginPartitionAssignment> plugins;
    private final PluginPartitionAssignment buffer;
    private final Map<String, ServiceAllowlistEntry> services;

    @JsonCreator
    public PartitionPlanReplaceRequest(@JsonProperty("expectedVersion") int expectedVersion, @JsonProperty("topicPartitionCount") int topicPartitionCount, @JsonProperty("plugins") Map<String, PluginPartitionAssignment> plugins, @JsonProperty("buffer") PluginPartitionAssignment buffer, @JsonProperty("services") Map<String, ServiceAllowlistEntry> services) {
        this.expectedVersion     = expectedVersion;
        this.topicPartitionCount = topicPartitionCount;
        this.plugins             = copyPlugins(plugins);
        this.buffer              = buffer != null ? buffer : PluginPartitionAssignment.empty();
        this.services            = copyServices(services);
    }

    public PartitionPlanReplaceRequest(int expectedVersion, int topicPartitionCount, Map<String, PluginPartitionAssignment> plugins, PluginPartitionAssignment buffer) {
        this(expectedVersion, topicPartitionCount, plugins, buffer, null);
    }

    private static Map<String, PluginPartitionAssignment> copyPlugins(Map<String, PluginPartitionAssignment> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(plugins));
    }

    private static Map<String, ServiceAllowlistEntry> copyServices(Map<String, ServiceAllowlistEntry> services) {
        if (services == null) {
            return null;
        }
        if (services.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(services));
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }

    public int getTopicPartitionCount() {
        return topicPartitionCount;
    }

    public Map<String, PluginPartitionAssignment> getPlugins() {
        return plugins;
    }

    public PluginPartitionAssignment getBuffer() {
        return buffer;
    }

    /** When null, the current plan's services map is preserved. */
    public Map<String, ServiceAllowlistEntry> getServices() {
        return services;
    }

    /** Converts the REST PUT body into a proposed plan for append-only validation. */
    public PartitionPlan toProposedPlan(PartitionPlan current, String updatedBy) {
        PartitionPlan.Builder builder = PartitionPlan.builder()
                .topic(current.getTopic())
                .topicPartitionCount(topicPartitionCount)
                .plugins(plugins)
                .buffer(buffer)
                .updatedAt(Instant.now().toString())
                .updatedBy(updatedBy);
        if (services != null) {
            builder.services(services);
        } else {
            builder.services(current.getServices());
        }
        return builder.build();
    }
}
