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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanValidator;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.provider.MiscUtil;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable partition routing plan stored in the Kafka compacted registry topic. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PartitionPlan {
    private final String topic;
    private final int version;
    private final int topicPartitionCount;
    private final String updatedAt;
    private final String updatedBy;
    private final Map<String, PluginPartitionAssignment> plugins;
    private final PluginPartitionAssignment buffer;

    @JsonCreator
    public PartitionPlan(@JsonProperty("topic") String topic, @JsonProperty("version") int version, @JsonProperty("topicPartitionCount") int topicPartitionCount, @JsonProperty("updatedAt") String updatedAt, @JsonProperty("updatedBy") String updatedBy, @JsonProperty("plugins") Map<String, PluginPartitionAssignment> plugins, @JsonProperty("buffer") PluginPartitionAssignment buffer) {
        this.topic               = topic;
        this.version             = version;
        this.topicPartitionCount = topicPartitionCount;
        this.updatedAt           = updatedAt;
        this.updatedBy           = updatedBy;
        this.plugins             = copyPlugins(plugins);
        this.buffer              = buffer != null ? buffer : PluginPartitionAssignment.empty();
    }

    private static Map<String, PluginPartitionAssignment> copyPlugins(Map<String, PluginPartitionAssignment> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(plugins));
    }

    public String getTopic() {
        return topic;
    }

    public int getVersion() {
        return version;
    }

    public int getTopicPartitionCount() {
        return topicPartitionCount;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public Map<String, PluginPartitionAssignment> getPlugins() {
        return plugins;
    }

    public PluginPartitionAssignment getBuffer() {
        return buffer;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Serializes this plan for the compacted Kafka registry topic. */
    public String toJson() {
        try {
            return MiscUtil.getMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to serialize partition plan", e);
        }
    }

    /** Parses and validates a plan JSON payload from Kafka or REST. */
    public static PartitionPlan fromJson(String json) {
        try {
            PartitionPlan plan = MiscUtil.getMapper().readValue(json, PartitionPlan.class);
            PartitionPlanValidator.validate(plan);
            return plan;
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to deserialize partition plan", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionPlan that = (PartitionPlan) o;
        return version == that.version && topicPartitionCount == that.topicPartitionCount && Objects.equals(topic, that.topic) && Objects.equals(updatedAt, that.updatedAt) && Objects.equals(updatedBy, that.updatedBy) && Objects.equals(plugins, that.plugins) && Objects.equals(buffer, that.buffer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, version, topicPartitionCount, updatedAt, updatedBy, plugins, buffer);
    }

    @Override
    public String toString() {
        return "PartitionPlan{topic='" + topic + "', version=" + version + ", topicPartitionCount=" + topicPartitionCount + ", plugins=" + plugins.keySet() + ", bufferSize=" + buffer.size() + '}';
    }

    public static final class Builder {
        private String topic;
        private int version = 1;
        private int topicPartitionCount;
        private String updatedAt;
        private String updatedBy;
        private Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>();
        private PluginPartitionAssignment buffer = PluginPartitionAssignment.empty();

        private Builder() {
        }

        private Builder(PartitionPlan plan) {
            this.topic               = plan.topic;
            this.version             = plan.version;
            this.topicPartitionCount = plan.topicPartitionCount;
            this.updatedAt           = plan.updatedAt;
            this.updatedBy           = plan.updatedBy;
            this.plugins             = new LinkedHashMap<>(plan.plugins);
            this.buffer              = plan.buffer;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder topicPartitionCount(int topicPartitionCount) {
            this.topicPartitionCount = topicPartitionCount;
            return this;
        }

        public Builder updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder updatedBy(String updatedBy) {
            this.updatedBy = updatedBy;
            return this;
        }

        public Builder plugins(Map<String, PluginPartitionAssignment> plugins) {
            this.plugins = plugins == null ? new LinkedHashMap<>() : new LinkedHashMap<>(plugins);
            return this;
        }

        public Builder putPlugin(String pluginId, PluginPartitionAssignment assignment) {
            this.plugins.put(pluginId, assignment);
            return this;
        }

        public Builder buffer(PluginPartitionAssignment buffer) {
            this.buffer = buffer != null ? buffer : PluginPartitionAssignment.empty();
            return this;
        }

        public PartitionPlan build() {
            return new PartitionPlan(topic, version, topicPartitionCount, updatedAt, updatedBy, plugins, buffer);
        }
    }
}
