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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Per-repo service allowlist entry stored in the unified partition-plan registry document. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ServiceAllowlistEntry {
    private final List<String> allowedUsers;
    private final String source;
    private final String notes;

    @JsonCreator
    public ServiceAllowlistEntry(@JsonProperty("allowedUsers") List<String> allowedUsers, @JsonProperty("source") String source, @JsonProperty("notes") String notes) {
        this.allowedUsers = copyAllowedUsers(allowedUsers);
        this.source       = source;
        this.notes        = notes;
    }

    public static ServiceAllowlistEntry ofUsers(String... users) {
        return new ServiceAllowlistEntry(List.of(users), null, null);
    }

    public static ServiceAllowlistEntry ofUsers(List<String> users) {
        return new ServiceAllowlistEntry(users, null, null);
    }

    private static List<String> copyAllowedUsers(List<String> allowedUsers) {
        if (allowedUsers == null || allowedUsers.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String user : allowedUsers) {
            if (user != null && !user.isBlank()) {
                unique.add(user.trim());
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(unique));
    }

    public List<String> getAllowedUsers() {
        return allowedUsers;
    }

    public String getSource() {
        return source;
    }

    public String getNotes() {
        return notes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServiceAllowlistEntry that = (ServiceAllowlistEntry) o;
        return Objects.equals(allowedUsers, that.allowedUsers) && Objects.equals(source, that.source) && Objects.equals(notes, that.notes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowedUsers, source, notes);
    }
}
