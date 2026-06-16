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
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.server.AuditServerConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.ranger.audit.server.AuditServerConstants.PROP_PREFIX_AUDIT_SERVER_SERVICE;
import static org.apache.ranger.audit.server.AuditServerConstants.PROP_SUFFIX_ALLOWED_USERS;

/** Loads service allowlist entries from ingestor site XML for registry bootstrap and brownfield merge. */
public final class ServiceAllowlistBootstrap {
    private static final String BOOTSTRAP_SOURCE = "xml-bootstrap";

    private ServiceAllowlistBootstrap() {
    }

  /** Scans {@code ranger.audit.ingestor.service.<repo>.allowed.users} properties. */
    public static Map<String, ServiceAllowlistEntry> loadFromProperties(Properties props) {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        if (props == null) {
            return services;
        }
        for (String key : props.stringPropertyNames()) {
            if (!key.startsWith(PROP_PREFIX_AUDIT_SERVER_SERVICE) || !key.endsWith(PROP_SUFFIX_ALLOWED_USERS)) {
                continue;
            }
            String repo = key.substring(PROP_PREFIX_AUDIT_SERVER_SERVICE.length(), key.length() - PROP_SUFFIX_ALLOWED_USERS.length());
            if (StringUtils.isBlank(repo)) {
                continue;
            }
            String value = props.getProperty(key);
            if (StringUtils.isBlank(value)) {
                continue;
            }
            List<String> users = new ArrayList<>();
            for (String part : value.split(",")) {
                if (part != null) {
                    String trimmed = part.trim();
                    if (StringUtils.isNotBlank(trimmed)) {
                        users.add(trimmed);
                    }
                }
            }
            if (users.isEmpty()) {
                continue;
            }
            services.put(repo.trim(), new ServiceAllowlistEntry(users, BOOTSTRAP_SOURCE, null));
        }
        return services;
    }

    /** Loads allowlist entries from the running ingestor configuration singleton. */
    public static Map<String, ServiceAllowlistEntry> loadFromAuditServerConfig() {
        return loadFromProperties(AuditServerConfig.getInstance().getProperties());
    }

    /**
     * Brownfield helper: when the Kafka plan has no {@code services} block, merge XML entries in memory only
     * (does not bump version or write to Kafka).
     */
    public static PartitionPlan enrichServicesFromXmlIfMissing(PartitionPlan plan, Properties props) {
        if (plan == null || (plan.getServices() != null && !plan.getServices().isEmpty())) {
            return plan;
        }
        Map<String, ServiceAllowlistEntry> fromXml = loadFromProperties(props);
        if (fromXml.isEmpty()) {
            return plan;
        }
        return plan.toBuilder().services(fromXml).build();
    }
}
