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
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.server.AuditServerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Builds effective {@code auth_to_local} rules from the XML catalog and the union of partition-plan
 * {@code services[].allowedUsers}. Rules are not stored in the partition-plan JSON document.
 */
public final class AuthToLocalRuleComposer {
    private static final Logger LOG = LoggerFactory.getLogger(AuthToLocalRuleComposer.class);

    private static final AuthToLocalRuleComposer INSTANCE = new AuthToLocalRuleComposer();

    private volatile AuthToLocalRuleCatalog ruleCatalog;
    private volatile String                 lastAppliedRuleText;
    private volatile int                    lastAppliedPartitionPlanVersion;
    private volatile Boolean                partitionPlanTopicExistsTestOverride;

    private AuthToLocalRuleComposer() {
    }

    public static AuthToLocalRuleComposer getInstance() {
        return INSTANCE;
    }

    /** Loads the rule catalog from {@code ranger.audit.ingestor.auth.to.local} site XML. */
    public synchronized void initializeFromConfig() {
        Properties ingestorProperties = AuditServerConfig.getInstance().getProperties();
        initializeFromProperties(ingestorProperties);
    }

    synchronized void initializeFromProperties(Properties ingestorProperties) {
        String rawAuthToLocalRules = ingestorProperties.getProperty(AuditServerConstants.PROP_AUTH_TO_LOCAL);
        ruleCatalog = AuthToLocalRuleCatalog.parse(rawAuthToLocalRules);
        lastAppliedRuleText              = null;
        lastAppliedPartitionPlanVersion  = 0;
        LOG.debug("Loaded auth_to_local catalog with {} primary rules", ruleCatalog.getPrimaryCatalogRuleCount());
    }

    /** Applies the full XML catalog (non-dynamic / startup fallback). */
    public void applyStaticRules() {
        AuthToLocalRuleCatalog loadedRuleCatalog = requireRuleCatalog();
        applyKerberosNameRules(loadedRuleCatalog.composeFullCatalogRules(), 0);
    }

    /**
     * Dynamic-mode startup: when the partition-plan Kafka topic does not exist yet, apply the full XML
     * catalog so Kerberos mapping works before {@link PartitionPlanWatcher} bootstraps the registry.
     * When the topic already exists, defer to composed rules on {@link PartitionPlanHolder#install}.
     */
    public void applyStartupRulesForDynamicMode(Properties ingestorProperties, String ingestorPropertyPrefix) {
        if (!PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(ingestorProperties, ingestorPropertyPrefix)) {
            return;
        }
        requireRuleCatalog();
        if (isPartitionPlanTopicPresent(ingestorProperties, ingestorPropertyPrefix)) {
            LOG.info("Partition plan topic exists; auth_to_local rules will be composed from allowlisted short names on plan install");
        } else {
            applyStaticRules();
            LOG.info("Partition plan topic does not exist yet; applied full auth_to_local catalog from XML until plan bootstrap");
        }
    }

    /**
     * When dynamic partition-plan mode is enabled, compose rules from the union of allowlisted short
     * names and install them before audit REST authorization runs.
     */
    public void applyForPlan(PartitionPlan partitionPlan) {
        Properties ingestorProperties = AuditServerConfig.getInstance().getProperties();
        if (!PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(ingestorProperties, PartitionPlanService.INGESTOR_PROP_PREFIX)) {
            return;
        }
        if (partitionPlan == null) {
            return;
        }

        AuthToLocalRuleCatalog loadedRuleCatalog = requireRuleCatalog();
        Set<String> allowedUserShortNames = collectAllowedUserShortNames(partitionPlan);
        String composedRuleText = loadedRuleCatalog.composeRulesForAllowedShortNames(allowedUserShortNames);
        applyKerberosNameRules(composedRuleText, partitionPlan.getVersion());
        LOG.info("Applied composed auth_to_local rules for plan version {} ({} active short names)",
                partitionPlan.getVersion(), allowedUserShortNames.size());
    }

    /** Visible for tests — composes without applying Kerberos rules. */
    String composeKerberosRulesForAllowedShortNames(Set<String> allowedUserShortNames) {
        return requireRuleCatalog().composeRulesForAllowedShortNames(allowedUserShortNames);
    }

    /** Clears cached apply state between unit tests. */
    public synchronized void resetForTests() {
        lastAppliedRuleText                     = null;
        lastAppliedPartitionPlanVersion         = 0;
        partitionPlanTopicExistsTestOverride    = null;
    }

    /** When non-null, overrides {@link AuditMessageQueueUtils#partitionPlanTopicExists} for unit tests. */
    void setPartitionPlanTopicExistsTestOverride(Boolean topicExists) {
        partitionPlanTopicExistsTestOverride = topicExists;
    }

    private boolean isPartitionPlanTopicPresent(Properties ingestorProperties, String ingestorPropertyPrefix) {
        Boolean topicExistsOverride = partitionPlanTopicExistsTestOverride;
        if (topicExistsOverride != null) {
            return topicExistsOverride;
        }
        return AuditMessageQueueUtils.partitionPlanTopicExists(ingestorProperties, ingestorPropertyPrefix);
    }

    static Set<String> collectAllowedUserShortNames(PartitionPlan partitionPlan) {
        Set<String> allowedUserShortNames = new LinkedHashSet<>();
        if (partitionPlan == null || partitionPlan.getServices() == null) {
            return allowedUserShortNames;
        }
        for (Map.Entry<String, ServiceAllowlistEntry> serviceEntry : partitionPlan.getServices().entrySet()) {
            ServiceAllowlistEntry allowlistEntry = serviceEntry.getValue();
            if (allowlistEntry == null) {
                continue;
            }
            for (String allowedUserShortName : allowlistEntry.getAllowedUsers()) {
                if (StringUtils.isNotBlank(allowedUserShortName)) {
                    allowedUserShortNames.add(allowedUserShortName.trim());
                }
            }
        }
        return allowedUserShortNames;
    }

    private AuthToLocalRuleCatalog requireRuleCatalog() {
        AuthToLocalRuleCatalog loadedRuleCatalog = ruleCatalog;
        if (loadedRuleCatalog == null) {
            initializeFromConfig();
            loadedRuleCatalog = ruleCatalog;
        }
        if (loadedRuleCatalog == null) {
            throw new IllegalStateException("auth_to_local catalog is not loaded");
        }
        return loadedRuleCatalog;
    }

    private synchronized void applyKerberosNameRules(String composedRuleText, int partitionPlanVersion) {
        if (StringUtils.isBlank(composedRuleText)) {
            LOG.warn("Skipping auth_to_local apply: composed rules are blank");
            return;
        }
        if (composedRuleText.equals(lastAppliedRuleText) && partitionPlanVersion == lastAppliedPartitionPlanVersion) {
            return;
        }
        try {
            KerberosName.setRules(composedRuleText);
            lastAppliedRuleText             = composedRuleText;
            lastAppliedPartitionPlanVersion = partitionPlanVersion;
            LOG.debug("KerberosName auth_to_local rules updated (partitionPlanVersion={})", partitionPlanVersion);
        } catch (Exception e) {
            LOG.error("Failed to apply composed auth_to_local rules for plan version {}: {}",
                    partitionPlanVersion, e.getMessage(), e);
        }
    }
}
