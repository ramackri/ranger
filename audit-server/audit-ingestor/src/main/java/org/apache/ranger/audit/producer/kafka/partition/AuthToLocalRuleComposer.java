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

    private volatile AuthToLocalRuleCatalog catalog;
    private volatile String                 lastAppliedRules;
    private volatile int                    lastAppliedPlanVersion;
    private volatile Boolean                planTopicExistsOverrideForTests;

    private AuthToLocalRuleComposer() {
    }

    public static AuthToLocalRuleComposer getInstance() {
        return INSTANCE;
    }

    /** Loads the rule catalog from {@code ranger.audit.ingestor.auth.to.local} site XML. */
    public synchronized void initializeFromConfig() {
        Properties props = AuditServerConfig.getInstance().getProperties();
        initializeFromProperties(props);
    }

    synchronized void initializeFromProperties(Properties props) {
        String raw = props.getProperty(AuditServerConstants.PROP_AUTH_TO_LOCAL);
        catalog = AuthToLocalRuleCatalog.parse(raw);
        lastAppliedRules         = null;
        lastAppliedPlanVersion   = 0;
        LOG.debug("Loaded auth_to_local catalog with {} primary rules", catalog.getPrimaryRuleCount());
    }

    /** Applies the full XML catalog (non-dynamic / startup fallback). */
    public void applyStaticRules() {
        AuthToLocalRuleCatalog loaded = requireCatalog();
        applyRules(loaded.composeFull(), 0);
    }

    /**
     * Dynamic-mode startup: when the partition-plan Kafka topic does not exist yet, apply the full XML
     * catalog so Kerberos mapping works before {@link PartitionPlanWatcher} bootstraps the registry.
     * When the topic already exists, defer to composed rules on {@link PartitionPlanHolder#install}.
     */
    public void applyStartupRulesForDynamicMode(Properties props, String ingestorPropPrefix) {
        if (!PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(props, ingestorPropPrefix)) {
            return;
        }
        requireCatalog();
        if (isPlanTopicPresent(props, ingestorPropPrefix)) {
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
    public void applyForPlan(PartitionPlan plan) {
        Properties props = AuditServerConfig.getInstance().getProperties();
        if (!PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(props, PartitionPlanService.INGESTOR_PROP_PREFIX)) {
            return;
        }
        if (plan == null) {
            return;
        }

        AuthToLocalRuleCatalog loaded = requireCatalog();
        Set<String> activeShortNames = collectAllowedUserShortNames(plan);
        String      rules            = loaded.compose(activeShortNames);
        applyRules(rules, plan.getVersion());
        LOG.info("Applied composed auth_to_local rules for plan version {} ({} active short names)", plan.getVersion(), activeShortNames.size());
    }

    /** Visible for tests — composes without applying Kerberos rules. */
    String composeRulesForShortNames(Set<String> activeShortNames) {
        return requireCatalog().compose(activeShortNames);
    }

    /** Clears cached apply state between unit tests. */
    public synchronized void resetForTests() {
        lastAppliedRules             = null;
        lastAppliedPlanVersion       = 0;
        planTopicExistsOverrideForTests = null;
    }

    /** When non-null, overrides {@link AuditMessageQueueUtils#partitionPlanTopicExists} for unit tests. */
    void setPlanTopicExistsOverrideForTests(Boolean exists) {
        planTopicExistsOverrideForTests = exists;
    }

    private boolean isPlanTopicPresent(Properties props, String ingestorPropPrefix) {
        Boolean override = planTopicExistsOverrideForTests;
        if (override != null) {
            return override;
        }
        return AuditMessageQueueUtils.partitionPlanTopicExists(props, ingestorPropPrefix);
    }

    static Set<String> collectAllowedUserShortNames(PartitionPlan plan) {
        Set<String> active = new LinkedHashSet<>();
        if (plan == null || plan.getServices() == null) {
            return active;
        }
        for (Map.Entry<String, ServiceAllowlistEntry> entry : plan.getServices().entrySet()) {
            ServiceAllowlistEntry allowlist = entry.getValue();
            if (allowlist == null) {
                continue;
            }
            for (String user : allowlist.getAllowedUsers()) {
                if (StringUtils.isNotBlank(user)) {
                    active.add(user.trim());
                }
            }
        }
        return active;
    }

    private AuthToLocalRuleCatalog requireCatalog() {
        AuthToLocalRuleCatalog loaded = catalog;
        if (loaded == null) {
            initializeFromConfig();
            loaded = catalog;
        }
        if (loaded == null) {
            throw new IllegalStateException("auth_to_local catalog is not loaded");
        }
        return loaded;
    }

    private synchronized void applyRules(String rules, int planVersion) {
        if (StringUtils.isBlank(rules)) {
            LOG.warn("Skipping auth_to_local apply: composed rules are blank");
            return;
        }
        if (rules.equals(lastAppliedRules) && planVersion == lastAppliedPlanVersion) {
            return;
        }
        try {
            KerberosName.setRules(rules);
            lastAppliedRules       = rules;
            lastAppliedPlanVersion = planVersion;
            LOG.debug("KerberosName auth_to_local rules updated (planVersion={})", planVersion);
        } catch (Exception e) {
            LOG.error("Failed to apply composed auth_to_local rules for plan version {}: {}", planVersion, e.getMessage(), e);
        }
    }
}
