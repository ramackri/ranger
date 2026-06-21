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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Parsed {@code ranger.audit.ingestor.auth.to.local} catalog for dynamic rule composition. */
final class AuthToLocalRuleCatalog {
    private static final Pattern SUBSTITUTION_TARGET = Pattern.compile("s/\\.\\*/([^/]+)/\\s*$");
    private static final String    SIMPLE_RULE_TEMPLATE = "RULE:[2:$1/$2@$0](%s/.*@.*)s/.*/%s/";

    private final List<CatalogEntry> primaryRulesInOrder;
    private final List<String>       tailRules;

    AuthToLocalRuleCatalog(List<CatalogEntry> primaryRulesInOrder, List<String> tailRules) {
        this.primaryRulesInOrder = Collections.unmodifiableList(new ArrayList<>(primaryRulesInOrder));
        this.tailRules           = Collections.unmodifiableList(new ArrayList<>(tailRules));
    }

    static AuthToLocalRuleCatalog parse(String rawRules) {
        List<CatalogEntry> primary = new ArrayList<>();
        List<String>       tail    = new ArrayList<>();
        for (String token : tokenizeRules(rawRules)) {
            if (isTailRule(token)) {
                tail.add(token);
            } else {
                primary.add(new CatalogEntry(token, extractTargetShortName(token)));
            }
        }
        return new AuthToLocalRuleCatalog(primary, tail);
    }

    /** Full static rule set (all primary rules in catalog order + tail). */
    String composeFull() {
        List<String> lines = new ArrayList<>(primaryRulesInOrder.size() + tailRules.size());
        for (CatalogEntry entry : primaryRulesInOrder) {
            lines.add(entry.ruleLine);
        }
        lines.addAll(tailRules);
        return joinRules(lines);
    }

    /**
     * Subset of catalog rules for the union of active short usernames from partition-plan allowlists.
     * Unknown names get a generated {@code service/host@REALM -> service} rule before tail rules.
     */
    String compose(Set<String> activeShortNames) {
        if (activeShortNames == null || activeShortNames.isEmpty()) {
            return composeFull();
        }

        Set<String> active = activeShortNames.stream()
                .filter(StringUtils::isNotBlank)
                .map(String::trim)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (active.isEmpty()) {
            return composeFull();
        }

        List<String>       lines   = new ArrayList<>();
        Set<String>        covered = new LinkedHashSet<>();
        Map<String, String> catalogByShortName = catalogRulesByShortName();

        for (CatalogEntry entry : primaryRulesInOrder) {
            if (entry.targetShortName != null && active.contains(entry.targetShortName)) {
                lines.add(entry.ruleLine);
                covered.add(entry.targetShortName);
            }
        }

        List<String> generated = active.stream()
                .filter(name -> !covered.contains(name))
                .sorted()
                .map(AuthToLocalRuleCatalog::simpleRuleForShortName)
                .collect(Collectors.toList());
        lines.addAll(generated);
        lines.addAll(tailRules);
        return joinRules(lines);
    }

    private Map<String, String> catalogRulesByShortName() {
        Map<String, String> ret = new LinkedHashMap<>();
        for (CatalogEntry entry : primaryRulesInOrder) {
            if (entry.targetShortName != null) {
                ret.putIfAbsent(entry.targetShortName, entry.ruleLine);
            }
        }
        return ret;
    }

    private static List<String> tokenizeRules(String rawRules) {
        if (StringUtils.isBlank(rawRules)) {
            return Collections.emptyList();
        }
        return Arrays.stream(rawRules.split("\\s+"))
                .map(String::trim)
                .filter(s -> s.startsWith("RULE:") || "DEFAULT".equals(s))
                .collect(Collectors.toList());
    }

    private static boolean isTailRule(String ruleLine) {
        return "DEFAULT".equals(ruleLine)
                || ruleLine.startsWith("RULE:[1:")
                || ruleLine.contains("s/@.*//");
    }

    static String extractTargetShortName(String ruleLine) {
        if (StringUtils.isBlank(ruleLine)) {
            return null;
        }
        Matcher matcher = SUBSTITUTION_TARGET.matcher(ruleLine);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    static String simpleRuleForShortName(String shortName) {
        return String.format(SIMPLE_RULE_TEMPLATE, shortName, shortName);
    }

    private static String joinRules(List<String> lines) {
        return String.join("\n", lines);
    }

    int getPrimaryRuleCount() {
        return primaryRulesInOrder.size();
    }

    static final class CatalogEntry {
        private final String ruleLine;
        private final String targetShortName;

        CatalogEntry(String ruleLine, String targetShortName) {
            this.ruleLine        = ruleLine;
            this.targetShortName = targetShortName;
        }
    }
}
