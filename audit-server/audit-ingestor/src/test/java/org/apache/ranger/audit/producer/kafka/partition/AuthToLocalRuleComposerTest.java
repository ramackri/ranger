/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthToLocalRuleComposerTest {
    private static final String SAMPLE_CATALOG =
            "RULE:[2:$1/$2@$0]([ndj]n/.*@.*|hdfs/.*@.*)s/.*/hdfs/ "
                    + "RULE:[2:$1/$2@$0]([rn]m/.*@.*|yarn/.*@.*)s/.*/yarn/ "
                    + "RULE:[2:$1/$2@$0](jhs/.*@.*)s/.*/mapred/ "
                    + "RULE:[2:$1/$2@$0](hive/.*@.*)s/.*/hive/ "
                    + "RULE:[2:$1/$2@$0](rangerkms/.*@.*)s/.*/rangerkms/ "
                    + "RULE:[2:$1/$2@$0](om/.*@.*)s/.*/om/ "
                    + "RULE:[2:$1/$2@$0](ozone/.*@.*)s/.*/ozone/ "
                    + "RULE:[1:$1@$0](.*@.*)s/@.*// "
                    + "DEFAULT";

    private AuthToLocalRuleComposer composer;

    @BeforeEach
    public void setUp() {
        composer = AuthToLocalRuleComposer.getInstance();
        composer.resetForTests();
        Properties props = new Properties();
        props.setProperty(AuditServerConstants.PROP_AUTH_TO_LOCAL, SAMPLE_CATALOG);
        composer.initializeFromProperties(props);
    }

    @AfterEach
    public void tearDown() {
        composer.resetForTests();
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testComposeKmsOnlyIncludesRangerkmsRuleAndTail() {
        String rules = composer.composeForActiveShortNames(Set.of("rangerkms"));

        assertTrue(rules.contains("(rangerkms/.*@.*)s/.*/rangerkms/"));
        assertFalse(rules.contains("(hive/.*@.*)s/.*/hive/"));
        assertTrue(rules.contains("RULE:[1:$1@$0](.*@.*)s/@.*//"));
        assertTrue(rules.endsWith("DEFAULT"));
    }

    @Test
    public void testComposeHdfsUsesCatalogRegexNotSimpleRule() {
        String rules = composer.composeForActiveShortNames(Set.of("hdfs"));

        assertTrue(rules.contains("([ndj]n/.*@.*|hdfs/.*@.*)s/.*/hdfs/"));
        assertFalse(rules.contains("(hdfs/.*@.*)s/.*/hdfs/"));
    }

    @Test
    public void testComposeOzoneIncludesMultipleShortNames() {
        String rules = composer.composeForActiveShortNames(Set.of("ozone", "om", "scm", "dn"));

        assertTrue(rules.contains("(ozone/.*@.*)s/.*/ozone/"));
        assertTrue(rules.contains("(om/.*@.*)s/.*/om/"));
    }

    @Test
    public void testComposeUnknownShortNameGeneratesSimpleRule() {
        String rules = composer.composeForActiveShortNames(Set.of("myapp"));

        assertTrue(rules.contains("(myapp/.*@.*)s/.*/myapp/"));
    }

    @Test
    public void testUnionAllowedUsersFromPlan() {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_kms", ServiceAllowlistEntry.ofUsers("rangerkms"));
        services.put("dev_ozone", ServiceAllowlistEntry.ofUsers("om", "ozone"));
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(2)
                .topicPartitionCount(6)
                .plugins(Map.of("kms", PluginPartitionAssignment.of(0, 1)))
                .buffer(PluginPartitionAssignment.of(2, 3, 4, 5))
                .services(services)
                .build();

        Set<String> active = AuthToLocalRuleComposer.unionAllowedUsers(plan);

        assertEquals(Set.of("rangerkms", "om", "ozone"), active);
    }

    @Test
    public void testKerberosMappingForComposedKmsRules() throws Exception {
        String rules = composer.composeForActiveShortNames(Set.of("rangerkms"));
        KerberosName.setRules(rules);

        assertEquals("rangerkms", new KerberosName("rangerkms/ranger-kms.rangernw@EXAMPLE.COM").getShortName());
    }

    @Test
    public void testKerberosMappingForComposedHdfsRules() throws Exception {
        String rules = composer.composeForActiveShortNames(Set.of("hdfs"));
        KerberosName.setRules(rules);

        assertEquals("hdfs", new KerberosName("nn/namenode.rangernw@EXAMPLE.COM").getShortName());
    }

    @Test
    public void testExtractTargetShortNameFromCatalogLines() {
        assertEquals("hdfs", AuthToLocalRuleCatalog.extractTargetShortName(
                "RULE:[2:$1/$2@$0]([ndj]n/.*@.*|hdfs/.*@.*)s/.*/hdfs/"));
        assertEquals("rangerkms", AuthToLocalRuleCatalog.extractTargetShortName(
                "RULE:[2:$1/$2@$0](rangerkms/.*@.*)s/.*/rangerkms/"));
        assertEquals("mapred", AuthToLocalRuleCatalog.extractTargetShortName(
                "RULE:[2:$1/$2@$0](jhs/.*@.*)s/.*/mapred/"));
    }
}
