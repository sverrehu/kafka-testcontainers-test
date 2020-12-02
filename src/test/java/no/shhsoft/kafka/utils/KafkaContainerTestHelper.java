package no.shhsoft.kafka.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class KafkaContainerTestHelper {

    public static final String TOPIC_NAME = "my-test-topic";
    public static final String CONSUMER_GROUP_NAME = "test-consumer-group";

    private KafkaContainerTestHelper() {
    }

    public static Admin getSaslAdmin(final String boostrapServers) {
        return Admin.create(getSaslConfig(boostrapServers, "kafka", "kafka"));
    }

    public static TestProducer getSaslTestProducer(final String bootstrapServers) {
        return TestProducer.create(getSaslConfig(bootstrapServers, "alice", "alice-secret"));
    }

    public static TestConsumer getSaslTestConsumer(final String bootstrapServers) {
        return TestConsumer.create(getSaslConfig(bootstrapServers, "bob", "bob-secret"), CONSUMER_GROUP_NAME);
    }

    private static Map<String, Object> getSaslConfig(final String bootstrapServers, final String userName, final String password) {
        final Map<String, Object> map = getBaseConfig(bootstrapServers);
        map.put("security.protocol", "SASL_PLAINTEXT");
        map.put("sasl.mechanism", "PLAIN");
        map.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=" + escape(userName) + " password=" + escape(password) + ";");
        return map;
    }

    private static String escape(final String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    public static Admin getNoAuthAdmin(final String boostrapServers) {
        return Admin.create(getBaseConfig(boostrapServers));
    }

    public static TestProducer getNoAuthTestProducer(final String bootstrapServers) {
        return TestProducer.create(getBaseConfig(bootstrapServers));
    }

    public static TestConsumer getNoAuthTestConsumer(final String bootstrapServers) {
        return TestConsumer.create(getBaseConfig(bootstrapServers), CONSUMER_GROUP_NAME);
    }

    private static Map<String, Object> getBaseConfig(final String bootstrapServers) {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return map;
    }

    public static void enableAclsForProducerAndConsumer(final Admin admin) {
        final List<AclBinding> aclBindings = new ArrayList<>();
        aclBindings.addAll(createAclBindingsForAllOperationsOnTopic("alice"));
        aclBindings.addAll(createAclBindingsForAllOperationsOnTopic("bob"));
        admin.createAcls(aclBindings);
    }

    private static List<AclBinding> createAclBindingsForAllOperationsOnTopic(final String userName) {
        final List<AclBinding> aclBindings = new ArrayList<>();
        aclBindings.add(allow(userName, AclOperation.READ, ResourceType.TOPIC, TOPIC_NAME));
        aclBindings.add(allow(userName, AclOperation.WRITE, ResourceType.TOPIC, TOPIC_NAME));
        aclBindings.add(allow(userName, AclOperation.DESCRIBE, ResourceType.TOPIC, TOPIC_NAME));
        aclBindings.add(allow(userName, AclOperation.READ, ResourceType.GROUP, CONSUMER_GROUP_NAME));
        aclBindings.add(allow(userName, AclOperation.WRITE, ResourceType.GROUP, CONSUMER_GROUP_NAME));
        aclBindings.add(allow(userName, AclOperation.DESCRIBE, ResourceType.GROUP, CONSUMER_GROUP_NAME));
        return aclBindings;
    }

    private static AclBinding allow(final String userName, final AclOperation operation, final ResourceType resourceType, final String resourceName) {
        final AccessControlEntry accessControlEntry = new AccessControlEntry("User:" + userName, "*", operation, AclPermissionType.ALLOW);
        final ResourcePattern topicResourcePattern = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
        return new AclBinding(topicResourcePattern, accessControlEntry);
    }

}
