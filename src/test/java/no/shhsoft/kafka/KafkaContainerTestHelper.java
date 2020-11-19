package no.shhsoft.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;

import java.util.HashMap;
import java.util.Map;

public final class KafkaContainerTestHelper {

    private KafkaContainerTestHelper() {
    }

    public static Admin getAdmin(final String boostrapServers) {
        return Admin.create(getConfig(boostrapServers));
    }

    private static Map<String, Object> getConfig(final String bootstrapServers) {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        map.put("security.protocol", "SASL_PLAINTEXT");
        map.put("sasl.mechanism", "PLAIN");
        map.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka\";");
        return map;
    }

}
