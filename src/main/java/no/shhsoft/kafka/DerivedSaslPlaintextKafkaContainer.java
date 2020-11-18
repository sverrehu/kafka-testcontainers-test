package no.shhsoft.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DerivedSaslPlaintextKafkaContainer
extends KafkaContainer {

    /* NOTES:
     *   * Need to know that "BROKER" is the name of the internal listener.
     *   * Cannot use original getBoostrapServers, since it prefixes with "PLAINTEXT://".
     */
    private static final String JAAS_CONFIG_FILE = "/tmp/broker_jaas.conf";

    public DerivedSaslPlaintextKafkaContainer() {
        super();
        final String interBrokerListenerName = getEnvMap().get("KAFKA_INTER_BROKER_LISTENER_NAME");
        withEnv("KAFKA_LISTENERS", "SASL_PLAINTEXT://0.0.0.0:" + KAFKA_PORT + "," + interBrokerListenerName + "://127.0.0.1:2" + KAFKA_PORT);
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SASL_PLAINTEXT:SASL_PLAINTEXT," + interBrokerListenerName + ":SASL_PLAINTEXT");
        withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
        withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
        /* TODO: kafka.security.authorizer.AclAuthorizer when moving to 6.0.0 */
        withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.auth.SimpleAclAuthorizer");
        withEnv("KAFKA_SUPER_USERS", "User:kafka");
        withEnv("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka\" user_kafka=\"kafka\" user_alice=\"alice-secret\" user_bob=\"bob-secret\"");
        withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=" + JAAS_CONFIG_FILE);
    }

    @Override
    public String getBootstrapServers() {
        return super.getBootstrapServers().replaceAll("PLAINTEXT://", "SASL_PLAINTEXT://");
    }

    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        followOutput(new TtyConsumer("Container"));
        super.containerIsStarting(containerInfo, reused);
        final String jaas = "KafkaServer { org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka\" user_kafka=\"kafka\" user_alice=\"alice-secret\" user_bob=\"bob-secret\"; };\n";
        copyFileToContainer(Transferable.of(jaas.getBytes(StandardCharsets.UTF_8), 0600), JAAS_CONFIG_FILE);
        try {
            execInContainer("sh", "-c", "perl -p -i -e 's/BROKER:.*:\\d+/BROKER:\\/\\/localhost:29093/g' /testcontainers_start.sh; cat /testcontainers_start.sh");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> getAdminConfig() {
        return getCommonConfig();
    }

    public Admin getAdmin() {
        return Admin.create(getAdminConfig());
    }

    public Map<String, Object> getProducerConfig() {
        return getCommonConfig();
    }

    public Producer<?, ?> getProducer() {
        return new KafkaProducer<>(getProducerConfig());
    }

    public Map<String, Object> getConsumerConfig() {
        return getCommonConfig();
    }

    public Consumer<?, ?> getConsumer() {
        return new KafkaConsumer<>(getConsumerConfig());
    }

    private Map<String, Object> getCommonConfig() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        map.put("security.protocol", "SASL_PLAINTEXT");
        map.put("sasl.mechanism", "PLAIN");
        map.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka\";");
        return map;
    }

}
