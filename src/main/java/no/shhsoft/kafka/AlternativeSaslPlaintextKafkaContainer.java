package no.shhsoft.kafka;

import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;

/**
 * How SaslPlaintextKafkaContainer would look if KafkaContainer from the
 * testcontainers project was easier to extend.
 *
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class AlternativeSaslPlaintextKafkaContainer
extends AlternativeKafkaContainer {

    private static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("confluentinc/cp-kafka").withTag("6.0.0");
    private static final String JAAS_CONFIG_FILE = "/tmp/broker_jaas.conf";

    public AlternativeSaslPlaintextKafkaContainer() {
        this(DEFAULT_IMAGE);
    }

    public AlternativeSaslPlaintextKafkaContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        /* Note difference between 0.0.0.0 and localhost: The former will be replaced by the container IP. */
        withEnv("KAFKA_LISTENERS", "SASL_PLAINTEXT://0.0.0.0:" + KAFKA_PORT + "," + INTERNAL_LISTENER_NAME + "://127.0.0.1:2" + KAFKA_PORT);
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SASL_PLAINTEXT:SASL_PLAINTEXT," + INTERNAL_LISTENER_NAME + ":SASL_PLAINTEXT");
        withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
        withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
        withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer");
        withEnv("KAFKA_SUPER_USERS", "User:kafka");
        withEnv("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka\" user_kafka=\"kafka\" user_alice=\"alice-secret\" user_bob=\"bob-secret\"");
        withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=" + JAAS_CONFIG_FILE);
    }

    @Override
    protected void beforeStartupPreparations() {
        uploadJaasConfig();
    }

    private void uploadJaasConfig() {
        final String jaas = "KafkaServer { org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"kafka\" password=\"kafka\" "
                            + "user_kafka=\"kafka\" user_alice=\"alice-secret\" user_bob=\"bob-secret\"; };\n";
        copyFileToContainer(Transferable.of(jaas.getBytes(StandardCharsets.UTF_8), 0644), JAAS_CONFIG_FILE);
    }

}
