package no.shhsoft.kafka;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public final class DerivedSaslPlaintextKafkaContainer
extends KafkaContainer {

    private static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("confluentinc/cp-kafka").withTag("6.0.0");
    /* STARTER_SCRIPT is private in KafkaContainer, so duplicate here. */
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final String JAAS_CONFIG_FILE = "/tmp/broker_jaas.conf";
    private boolean startInitiated;

    public DerivedSaslPlaintextKafkaContainer() {
        super(DEFAULT_IMAGE);
        /* KafkaContainer hard codes the broker-internal listener name
         * as BROKER, without providing a constant we can use. We will
         * need to reference that name, so try to pick it up without
         * repeating the hard coding. */
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
        /* KafkaContainer hard codes the listener name as PLAINTEXT, which
         * is not correct in our case. Since listener name is not needed,
         * we strip it off. Also, the original method has a readiness
         * check that I would like to keep, but it is based on a private
         * port field, so I made an alternative. */
        if (!startInitiated) {
            throw new IllegalStateException("You should start Kafka container first");
        }
        return String.format("%s:%s", getHost(), getMappedPort(KAFKA_PORT));
    }

    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        /* The original KafkaContainer.containerIsStarting does several
         * unrelated things without providing overridable methods for
         * each thing, so it is hard to modify some but not all. One of
         * the things we need to modify, is the KAFKA_ADVERTISED_LISTENERS
         * that is included in the startup script. It will look like this:
         * SASL_PLAINTEXT://172.17.0.1:32925,BROKER://172.17.0.3:9092
         * The BROKER listener has been rewritten to use the external IP
         * address, making it inaccessible to the local Controller that
         * wants to access it. I spent a lot of time trying to call
         * the original KafkaContainer.containerIsStarting and massaging
         * the startup script afterwards, but KakfaContainer.onStart
         * would pick it up and run it before I managed to rewrite.
         * KafkaContainer.onStart cannot be overridden in a meaningful
         * way, because it blocks my override from calling
         * "super.super.onStart". So I ended up reimplementing
         * the entire containerIsStarting. Without the external Zookeeper
         * thing, as I don't need it. */

        if (reused) {
            return;
        }
        startInitiated = true;
        followOutput(new TtyConsumer("Container"));

        /* Provide a JAAS file for the kafka super user and a couple of test users. */
        uploadJaasConfig();

        final String zookeeperConnect = externalZookeeperConnect != null ? externalZookeeperConnect : startZookeeper();
        createAlternativeStartupScript(zookeeperConnect);
    }

    private void uploadJaasConfig() {
        final String jaas = "KafkaServer { org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"kafka\" password=\"kafka\" "
                            + "user_kafka=\"kafka\" user_alice=\"alice-secret\" user_bob=\"bob-secret\"; };\n";
        copyFileToContainer(Transferable.of(jaas.getBytes(StandardCharsets.UTF_8), 0600), JAAS_CONFIG_FILE);
    }

    private void createAlternativeStartupScript(final String zookeeperConnect) {
        final String listeners = getEnvMap().get("KAFKA_LISTENERS");
        if (listeners == null) {
            throw new RuntimeException("Need environment variable KAFKA_LISTENERS");
        }
        final String advertisedListeners = listeners.replaceAll(":" + KAFKA_PORT, ":" + getMappedPort(KAFKA_PORT))
                                                    .replaceAll("0\\.0\\.0\\.0", getContainerIpAddress());
        final String starterScript = "#!/bin/bash\n"
                                     + "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n"
                                     + "export KAFKA_ADVERTISED_LISTENERS='" + advertisedListeners + "'\n"
                                     + ". /etc/confluent/docker/bash-config\n"
                                     + "/etc/confluent/docker/configure\n"
                                     + "/etc/confluent/docker/launch\n";
        copyFileToContainer(Transferable.of(starterScript.getBytes(StandardCharsets.UTF_8), 0755), STARTER_SCRIPT);
    }

    private String startZookeeper() {
        /* So, after realizing that I needed to reimplement
         * containerIsStarting, I also realized that startZookeeper
         * is private in KafkaContainer, so I cannot reuse that
         * either. */
        final ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
            .withCmd("sh", "-c", "echo '*** Starting Zookeeper'\n"
                                 + "printf 'clientPort=" + ZOOKEEPER_PORT + "\n"
                                 + "dataDir=/var/lib/zookeeper/data\ndataLogDir=/var/lib/zookeeper/log' > zookeeper.properties\n"
                                 + "zookeeper-server-start zookeeper.properties\n")
            .withAttachStderr(true)
            .withAttachStdout(true)
            .exec();
        try {
            dockerClient.execStartCmd(execCreateCmdResponse.getId()).start().awaitStarted(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        return "127.0.0.1:" + ZOOKEEPER_PORT;
    }

}
