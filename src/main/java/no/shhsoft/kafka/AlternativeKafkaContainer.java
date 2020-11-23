package no.shhsoft.kafka;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * An alternative KafkaContainer that is easier to extend than the one from the testcontainers project version 1.15.0.
 *
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public class AlternativeKafkaContainer
extends GenericContainer<AlternativeKafkaContainer> {

    private static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("confluentinc/cp-kafka").withTag("6.0.0");
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    public static final String INTERNAL_LISTENER_NAME = "BROKER";
    public static final int KAFKA_PORT = 9092;
    public static final int KAFKA_INTERNAL_PORT = 9093;
    public static final int ZOOKEEPER_PORT = 2181;
    /* Note difference between 0.0.0.0 and localhost: The former will be replaced by the container IP. */
    private static final String LISTENERS = "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + "," + INTERNAL_LISTENER_NAME + "://127.0.0.1:" + KAFKA_INTERNAL_PORT;
    private static final int PORT_NOT_ASSIGNED = -1;
    private int port = PORT_NOT_ASSIGNED;
    protected String externalZookeeperConnect = null;

    public AlternativeKafkaContainer() {
        this(DEFAULT_IMAGE);
    }

    public AlternativeKafkaContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        withExposedPorts(KAFKA_PORT);
        withEnv("KAFKA_LISTENERS", LISTENERS);
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT," + INTERNAL_LISTENER_NAME + ":PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", INTERNAL_LISTENER_NAME);

        withEnv("KAFKA_BROKER_ID", "1");
        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
        withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
        withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    }

    public final AlternativeKafkaContainer withEmbeddedZookeeper() {
        externalZookeeperConnect = null;
        return self();
    }

    public final AlternativeKafkaContainer withExternalZookeeper(final String connectString) {
        externalZookeeperConnect = connectString;
        return self();
    }

    public final String getBootstrapServers() {
        if (port == PORT_NOT_ASSIGNED) {
            throw new IllegalStateException("You should start Kafka container first");
        }
        return modifyBoostrapServers(String.format("%s:%s", getHost(), port));
    }

    protected String modifyBoostrapServers(final String bootstrapServers) {
        return bootstrapServers;
    }

    @Override
    protected final void doStart() {
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        if (externalZookeeperConnect == null) {
            addExposedPort(ZOOKEEPER_PORT);
        }
        beforeStart();
        super.doStart();
    }

    protected void beforeStart() {
    }

    @Override
    protected final void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);
        followOutput(new TtyConsumer(getContainerId().substring(0, 12)));
        port = getMappedPort(KAFKA_PORT);
        if (reused) {
            return;
        }
        beforeStartupPreparations();
        final String zookeeperConnect = externalZookeeperConnect != null ? externalZookeeperConnect : startZookeeper();
        createStartupScript(zookeeperConnect);
    }

    protected void beforeStartupPreparations() {
    }

    private void createStartupScript(final String zookeeperConnect) {
        final String listeners = getEnvMap().get("KAFKA_LISTENERS");
        if (listeners == null) {
            throw new RuntimeException("Need environment variable KAFKA_LISTENERS");
        }
        final String advertisedListeners = modifyAdvertisedListeners(
            listeners.replaceAll(":" + KAFKA_PORT, ":" + getMappedPort(KAFKA_PORT))
            .replaceAll("0\\.0\\.0\\.0", getContainerIpAddress()));
        final String startupScript = modifyStartupScript(
            "#!/bin/bash\n"
            + "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n"
            + "export KAFKA_ADVERTISED_LISTENERS='" + advertisedListeners + "'\n"
            + ". /etc/confluent/docker/bash-config\n"
            + "/etc/confluent/docker/configure\n"
            + "/etc/confluent/docker/launch\n");
        copyFileToContainer(Transferable.of(startupScript.getBytes(StandardCharsets.UTF_8), 0755), STARTER_SCRIPT);
    }

    protected String modifyAdvertisedListeners(final String advertisedListeners) {
        return advertisedListeners;
    }

    protected String modifyStartupScript(final String startupScript) {
        return startupScript;
    }

    private String startZookeeper() {
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
