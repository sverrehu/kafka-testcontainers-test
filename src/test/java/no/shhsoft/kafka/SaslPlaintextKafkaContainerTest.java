package no.shhsoft.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class SaslPlaintextKafkaContainerTest {

    private static SaslPlaintextKafkaContainer container;

    @BeforeClass
    public static void beforeClass() {
        container = new SaslPlaintextKafkaContainer();
        container.start();
    }

    @AfterClass
    public static void afterClass() {
        container.stop();
    }

    @Test
    public void testSomething() {
        final Admin admin = container.getAdmin();
        admin.createTopics(Collections.singleton(new NewTopic("my-test-topic", 1, (short) 1)));
        final ListTopicsResult topics = admin.listTopics();
        try {
            System.out.println("Topics: " + topics.names().get().size());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
