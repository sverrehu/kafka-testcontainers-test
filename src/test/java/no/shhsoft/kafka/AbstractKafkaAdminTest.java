package no.shhsoft.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;

import java.util.Collections;

public abstract class AbstractKafkaAdminTest {

    @Test
    public final void testSomething() {
        final Admin admin = getAdmin();
        admin.createTopics(Collections.singleton(new NewTopic("my-test-topic", 1, (short) 1)));
        final ListTopicsResult topics = admin.listTopics();
        try {
            System.out.println("Topics: " + topics.names().get().size());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract Admin getAdmin();

}
