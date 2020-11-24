package no.shhsoft.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractKafkaAdminTest {

    private static final String TOPIC_NAME = "my-test-topic";

    @Test
    public final void shouldAddAndListTopic() {
        final Admin admin = getAdmin();
        admin.createTopics(Collections.singleton(new NewTopic(TOPIC_NAME, 1, (short) 1)));
        final ListTopicsResult topics = admin.listTopics();
        try {
            final Set<String> topicNames = topics.names().get();
            assertFalse("List of topic names is empty. Expected at least one.", topicNames.isEmpty());
            assertTrue("Expected to find topic named " + TOPIC_NAME, topicNames.contains(TOPIC_NAME));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract Admin getAdmin();

}
