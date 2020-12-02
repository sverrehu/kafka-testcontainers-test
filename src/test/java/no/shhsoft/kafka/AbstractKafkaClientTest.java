package no.shhsoft.kafka;

import no.shhsoft.kafka.utils.KafkaContainerTestHelper;
import no.shhsoft.kafka.utils.TestConsumer;
import no.shhsoft.kafka.utils.TestProducer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public abstract class AbstractKafkaClientTest {

    @Test
    public final void shouldAddAndListTopic() {
        try (final Admin admin = getAdmin()) {
            admin.createTopics(Collections.singleton(new NewTopic(KafkaContainerTestHelper.TOPIC_NAME, 1, (short) 1)));
            final ListTopicsResult topics = admin.listTopics();
            try {
                final Set<String> topicNames = topics.names().get();
                Assert.assertFalse("List of topic names is empty. Expected at least one.", topicNames.isEmpty());
                Assert.assertTrue("Expected to find topic named " + KafkaContainerTestHelper.TOPIC_NAME, topicNames.contains(KafkaContainerTestHelper.TOPIC_NAME));
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void shouldProduceAndConsume() {
        enableAccessForProducerAndConsumer();
        try (final TestProducer producer = getTestProducer(); final TestConsumer consumer = getTestConsumer()) {
            final Set<String> values = producer.produceSomeStrings(KafkaContainerTestHelper.TOPIC_NAME);
            consumer.consumeForAWhile(KafkaContainerTestHelper.TOPIC_NAME, (key, value) -> {
                values.remove(value);
                return !values.isEmpty();
            });
            if (!values.isEmpty()) {
                Assert.fail("Unable to consume all messages.");
            }
        }
    }

    protected abstract String getBootstrapServers();

    protected abstract Admin getAdmin();

    protected abstract TestProducer getTestProducer();

    protected abstract TestConsumer getTestConsumer();

    protected abstract void enableAccessForProducerAndConsumer();

}
