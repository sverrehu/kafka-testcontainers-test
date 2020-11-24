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
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractKafkaClientTest {

    private static final int NUM_VALUES_TO_PRODUCE = 3;
    private static final long MAX_MS_TO_CONSUME = 20 * 1000L;

    @Test
    public final void shouldAddAndListTopic() {
        final Admin admin = getAdmin();
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

    @Test
    public void shouldProduceAndConsume() {
        enableAccessForProducerAndConsumer();

        final TestProducer<String> producer = getTestProducer();
        final TestConsumer<String> consumer = getTestConsumer();
        try {
            final Set<String> values = new HashSet<>();
            for (int q = 0; q < NUM_VALUES_TO_PRODUCE; q++) {
                final String value = (q + 1) + "-" + System.currentTimeMillis();
                values.add(value);
                producer.produce(KafkaContainerTestHelper.TOPIC_NAME, value);
            }

            consumer.consumeForAWhile(KafkaContainerTestHelper.TOPIC_NAME, MAX_MS_TO_CONSUME, (key, value) -> {
                values.remove(value);
                return !values.isEmpty();
            });
            if (!values.isEmpty()) {
                Assert.fail("Consumed " + (NUM_VALUES_TO_PRODUCE - values.size()) + " of " + NUM_VALUES_TO_PRODUCE + " messages.");
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }

    protected abstract Admin getAdmin();

    protected abstract TestProducer<String> getTestProducer();

    protected abstract TestConsumer<String> getTestConsumer();

    protected abstract void enableAccessForProducerAndConsumer();

}
