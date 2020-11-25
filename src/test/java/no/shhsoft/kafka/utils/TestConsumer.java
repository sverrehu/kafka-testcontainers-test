package no.shhsoft.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class TestConsumer<V>
implements Closeable {

    private final KafkaConsumer<String, V> consumer;

    public interface RecordHandler<V> {

        /**
         * @return <code>true</code> to continue processing, <code>false</code> if no more records are requested.
         */
        boolean handle(String key, V value);

    }

    private TestConsumer(final KafkaConsumer<String, V> consumer) {
        this.consumer = consumer;
    }

    public static TestConsumer<String> forStringValues(final Map<String, Object> config, final String consumerGroup) {
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new TestConsumer<>(new KafkaConsumer<>(config));
    }

    public void consumeForAWhile(final String topicName, final long durationMs, final RecordHandler<V> handler) {
        consumer.subscribe(Collections.singleton(topicName));
        final long endTime = System.currentTimeMillis() + durationMs;
        while (System.currentTimeMillis() < endTime) {
            final ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(1000));
            for (final ConsumerRecord<String, V> record : records) {
                if (handler != null && !handler.handle(record.key(), record.value())) {
                    break;
                }
                consumer.commitAsync();
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }

}
