package no.shhsoft.kafka.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class TestProducer<V> {

    private final KafkaProducer<String, V> producer;

    private TestProducer(final KafkaProducer<String, V> producer) {
        this.producer = producer;
    }

    public static TestProducer<String> forStringValues(final Map<String, Object> config) {
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000");
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        config.put(ProducerConfig.RETRIES_CONFIG, "0");
        return new TestProducer<>(new KafkaProducer<>(config));
    }

    public void produce(final String topicName, final V recordValue) {
        produce(topicName, null, recordValue);
    }

    public void produce(final String topicName, final String key, final V recordValue) {
        final ProducerRecord<String, V> record = new ProducerRecord<>(topicName, key, recordValue);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                throw new RuntimeException("Got exception while sending", exception);
            }
        });
        producer.flush();
    }

    public void close() {
        producer.close();
    }

}
