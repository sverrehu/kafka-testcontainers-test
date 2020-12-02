package no.shhsoft.kafka;

import no.shhsoft.kafka.utils.KafkaContainerTestHelper;
import no.shhsoft.kafka.utils.TestConsumer;
import no.shhsoft.kafka.utils.TestProducer;
import org.apache.kafka.clients.admin.Admin;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public abstract class AbstractSaslPlaintextKafkaTestClient
extends AbstractKafkaClientTest {

    @Override
    protected final void enableAccessForProducerAndConsumer() {
        try (final Admin admin = getAdmin()) {
            KafkaContainerTestHelper.enableAclsForProducerAndConsumer(admin);
        }
    }

    @Override
    protected final Admin getAdmin() {
        return KafkaContainerTestHelper.getSaslAdmin(getBootstrapServers());
    }

    @Override
    protected final TestProducer getTestProducer() {
        return KafkaContainerTestHelper.getSaslTestProducer(getBootstrapServers());
    }

    @Override
    protected final TestConsumer getTestConsumer() {
        return KafkaContainerTestHelper.getSaslTestConsumer(getBootstrapServers());
    }

}
