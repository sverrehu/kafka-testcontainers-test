package no.shhsoft.kafka;

import no.shhsoft.kafka.utils.KafkaContainerTestHelper;
import no.shhsoft.kafka.utils.TestConsumer;
import no.shhsoft.kafka.utils.TestProducer;
import org.apache.kafka.clients.admin.Admin;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public abstract class AbstractNoAuthKafkaContainerTest
extends AbstractKafkaClientTest {

    @Override
    protected final Admin getAdmin() {
        return KafkaContainerTestHelper.getNoAuthAdmin(getBootstrapServers());
    }

    @Override
    protected final TestProducer getTestProducer() {
        return KafkaContainerTestHelper.getNoAuthTestProducer(getBootstrapServers());
    }

    @Override
    protected final TestConsumer getTestConsumer() {
        return KafkaContainerTestHelper.getNoAuthTestConsumer(getBootstrapServers());
    }

    @Override
    protected final void enableAccessForProducerAndConsumer() {
    }

}
