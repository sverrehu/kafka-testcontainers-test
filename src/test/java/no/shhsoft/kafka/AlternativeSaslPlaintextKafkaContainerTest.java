package no.shhsoft.kafka;

import no.shhsoft.kafka.utils.KafkaContainerTestHelper;
import no.shhsoft.kafka.utils.TestConsumer;
import no.shhsoft.kafka.utils.TestProducer;
import org.apache.kafka.clients.admin.Admin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class AlternativeSaslPlaintextKafkaContainerTest
extends AbstractKafkaClientTest {

    private static AlternativeSaslPlaintextKafkaContainer container;

    @BeforeClass
    public static void beforeClass() {
        container = new AlternativeSaslPlaintextKafkaContainer();
        container.start();
    }

    @AfterClass
    public static void afterClass() {
        container.stop();
    }

    @Override
    protected Admin getAdmin() {
        return KafkaContainerTestHelper.getSaslAdmin(container.getBootstrapServers());
    }

    @Override
    protected TestProducer<String> getTestProducer() {
        return KafkaContainerTestHelper.getSaslTestProducer(container.getBootstrapServers());
    }

    @Override
    protected TestConsumer<String> getTestConsumer() {
        return KafkaContainerTestHelper.getSaslTestConsumer(container.getBootstrapServers());
    }

    @Override
    protected void enableAccessForProducerAndConsumer() {
        KafkaContainerTestHelper.enableAclsForProducerAndConsumer(getAdmin());
    }

}
