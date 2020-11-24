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
public final class AlternativeKafkaContainerTest
extends AbstractKafkaClientTest {

    private static AlternativeKafkaContainer container;

    @BeforeClass
    public static void beforeClass() {
        container = new AlternativeKafkaContainer();
        container.start();
    }

    @AfterClass
    public static void afterClass() {
        container.stop();
    }

    @Override
    protected Admin getAdmin() {
        return KafkaContainerTestHelper.getNoAuthAdmin(container.getBootstrapServers());
    }

    @Override
    protected TestProducer<String> getTestProducer() {
        return KafkaContainerTestHelper.getNoAuthTestProducer(container.getBootstrapServers());
    }

    @Override
    protected TestConsumer<String> getTestConsumer() {
        return KafkaContainerTestHelper.getNoAuthTestConsumer(container.getBootstrapServers());
    }

    @Override
    protected void enableAccessForProducerAndConsumer() {
    }

}
