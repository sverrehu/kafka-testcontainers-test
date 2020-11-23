package no.shhsoft.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class AlternativeSaslPlaintextKafkaContainerTest
extends AbstractKafkaAdminTest {

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

}
