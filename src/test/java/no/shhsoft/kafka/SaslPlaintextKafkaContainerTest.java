package no.shhsoft.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class SaslPlaintextKafkaContainerTest
extends AbstractKafkaAdminTest {

    private static SaslPlaintextKafkaContainer container;

    @BeforeClass
    public static void beforeClass() {
        container = new SaslPlaintextKafkaContainer();
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
