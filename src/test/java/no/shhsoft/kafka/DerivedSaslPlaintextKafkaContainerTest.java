package no.shhsoft.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class DerivedSaslPlaintextKafkaContainerTest
extends AbstractKafkaAdminTest {

    private static DerivedSaslPlaintextKafkaContainer container;

    @BeforeClass
    public static void beforeClass() {
        container = new DerivedSaslPlaintextKafkaContainer();
        container.start();
    }

    @AfterClass
    public static void afterClass() {
        container.stop();
    }

    @Override
    protected Admin getAdmin() {
        return KafkaContainerTestHelper.getAdmin(container.getBootstrapServers());
    }

}
