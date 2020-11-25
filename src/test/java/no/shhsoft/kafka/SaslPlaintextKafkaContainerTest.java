package no.shhsoft.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class SaslPlaintextKafkaContainerTest
extends AbstractSaslPlaintextKafkaTestClient {

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
    protected String getBootstrapServers() {
        return container.getBootstrapServers();
    }

}
