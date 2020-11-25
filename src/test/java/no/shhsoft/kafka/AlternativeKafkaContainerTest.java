package no.shhsoft.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author <a href="mailto:shh@thathost.com">Sverre H. Huseby</a>
 */
public final class AlternativeKafkaContainerTest
extends AbstractNoAuthKafkaContainerTest {

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
    protected String getBootstrapServers() {
        return container.getBootstrapServers();
    }

}
