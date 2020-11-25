package no.shhsoft.kafka;

import no.shhsoft.kafka.utils.KafkaContainerTestHelper;
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

}
