# kafka-testcontainers-test

I was tasked with creating a [Testcontainers](https://www.testcontainers.org/) -based container that would
run Kafka with SASL enabled, and found that the original KafkaContainer
was not very extendable. This project contains my experiments.

(DerivedSaslPlaintextKafkaContainer)[./src/main/java/no/shhsoft/kafka/DerivedSaslPlaintextKafkaContainer]
is an attempt at extending the original KafkaContainer. As explained in the comments, this class ends up re-implementing most of KafkaContainer,
while still being vulnerable to changes in the internals of the parent class.

