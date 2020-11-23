# kafka-testcontainers-test

I was tasked with creating a [Testcontainers](https://www.testcontainers.org/) -based container that would
run Kafka with SASL enabled, and found that the original KafkaContainer
was not very extendable. This project contains my experiments.

* [DerivedSaslPlaintextKafkaContainer](./src/main/java/no/shhsoft/kafka/DerivedSaslPlaintextKafkaContainer.java)
  is an attempt at extending the original KafkaContainer. As explained
  in the comments, my class ends up re-implementing most of
  KafkaContainer, while still being vulnerable to changes in the
  internals of the parent class.

* [SaslPlaintextKafkaContainer](./src/main/java/no/shhsoft/kafka/SaslPlaintextKafkaContainer.java)
  is what I ended up using for my current needs. It does not inherit
  from KafkaContainer.

* [AlternativeKafkaContainer](./src/main/java/no/shhsoft/kafka/AlternativeKafkaContainer.java)
  is an attempt at creating a more extendable KafkaContainer that
  provides hooks that subclasses may override. I do not like
  extendability that is based on overriding non-empty methods.

* [AlternativeSaslPlaintextKafkaContainer](./src/main/java/no/shhsoft/kafka/AlternativeSaslPlaintextKafkaContainer.java)
  is what my initial task would look like if KafkaContainer is
  my alternative implementation.

