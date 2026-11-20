package dev.demo.payflow.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Topic layout. Compose uses 6 partitions, RF 3, min ISR 2; tests use one broker. */
@ConfigurationProperties("payflow.topic")
public record TopicProperties(int partitions, short replicas, int minInsyncReplicas) {
}
