package dev.demo.payflow.gateway;

import dev.demo.payflow.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
class KafkaTopicConfig {

	/**
	 * Created at startup if missing (brokers have auto-create disabled). With acks=all and
	 * min.insync.replicas=2, a write is acknowledged only once 2 of 3 replicas have it, so losing one
	 * broker loses no acknowledged payment.
	 */
	@Bean
	NewTopic paymentsInitiated(TopicProperties t) {
		return TopicBuilder.name(Topics.PAYMENTS_INITIATED)
			.partitions(t.partitions())
			.replicas(t.replicas())
			.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(t.minInsyncReplicas()))
			.build();
	}
}
