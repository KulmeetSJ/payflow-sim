package dev.demo.payflow.processor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

@Configuration(proxyBeanMethods = false)
class KafkaErrorConfig {

	/**
	 * Phase 1: retry transient failures (e.g. DB blip) 3 times, 1 s apart, then log and skip.
	 * Malformed JSON is never retried. Phase 2 replaces "log and skip" with retry topics + a DLT.
	 */
	@Bean
	DefaultErrorHandler errorHandler() {
		var handler = new DefaultErrorHandler(new FixedBackOff(1000L, 3));
		handler.addNotRetryableExceptions(JacksonException.class);
		return handler;
	}
}
