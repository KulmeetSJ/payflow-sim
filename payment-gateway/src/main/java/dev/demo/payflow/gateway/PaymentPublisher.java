package dev.demo.payflow.gateway;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
class PaymentPublisher {

	private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

	private final KafkaTemplate<String, String> kafka;

	private final JsonMapper json;

	PaymentPublisher(KafkaTemplate<String, String> kafka, JsonMapper json) {
		this.kafka = kafka;
		this.json = json;
	}

	/** Blocks until the broker acknowledges (acks=all), so 202 is only returned for durable events. */
	void publish(PaymentInitiated event) {
		try {
			kafka.send(Topics.PAYMENTS_INITIATED, event.fromAccount(), json.writeValueAsString(event))
				.get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new PublishFailedException(ex);
		}
		catch (ExecutionException | TimeoutException ex) {
			throw new PublishFailedException(ex);
		}
	}
}
