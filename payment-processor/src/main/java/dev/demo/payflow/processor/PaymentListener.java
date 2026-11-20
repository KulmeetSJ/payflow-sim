package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
class PaymentListener {

	private final PaymentService service;

	private final JsonMapper json;

	PaymentListener(PaymentService service, JsonMapper json) {
		this.service = service;
		this.json = json;
	}

	/**
	 * Manual ack, AFTER the DB transaction has committed. If the process dies between commit and
	 * ack, the event is redelivered and processed_events turns it into a no-op.
	 */
	@KafkaListener(topics = Topics.PAYMENTS_INITIATED, groupId = "payment-processor")
	void onPayment(ConsumerRecord<String, String> rec, Acknowledgment ack) {
		PaymentInitiated p = json.readValue(rec.value(), PaymentInitiated.class);
		service.process(p, rec.partition(), rec.offset());
		ack.acknowledge();
	}
}
