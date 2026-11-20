package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
class PaymentListener {

	private final PaymentService service;

	private final DeadLetterService deadLetters;

	private final FaultInjector faults;

	private final JsonMapper json;

	PaymentListener(PaymentService service, DeadLetterService deadLetters, FaultInjector faults, JsonMapper json) {
		this.service = service;
		this.deadLetters = deadLetters;
		this.faults = faults;
		this.json = json;
	}

	/**
	 * Manual ack, AFTER the DB transaction has committed. If the process dies between commit and
	 * ack, the event is redelivered and processed_events turns it into a no-op.
	 *
	 * <p>Non-blocking retries: 4 attempts in total (main topic + 3 retry topics, 1 s / 2 s / 4 s),
	 * then {@code payments.initiated-dlt}. Retry topics get the same partition count, and the
	 * record key is kept, so retried payments stay on "their" partition number.
	 */
	@RetryableTopic(attempts = "${payflow.retry.attempts:4}",
			backOff = @BackOff(delayString = "${payflow.retry.initial-delay-ms:1000}", multiplier = 2.0,
					maxDelayString = "${payflow.retry.max-delay-ms:10000}"),
			numPartitions = "${payflow.topic.partitions:6}",
			exclude = { InvalidEventException.class, JacksonException.class }, traversingCauses = "true")
	@KafkaListener(topics = Topics.PAYMENTS_INITIATED, groupId = "payment-processor")
	void onPayment(ConsumerRecord<String, String> rec, Acknowledgment ack) {
		PaymentInitiated p = json.readValue(rec.value(), PaymentInitiated.class);
		EventValidator.validate(p);
		faults.maybeFail(p);
		service.process(p, rec.partition(), rec.offset(), attempt(rec));
		ack.acknowledge();
	}

	/** Every dead letter is indexed in PostgreSQL for the inspector; the DLT topic stays the source of truth. */
	@DltHandler
	void onDeadLetter(ConsumerRecord<String, String> rec, Acknowledgment ack) {
		deadLetters.record(rec);
		ack.acknowledge();
	}

	/** 1 on the main topic; the retry-topic attempt number after that. */
	static int attempt(ConsumerRecord<?, ?> rec) {
		Header h = rec.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		if (h == null) {
			return 1;
		}
		byte[] v = h.value();
		return v.length == Integer.BYTES ? ByteBuffer.wrap(v).getInt() : Integer.parseInt(new String(v).trim());
	}
}
