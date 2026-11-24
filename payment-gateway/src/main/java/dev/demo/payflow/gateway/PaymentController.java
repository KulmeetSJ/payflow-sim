package dev.demo.payflow.gateway;

import dev.demo.payflow.events.PaymentInitiated;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
// No @Validated here: Spring MVC's built-in method validation then turns a bad header into a 400.
// (@Validated would route it through the AOP proxy, which throws ConstraintViolationException = 500.)
class PaymentController {

	private final PaymentPublisher publisher;

	private final Clock clock;

	/**
	 * Denominator of the latency SLI ("99% of ACCEPTED payments committed within 2 s"). Counting at
	 * acceptance, not at completion, keeps the SLI honest when processing stalls: payments stuck in
	 * the backlog count as late as soon as they are, not hours later when they finally complete.
	 */
	private final Counter accepted;

	PaymentController(PaymentPublisher publisher, Clock clock, MeterRegistry registry) {
		this.publisher = publisher;
		this.clock = clock;
		this.accepted = Counter.builder("payments.accepted")
			.description("Payments durably accepted (202), including client retries of the same Idempotency-Key")
			.register(registry);
	}

	/**
	 * 202 Accepted means: the event is durably in Kafka (acks=all, min ISR 2). Processing happens
	 * asynchronously. A retry with the same Idempotency-Key returns the same paymentId and is
	 * de-duplicated by the processor.
	 */
	@PostMapping
	ResponseEntity<Accepted> create(
			@RequestHeader("Idempotency-Key") @Pattern(regexp = "[A-Za-z0-9_-]{8,64}") String idempotencyKey,
			@Valid @RequestBody PaymentRequest req) {
		var event = PaymentInitiated.of(idempotencyKey, req.fromAccount(), req.toAccount(), req.amount(),
				req.currency(), Instant.now(clock));
		publisher.publish(event);
		accepted.increment();
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(new Accepted(event.eventId(), "ACCEPTED"));
	}

	record Accepted(UUID paymentId, String status) {
	}
}
