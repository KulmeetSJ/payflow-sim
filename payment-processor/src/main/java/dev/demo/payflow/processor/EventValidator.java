package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Structural checks on an incoming event (business rules are {@link PaymentRules}). A failure here
 * means the event is poison: retrying cannot fix it, so it is not retried.
 */
final class EventValidator {

	private EventValidator() {
	}

	static void validate(PaymentInitiated p) {
		if (p.schemaVersion() != PaymentInitiated.SCHEMA_VERSION) {
			throw new InvalidEventException("unsupported schemaVersion " + p.schemaVersion());
		}
		if (Stream.of(p.eventId(), p.idempotencyKey(), p.fromAccount(), p.toAccount(), p.amount(), p.currency(),
				p.acceptedAt()).anyMatch(Objects::isNull)) {
			throw new InvalidEventException("missing required field");
		}
		// eventId must be derived from the Idempotency-Key, otherwise dedupe could be bypassed
		if (!p.eventId().equals(PaymentInitiated.eventIdFor(p.idempotencyKey()))) {
			throw new InvalidEventException("eventId does not match idempotencyKey");
		}
	}
}
