package dev.demo.payflow.events;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Published by payment-gateway to {@value Topics#PAYMENTS_INITIATED}, keyed by {@code fromAccount}
 * so that all payments from one account land on one partition and are processed in order.
 *
 * <p>{@code eventId} is derived from the client's Idempotency-Key, so a client retry produces the
 * SAME event id and the processor can drop it as a duplicate.
 */
public record PaymentInitiated(
		int schemaVersion,
		UUID eventId,
		String idempotencyKey,
		String fromAccount,
		String toAccount,
		BigDecimal amount,
		String currency,
		Instant acceptedAt) {

	public static final int SCHEMA_VERSION = 1;

	/** Deterministic id for an Idempotency-Key (name-based UUID, so no lookup is needed). */
	public static UUID eventIdFor(String idempotencyKey) {
		return UUID.nameUUIDFromBytes(("payflow:payment:" + idempotencyKey).getBytes(StandardCharsets.UTF_8));
	}

	public static PaymentInitiated of(String idempotencyKey, String fromAccount, String toAccount,
			BigDecimal amount, String currency, Instant acceptedAt) {
		return new PaymentInitiated(SCHEMA_VERSION, eventIdFor(idempotencyKey), idempotencyKey, fromAccount,
				toAccount, amount, currency, acceptedAt);
	}
}
