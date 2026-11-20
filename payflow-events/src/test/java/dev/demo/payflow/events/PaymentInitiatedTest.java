package dev.demo.payflow.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PaymentInitiatedTest {

	@Test
	void sameIdempotencyKeyGivesSameEventId() {
		var a = PaymentInitiated.of("key-123", "ACC-000001", "ACC-000002", BigDecimal.TEN, "GBP", Instant.now());
		var b = PaymentInitiated.of("key-123", "ACC-000001", "ACC-000002", BigDecimal.TEN, "GBP", Instant.now());
		assertThat(a.eventId()).isEqualTo(b.eventId());
	}

	@Test
	void differentKeysGiveDifferentEventIds() {
		assertThat(PaymentInitiated.eventIdFor("key-1")).isNotEqualTo(PaymentInitiated.eventIdFor("key-2"));
	}
}
