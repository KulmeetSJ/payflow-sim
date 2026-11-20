package dev.demo.payflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.TestConfiguration;
import tools.jackson.databind.json.JsonMapper;

/** Real Kafka + real PostgreSQL (Testcontainers): event in -> rows out. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import({ TestcontainersConfiguration.class, PaymentFlowIT.Topic.class })
class PaymentFlowIT {

	@TestConfiguration(proxyBeanMethods = false)
	static class Topic {

		@Bean
		NewTopic paymentsInitiated() {
			return new NewTopic(Topics.PAYMENTS_INITIATED, 6, (short) 1);
		}
	}

	@Autowired
	KafkaTemplate<String, String> kafka;

	@Autowired
	JsonMapper json;

	@Autowired
	JdbcClient db;

	static final Duration WAIT = Duration.ofSeconds(20);

	PaymentInitiated send(String key, String from, String to, String amount, String ccy) throws Exception {
		var p = PaymentInitiated.of(key, from, to, new BigDecimal(amount), ccy, Instant.now());
		kafka.send(Topics.PAYMENTS_INITIATED, p.fromAccount(), json.writeValueAsString(p)).get();
		return p;
	}

	String status(UUID id) {
		return db.sql("SELECT status FROM payments WHERE payment_id = ?").param(id).query(String.class).optional().orElse(null);
	}

	BigDecimal balance(String acc) {
		return db.sql("SELECT balance FROM accounts WHERE account_id = ?").param(acc).query(BigDecimal.class).single();
	}

	@Test
	void validPaymentIsBookedWithBalancedLedger() throws Exception {
		// ACC-000003 and ACC-000006 are both GBP (n % 3 == 0)
		var before3 = balance("ACC-000003");
		var before6 = balance("ACC-000006");
		var p = send("it-valid-" + UUID.randomUUID(), "ACC-000003", "ACC-000006", "250.75", "GBP");

		await().atMost(WAIT).until(() -> "COMPLETED".equals(status(p.eventId())));
		assertThat(balance("ACC-000003")).isEqualByComparingTo(before3.subtract(new BigDecimal("250.75")));
		assertThat(balance("ACC-000006")).isEqualByComparingTo(before6.add(new BigDecimal("250.75")));
		BigDecimal net = db.sql("""
				SELECT sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
				FROM ledger_entries WHERE payment_id = ?""").param(p.eventId()).query(BigDecimal.class).single();
		assertThat(net).isEqualByComparingTo("0");
	}

	@Test
	void businessRuleViolationIsRecordedAsRejectedWithoutLedger() throws Exception {
		// ACC-000001 is EUR, ACC-000003 is GBP
		var p = send("it-ccy-" + UUID.randomUUID(), "ACC-000003", "ACC-000001", "10.00", "GBP");

		await().atMost(WAIT).until(() -> "REJECTED".equals(status(p.eventId())));
		assertThat(db.sql("SELECT reject_reason FROM payments WHERE payment_id = ?").param(p.eventId())
			.query(String.class).single()).isEqualTo("CURRENCY_MISMATCH");
		assertThat(db.sql("SELECT count(*) FROM ledger_entries WHERE payment_id = ?").param(p.eventId())
			.query(Long.class).single()).isZero();
	}

	@Test
	void sameEventDeliveredThreeTimesIsBookedOnce() throws Exception {
		var key = "it-dup-" + UUID.randomUUID();
		var before = balance("ACC-000009");
		var p = send(key, "ACC-000009", "ACC-000012", "5.00", "GBP");
		send(key, "ACC-000009", "ACC-000012", "5.00", "GBP");
		send(key, "ACC-000009", "ACC-000012", "5.00", "GBP");

		await().atMost(WAIT).until(() -> "COMPLETED".equals(status(p.eventId())));
		// all three are on the same partition (same key), so once the next marker is processed, all three were
		var marker = send("it-dup-marker-" + UUID.randomUUID(), "ACC-000009", "ACC-000012", "0.01", "GBP");
		await().atMost(WAIT).until(() -> status(marker.eventId()) != null);

		assertThat(db.sql("SELECT count(*) FROM ledger_entries WHERE payment_id = ?").param(p.eventId())
			.query(Long.class).single()).isEqualTo(2);
		assertThat(balance("ACC-000009")).isEqualByComparingTo(before.subtract(new BigDecimal("5.01")));
	}
}
