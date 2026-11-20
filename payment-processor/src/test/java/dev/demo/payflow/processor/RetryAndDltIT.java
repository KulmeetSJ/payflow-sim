package dev.demo.payflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * Retry and DLT behaviour against real Kafka + PostgreSQL. Timings: the non-blocking path for a
 * payment that keeps failing takes at least 1 + 2 + 4 s of back-off, so "arrived in the DLT within
 * 5 s" proves an event skipped the retry topics.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import({ TestcontainersConfiguration.class, PaymentFlowIT.Topic.class })
class RetryAndDltIT {

	static final Duration SLOW = Duration.ofSeconds(40);

	static final Duration FAST = Duration.ofSeconds(5);

	@Autowired
	KafkaTemplate<String, String> kafka;

	@Autowired
	JsonMapper json;

	@Autowired
	JdbcClient db;

	@Autowired
	FaultInjector faults;

	@Autowired
	DeadLetterService deadLetters;

	@AfterEach
	void clearFaults() {
		faults.clear();
	}

	PaymentInitiated payment(String key) {
		// ACC-000015 -> ACC-000018: both GBP
		return PaymentInitiated.of(key, "ACC-000015", "ACC-000018", new BigDecimal("1.00"), "GBP", Instant.now());
	}

	void send(String key, String value) throws Exception {
		kafka.send(Topics.PAYMENTS_INITIATED, key, value).get();
	}

	Map<String, Object> paymentRow(UUID id) {
		return db.sql("SELECT status, delivery_attempt FROM payments WHERE payment_id = ?").param(id).query()
			.listOfRows().stream().findFirst().orElse(null);
	}

	Map<String, Object> deadLetterFor(String messageKey, String payloadFragment) {
		return db.sql("SELECT * FROM dead_letters WHERE message_key = ? AND payload LIKE ? ORDER BY id DESC LIMIT 1")
			.params(messageKey, "%" + payloadFragment + "%").query().listOfRows().stream().findFirst().orElse(null);
	}

	@Test
	void transientFailureIsRetriedViaRetryTopicAndBookedOnce() throws Exception {
		String key = "it-retry-" + UUID.randomUUID();
		// 3 failures = the initial attempt + 2 blocking retries on the main topic, so the
		// payment has to move to the first retry topic to succeed
		faults.failNext(key, 3);
		var p = payment(key);
		send(p.fromAccount(), json.writeValueAsString(p));

		await().atMost(SLOW).until(() -> paymentRow(p.eventId()) != null);
		assertThat(paymentRow(p.eventId())).containsEntry("status", "COMPLETED").containsEntry("delivery_attempt", 2);
		assertThat(db.sql("SELECT count(*) FROM ledger_entries WHERE payment_id = ?").param(p.eventId())
			.query(Long.class).single()).isEqualTo(2);
		assertThat(deadLetterFor(p.fromAccount(), key)).isNull();
	}

	@Test
	void persistentFailureEndsInDltAndReplaySucceedsOnceFixed() throws Exception {
		String key = "it-dlt-" + UUID.randomUUID();
		faults.failNext(key, Integer.MAX_VALUE);
		var p = payment(key);
		send(p.fromAccount(), json.writeValueAsString(p));

		await().atMost(SLOW).until(() -> deadLetterFor(p.fromAccount(), key) != null);
		var dl = deadLetterFor(p.fromAccount(), key);
		assertThat(dl).containsEntry("exception_class", SimulatedTransientFault.class.getName())
			.containsEntry("original_topic", Topics.PAYMENTS_INITIATED)
			.containsEntry("attempts", 4); // main topic + 3 retry topics
		assertThat(paymentRow(p.eventId())).isNull();

		// "the database is fixed": stop failing, replay from the inspector
		faults.failNext(key, 0);
		var result = deadLetters.replay(((Number) dl.get("id")).longValue());
		assertThat(result).isInstanceOf(DeadLetterService.ReplayResult.Replayed.class);
		await().atMost(SLOW).until(() -> paymentRow(p.eventId()) != null);
		assertThat(paymentRow(p.eventId())).containsEntry("status", "COMPLETED");

		// replaying again is harmless: dedupe makes it a no-op
		deadLetters.replay(((Number) dl.get("id")).longValue());
		var marker = payment("it-dlt-marker-" + UUID.randomUUID());
		send(marker.fromAccount(), json.writeValueAsString(marker));
		await().atMost(SLOW).until(() -> paymentRow(marker.eventId()) != null);
		assertThat(db.sql("SELECT count(*) FROM ledger_entries WHERE payment_id = ?").param(p.eventId())
			.query(Long.class).single()).isEqualTo(2);
	}

	@Test
	void malformedJsonSkipsRetriesAndDoesNotBlockThePartition() throws Exception {
		String marker = "it-poison-" + UUID.randomUUID();
		send("ACC-000015", "{not json " + marker);

		await().atMost(FAST).until(() -> deadLetterFor("ACC-000015", marker) != null);
		assertThat((String) deadLetterFor("ACC-000015", marker).get("exception_class")).startsWith("tools.jackson");
		assertThat(deadLetterFor("ACC-000015", marker)).containsEntry("attempts", 1);

		// same key = same partition: the next payment still goes through
		var next = payment("it-after-poison-" + UUID.randomUUID());
		send(next.fromAccount(), json.writeValueAsString(next));
		await().atMost(FAST).until(() -> paymentRow(next.eventId()) != null);

		long id = ((Number) deadLetterFor("ACC-000015", marker).get("id")).longValue();
		assertThat(deadLetters.replay(id)).isInstanceOf(DeadLetterService.ReplayResult.Unreplayable.class);
	}

	@Test
	void eventIdNotDerivedFromIdempotencyKeyIsRejectedAsInvalid() throws Exception {
		String key = "it-tamper-" + UUID.randomUUID();
		var good = payment(key);
		var tampered = new PaymentInitiated(good.schemaVersion(), UUID.randomUUID(), key, good.fromAccount(),
				good.toAccount(), good.amount(), good.currency(), good.acceptedAt());
		send(tampered.fromAccount(), json.writeValueAsString(tampered));

		await().atMost(FAST).until(() -> deadLetterFor(tampered.fromAccount(), key) != null);
		assertThat(deadLetterFor(tampered.fromAccount(), key))
			.containsEntry("exception_class", InvalidEventException.class.getName())
			.containsEntry("attempts", 1);
		assertThat(paymentRow(tampered.eventId())).isNull();
	}
}
