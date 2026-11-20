package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Service
class DeadLetterService {

	private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

	sealed interface ReplayResult {

		record Replayed(long id, int replayCount) implements ReplayResult {
		}

		record NotFound(long id) implements ReplayResult {
		}

		/** Still unparseable or invalid: replaying would only dead-letter it again. */
		record Unreplayable(long id, String reason) implements ReplayResult {
		}
	}

	private final JdbcClient db;

	private final KafkaTemplate<String, String> kafka;

	private final JsonMapper json;

	DeadLetterService(JdbcClient db, KafkaTemplate<String, String> kafka, JsonMapper json) {
		this.db = db;
		this.kafka = kafka;
		this.json = json;
	}

	/**
	 * Header names differ by path: an event dead-lettered via the retry-topic chain carries
	 * {@code kafka_exception-*} / {@code kafka_original-*}; a plain DeadLetterPublishingRecoverer
	 * writes {@code kafka_dlt-*}. Both are read.
	 */
	void record(ConsumerRecord<String, String> rec) {
		String cause = header(rec, KafkaHeaders.EXCEPTION_CAUSE_FQCN, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
		String exception = cause != null ? cause : header(rec, KafkaHeaders.EXCEPTION_FQCN, KafkaHeaders.DLT_EXCEPTION_FQCN);
		int inserted = db.sql("""
				INSERT INTO dead_letters (dlt_topic, dlt_partition, dlt_offset, message_key, payload, attempts,
				    original_topic, original_partition, original_offset, exception_class, exception_message, stacktrace)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (dlt_topic, dlt_partition, dlt_offset) DO NOTHING""")
			.params(rec.topic(), rec.partition(), rec.offset(), rec.key(), String.valueOf(rec.value()),
					// the attempts header holds the NEXT attempt number, so on the DLT it is attempts made + 1
					PaymentListener.attempt(rec) - 1,
					header(rec, KafkaHeaders.ORIGINAL_TOPIC, KafkaHeaders.DLT_ORIGINAL_TOPIC),
					intHeader(rec, KafkaHeaders.ORIGINAL_PARTITION, KafkaHeaders.DLT_ORIGINAL_PARTITION),
					longHeader(rec, KafkaHeaders.ORIGINAL_OFFSET, KafkaHeaders.DLT_ORIGINAL_OFFSET), exception,
					header(rec, KafkaHeaders.EXCEPTION_MESSAGE, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
					header(rec, KafkaHeaders.EXCEPTION_STACKTRACE, KafkaHeaders.DLT_EXCEPTION_STACKTRACE))
			.update();
		if (inserted == 1) {
			log.warn("dead letter {}-{}@{} key={} exception={}", rec.topic(), rec.partition(), rec.offset(), rec.key(),
					exception);
		}
	}

	List<Map<String, Object>> list(boolean pendingOnly, int limit) {
		return db.sql("""
				SELECT id, message_key, attempts, original_topic, original_partition, original_offset, exception_class,
				       left(exception_message, 300) AS exception_message, received_at, replay_count, last_replayed_at
				FROM dead_letters WHERE (? = false OR replay_count = 0) ORDER BY id DESC LIMIT ?""")
			.params(pendingOnly, limit)
			.query()
			.listOfRows();
	}

	Optional<Map<String, Object>> get(long id) {
		return db.sql("SELECT * FROM dead_letters WHERE id = ?").param(id).query().listOfRows().stream().findFirst();
	}

	/**
	 * Re-publishes the original payload to the main topic. Safe to repeat: the processor's
	 * processed_events dedupe makes a payment that was in fact booked a no-op.
	 */
	ReplayResult replay(long id) throws Exception {
		Optional<String> payload = db.sql("SELECT payload FROM dead_letters WHERE id = ?").param(id)
			.query(String.class).optional();
		if (payload.isEmpty()) {
			return new ReplayResult.NotFound(id);
		}
		PaymentInitiated p;
		try {
			p = json.readValue(payload.get(), PaymentInitiated.class);
			EventValidator.validate(p);
		}
		catch (JacksonException | InvalidEventException e) {
			return new ReplayResult.Unreplayable(id, e.getClass().getSimpleName() + ": " + e.getMessage());
		}
		kafka.send(Topics.PAYMENTS_INITIATED, p.fromAccount(), payload.get()).get(10, TimeUnit.SECONDS);
		int count = db.sql("""
				UPDATE dead_letters SET replay_count = replay_count + 1, last_replayed_at = now()
				WHERE id = ? RETURNING replay_count""").param(id).query(Integer.class).single();
		log.info("dead letter {} replayed (payment {}, replay #{})", id, p.eventId(), count);
		return new ReplayResult.Replayed(id, count);
	}

	private static byte[] raw(ConsumerRecord<?, ?> rec, String... names) {
		for (String name : names) {
			Header h = rec.headers().lastHeader(name);
			if (h != null) {
				return h.value();
			}
		}
		return null;
	}

	private static String header(ConsumerRecord<?, ?> rec, String... names) {
		byte[] v = raw(rec, names);
		return v == null ? null : new String(v, StandardCharsets.UTF_8);
	}

	private static Integer intHeader(ConsumerRecord<?, ?> rec, String... names) {
		byte[] v = raw(rec, names);
		return v == null || v.length != Integer.BYTES ? null : ByteBuffer.wrap(v).getInt();
	}

	private static Long longHeader(ConsumerRecord<?, ?> rec, String... names) {
		byte[] v = raw(rec, names);
		return v == null || v.length != Long.BYTES ? null : ByteBuffer.wrap(v).getLong();
	}
}
