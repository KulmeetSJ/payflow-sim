package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class PaymentService {

	private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

	enum Outcome { COMPLETED, REJECTED, DUPLICATE }

	private final JdbcClient db;

	PaymentService(JdbcClient db) {
		this.db = db;
	}

	/**
	 * One transaction: dedupe marker + payment row + ledger + balances. Either all of it commits or
	 * none of it does; the Kafka offset is committed only after this method returns.
	 *
	 * <p>3 statements + COMMIT = 4 round trips to PostgreSQL (was 7 + COMMIT before the
	 * 2026-11-20 DB-latency postmortem): every round trip is paid again whenever the network or the
	 * database is slow, on a thread that is the only consumer of its partitions.
	 */
	@Transactional
	Outcome process(PaymentInitiated p, int partition, long offset, int attempt) {
		int inserted = db.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING")
			.param(p.eventId())
			.update();
		if (inserted == 0) {
			log.info("duplicate event {} (partition {} offset {}) skipped", p.eventId(), partition, offset);
			return Outcome.DUPLICATE;
		}

		// Lock both accounts in a fixed order (by id), so A->B and B->A on different partitions
		// can't deadlock each other.
		Map<String, Account> accounts = db
			.sql("SELECT account_id, currency, balance FROM accounts WHERE account_id IN (?, ?) "
					+ "ORDER BY account_id FOR UPDATE")
			.params(p.fromAccount(), p.toAccount())
			.query(Account.class)
			.list()
			.stream()
			.collect(Collectors.toMap(Account::accountId, Function.identity()));

		Optional<String> reject = PaymentRules.check(p, accounts.get(p.fromAccount()), accounts.get(p.toAccount()));
		if (reject.isPresent()) {
			insertPayment(p, "REJECTED", reject.get(), partition, offset, attempt);
			log.info("payment {} rejected: {}", p.eventId(), reject.get());
			return Outcome.REJECTED;
		}

		book(p, partition, offset, attempt);
		return Outcome.COMPLETED;
	}

	/** Payment row, both ledger entries and both balance updates in ONE statement (data-modifying CTEs). */
	private void book(PaymentInitiated p, int partition, long offset, int attempt) {
		int updated = db.sql("""
				WITH payment AS (
				    INSERT INTO payments (payment_id, idempotency_key, from_account, to_account, amount, currency,
				                          status, accepted_at, kafka_partition, kafka_offset, delivery_attempt)
				    VALUES (:id, :key, :from, :to, :amount, :ccy, 'COMPLETED', :acceptedAt, :partition, :offset, :attempt)
				), ledger AS (
				    INSERT INTO ledger_entries (payment_id, account_id, direction, amount)
				    VALUES (:id, :from, 'DEBIT', :amount), (:id, :to, 'CREDIT', :amount)
				)
				UPDATE accounts
				   SET balance = balance + CASE WHEN account_id = :from THEN -CAST(:amount AS NUMERIC)
				                                ELSE CAST(:amount AS NUMERIC) END,
				       updated_at = now()
				 WHERE account_id IN (:from, :to)
				""")
			.param("id", p.eventId())
			.param("key", p.idempotencyKey())
			.param("from", p.fromAccount())
			.param("to", p.toAccount())
			.param("amount", p.amount())
			.param("ccy", p.currency())
			.param("acceptedAt", Timestamp.from(p.acceptedAt()))
			.param("partition", partition)
			.param("offset", offset)
			.param("attempt", attempt)
			.update();
		if (updated != 2) { // both rows are locked FOR UPDATE above, so this would be a bug, not a race
			throw new IllegalStateException("expected 2 balance updates for " + p.eventId() + ", got " + updated);
		}
	}

	private void insertPayment(PaymentInitiated p, String status, String reason, int partition, long offset,
			int attempt) {
		db.sql("""
				INSERT INTO payments (payment_id, idempotency_key, from_account, to_account, amount, currency,
				                      status, reject_reason, accepted_at, kafka_partition, kafka_offset, delivery_attempt)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""")
			.params(p.eventId(), p.idempotencyKey(), p.fromAccount(), p.toAccount(), p.amount(), p.currency(), status,
					reason, Timestamp.from(p.acceptedAt()), partition, offset, attempt)
			.update();
	}
}
