package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import java.sql.Timestamp;
import java.util.List;
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
		insertPayment(p, reject.isPresent() ? "REJECTED" : "COMPLETED", reject.orElse(null), partition, offset, attempt);
		if (reject.isPresent()) {
			log.info("payment {} rejected: {}", p.eventId(), reject.get());
			return Outcome.REJECTED;
		}

		for (var e : List.of(new Object[] { p.fromAccount(), "DEBIT" }, new Object[] { p.toAccount(), "CREDIT" })) {
			db.sql("INSERT INTO ledger_entries (payment_id, account_id, direction, amount) VALUES (?, ?, ?, ?)")
				.params(p.eventId(), e[0], e[1], p.amount())
				.update();
		}
		db.sql("UPDATE accounts SET balance = balance - ?, updated_at = now() WHERE account_id = ?")
			.params(p.amount(), p.fromAccount())
			.update();
		db.sql("UPDATE accounts SET balance = balance + ?, updated_at = now() WHERE account_id = ?")
			.params(p.amount(), p.toAccount())
			.update();
		return Outcome.COMPLETED;
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
