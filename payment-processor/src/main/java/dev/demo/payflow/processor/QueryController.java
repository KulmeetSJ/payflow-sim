package dev.demo.payflow.processor;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Read side for demos and load tests: payment status and account balance. */
@RestController
class QueryController {

	private final JdbcClient db;

	QueryController(JdbcClient db) {
		this.db = db;
	}

	@GetMapping("/payments/{id}")
	ResponseEntity<Map<String, Object>> payment(@PathVariable UUID id) {
		return ResponseEntity.of(db.sql("""
				SELECT payment_id, from_account, to_account, amount, currency, status, reject_reason,
				       accepted_at, committed_at,
				       round(extract(epoch FROM committed_at - accepted_at) * 1000) AS e2e_ms
				FROM payments WHERE payment_id = ?""")
			.param(id)
			.query()
			.listOfRows()
			.stream()
			.findFirst());
	}

	@GetMapping("/accounts/{id}")
	ResponseEntity<Map<String, Object>> account(@PathVariable String id) {
		return ResponseEntity.of(db.sql("SELECT account_id, currency, balance FROM accounts WHERE account_id = ?")
			.param(id)
			.query()
			.listOfRows()
			.stream()
			.findFirst());
	}
}
