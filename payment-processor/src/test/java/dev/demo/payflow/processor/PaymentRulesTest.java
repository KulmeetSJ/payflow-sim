package dev.demo.payflow.processor;

import static org.assertj.core.api.Assertions.assertThat;

import dev.demo.payflow.events.PaymentInitiated;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PaymentRulesTest {

	static final Account GBP_A = new Account("ACC-000003", "GBP", new BigDecimal("100.00"));

	static final Account GBP_B = new Account("ACC-000006", "GBP", new BigDecimal("0.00"));

	static final Account EUR_C = new Account("ACC-000001", "EUR", new BigDecimal("100.00"));

	static PaymentInitiated pay(String from, String to, String amount, String ccy) {
		return PaymentInitiated.of("k-" + from + to + amount, from, to, new BigDecimal(amount), ccy, Instant.now());
	}

	static Account acc(String id) {
		return switch (id) {
			case "ACC-000003" -> GBP_A;
			case "ACC-000006" -> GBP_B;
			case "ACC-000001" -> EUR_C;
			default -> null;
		};
	}

	@ParameterizedTest(name = "{0} -> {1} {2} {3}: {4}")
	@CsvSource({
		"ACC-000003, ACC-000006, 100.00,   GBP, OK",
		"ACC-000003, ACC-000006, 100.01,   GBP, INSUFFICIENT_FUNDS",
		"ACC-000003, ACC-000003, 1.00,     GBP, SAME_ACCOUNT",
		"ACC-000003, ACC-000006, 0.00,     GBP, INVALID_AMOUNT",
		"ACC-000003, ACC-000006, 1.001,    GBP, INVALID_AMOUNT",
		"ACC-000003, ACC-000006, 50000.01, GBP, LIMIT_EXCEEDED",
		"ACC-999999, ACC-000006, 1.00,     GBP, UNKNOWN_FROM_ACCOUNT",
		"ACC-000003, ACC-999999, 1.00,     GBP, UNKNOWN_TO_ACCOUNT",
		"ACC-000003, ACC-000001, 1.00,     GBP, CURRENCY_MISMATCH",
		"ACC-000003, ACC-000006, 1.00,     EUR, CURRENCY_MISMATCH",
	})
	void rules(String from, String to, String amount, String ccy, String expected) {
		var reason = PaymentRules.check(pay(from, to, amount, ccy), acc(from), acc(to));
		assertThat(reason.orElse("OK")).isEqualTo(expected);
	}
}
