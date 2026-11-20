package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import java.math.BigDecimal;
import java.util.Optional;

/** Business rules. Pure function: easy to unit-test, no I/O. */
final class PaymentRules {

	static final BigDecimal MAX_AMOUNT = new BigDecimal("50000.00");

	private PaymentRules() {
	}

	/** @return a reject reason, or empty if the payment may be booked */
	static Optional<String> check(PaymentInitiated p, Account from, Account to) {
		if (p.fromAccount().equals(p.toAccount())) {
			return Optional.of("SAME_ACCOUNT");
		}
		if (p.amount().signum() <= 0 || p.amount().scale() > 2) {
			return Optional.of("INVALID_AMOUNT");
		}
		if (p.amount().compareTo(MAX_AMOUNT) > 0) {
			return Optional.of("LIMIT_EXCEEDED");
		}
		if (from == null) {
			return Optional.of("UNKNOWN_FROM_ACCOUNT");
		}
		if (to == null) {
			return Optional.of("UNKNOWN_TO_ACCOUNT");
		}
		if (!from.currency().equals(p.currency()) || !to.currency().equals(p.currency())) {
			return Optional.of("CURRENCY_MISMATCH");
		}
		if (from.balance().compareTo(p.amount()) < 0) {
			return Optional.of("INSUFFICIENT_FUNDS");
		}
		return Optional.empty();
	}
}
