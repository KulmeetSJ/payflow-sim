package dev.demo.payflow.gateway;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;

/**
 * Syntactic checks only. Business rules (account exists, same currency, enough funds) need the
 * ledger, so the processor applies them and records a REJECTED payment.
 */
public record PaymentRequest(
		@NotNull @Pattern(regexp = "ACC-\\d{6}") String fromAccount,
		@NotNull @Pattern(regexp = "ACC-\\d{6}") String toAccount,
		@NotNull @DecimalMin(value = "0.01") @Digits(integer = 12, fraction = 2) BigDecimal amount,
		@NotNull @Pattern(regexp = "[A-Z]{3}") String currency) {
}
