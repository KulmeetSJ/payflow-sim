package dev.demo.payflow.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * SLI metrics (see docs/slo.md):
 * <ul>
 * <li>{@code payments_processed_total{outcome}}: completed / rejected / duplicate</li>
 * <li>{@code payment_e2e_latency_seconds}: gateway accept -> DB commit. Histogram with an explicit
 * 2 s bucket, so the latency SLI ("committed within 2 s") is an exact bucket ratio, not an
 * estimate from interpolated quantiles.</li>
 * <li>{@code payments_dead_lettered_total{kind}}: exhausted (retries ran out, counts against the
 * success SLO) or poison (malformed input, does not)</li>
 * </ul>
 * All series are registered at startup with value 0, so SLO ratios exist before the first failure.
 */
@Component
class PaymentMetrics {

	static final Duration LATENCY_SLO = Duration.ofSeconds(2);

	enum DeadLetterKind { EXHAUSTED, POISON }

	private final Map<PaymentService.Outcome, Counter> processed = new EnumMap<>(PaymentService.Outcome.class);

	private final Map<DeadLetterKind, Counter> deadLettered = new EnumMap<>(DeadLetterKind.class);

	private final Timer e2e;

	private final Clock clock = Clock.systemUTC();

	PaymentMetrics(MeterRegistry registry) {
		for (var o : PaymentService.Outcome.values()) {
			processed.put(o, Counter.builder("payments.processed").description("Payments taken off the topic")
				.tag("outcome", o.name().toLowerCase()).register(registry));
		}
		for (var k : DeadLetterKind.values()) {
			deadLettered.put(k, Counter.builder("payments.dead.lettered").description("Events sent to the DLT")
				.tag("kind", k.name().toLowerCase()).register(registry));
		}
		e2e = Timer.builder("payment.e2e.latency")
			.description("Gateway acceptance to ledger commit")
			.serviceLevelObjectives(Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1), LATENCY_SLO,
					Duration.ofSeconds(5), Duration.ofSeconds(10))
			.publishPercentileHistogram()
			.minimumExpectedValue(Duration.ofMillis(1))
			.maximumExpectedValue(Duration.ofSeconds(30))
			.register(registry);
	}

	/** Call only after the DB transaction has committed. */
	void processed(PaymentService.Outcome outcome, Instant acceptedAt) {
		processed.get(outcome).increment();
		if (outcome != PaymentService.Outcome.DUPLICATE) {
			e2e.record(Duration.between(acceptedAt, clock.instant()));
		}
	}

	void deadLettered(String exceptionClass) {
		boolean poison = exceptionClass != null && (exceptionClass.equals(InvalidEventException.class.getName())
				|| exceptionClass.startsWith("tools.jackson."));
		deadLettered.get(poison ? DeadLetterKind.POISON : DeadLetterKind.EXHAUSTED).increment();
	}
}
