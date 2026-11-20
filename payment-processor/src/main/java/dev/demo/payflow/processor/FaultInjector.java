package dev.demo.payflow.processor;

import dev.demo.payflow.events.PaymentInitiated;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Deliberate transient failures, for retry/DLT demos and tests (and later the SLO burn demo).
 * Off unless switched on through {@code /admin/faults} or a test. Two modes:
 * <ul>
 * <li>fail the next N processing attempts of payments whose Idempotency-Key starts with a prefix
 * (deterministic, used by tests and demos)</li>
 * <li>fail a random fraction of all attempts (load / SLO demos)</li>
 * </ul>
 */
@Component
class FaultInjector {

	private final Map<String, AtomicInteger> remainingByPrefix = new ConcurrentHashMap<>();

	private final AtomicLong injected = new AtomicLong();

	private volatile double rate;

	void failNext(String keyPrefix, int attempts) {
		remainingByPrefix.put(keyPrefix, new AtomicInteger(attempts));
	}

	void setRate(double rate) {
		if (rate < 0 || rate > 1) {
			throw new IllegalArgumentException("rate must be between 0 and 1");
		}
		this.rate = rate;
	}

	void clear() {
		remainingByPrefix.clear();
		rate = 0;
	}

	void maybeFail(PaymentInitiated p) {
		for (var e : remainingByPrefix.entrySet()) {
			if (p.idempotencyKey().startsWith(e.getKey()) && e.getValue().getAndUpdate(v -> Math.max(0, v - 1)) > 0) {
				fail("prefix " + e.getKey());
			}
		}
		if (rate > 0 && ThreadLocalRandom.current().nextDouble() < rate) {
			fail("rate " + rate);
		}
	}

	private void fail(String why) {
		injected.incrementAndGet();
		throw new SimulatedTransientFault("injected transient fault (" + why + ")");
	}

	Map<String, Object> state() {
		var m = new LinkedHashMap<String, Object>();
		m.put("rate", rate);
		m.put("remainingByPrefix", Map.copyOf(remainingByPrefix));
		m.put("injectedTotal", injected.get());
		return m;
	}
}
