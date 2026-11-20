package dev.demo.payflow.processor;

import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/**
 * Two retry layers (see README "Retries and the DLT"):
 * <ol>
 * <li><b>Blocking</b>: 2 quick in-place retries, 200 ms apart, for transient errors. A short blip
 * is absorbed without the payment leaving its partition, so per-account order is kept.</li>
 * <li><b>Non-blocking</b> ({@code @RetryableTopic} on the listener): if it still fails, the event
 * moves to a retry topic with exponential back-off, then to the DLT. The partition keeps flowing,
 * at the cost of that one payment possibly being applied after later ones from the same account.</li>
 * </ol>
 * Poison events (unparseable, structurally invalid) skip both layers and go straight to the DLT.
 */
@Configuration(proxyBeanMethods = false)
class RetryConfig extends RetryTopicConfigurationSupport {

	/** Resumes paused retry-topic partitions when a back-off expires. */
	@Bean
	RetryTopicSchedulerWrapper retryTopicScheduler() {
		var scheduler = new ThreadPoolTaskScheduler();
		scheduler.setThreadNamePrefix("retry-backoff-");
		scheduler.initialize();
		return new RetryTopicSchedulerWrapper(scheduler);
	}

	@Override
	protected void configureBlockingRetries(BlockingRetriesConfigurer blocking) {
		blocking.retryOn(SimulatedTransientFault.class, TransientDataAccessException.class,
				DataAccessResourceFailureException.class)
			.backOff(new FixedBackOff(200L, 2));
	}

	@Override
	protected void manageNonBlockingFatalExceptions(List<Class<? extends Throwable>> fatal) {
		fatal.add(JacksonException.class);
		fatal.add(InvalidEventException.class);
	}
}
