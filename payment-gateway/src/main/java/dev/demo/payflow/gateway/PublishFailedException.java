package dev.demo.payflow.gateway;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/** Kafka did not acknowledge in time: the client should retry with the same Idempotency-Key. */
@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
class PublishFailedException extends RuntimeException {

	PublishFailedException(Throwable cause) {
		super("payment not accepted: event could not be published", cause);
	}
}
