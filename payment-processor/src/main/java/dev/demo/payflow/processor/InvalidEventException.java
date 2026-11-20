package dev.demo.payflow.processor;

/** The event can never be processed, however often it is retried: it goes straight to the DLT. */
class InvalidEventException extends RuntimeException {

	InvalidEventException(String message) {
		super(message);
	}
}
