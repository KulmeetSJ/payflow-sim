package dev.demo.payflow.processor;

/** Thrown by {@link FaultInjector}; stands in for a DB blip, connection-pool timeout etc. */
class SimulatedTransientFault extends RuntimeException {

	SimulatedTransientFault(String message) {
		super(message);
	}
}
