package dev.demo.payflow.processor;

import java.util.List;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator endpoints: DLT inspector/replay and fault injection. Off by default
 * ({@code payflow.admin.enabled}); the compose demo turns them on. They have no auth because this
 * is a local simulator; a real system would put them behind authn/z and audit every replay.
 */
@RestController
@RequestMapping("/admin")
@ConditionalOnBooleanProperty("payflow.admin.enabled")
class AdminController {

	private final DeadLetterService deadLetters;

	private final FaultInjector faults;

	AdminController(DeadLetterService deadLetters, FaultInjector faults) {
		this.deadLetters = deadLetters;
		this.faults = faults;
	}

	@GetMapping("/dlt")
	List<Map<String, Object>> list(@RequestParam(defaultValue = "false") boolean pending,
			@RequestParam(defaultValue = "50") int limit) {
		return deadLetters.list(pending, Math.min(limit, 500));
	}

	@GetMapping("/dlt/{id}")
	ResponseEntity<Map<String, Object>> get(@PathVariable long id) {
		return ResponseEntity.of(deadLetters.get(id));
	}

	@PostMapping("/dlt/{id}/replay")
	ResponseEntity<Map<String, Object>> replay(@PathVariable long id) throws Exception {
		return switch (deadLetters.replay(id)) {
			case DeadLetterService.ReplayResult.Replayed r ->
				ResponseEntity.accepted().body(Map.of("id", r.id(), "replayCount", r.replayCount()));
			case DeadLetterService.ReplayResult.NotFound n -> ResponseEntity.notFound().build();
			case DeadLetterService.ReplayResult.Unreplayable u ->
				ResponseEntity.status(HttpStatus.UNPROCESSABLE_CONTENT).body(Map.of("id", u.id(), "reason", u.reason()));
		};
	}

	@GetMapping("/faults")
	Map<String, Object> faults() {
		return faults.state();
	}

	/** e.g. {@code POST /admin/faults?keyPrefix=demo-retry-&failures=3} or {@code ?rate=0.05}. */
	@PostMapping("/faults")
	Map<String, Object> setFaults(@RequestParam(required = false) String keyPrefix,
			@RequestParam(defaultValue = "0") int failures, @RequestParam(required = false) Double rate) {
		if (keyPrefix != null) {
			faults.failNext(keyPrefix, failures);
		}
		if (rate != null) {
			faults.setRate(rate);
		}
		return faults.state();
	}

	@DeleteMapping("/faults")
	Map<String, Object> clearFaults() {
		faults.clear();
		return faults.state();
	}
}
