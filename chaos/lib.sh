# Shared helpers for the chaos experiments (sourced, not run).
PROM=http://localhost:9090
G='consumergroup="payment-processor",topic="payments.initiated"'
log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT"; }
q() { python3 demos/promq.py "$1" "$PROM" ${2:-}; }
lag() { q "sum(kafka_consumergroup_lag{$G})" | cut -d. -f1; }
psql_() { docker compose exec -T postgres psql -U payflow -d payflow -tAc "$1"; }
wait_lag_zero() { local t0=$SECONDS; until [[ $(lag) == 0 ]] || ((SECONDS - t0 > ${1:-600})); do sleep 5; done; echo $((SECONDS - t0)); }

# k6 in the background at a fixed rate; RUN tags the idempotency keys so the run can be reconciled
start_load() { # rate seconds
	RUN=$(basename "$0" .sh)-$(date +%H%M%S)
	k6 run -q --no-color -e RATE="$1" -e DURATION="$2s" -e RUN="$RUN" --summary-export "/tmp/$RUN.json" \
		load/payments.js >/dev/null 2>&1 &
	K6=$!
	log "load: $1/s for $2s (run $RUN)"
}

# Every 202 must end up as exactly one payment row, and money must be conserved.
reconcile() {
	wait $K6 || true
	local drain; drain=$(wait_lag_zero 600)
	sleep 3
	read -r accepted failed < <(python3 -c "
import json; m=json.load(open('/tmp/$RUN.json'))['metrics']
print(int(m.get('payments_accepted',{}).get('count',0)), int(m['http_req_failed'].get('passes',0)))")
	local rows dup bal debit credit
	rows=$(psql_ "SELECT count(*) FROM payments WHERE idempotency_key LIKE 'k6-$RUN-%'")
	bal=$(psql_ "SELECT sum(balance) FROM accounts")
	read -r debit credit < <(psql_ "SELECT sum(amount) FILTER (WHERE direction='DEBIT'), sum(amount) FILTER (WHERE direction='CREDIT') FROM ledger_entries" | tr '|' ' ')
	log "reconcile: k6 got 202 for $accepted, non-2xx/errors $failed; payment rows for the run $rows (lag drained ${drain}s after load stopped)"
	log "reconcile: lost $((accepted - rows)), total balance $bal (expected 100000000.00), ledger debits $debit = credits $credit"
}
