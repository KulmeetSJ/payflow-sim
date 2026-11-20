#!/usr/bin/env bash
# Consumer-group demo: scale payment-processor 1 -> 3 -> 6 -> 7 -> 3 while payments keep flowing,
# record who owns which partition after each step, then prove nothing was lost or double-booked.
#
#   docker compose up -d --build && demos/consumer-scaling.sh
#   RATE=40 STEP_SECONDS=30 demos/consumer-scaling.sh
#
# payments.initiated has 6 partitions, so the 7th instance must sit idle: within a group a
# partition has exactly one owner (that's what gives per-account ordering).
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-40}
STEP_SECONDS=${STEP_SECONDS:-30}
STEPS=(${STEPS:-1 3 6 7 3})
STAMP=$(date +%Y%m%d-%H%M%S)
OUT=demos/output/consumer-scaling-$STAMP.txt
WORK=$(mktemp -d)
trap 'kill "${gen_pid:-}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

sql() { docker compose exec -T postgres psql -U payflow -d payflow -tAc "$1"; }
kafka() { docker compose exec -T kafka-1 /opt/kafka/bin/"$1" --bootstrap-server localhost:9092 "${@:2}"; }
log() { echo "$*" | tee -a "$OUT"; }
members() { kafka kafka-consumer-groups.sh --describe --group payment-processor --members --verbose 2>/dev/null | awk 'NF && $1 != "GROUP"'; }
# accumulate processor logs before containers are removed by a scale-down
save_logs() { docker compose logs --no-log-prefix payment-processor 2>/dev/null >>"$WORK/logs.txt" || true; }

# instance name for each member host IP
instance_table() {
	local map="$WORK/ips"
	for c in $(docker compose ps -q payment-processor); do
		docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} {{.Name}}' "$c"
	done | sed 's|/payflow-sim-||' >"$map"
	members | awk -v map="$map" '
		BEGIN { while ((getline l < map) > 0) { split(l, f, " "); name[f[1]] = f[2] } }
		{ ip = $3; sub("^/", "", ip); parts = $7; sub("payments.initiated:", "", parts)
		  if ($5 == 0) parts = "(idle: no partitions)"
		  printf "    %-32s partitions: %s\n", name[ip], parts }' | sort -V
}

wait_stable() { # $1 = expected instances
	local start=$SECONDS n parts state
	while ((SECONDS - start < 120)); do
		state=$(kafka kafka-consumer-groups.sh --describe --group payment-processor --state 2>/dev/null | awk 'NF && $1 != "GROUP" {print $(NF-1)}')
		n=$(members | wc -l | tr -d ' ')
		parts=$(members | awk '{s += $5} END {print s + 0}')
		if [[ $state == Stable && $n == "$1" && $parts == 6 ]]; then
			echo $((SECONDS - start))
			return
		fi
		sleep 1
	done
	echo "timeout"
}

balances() { sql "SELECT string_agg(currency || '=' || total, ' ' ORDER BY currency) FROM (SELECT currency, sum(balance) total FROM accounts GROUP BY currency) t"; }

log "# Consumer-group scaling demo, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "# load: ~${RATE}/s via tools/generate.py; steps: ${STEPS[*]}; ${STEP_SECONDS}s per step"
log "# assignor: CooperativeStickyAssignor (classic group protocol)"
log ""

docker compose up -d --scale payment-processor="${STEPS[0]}" --no-recreate payment-processor >/dev/null 2>&1
wait_stable "${STEPS[0]}" >/dev/null
payments_before=$(sql "SELECT count(*) FROM payments")
dlt_before=$(sql "SELECT count(*) FROM dead_letters")
balances_before=$(balances)

count=$((RATE * STEP_SECONDS * ${#STEPS[@]}))
python3 tools/generate.py --count "$count" --rate "$RATE" --seed 7 >"$WORK/gen.txt" 2>&1 &
gen_pid=$!

for n in "${STEPS[@]}"; do
	save_logs
	t0=$SECONDS
	docker compose up -d --scale payment-processor="$n" --no-recreate payment-processor >/dev/null 2>&1
	settle=$(wait_stable "$n")
	log "## ${n} instance(s): group stable $((SECONDS - t0))s after the scale command (settle wait ${settle}s)"
	instance_table | tee -a "$OUT"
	log ""
	sleep "$STEP_SECONDS"
done

log "waiting for the generator to finish..."
wait "$gen_pid"
gen=$(tail -1 "$WORK/gen.txt")
accepted=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d['valid'] + d['invalid'])" "$gen")

# drain: every accepted payment must reach a final state
for _ in $(seq 1 120); do
	done_count=$(( $(sql "SELECT count(*) FROM payments") - payments_before ))
	((done_count >= accepted)) && break
	sleep 1
done
save_logs

log "## Invariants"
log "    generator:                      $gen"
log "    accepted by gateway (202):      $accepted"
log "    reached a final state:          $done_count   (lost: $((accepted - done_count)))"
log "    completed without 2 ledger rows: $(sql "SELECT count(*) FROM payments p WHERE status = 'COMPLETED' AND (SELECT count(*) FROM ledger_entries l WHERE l.payment_id = p.payment_id) <> 2")"
log "    ledger net (credits - debits):   $(sql "SELECT coalesce(sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0) FROM ledger_entries")"
log "    balances before: $balances_before"
log "    balances after:  $(balances)"
log "    new dead letters:                $(( $(sql "SELECT count(*) FROM dead_letters") - dlt_before ))"
log "    redeliveries skipped by dedupe:  $(sort -u "$WORK/logs.txt" | grep -c 'duplicate event' || true)"
log "    group generations seen:          $({ sort -u "$WORK/logs.txt" | grep 'groupId=payment-processor\]' | grep -o 'joined group with generation Generation{generationId=[0-9]*' | sort -u | wc -l | tr -d ' '; } || true)"
log ""
log "saved to $OUT"
