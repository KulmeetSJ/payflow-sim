#!/usr/bin/env bash
# Forced error spike -> fast-burn page, end to end through Prometheus and Alertmanager.
#   1. steady load (~20/s), clean baseline
#   2. inject transient faults on 60% of processing attempts. ~22% of payments fail all 3
#      in-place tries, go through a retry topic and commit after > 2 s: the latency SLI drops
#      to ~78%, a burn rate of ~22x against a 1% budget (page threshold: 14.4x)
#   3. record when PaymentLatencyBudgetFastBurn goes pending, fires, and reaches Alertmanager
#   4. clear the fault and record when the alert resolves (the 5m window lets it reset fast)
#
#   docker compose up -d --build && demos/slo-burn.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-20}
BASELINE_S=${BASELINE_S:-120}
FAULT_RATE=${FAULT_RATE:-0.6}
PROC=http://localhost:8081
PROM=http://localhost:9090
OUT=demos/output/slo-burn-$(date +%Y%m%d-%H%M%S).txt
ALERT=PaymentLatencyBudgetFastBurn

log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT"; }
q() { python3 demos/promq.py "$1" "$PROM"; }
alert_state() { curl -s "$PROM/api/v1/alerts" | python3 -c "import json,sys; a=[x['state'] for x in json.load(sys.stdin)['data']['alerts'] if x['labels']['alertname']=='$ALERT']; print(a[0] if a else 'inactive')"; }
in_alertmanager() { curl -s "http://localhost:9093/api/v2/alerts?filter=alertname=$ALERT" | python3 -c 'import json,sys; a=json.load(sys.stdin); print(",".join(r["name"] for x in a for r in x["receivers"]) or "none")'; }
burn() { echo "burn 5m=$(q 'slo:payment_latency:error_ratio_rate5m / 0.01')x 1h=$(q 'slo:payment_latency:error_ratio_rate1h / 0.01')x"; }

docker compose up -d --scale payment-processor=1 --no-recreate payment-processor >/dev/null 2>&1
curl -sf -X DELETE "$PROC/admin/faults" >/dev/null

log "# SLO burn demo: load ${RATE}/s, fault rate ${FAULT_RATE} per processing attempt"
python3 tools/generate.py --count 1000000 --rate "$RATE" --seed 11 >/dev/null 2>&1 &
gen=$!
trap 'kill $gen 2>/dev/null || true; curl -sf -X DELETE "$PROC/admin/faults" >/dev/null || true' EXIT

sleep "$BASELINE_S"
log "baseline done: $(burn), alert=$(alert_state)"

curl -sf -X POST "$PROC/admin/faults?rate=$FAULT_RATE" >/dev/null
t_fault=$SECONDS
log "FAULT ON"
pending="" firing="" am=""
while [[ -z $am ]] && ((SECONDS - t_fault < 1200)); do
	state=$(alert_state)
	[[ -z $pending && $state != inactive ]] && pending=$((SECONDS - t_fault)) && log "  alert pending after ${pending}s ($(burn))"
	[[ -z $firing && $state == firing ]] && firing=$((SECONDS - t_fault)) && log "  alert FIRING after ${firing}s ($(burn))"
	if [[ -n $firing ]]; then
		r=$(in_alertmanager)
		[[ $r != none ]] && am=$((SECONDS - t_fault)) && log "  in Alertmanager after ${am}s, routed to receiver: $r"
	fi
	sleep 5
done

curl -sf -X DELETE "$PROC/admin/faults" >/dev/null
t_fix=$SECONDS
log "FAULT OFF (fixed)"
while [[ $(alert_state) != inactive ]] && ((SECONDS - t_fix < 900)); do sleep 5; done
log "  alert resolved $((SECONDS - t_fix))s after the fix ($(burn))"
log "  latency SLO error budget left (28d window): $(q 'slo:payment_latency:error_budget_remaining')"

# a retried payment's trace: the retry hop shows up as extra producer/consumer spans
tid=$(curl -s -G http://localhost:3200/api/search --data-urlencode 'q={ name="payments.initiated-retry-1000 process" }' --data-urlencode limit=1 |
	python3 -c 'import json,sys; t=json.load(sys.stdin).get("traces",[]); print(t[0]["traceID"] if t else "")')
[[ -n $tid ]] && python3 demos/trace_spans.py "$tid" | tee -a "$OUT"
log "saved to $OUT"
