#!/usr/bin/env bash
# Step the offered load up until the system stops keeping up, and record each step.
# Each step: k6 at a fixed arrival rate for STEP_S seconds, then Prometheus is asked what the
# processors did in that window. Between steps the backlog drains, so steps don't bleed into each other.
# Saturated = the e2e p99 breaks the 2 s SLO or consumer lag is still growing at the end of the step.
#
#   docker compose up -d --build --scale payment-processor=3 && load/find-knee.sh
#   RATES="50 100 200" STEP_S=60 LABEL=concurrency2 load/find-knee.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RATES=${RATES:-"50 100 150 200 300 400 500"}
STEP_S=${STEP_S:-60}
LABEL=${LABEL:-baseline}
OUT=load/results/knee-$LABEL-$(date +%Y%m%d-%H%M%S).md
mkdir -p load/results
q() { python3 demos/promq.py "$1" http://localhost:9090 ${AT:-}; }
G='consumergroup="payment-processor",topic="payments.initiated"'
lag() { q "sum(kafka_consumergroup_lag{$G})"; }

procs=$(docker compose ps -q payment-processor | wc -l | tr -d ' ')
{
	echo "# Knee search: $LABEL, $procs processors, ${STEP_S}s steps, $(date -u +%FT%TZ)"
	echo
	echo "| offered/s | accepted/s | gateway p99 | committed/s | e2e p50 | e2e p95 | e2e p99 | lag max | lag at end | drain | verdict |"
	echo "|---|---|---|---|---|---|---|---|---|---|---|"
} | tee "$OUT"

# JIT warm-up, not recorded
k6 run -q --no-color -e RATE=50 -e DURATION=20s -e RUN="$LABEL-warmup-$RANDOM" load/payments.js >/dev/null 2>&1 || true
until (( $(lag | cut -d. -f1) == 0 )); do sleep 5; done

for rate in $RATES; do
	AT=
	k6 run -q --no-color -e RATE="$rate" -e DURATION="${STEP_S}s" -e RUN="$LABEL-$rate-$RANDOM" \
		--summary-export /tmp/k6-step.json load/payments.js >/dev/null 2>&1 || true
	AT=$(date +%s)   # every window query below is evaluated at the end of the step
	sleep 12         # one more scrape, so the last seconds of the step are in Prometheus
	w="${STEP_S}s"
	read -r acc p99gw < <(python3 -c "
import json; m=json.load(open('/tmp/k6-step.json'))['metrics']
print(round(m.get('payments_accepted',{}).get('rate',0),1), round(m['http_req_duration']['p(99)'],1))")
	committed=$(q "sum(rate(payments_processed_total{outcome!=\"duplicate\"}[$w]))")
	p() { q "histogram_quantile($1, sum by (le) (rate(payment_e2e_latency_seconds_bucket[$w])))"; }
	p50=$(p 0.50) p95=$(p 0.95) p99=$(p 0.99)
	lagmax=$(q "max_over_time(sum(kafka_consumergroup_lag{$G})[$w:5s])")
	lagend=$(lag)
	growing=$(q "deriv(sum(kafka_consumergroup_lag{$G})[30s:5s]) > 1" )
	AT=
	t0=$SECONDS
	while (( $(lag | cut -d. -f1) > 0 )) && ((SECONDS - t0 < 300)); do sleep 5; done
	drain=$((SECONDS - t0))
	verdict=ok
	if [[ $p99 != n/a ]] && (( $(echo "$p99 > 2" | bc) )); then verdict="SLO broken"; fi
	if [[ $growing != n/a ]]; then verdict="$verdict, lag growing"; fi
	printf '| %s | %s | %s ms | %.1f | %.3f s | %.3f s | %.3f s | %.0f | %.0f | %ss | %s |\n' \
		"$rate" "$acc" "$p99gw" "$committed" "$p50" "$p95" "$p99" "$lagmax" "$lagend" "$drain" "${verdict#ok, }" | tee -a "$OUT"
	[[ $verdict != ok ]] && break
	sleep 5
done
echo; echo "saved to $OUT"
