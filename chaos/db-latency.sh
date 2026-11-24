#!/usr/bin/env bash
# Chaos 3: +500 ms on every PostgreSQL response to the processors (Toxiproxy), under steady load.
# Hypothesis: the gateway is unaffected (it never touches the DB) and keeps returning 202s. Each
# payment transaction makes 4 DB round trips (8 before postmortems/2026-11-20-db-latency.md), so a
# processor thread drops from hundreds of payments/s to 1 / (4 x 0.5 s) = 0.5/s. Lag grows, accepted
# payments stop being committed within 2 s, and the fast-burn page fires once the 1h ratio crosses 14.4%
# (~9 min after a full healthy hour, then for: 2m). After the toxic is removed the backlog drains,
# nothing is lost and the alert resolves. Use BASELINE_S=3600 for a realistic time-to-page.
#   docker compose -f docker-compose.yml -f chaos/compose.toxiproxy.yml up -d --build --scale payment-processor=3
#   chaos/db-latency.sh
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=chaos/results/db-latency-$(date +%Y%m%d-%H%M%S).txt; mkdir -p chaos/results
source chaos/lib.sh
RATE=${RATE:-20} LATENCY_MS=${LATENCY_MS:-500} BASELINE_S=${BASELINE_S:-120} HOLD_S=${HOLD_S:-60}
TOXI=http://localhost:8474
ALERT=PaymentLatencyBudgetFastBurn
alert_state() { curl -s "$PROM/api/v1/alerts" | python3 -c "import json,sys; a=[x['state'] for x in json.load(sys.stdin)['data']['alerts'] if x['labels']['alertname']=='$ALERT']; print(a[0] if a else 'inactive')"; }
in_am() { curl -s "http://localhost:9093/api/v2/alerts?filter=alertname=$ALERT" | python3 -c 'import json,sys; a=json.load(sys.stdin); print(",".join(r["name"] for x in a for r in x["receivers"]) or "none")'; }
firing_alerts() { curl -s "$PROM/api/v1/alerts" | python3 -c "import json,sys; print(' '.join(sorted({x['labels']['alertname'] for x in json.load(sys.stdin)['data']['alerts'] if x['state']=='firing'})) or 'none')"; }
burn() { echo "burn 5m=$(q 'slo:payment_latency:error_ratio_rate5m / 0.01')x 1h=$(q 'slo:payment_latency:error_ratio_rate1h / 0.01')x"; }
rate_() { q 'sum(rate(payments_processed_total{outcome!="duplicate"}[1m]))'; }
gw_ok() { q 'sum(rate(http_server_requests_seconds_count{job="payment-gateway",uri="/payments",status="202"}[1m]))'; }
gw_p99() { q 'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{job="payment-gateway",uri="/payments"}[1m])))'; }

curl -sf "$TOXI/proxies/postgres" >/dev/null || { echo "toxiproxy not running: use chaos/compose.toxiproxy.yml (see header)"; exit 1; }
curl -s -X DELETE "$TOXI/proxies/postgres/toxics/latency" >/dev/null || true

log "# DB latency: +${LATENCY_MS}ms on PostgreSQL responses, load ${RATE}/s, $(docker compose ps -q payment-processor | wc -l | tr -d ' ') processors"
start_load "$RATE" 100000
trap 'kill $K6 2>/dev/null || true; curl -s -X DELETE "$TOXI/proxies/postgres/toxics/latency" >/dev/null || true' EXIT
sleep "$BASELINE_S"
log "baseline: committed $(rate_)/s, gateway 202s $(gw_ok)/s p99 $(gw_p99)s, $(burn), alert=$(alert_state)"

curl -sf -X POST "$TOXI/proxies/postgres/toxics" -H 'Content-Type: application/json' \
	-d "{\"name\":\"latency\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":$LATENCY_MS,\"jitter\":0}}" >/dev/null
t_bad=$SECONDS
log "TOXIC ON: +${LATENCY_MS}ms"
pending="" firing="" am="" t=0
while [[ -z $am ]] && ((SECONDS - t_bad < 900)); do
	s=$(alert_state)
	[[ -z $pending && $s != inactive ]] && pending=$((SECONDS - t_bad)) && log "  alert pending after ${pending}s ($(burn))"
	[[ -z $firing && $s == firing ]] && firing=$((SECONDS - t_bad)) && log "  alert FIRING after ${firing}s ($(burn))"
	[[ -n $firing ]] && r=$(in_am) && [[ $r != none ]] && am=$((SECONDS - t_bad)) && log "  in Alertmanager after ${am}s, receiver: $r"
	if ((SECONDS - t_bad >= t + 60)); then
		t=$((SECONDS - t_bad))
		log "  +${t}s: committed $(rate_)/s, lag $(lag), gateway 202s $(gw_ok)/s p99 $(gw_p99)s"
	fi
	sleep 5
done
sleep "$HOLD_S"
log "before the fix: committed $(rate_)/s, lag $(lag), gateway 202s $(gw_ok)/s p99 $(gw_p99)s; firing: $(firing_alerts)"
log "processor log lines mentioning max.poll / rebalance / Hikari timeouts: $(docker compose logs --since "$((SECONDS - t_bad + 5))s" payment-processor 2>/dev/null | /usr/bin/grep -ciE 'max\.poll|poll timeout|rebalanc|Connection is not available' || true)"

curl -sf -X DELETE "$TOXI/proxies/postgres/toxics/latency" >/dev/null
t_fix=$SECONDS
log "TOXIC OFF after $((t_fix - t_bad))s"
peak=0
until [[ $(lag) == 0 ]] || ((SECONDS - t_fix > 600)); do r=$(rate_); [[ ${r%.*} -gt $peak ]] && peak=${r%.*}; sleep 5; done
log "  backlog drained $((SECONDS - t_fix))s after the fix (peak catch-up rate ~${peak}/s)"
while [[ $(alert_state) != inactive ]] && ((SECONDS - t_fix < 900)); do sleep 5; done
log "  $ALERT resolved $((SECONDS - t_fix))s after the fix ($(burn))"
log "  latency error budget left (28d): $(q 'slo:payment_latency:error_budget_remaining')"
kill -INT $K6 2>/dev/null || true   # k6 stops gracefully and still writes its summary
reconcile
log "saved to $OUT"
