#!/usr/bin/env bash
# Retry + DLT demo against the compose stack, using the processor's admin endpoints.
#   1. a transient fault the blocking retries absorb      -> booked on attempt 1, order kept
#   2. a fault that outlasts the blocking retries          -> booked from a retry topic (attempt 2)
#   3. a fault that never clears                           -> DLT; "fix" it; replay; booked once
#   4. a poison message (not JSON)                         -> DLT immediately, 1 attempt, not replayable
#
#   docker compose up -d --build && demos/retry-dlt.sh
set -euo pipefail
cd "$(dirname "$0")/.."

GW=${GW:-http://localhost:8080}
PROC=${PROC:-http://localhost:8081}
OUT=demos/output/retry-dlt-$(date +%Y%m%d-%H%M%S).txt
RUN=$(date +%s)

log() { echo "$*" | tee -a "$OUT"; }
sql() { docker compose exec -T postgres psql -U payflow -d payflow -tAc "$1"; }
pay() { # $1 = idempotency key; prints the payment id
	curl -sf -X POST "$GW/payments" -H 'Content-Type: application/json' -H "Idempotency-Key: $1" \
		-d '{"fromAccount":"ACC-000030","toAccount":"ACC-000033","amount":2.50,"currency":"GBP"}' |
		python3 -c 'import json,sys; print(json.load(sys.stdin)["paymentId"])'
}
row() { sql "SELECT status || ' delivery_attempt=' || delivery_attempt || ' e2e_ms=' || round(extract(epoch FROM committed_at - accepted_at) * 1000) FROM payments WHERE payment_id = '$1'"; }
wait_row() { for _ in $(seq 1 60); do r=$(row "$1"); [[ -n $r ]] && { echo "$r"; return; }; sleep 1; done; echo "NOT BOOKED"; }
dlt_for() { sql "SELECT id || ' attempts=' || attempts || ' exception=' || exception_class FROM dead_letters WHERE payload LIKE '%$1%' ORDER BY id DESC LIMIT 1"; }
wait_dlt() { for _ in $(seq 1 60); do r=$(dlt_for "$1"); [[ -n $r ]] && { echo "$r"; return; }; sleep 1; done; echo "NOT IN DLT"; }
faults() { curl -sf -X POST "$PROC/admin/faults?$1" >/dev/null; }

# fault-injection state lives in each instance, so run a single processor for a deterministic demo
docker compose up -d --scale payment-processor=1 --no-recreate payment-processor >/dev/null 2>&1
for _ in $(seq 1 60); do curl -sf "$PROC/actuator/health/readiness" >/dev/null && break; sleep 1; done
curl -sf -X DELETE "$PROC/admin/faults" >/dev/null

log "# Retry + DLT demo, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "# retries: 2 blocking (200 ms) per attempt, then retry topics 1 s / 2 s / 4 s, then payments.initiated-dlt"
log ""

key="demo-blip-$RUN"
faults "keyPrefix=$key&failures=2"
id=$(pay "$key")
log "1. fault x2 (absorbed by blocking retries):   $(wait_row "$id")"

key="demo-retry-$RUN"
faults "keyPrefix=$key&failures=3"
id=$(pay "$key")
log "2. fault x3 (needs the first retry topic):    $(wait_row "$id")"

key="demo-dlt-$RUN"
faults "keyPrefix=$key&failures=1000"
id=$(pay "$key")
dl=$(wait_dlt "$key")
log "3. fault never clears:                        dead letter $dl"
log "   booked before replay?                      $(row "$id" | grep -q . && echo yes || echo no)"
faults "keyPrefix=$key&failures=0"
dl_id=${dl%% *}
log "   fault cleared; replay -> HTTP $(curl -s -o /dev/null -w '%{http_code}' -X POST "$PROC/admin/dlt/$dl_id/replay")"
log "   after replay:                              $(wait_row "$id")"
log "   replay again -> HTTP $(curl -s -o /dev/null -w '%{http_code}' -X POST "$PROC/admin/dlt/$dl_id/replay")"
sleep 3
log "   ledger rows for the payment (want 2):      $(sql "SELECT count(*) FROM ledger_entries WHERE payment_id = '$id'")"

marker="demo-poison-$RUN"
docker compose exec -T kafka-1 bash -c "echo 'ACC-000030|{not json $marker' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments.initiated --reader-property parse.key=true --reader-property key.separator='|'" 2>/dev/null
dl=$(wait_dlt "$marker")
log "4. poison message:                            dead letter $dl"
log "   replay -> HTTP $(curl -s -o /dev/null -w '%{http_code}' -X POST "$PROC/admin/dlt/${dl%% *}/replay") (422 = refused, still unparseable)"
id=$(pay "demo-after-poison-$RUN")
log "   next payment on the same partition:        $(wait_row "$id")"
log ""
log "faults injected so far: $(curl -sf "$PROC/admin/faults")"
log "saved to $OUT"
