#!/usr/bin/env bash
# Chaos 2: SIGKILL 1 of 3 payment processors under load (no graceful shutdown, no LeaveGroup).
# Hypothesis: its partitions sit unconsumed until the broker gives up on it (session.timeout.ms,
# 45 s by default), then the cooperative-sticky rebalance hands them to the 2 survivors without
# stopping their own partitions. Lag builds on the orphaned partitions and drains afterwards.
# Records it processed but had not committed yet are redelivered and absorbed by the dedupe table.
#   docker compose up -d --build --scale payment-processor=3 && chaos/consumer-kill.sh
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=chaos/results/consumer-kill-$(date +%Y%m%d-%H%M%S).txt; mkdir -p chaos/results
source chaos/lib.sh
RATE=${RATE:-50}
VICTIM=$(docker compose ps -q payment-processor | tail -1)
VNAME=$(docker inspect -f '{{.Name}}' "$VICTIM" | tr -d /)

owned() { # partitions of payments.initiated owned per consumer host, from the broker's point of view
	docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 \
		--describe --group payment-processor 2>/dev/null |
		awk '$2=="payments.initiated" && $NF!="-" {n[$(NF-1)]++} $2=="payments.initiated" && $NF=="-" {u++}
		     END {for (h in n) printf "%s=%d ", h, n[h]; if (u) printf "unassigned=%d", u}'
}
dups() { q 'sum(payments_processed_total{outcome="duplicate"})' | cut -d. -f1; }

log "# consumer kill: $VNAME, load ${RATE}/s"
log "ownership before: $(owned)"
start_load "$RATE" 240
sleep 60
d0=$(dups)
assigned_since() { # count of main-topic assignment log lines since a unix time (no grep -q: pipefail + SIGPIPE)
	"$@" 2>/dev/null | /usr/bin/grep -c 'partitions assigned: \[payments\.initiated-[0-9]' || true
}
docker kill -s SIGKILL "$VICTIM" >/dev/null
t_kill=$SECONDS kill_ts=$(date +%s)
log "KILLED $VNAME (SIGKILL)"
reassigned="" peak=0
while ((SECONDS - t_kill < 300)); do
	l=$(lag); (( l > peak )) && peak=$l
	if [[ -z $reassigned ]] && (( $(assigned_since docker compose logs --since "$kill_ts" payment-processor) > 0 )); then
		reassigned=$((SECONDS - t_kill))
		log "orphaned partitions reassigned ${reassigned}s after the kill; ownership now: $(owned)"
	fi
	[[ -n $reassigned ]] && ((SECONDS - t_kill > reassigned + 20)) && (( l < 25 )) && break
	sleep 2
done
log "lag peaked at ~$peak (kafka-exporter, scraped every 10 s), back under 25 $((SECONDS - t_kill))s after the kill"
docker start "$VICTIM" >/dev/null
t_up=$SECONDS up_ts=$(date +%s)
until (( $(assigned_since docker logs --since "$up_ts" "$VICTIM") > 0 )) || ((SECONDS - t_up > 120)); do sleep 1; done
log "$VNAME restarted, got partitions back $((SECONDS - t_up))s later (JVM start + join); ownership: $(owned)"
reconcile
log "redeliveries absorbed by dedupe during the run: $(( $(dups) - d0 ))"
log "saved to $OUT"
