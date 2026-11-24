#!/usr/bin/env bash
# Chaos 1: SIGKILL one of the 3 Kafka brokers under load, bring it back a minute later.
# Hypothesis: no accepted payment is lost (RF 3, min ISR 2, acks=all: every 202 is on 2 brokers),
# producers and consumers fail over to the new partition leaders within seconds, latency spikes briefly,
# and the broker catches up (back in the ISR) after it restarts.
#   docker compose up -d --build --scale payment-processor=3 && chaos/broker-kill.sh
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=chaos/results/broker-kill-$(date +%Y%m%d-%H%M%S).txt; mkdir -p chaos/results
source chaos/lib.sh
RATE=${RATE:-50} VICTIM=${VICTIM:-kafka-2}

urp() { docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
	--describe --under-replicated-partitions 2>/dev/null | /usr/bin/grep -c Partition: || true; }
leaders() { docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
	--describe --topic payments.initiated 2>/dev/null | awk '/Partition:/ {for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}' | sort | uniq -c | tr -s ' \n' ' '; }
GW='histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{job="payment-gateway",uri="/payments"}[30s])))'
E2E='histogram_quantile(0.99, sum by (le) (rate(payment_e2e_latency_seconds_bucket[30s])))'

log "# broker kill: $VICTIM, load ${RATE}/s, $(docker compose ps -q payment-processor | wc -l | tr -d ' ') processors"
log "partition leaders (count x broker id): $(leaders)"
start_load "$RATE" 240
sleep 60
log "before: gateway p99 $(q "$GW")s, e2e p99 $(q "$E2E")s"
docker compose kill -s SIGKILL "$VICTIM" >/dev/null 2>&1
t_kill=$SECONDS
log "KILLED $VICTIM"
sleep 15
log "leaders after kill: $(leaders); under-replicated partitions (all topics): $(urp)"
sleep 45
log "during (worst 30s window): gateway p99 $(q "max_over_time(($GW)[60s:5s])")s, e2e p99 $(q "max_over_time(($E2E)[60s:5s])")s, lag max $(q "max_over_time(sum(kafka_consumergroup_lag{$G})[60s:5s])")"
docker compose start "$VICTIM" >/dev/null 2>&1
t_start=$SECONDS
log "RESTARTED $VICTIM after $((t_start - t_kill))s down"
until [[ $(urp) == 0 ]] || ((SECONDS - t_start > 300)); do sleep 3; done
log "$VICTIM back in every ISR $((SECONDS - t_start))s after restart (under-replicated partitions: $(urp))"
log "leaders now: $(leaders) (auto.leader.rebalance.enable moves leadership back within ~5 min)"
reconcile
log "processor errors logged during the run: $(docker compose logs --since 5m payment-processor 2>/dev/null | /usr/bin/grep -c '"log.level":"ERROR"' || true)"
log "saved to $OUT"
