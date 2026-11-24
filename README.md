# PayFlow Sim

[![ci](https://github.com/KulmeetSJ/payflow-sim/actions/workflows/ci.yml/badge.svg)](https://github.com/KulmeetSJ/payflow-sim/actions/workflows/ci.yml)

An event-driven payments simulator built to practise what payment platforms need from SRE and platform teams:
- durable acceptance
- idempotent processing
- per-account ordering
- retries and dead-lettering
- tracing, SLOs and burn-rate alerts
- load and chaos testing

> **All data is synthetic.** Accounts, amounts and formats are generated; nothing resembles a real bank's schemes or fields.
> Every number in this repo is a personal-project benchmark on a laptop.

## Architecture

```mermaid
flowchart LR
  client["client / tools/generate.py"] -->|"POST /payments<br/>Idempotency-Key"| gw[payment-gateway]
  gw -->|"acks=all, idempotent producer<br/>key = fromAccount"| topic[("payments.initiated<br/>6 partitions · RF 3 · min ISR 2")]
  subgraph K["Kafka (KRaft, 3 brokers)"]
    topic
  end
  topic --> proc["payment-processor<br/>consumer group"]
  proc -->|"one transaction:<br/>dedupe + payment + ledger + balances"| pg[(PostgreSQL)]
  proc -.->|"commit offset AFTER<br/>the DB commit"| topic
  proc -->|"still failing after<br/>2 blocking retries"| retry[("retry-1000 → retry-2000 → retry-4000")]
  retry --> proc
  proc -->|"poison, or retries exhausted"| dlt[("payments.initiated-dlt")]
  dlt --> idx["DLT handler → dead_letters table<br/>/admin/dlt inspector + replay"]
  idx -.->|replay| topic
```

| Guarantee | How |
|---|---|
| `202 Accepted` means durably stored | The gateway waits for the broker ack; `acks=all` with `min.insync.replicas=2` means 2 of 3 replicas have it |
| Client retries don't double-pay | `eventId` is derived from the `Idempotency-Key`; the processor's `processed_events` primary key turns repeats into no-ops |
| Redelivery after a crash doesn't double-pay | The offset is committed only after the DB transaction. A crash in between causes redelivery, which the same dedupe absorbs. At-least-once delivery, exactly-once effect |
| Payments from one account are applied in order | Message key = `fromAccount`, so the account always maps to the same partition, and one consumer owns a partition at a time |
| No deadlocks between opposite transfers | Both account rows are locked in a fixed order (`ORDER BY account_id FOR UPDATE`) |
| Ledger always balances | A DEBIT and a CREDIT are written in the same transaction as the balance updates; `balance >= 0` is a DB constraint |
| A failing payment doesn't block its partition | Transient errors: 2 quick in-place retries, then retry topics (1 s / 2 s / 4 s), then the DLT. Poison messages go to the DLT on the first attempt |
| Business-rule failures are visible, not lost | Currency mismatch, unknown account, over the limit, insufficient funds → payment row `REJECTED` with a reason, and no ledger entries |

## Run it
Requires Docker (about 5 GB of memory for the stack including observability) and JDK 21 for the tests.
```bash
echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)" > .env   # gitignored; generated per machine
docker compose up -d --build        # 3 Kafka brokers, PostgreSQL, gateway (:8080), processor (:8081)

curl -s -X POST localhost:8080/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-0000001' \
  -d '{"fromAccount":"ACC-000003","toAccount":"ACC-000006","amount":25.00,"currency":"GBP"}'
# {"paymentId":"…","status":"ACCEPTED"}

curl -s localhost:8081/payments/<paymentId>     # status, reject_reason, e2e_ms
tools/generate.py --count 500 --rate 50         # synthetic load, ~2% deliberately invalid
docker compose down -v
```
| UI | URL |
|---|---|
| Grafana (dashboard, traces, logs) | http://localhost:3000 |
| Prometheus (rules, alerts) | http://localhost:9090/alerts |
| Alertmanager (routing) | http://localhost:9093 |
Accounts `ACC-000001`…`ACC-001000` are seeded with 100,000.00 each. The currency is GBP when n % 3 = 0, EUR when n % 3 = 1, and USD when n % 3 = 2.

## Partitioning and ordering
The gateway keys every event by `fromAccount`. Kafka hashes the key to a partition, and within a consumer group a partition has exactly one owner, so all payments from one account are applied one at a time, in the order they were accepted. This matters because the balance check depends on order: "deposit then spend" and "spend then deposit" can give different answers.

With round-robin (no key), two payments from the same account could land on different partitions and be processed in parallel by different instances. The second could then overtake the first. The row locks would stop the balance from going negative, but the *outcome* (which payment is rejected for insufficient funds) would depend on timing. `PaymentFlowIT.paymentsFromOneAccountAreAppliedInSendOrder` sends 20 payments from one account and checks they share a partition and were committed in send order.

## Retries and the DLT
Business-rule failures (currency mismatch, insufficient funds, …) are not errors: they are recorded as `REJECTED`. Exceptions mean something else went wrong, and they are handled in two layers ([`RetryConfig`](payment-processor/src/main/java/dev/demo/payflow/processor/RetryConfig.java)):

| Failure | Handling | Why |
|---|---|---|
| Transient (DB blip, pool timeout) | **2 blocking retries**, 200 ms apart, in place | A short blip is absorbed without the payment leaving its partition, so per-account order is kept |
| Still failing | **Non-blocking** `@RetryableTopic`: `retry-1000` → `retry-2000` → `retry-4000` → DLT | The partition keeps flowing. **Trade-off:** that one payment may now be applied after later payments from the same account |
| Poison (unparseable JSON, unknown schema, `eventId` not derived from the Idempotency-Key) | Straight to the DLT, 1 attempt | Retrying can't fix it; retrying would only delay everything behind it |

Every dead letter is indexed in a `dead_letters` table (original topic/partition/offset, exception class and message, stack trace, attempts). The DLT topic stays the source of truth. With `ADMIN_ENDPOINTS_ENABLED=true` (on in compose, off by default, no auth because this is a local simulator):

```bash
curl -s localhost:8081/admin/dlt?pending=true              # list
curl -s -X POST localhost:8081/admin/dlt/42/replay         # 202, or 422 if it is still unparseable
curl -s -X POST 'localhost:8081/admin/faults?keyPrefix=demo-&failures=3'   # inject transient faults
```
Replay is always safe: if the payment was in fact booked, the `processed_events` dedupe turns the replay into a no-op.

[`demos/retry-dlt.sh`](demos/retry-dlt.sh) runs each path against the compose stack ([output](demos/output/)):

| Case | Result |
|---|---|
| Fault ×2 | absorbed by the blocking retries; booked on attempt 1 |
| Fault ×3 | booked from the first retry topic (`delivery_attempt=2`), about 2 s end to end |
| Fault never clears | DLT after 4 attempts (12 tries in total, counting blocking retries); not booked; replayed once fixed → booked; a second replay → still exactly 2 ledger rows |
| Not JSON | DLT on attempt 1; replay refused with 422; the next payment on the same partition goes straight through |

**Something I didn't expect:** each blocking retry costs about 500 ms, not 200 ms. After the error handler seeks back, the consumer still has a long-poll fetch in flight for that partition, and it waits up to `fetch.max.wait.ms` (default 500 ms) before refetching. Lowering that setting would speed retries up but make idle consumers poll the brokers harder.

## Consumer scaling
`payments.initiated` has 6 partitions. [`demos/consumer-scaling.sh`](demos/consumer-scaling.sh) scales the processor 1 → 3 → 6 → 7 → 3 while ~40 payments/s keep flowing, and records the assignment after each step. Excerpt from [a run](demos/output/):

```
## 6 instance(s)                          ## 7 instance(s)
    payment-processor-1  partitions: 0        payment-processor-1  partitions: 0
    payment-processor-2  partitions: 3        ...  (unchanged)  ...
    ...                                       payment-processor-6  partitions: 5
    payment-processor-6  partitions: 5        payment-processor-7  partitions: (idle: no partitions)

accepted by gateway (202): 6000   reached a final state: 6000 (lost: 0)
ledger net 0.00 · balances per currency unchanged · redeliveries skipped by dedupe: 1
```
- **The 7th instance sits idle.** Parallelism is capped by the partition count, so partitions are sized for the maximum number of consumers you expect.
- **Cooperative-sticky assignor.** With the default eager protocol, every rebalance revokes *all* partitions from *all* members first, so the whole group stops. Cooperative rebalancing only revokes the partitions that move. In the run above, adding the 7th instance moved nothing, and going from 1 to 3 instances let instance 1 keep partitions 0 and 1 throughout.
- **One redelivery, absorbed.** In the run shown, one event was redelivered around a rebalance (processed, but its offset not yet committed when the partition moved). Dedupe skipped it. An earlier run had none.
- Kafka 4 also offers the new consumer group protocol (KIP-848, `group.protocol=consumer`), where the broker computes assignments incrementally. I stayed on the classic protocol + cooperative-sticky because it is what most production clusters still run.

## Observability and SLOs

![PayFlow Sim dashboard during the SLO burn demo](docs/img/dashboard.png)
*Two forced spikes (fault injection on 60% of processing attempts). The latency burn rate crosses the 14.4× page line, then the page clears while the slow-burn ticket stays open, because its 30 m window still contains the spike.*

```mermaid
flowchart LR
  apps["gateway + processors<br/>OTel Java agent"] -->|"OTLP traces + logs"| col[OTel Collector]
  col --> tempo[(Tempo)]
  col --> loki[(Loki)]
  apps -->|"/actuator/prometheus"| prom[(Prometheus)]
  kexp[kafka-exporter] -->|consumer lag| prom
  prom -->|"SLO rules,<br/>burn-rate alerts"| am[Alertmanager]
  tempo & loki & prom --> graf[Grafana]
```

- **Traces with no tracing code.** The OpenTelemetry Java agent (checksum-pinned in the [Dockerfile](Dockerfile)) instruments Spring MVC, the Kafka producer and consumer, and JDBC, and it carries the trace context in Kafka headers. One trace covers the whole path:
  ```
  +  0.0 ms  11.3 ms  payment-gateway    server    POST /payments
  +  1.4 ms   8.4 ms  payment-gateway    producer  payments.initiated publish     <- acks=all: most of the HTTP time
  + 10.3 ms   3.6 ms  payment-processor  consumer  payments.initiated process
  + 10.9 ms   0.3 ms  payment-processor  client    INSERT payflow.processed_events
  + 11.4 ms   0.4 ms  payment-processor  client    SELECT payflow.accounts
  + 12.4 ms   0.2 ms  payment-processor  client    INSERT payflow.payments
  ...                                              2 ledger INSERTs, 2 balance UPDATEs
  ```
  (Recorded in Phase 3. Since Phase 4 the payment row, both ledger entries and both balance updates are one statement, so a payment costs 4 DB round trips instead of 8; see the [DB-latency postmortem](postmortems/2026-11-20-db-latency.md).)
  A retried payment shows each attempt: three in-place tries ~0.5 s apart, the hop to `payments.initiated-retry-1000`, and the successful commit at 2.17 s ([full trace](demos/output/)).
- **Logs:** JSON on stdout (Spring Boot structured logging, ECS) with `trace_id`/`span_id`, and also shipped via OTLP to Loki. Grafana links each log line to its trace and each span to its logs.
- **SLI metrics** ([`PaymentMetrics`](payment-processor/src/main/java/dev/demo/payflow/processor/PaymentMetrics.java)): `payments_accepted_total` (gateway, the latency SLI denominator), `payments_processed_total{outcome}`, `payment_e2e_latency_seconds` (accept → commit, with an exact 2 s bucket), `payments_dead_lettered_total{kind}`. Consumer lag comes from kafka-exporter on the broker side, so it still reports when every consumer is down.
- **SLOs** ([docs/slo.md](docs/slo.md)): 99% of payments committed within 2 s; 99.9% of valid payments processed without being dead-lettered. Multi-window, multi-burn-rate alerts: **page** at 14.4× over 1 h and 5 m, **ticket** at 6× over 6 h and 30 m. The design choices, error budget policy and alert response steps are in the doc.
- **Alert rules are tested code.** `promtool test rules` runs in CI with 7 scenarios: exactly-on-budget traffic stays quiet, a 30× burn pages, an 8× burn only raises a ticket, a spike that has ended clears the page within minutes, dead letters page the success SLO, poison messages raise only their own ticket, and a processing stall (completions collapse while the gateway keeps accepting) pages within ~11 min.

### Forced error spike → page ([`demos/slo-burn.sh`](demos/slo-burn.sh), [output](demos/output/))
| | Result |
|---|---|
| Load / fault | ~20 payments/s; transient faults on 60% of processing attempts |
| Alert pending | 91 s after the fault started |
| `PaymentLatencyBudgetFastBurn` firing | 212 s (pending + the 2 m `for`) |
| Reached Alertmanager | 213 s, routed to the `page` receiver |
| Resolved | 324 s after the fault was switched off, once the 5 m window had recovered |

Notes from the runs:
- **The burn rate hit ~40×, not the ~22× I predicted.** I expected only the ~22% of payments that fail all three in-place tries (0.6³) to miss the 2 s target. But in-place retries hold the partition, and with one listener thread for 6 partitions, throughput fell to a few payments per second against 20/s offered. Lag grew to ~400 per partition and *every* queued payment went slow. Head-of-line blocking was the real damage, and it is the cost of the order-preserving retry layer. Listener threads per instance are now configurable (`LISTENER_CONCURRENCY`, default 1, capped in effect by the 6 partitions), but I haven't benchmarked them yet.
- **The long window wasn't clean in the recorded run.** Docker Compose kept Prometheus' anonymous data volume when I recreated the container, so the 1 h window still held an earlier run's spike (1 h burn 28.7× at the end of the baseline), and the 5 m window was what triggered the alert. An earlier run with a clean 1 h window gave almost the same timings (pending 96 s, firing 217 s), because on a stack with only minutes of history the 1 h window behaves like a short one.
- **"Error budget left: −3208%"** is correct and a little absurd: with ~25 minutes of history, a 4-minute incident is most of the traffic the 28-day window has seen. Budgets only mean something over real traffic volumes, and that's a good reason not to page on them directly.

## Load and chaos ([RESULTS.md](RESULTS.md))
k6 at a fixed arrival rate against the gateway, plus three scripted chaos experiments, each with a hypothesis. After every run, every 202 is reconciled against the database: each one must be exactly one payment row, and money must be conserved.

| Test | Result | Payments lost |
|---|---|---|
| Load knee, 3 processors | **750–1,000/s**: e2e p99 0.50 s at 750/s; lag starts growing at 1,000/s | — |
| Kill 1 of 3 brokers | 10 s leaderless; 72 requests got 503 (refused, never acknowledged) | **0** / 11,631 |
| Kill 1 of 3 consumers | partitions reassigned after 46 s (session timeout); lag recovered in 71 s | **0** / 12,000 |
| +500 ms on PostgreSQL | gateway unaffected; throughput 20 → 1.5/s; **page in 11.3 min** | **0** / 92,097 |

**Postmortem: [a slow database stalled payments and the page took ~51 minutes](postmortems/2026-11-20-db-latency.md).** The latency SLI's denominator was *completed* payments, so a stalled backlog didn't count against the SLO. I switched it to *accepted* payments and cut the DB round trips per payment from 8 to 4. On the re-run, the same fault paged in **11.3 min** after a full hour of healthy history, and throughput under the fault doubled. The stall is now a `promtool` test in CI.

## Tests
`./mvnw verify` runs unit tests and integration tests against **real Kafka and PostgreSQL** (Testcontainers):
- **Gateway:** an accepted payment lands on the topic keyed by account; a retry with the same key returns the same id; a missing or bad key and an invalid body get 400.
- **Processor:** a valid payment gets a balanced ledger and moves both balances; a currency mismatch is `REJECTED` with no ledger entries; the same event delivered 3× is booked once; 20 payments from one account are committed in send order.
- **Retries and DLT:** a fault that outlasts the blocking retries is booked from a retry topic exactly once; a permanent fault ends in the DLT after 4 attempts, then replay books it and a second replay changes nothing; malformed JSON and a tampered `eventId` reach the DLT on attempt 1 without blocking the partition.
- **Rules:** a table of 10 business-rule cases.
- **SLO alert rules:** 7 `promtool` scenarios (above).

## Roadmap
- [x] **Phase 1:** core flow, 3-broker KRaft cluster, idempotent producer and consumer, Testcontainers, CI
- [x] **Phase 2:** blocking + non-blocking retries, DLT with inspector and replay, consumer-group scaling demo (1→3→6→7→3), cooperative-sticky assignor
- [x] **Phase 3:** OpenTelemetry traces across Kafka, JSON logs in Loki linked to traces, RED + lag + SLO dashboard, multi-window burn-rate alerts (tested with promtool)
- [x] **Phase 4:** k6 load test, chaos (broker kill, consumer kill, DB latency), blameless postmortem; results in [RESULTS.md](RESULTS.md)

## License
MIT
