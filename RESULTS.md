# Results: load and chaos (Phase 4)

Personal-project benchmarks on one laptop, with synthetic data. Treat the numbers as relative (before/after,
healthy/faulted), not as absolute capacity. Each row links to the raw script output.

## Environment

| | |
|---|---|
| Machine | Apple M2, 8 cores, 16 GB RAM, macOS 27.0.1, on battery (Low Power Mode off) |
| Docker | Docker Desktop 29.6.1, VM with 8 CPUs and 7.8 GB |
| Stack | 3 × Kafka 4.3.2 brokers (KRaft, combined mode, 384 MB heap each), PostgreSQL 18, 1 gateway, 3 processors (1 consumer thread each), 6 partitions, plus the full observability stack (OTel agent and collector, Tempo, Loki, Prometheus, Alertmanager, Grafana) |
| Load | k6 2.4.0, open model (`constant-arrival-rate`) against `POST /payments` on the gateway ([load/payments.js](load/payments.js)) |
| Correctness check | After every chaos run: each 202 matched to exactly one payment row, total balance unchanged, ledger debits = credits ([chaos/lib.sh](chaos/lib.sh) `reconcile`) |

## Load: finding the knee ([load/find-knee.sh](load/find-knee.sh))

60 s steps at a fixed arrival rate. The backlog drains between steps. "Saturated" = e2e p99 above 2 s
(the SLO), or consumer lag still growing at the end of a step.

| offered/s | gateway p99 | committed/s | e2e p50 | e2e p95 | e2e p99 | lag max | verdict |
|---|---|---|---|---|---|---|---|
| 50 | 52.9 ms | 50.1 | 9 ms | 19 ms | 47 ms | 1 | ok |
| 200 | 65.5 ms | 200.1 | 7 ms | 27 ms | 89 ms | 5 | ok |
| 500 | 219.0 ms | 501.0 | 9 ms | 211 ms | 535 ms | 26 | ok |
| 750 | 131.8 ms | 745.0 | 7 ms | 234 ms | 496 ms | 34 | ok |
| 1000 | 134.4 ms | 998.3 | 30 ms | 886 ms | 1,244 ms | 832 | lag growing |
| 1250 (2026-11-20) | 297.6 ms | 1,240.8 | 1,216 ms | 3,461 ms | 4,237 ms | 3,826 | SLO broken |

**Knee: 750–1,000 payments/s with 3 processors.** Source: [2026-11-24, after the fixes](load/results/)
(`knee-after-fixes-warm-*`). The 1250 row comes from the 2026-11-20 run (`knee-baseline-high-*`), which
found the same knee: 1,000/s was just inside the SLO (p99 1.65 s) with lag building.

- The DB-round-trip fix (8 → 4) did **not** move the knee. Locally, PostgreSQL answers in well under a
  millisecond, so round trips aren't the limit. The laptop's CPU is: just after a 750/s step, the Docker VM's
  load average was 8.6 on 8 CPUs and each broker was still using ~150% CPU. The fix matters when the DB is slow (see below), not when it's local.
- **A cold start breaks the SLO at a rate a warm system handles.** A fresh stack sent 750/s straight away
  hit e2e p99 11.2 s and gateway p99 1.0 s. The same stack, warmed through 50–500/s first, held p99 0.50 s
  at 750/s ([cold run](load/results/), `knee-after-fixes-plain-*`). The takeaway for deploys is to ramp
  traffic onto new JVMs instead of switching all of it at once.

## Chaos ([chaos/](chaos/))

| Experiment | Hypothesis | Result | Lost |
|---|---|---|---|
| **Kill 1 of 3 brokers** (SIGKILL, 50/s, 64 s down) | No data loss, brief latency spike | 10.0 s with no leader for the dead broker's partitions (`broker.session.timeout.ms` 9 s + detection). Gateway p99 9.8 s in the worst 30 s window. **72 requests got 503**: the gateway's 10 s `delivery.timeout.ms` expired before failover finished, so it refused rather than acknowledged. Back in every ISR 5 s after restart | **0** of 11,631 |
| **Kill 1 of 3 consumers** (SIGKILL, 50/s) | Rebalance, lag spike, recovery | Orphaned partitions reassigned after **46 s** (`session.timeout.ms` 45 s + one heartbeat). Lag peaked ~800 and recovered 71 s after the kill. Restarted instance got its partitions back 11 s after starting | **0** of 12,000 |
| **+500 ms on PostgreSQL** (Toxiproxy, 20/s, 60 min healthy baseline) | Gateway unaffected; throughput collapses; fast-burn page fires | Gateway kept 20 × 202/s at p99 15–45 ms. Committed rate fell to **1.50/s**, lag grew ~18.5/s. Page reached Alertmanager **676 s (11.3 min)** after the fault. Backlog of 13,805 drained 10 s after the fault was removed | **0** of 92,097 |

Raw output: [broker kill](chaos/results/broker-kill-20261120-164355.txt) ·
[consumer kill](chaos/results/consumer-kill-20261120-165316.txt) ·
[DB latency, before](chaos/results/db-latency-20261120-165814.txt) ·
[DB latency, after](chaos/results/db-latency-20261124-201236.txt)

## Postmortem: DB latency ([postmortems/2026-11-20-db-latency.md](postmortems/2026-11-20-db-latency.md))

The first DB-latency run found that the page would have taken **~51 minutes**. The latency SLI used
*completed* payments as its denominator, so payments stuck in the backlog weren't counted at all.
Two action items were fixed and re-measured:

| | Before (2026-11-20) | After (2026-11-24) |
|---|---|---|
| SLI denominator | completed payments | **accepted** payments (`payments_accepted_total` at the gateway) |
| DB round trips per payment | 8 | **4** (one statement with data-modifying CTEs) |
| Committed/s under +500 ms | 0.74 | **1.50** |
| Time to page | ~51 min (projected for a full hour of history) | **11.3 min** (measured after a full hour of history) |
| Processor timeouts / rebalances | 6 | 0 |

The processing-stall case is now a `promtool` unit test in CI, so the SLI can't silently regress.

## What these tests did not cover

- Network partitions between brokers, or between the processors and Kafka (only process kills).
- PostgreSQL failover or data loss (single instance, no replica).
- Slow consumers caused by GC or CPU starvation rather than a slow dependency.
- Multi-hour soak tests, disk-full, or a broker losing its log directory.
- Producer-side duplicates under broker failover were checked only through the reconcile (no duplicates found), not provoked deliberately.
