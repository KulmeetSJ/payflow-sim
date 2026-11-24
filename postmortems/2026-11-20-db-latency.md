# Postmortem: a slow database stalled payments and the page took ~51 minutes

*Blameless format. This was an induced fault in a personal project (a chaos experiment on synthetic data),
written up as if it were a production incident. All numbers are personal-project benchmarks on a laptop.*

| | |
|---|---|
| Date of experiment | 2026-11-20 (fix verified 2026-11-24) |
| Fault | +500 ms latency on every PostgreSQL response to the processors (Toxiproxy) |
| Load | 20 payments/s, 3 processor instances, 1 consumer thread each |
| Customer impact | Payments accepted (HTTP 202) but committed minutes later instead of within 2 s. No payment lost, no money created or destroyed |
| Detection | **~51 min** to page (projected), **11.3 min** after the fix (measured) |
| Raw output | [before](../chaos/results/db-latency-20261120-165814.txt) · [after](../chaos/results/db-latency-20261124-201236.txt) |

## Summary

With the database answering 500 ms slower, processor throughput fell from 20 to 0.74 payments/s, so the
Kafka backlog grew by about 19 payments every second. The gateway kept accepting payments normally, so from
a client's view nothing was wrong. The latency SLO ("99% of payments committed within 2 s") was being
broken for almost every payment, yet the fast-burn page only went *pending* after 9.4 minutes. That
happened only because Prometheus had just 2 minutes of history. With a normal hour of history behind it,
the page would have taken about 51 minutes.

Two problems combined:

1. **The SLI hid the stall.** The latency SLI divided "committed within 2 s" by *completed* payments.
   A stuck payment isn't completed, so it wasn't counted at all until it finally drained. The 5-minute
   burn rate read 100x, but the 1-hour ratio crept up by only ~1.5 points a minute.
2. **Each payment paid the latency 8 times.** The booking transaction made 8 sequential round trips
   (dedupe marker, account lock, payment row, two ledger inserts, two balance updates, commit). At
   +500 ms per trip, a consumer thread could do 0.25 payments/s, and 3 threads did 0.75/s.

## Timeline (2026-11-20, UTC)

| Time | Event |
|---|---|
| 11:28:14 | Load starts, 20/s |
| 11:30:15 | Baseline healthy: 19.96 committed/s, burn 0x. Toxic applied (+500 ms) |
| 11:31:16 | Committed rate 0.74/s, lag 1,194 and climbing. Gateway still 20 × 202/s, p99 13 ms |
| 11:39:38 | `PaymentLatencyBudgetFastBurn` pending (+563 s). Burn 5m = 100x, 1h = 14.5x |
| 11:40:32 | Laptop went into clamshell sleep (on battery). Everything after this is distorted |
| 11:43:30 | FastBurn firing (per Prometheus history, during the sleep) |
| 11:52:05 | Toxic removed after 1,310 s. Backlog 13,359 |
| 11:53:03 | Backlog drained in 58 s (~287/s catch-up) |
| 11:58:06 | Reconcile: 23,267 × 202, 23,267 payment rows, **0 lost**, total balance and ledger balanced |

## What went well

- **Durability and correctness held.** Every accepted payment was booked exactly once and money was
  conserved. Offsets are committed only after the DB transaction, so a slow DB delays work but never drops it.
- **The blast radius stayed contained.** The gateway never touches the DB and kept serving 202s at normal latency.
- **Recovery was fast.** Once the DB was healthy, 13k backlogged payments drained in under a minute.

## What went wrong

- The page arrived far too late for a payments system: at 20/s, 51 minutes is ~60,000 late payments.
- The SLI's denominator was chosen for steady state, where "accepted" and "completed" are equal. It
  breaks in exactly the failure it exists to catch.
- The test run itself was disturbed by the laptop sleeping on battery, so part of the run had to be
  reconstructed from Prometheus history and `pmset -g log`.

## Root cause

The SLI measured *completions*, not *demand*. A stall reduces completions, which shrinks the denominator
along with the numerator, so the error ratio barely moves until the backlog drains. A secondary cause was
that latency to the DB was multiplied by the number of sequential round trips, which made the stall deeper.

## Fixes

| Fix | Change | Verified 2026-11-24 |
|---|---|---|
| SLI denominator = accepted payments | Gateway counts `payments_accepted_total` on every 202. The SLI is `1 - committed_within_2s / accepted`, clamped at 0 ([slo.yml](../observability/prometheus/rules/slo.yml), unit-tested in [slo_test.yml](../observability/prometheus/tests/slo_test.yml)) | Page fired in Alertmanager **676 s** after the toxic, after a full **60-minute** healthy baseline (was ~51 min projected) |
| 8 → 4 round trips per payment | Payment row, both ledger entries and both balance updates in one statement using data-modifying CTEs ([PaymentService.java](../payment-processor/src/main/java/dev/demo/payflow/processor/PaymentService.java)) | Throughput under +500 ms **1.50/s** (was 0.74/s), exactly 3 threads × 1/(4 × 0.5 s) |

Re-run on 2026-11-24 (3 processors, 20/s, 60-minute baseline):

| | Before | After |
|---|---|---|
| Committed/s under +500 ms | 0.74 | 1.50 |
| FastBurn pending | 563 s (with only 2 min of history) | 559 s (with 60 min of history) |
| Page in Alertmanager | ~51 min (projected with 60 min of history) | **676 s (11.3 min)** |
| Processor timeouts / rebalances in logs | 6 | 0 |
| Backlog drain after the fix | 58 s (13,359) | 10 s (13,805) |
| Lost payments | 0 of 23,267 | 0 of 92,097 |

The 11.3 minutes is close to the theoretical floor for this alert: the 1-hour ratio has to reach 14.4%
(14.4% of 60 min ≈ 8.6 min of near-100% bad), the 5-minute ratio has to agree, then `for: 2m`.

## Follow-ups (not done)

- **Lag-based page.** `PaymentConsumerLagGrowing` also fired in the re-run. An absolute "oldest unprocessed
  payment is older than N seconds" alert would page within a few minutes, independently of the burn-rate
  windows. It needs a lag-in-time metric (e.g. Burrow-style, or the age of the oldest uncommitted event).
- **Throughput headroom under DB latency.** One consumer thread per instance makes throughput
  `instances / (round trips × RTT)`. `LISTENER_CONCURRENCY` is now configurable (default 1), but the number
  of consumers is capped by the 6 partitions.
- **Test hygiene.** Check `pmset -g batt` before long runs, and use `caffeinate`.
