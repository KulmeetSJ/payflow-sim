# PayFlow Sim SLOs

Two user-facing promises, measured where the user would feel them. Rules: [`observability/prometheus/rules/slo.yml`](../observability/prometheus/rules/slo.yml), unit-tested with `promtool` in CI ([`tests/slo_test.yml`](../observability/prometheus/tests/slo_test.yml)).

> Personal-project numbers. The targets are chosen to make the mechanics visible, not taken from any real payment system.

## SLIs and SLOs

| | Latency | Success |
|---|---|---|
| **SLI** | Share of payments committed to the ledger **within 2 s** of the gateway accepting them | Share of **valid** payments processed (completed or rejected by a business rule) without being dead-lettered |
| **Good events** | `payment_e2e_latency_seconds_bucket{le="2.0"}` | `payments_processed_total{outcome=~"completed\|rejected"}` |
| **Valid events** | `payment_e2e_latency_seconds_count` | good + `payments_dead_lettered_total{kind="exhausted"}` |
| **SLO (28 days)** | **99%** | **99.9%** |
| **Error budget** | 1% ≈ 6.7 h of total failure, or 1 in 100 payments slow | 0.1%, 1 in 1,000 payments stuck |

Why these and not others:
- **Accept → commit**, not HTTP latency. The gateway answers in ~10 ms because it only waits for Kafka. The user cares about when the money actually moves, which also covers consumer lag, retries and DB time.
- **The 2 s bucket is exact.** The histogram has a bucket boundary at exactly 2 s, so the SLI is a ratio of counters, not an interpolated percentile.
- **Rejections count as success.** A payment rejected for insufficient funds was handled correctly. Treating it as an error would make the SLO depend on customers' balances.
- **Poison messages don't count against the success SLO.** Malformed events come from a broken producer, not from this service failing valid work. They raise their own ticket (`PoisonMessagesDeadLettered`).
- **Lag isn't an SLI.** A lag of 500 is fine at 1,000 payments/s and terrible at 1/s. Lag explains a latency burn; it doesn't define one. It has a cause-based ticket instead.
- **Duplicates are excluded.** A redelivered event that dedupe skips is neither good nor bad for the user.

## Alerts: multi-window, multi-burn-rate

Burn rate = how fast the budget is being spent relative to "exactly used up in 28 days" (1×).

| Alert | Burn rate | Long window | Short window | `for` | Severity | Budget spent before it fires |
|---|---|---|---|---|---|---|
| `*FastBurn` | 14.4× | 1 h | 5 m | 2 m | **page** | 2% in 1 h |
| `*SlowBurn` | 6× | 6 h | 30 m | 15 m | ticket | 5% in 6 h |

- **Both windows must be over the threshold.** The long window proves the burn is significant, not a blip. The short window makes the alert clear soon after the problem is fixed, instead of staying red for an hour while the 1 h average catches up. One `promtool` test covers exactly that: a spike that has ended clears the page within minutes even though the 1 h ratio is still high.
- **Thresholds come from the SRE Workbook.** I added `for: 2m` on top to absorb scrape jitter.
- **No 3-day/1× alert.** With retention set to 3 days on a laptop, it would only ever see part of its window.
- **Routing:** Alertmanager sends `severity=page` and `severity=ticket` to separate receivers. In a real setup these would be PagerDuty and a ticket queue; here they are empty receivers, so the routing is visible in the Alertmanager UI.

## Error budget policy

| Budget left (28 d) | What happens |
|---|---|
| > 50% | Normal: ship features, run chaos experiments |
| 25–50% | Releases need a rollback plan reviewed; no risky experiments in peak hours |
| < 25% | Feature freeze on the payment path. Only reliability fixes and changes that reduce risk ship |
| Exhausted | Freeze until the 28-day budget recovers above 25%, or until a postmortem's action items are done and agreed with the product owner |

A single incident that burns more than 20% of the budget gets a blameless postmortem, whatever is left over.

## Responding to alerts

Start from the dashboard (Grafana → *PayFlow Sim*):
1. **Which SLO, how fast?** The burn-rate panels show 5 m / 1 h / 6 h against the 6× and 14.4× lines.
2. **Latency burn:** check consumer lag by partition. One hot partition means a hot key or a stuck consumer; all partitions means capacity or a slow dependency. Then open a slow trace: is the time in the DB spans, or in a retry hop (`payments.initiated-retry-*` spans)?
3. **Success burn:** `GET /admin/dlt?pending=true` on the processor shows the exception class of recent dead letters. Fix the cause first, then replay. Replay is idempotent.
4. **Logs:** the dashboard's log panel links each line to its trace (and Tempo links back to logs by `trace_id`).

These steps are the seed for the runbook corpus in project #4.
