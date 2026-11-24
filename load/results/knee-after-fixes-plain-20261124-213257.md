# Knee search: after-fixes-plain, 3 processors, 60s steps, 2026-11-24T16:02:58Z

| offered/s | accepted/s | gateway p99 | committed/s | e2e p50 | e2e p95 | e2e p99 | lag max | lag at end | drain | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 750 | 748.4 | 1010.4 ms | 764.4 | 1.886 s | 9.739 s | 11.248 s | 5934 | 333 | 0s | SLO broken |

> **Cold start, not a valid knee measurement.** Fresh stack, 60 s after `up`, straight to 750/s with no
> warm-up steps. JIT-cold JVMs: gateway p99 1 s, e2e p99 11 s, Docker VM load average 8.6 on 8 CPUs.
> The same rate passed (e2e p99 0.50 s) once the stack had warmed up through 50-500/s
> (knee-after-fixes-warm-*). Lesson: a cold deploy into full traffic breaks the SLO at a rate the warm
> system handles easily, so ramp traffic onto new instances (or warm them up) after a deploy.
