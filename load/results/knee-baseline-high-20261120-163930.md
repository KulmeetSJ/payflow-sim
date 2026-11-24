# Knee search: baseline-high, 3 processors, 60s steps, 2026-11-20T11:09:30Z

| offered/s | accepted/s | gateway p99 | committed/s | e2e p50 | e2e p95 | e2e p99 | lag max | lag at end | drain | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 750 | 749.3 | 43.6 ms | 747.7 | 0.008 s | 0.060 s | 0.116 s | 22 | 18 | 0s | ok |
| 1000 | 999.8 | 135.8 ms | 998.1 | 0.021 s | 1.003 s | 1.646 s | 358 | 86 | 0s | ok |
| 1250 | 1249.8 | 297.6 ms | 1240.8 | 1.216 s | 3.461 s | 4.237 s | 3826 | 2279 | 0s | SLO broken, lag growing |
