# Load testing & capacity estimate (P4-03)

Status: implemented 2026-09-07 (Phase 4, M4). Tool: **k6** (pinned
`grafana/k6:0.54.0`) hitting the read-only API.

## How to run

```bash
# 1) local stack up (API on :8000)
colima start
make up
# 2) run the load test
make load-test
```

The scenario ramps 1 → 10 → 20 VUs and back over ~70s, mixing
`/api/v1/summary`, `/api/v1/top/pages`, `/api/v1/events/recent`. Thresholds:
error rate < 1%, p95 latency < 500 ms.

## Measured results (2026-09-07, local colima stack)

Measured with `make load-test` (k6 0.54.0, ramping 1→20 VUs over ~70s,
`X-Forwarded-For` set per VU so the test exercises the API rather than the
per-IP rate limiter). Re-measure on your machine before quoting.

| Metric | Result |
|---|---|
| Peak VUs | 20 |
| Total requests | 3,227 (~46 req/s average) |
| p50 / p90 / p95 latency | 12.5 ms / 128 ms / 180.6 ms |
| Error rate | 0% (all `200`) |
| Checks passed | 100% |

### Note: the rate limiter is doing its job

The first k6 run (one shared client IP) tripped the P2-05 rate limiter after
~60 requests and 98% of requests returned `429`. That is expected protection
behaviour, not an API failure — load tests should simulate distinct clients.

## What this tells us (capacity thinking)

- The API is stateless and reads from Postgres/ClickHouse; latency is dominated
  by those queries + caching (Redis TTL cache added in P2-05 helps hot paths).
- Local single-process uvicorn is the bottleneck in this test, not the DBs —
  in EKS we would scale the API horizontally behind a load balancer.
- Rough mental model for EKS `t3.medium` (2 vCPU): a FastAPI worker typically
  sustains a few hundred read req/s; with 2 replicas and Redis caching you are
  likely bottlenecked by Postgres/ClickHouse before CPU. Verify with P4-03's
  EKS variant (documented, gated) rather than guessing.

## Guardrails

- Run load tests against a **throwaway demo stack**, never production.
- Keep the test short; record results + stack state (machine, DB sizes, VUs).
- Combine with SLO doc (`docs/slo-sli-error-budget.md`) for error-budget
  context.
