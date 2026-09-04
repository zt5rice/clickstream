# clickstream — Real-Time Clickstream Streaming Pipeline

A resume-building project: **Kafka + Spark/Flink + EKS** real-time clickstream pipeline.

- **Design doc:** [DESIGN.md](DESIGN.md) — architecture, tech-stack coverage matrix, milestones, deployment paths.
- **Stack:** Python, Java/Scala (optional), Apache Kafka, Spark Structured Streaming, optional Flink, PostgreSQL,
  FastAPI (REST/JSON, optional gRPC/WebSockets), Docker, kind, AWS EKS (Terraform), Prometheus/Grafana, GitHub Actions, pytest.

## Quick Start (local)

```bash
make up      # docker compose up -d (Kafka, Spark, Postgres, Prometheus, Grafana, API)
make test    # run unit + integration tests
make down    # teardown
```

## Measured metrics (Phase 1)

Measured on a local run (macOS + colima/Docker, `make up`, steady mode, ~53 min);
see [docs/demo-checklist.md](docs/demo-checklist.md) for step-by-step reproduction
and [PLAN.md](PLAN.md) §9 for methodology.

| Metric | Value |
|---|---|
| Producer throughput (steady) | ~100.0 events/s, 0 failed |
| Events produced (run) | ~319k in ~53 min |
| End-to-end freshness | 0 s (latest 1-min window is the current minute) |
| DLQ depth | 0 |
| API p50/p95 — `/api/v1/summary` | 25.6 / 32.9 ms (client) · 21 / 46 ms (Prometheus) |
| API p50/p95 — `/api/v1/events/recent` | 46.9 / 73.1 ms (client) · 42 / 90 ms (Prometheus) |
| API p50/p95 — `/api/v1/top/pages` | 10.4 / 14.6 ms (client) · 8.5 / 22 ms (Prometheus) |
| Curated storage after ~53 min | Postgres `page_views_1m` 22,754 rows · ClickHouse `olap.clicks` 319,759 events |

Measured 2026-09-02 06:54 UTC (= 2026-09-01 23:54 PDT).

## Next Steps

Phase 1 (local core pipeline) is complete and verified end-to-end. Next up:
**Phase 2 — data-engineering depth** (Airflow, dbt, Delta Lake/Iceberg, data
quality, Redis caching/rate limiting, `go_ops`, Ansible, `ai_assistant`) — see
[PLAN.md](PLAN.md) §4 for the tracked milestones/tickets.

## Live demo

See [docs/demo-checklist.md](docs/demo-checklist.md) for a step-by-step checklist
(URLs, health checks, PromQL queries, Grafana panels) to run a live demo of the
local stack.

## Phase 2 add-ons — Airflow (P2-01)

Batch orchestration with Apache Airflow (`LocalExecutor`, pinned image + DAGs in
`etl/dags/`). Requires the Postgres service (the metadata DB is `airflow` on the
same Postgres container).

```bash
docker compose up -d airflow-db-init airflow-init airflow-scheduler airflow-webserver
open http://localhost:8082   # Airflow UI (admin / admin)
```

DAGs:

- `clickstream_freshness_check` — every 15 min; fails the run when the latest
  curated 1-minute window is older than `FRESHNESS_MAX_STALE_SECONDS` (default
  180s), i.e. an alert when the pipeline falls behind.
- `clickstream_daily_rollup` — daily, idempotent rollup of `page_views_1m` into
  `curated.daily_summary` (`ON CONFLICT` upsert).
