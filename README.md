# clickstream — Real-Time Clickstream Streaming Pipeline

> **Personal / portfolio project** — built from scratch for learning and job
> applications. Not affiliated with any employer; all data is synthetic and all
> metrics come from our own local/demo runs.

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

Phases 1–4 core are complete (local pipeline → data-engineering add-ons →
kind/Helm/GHCR/Terraform/EKS → SLO/runbooks/load-test/chaos). Next up:
**Phase 5 — portfolio packaging** (final metrics, architecture diagram,
LICENSE, interview/demo materials) — see [PLAN.md](PLAN.md) §7.

## Phase 4 results

SLO snapshot, load/capacity numbers and MTTR are summarized in
[docs/m4-results.md](docs/m4-results.md).

## Phase 4 add-ons — SLO/SLI + error budget (P4-01)

SLI definitions (availability / p95 / freshness / DLQ) with PromQL, demo SLO
targets and an error-budget + burn-rate policy are documented in
[docs/slo-sli-error-budget.md](docs/slo-sli-error-budget.md).

## Phase 4 add-ons — runbooks + PIR (P4-02)

Runbooks for the main alerts (API unhealthy, freshness stale, DLQ non-empty,
Kafka down, EKS cost guard) plus a blameless Post-Incident Review template —
see [docs/runbooks.md](docs/runbooks.md) and
[docs/pir-template.md](docs/pir-template.md).

## Phase 4 add-ons — load testing (P4-03)

k6 read-API load test (`make load-test`, pinned `grafana/k6`) plus capacity
notes — see [docs/load-testing-capacity.md](docs/load-testing-capacity.md).

## License

MIT — see [LICENSE](LICENSE).

## Phase 4 add-ons — chaos drills (P4-04)

Kind-based failure drills (delete API deployment / Kafka pod) that measure
self-healing MTTR — see [docs/chaos-drills.md](docs/chaos-drills.md).

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

## Phase 2 add-ons — dbt (P2-02)

dbt models + tests on the curated Postgres schema (project in `dbt/`). Requires
the Postgres service; the host port is **5433** so a local Postgres on 5432 does
not clash with the Docker one.

```bash
make dbt-run    # build staging views + daily marts
make dbt-test   # run 15 data tests (not_null / accepted_values / uniqueness)
```

Layers:

- `staging` — thin views over the `curated` source tables
  (`page_views_1m`, `campaign_stats_1m`).
- `marts` — daily rollups `daily_page_summary` (exact views, no double-counted
  approx users) and `daily_campaign_summary`.

Connection comes from `dbt/profiles.yml` and can be overridden with
`DBT_HOST` / `DBT_PORT` env vars.

## Phase 2 add-ons — Delta Lake (P2-03)

Local lakehouse demo with **Delta Lake** (no cloud account): reads a
deterministic sample fixture, writes it to a Delta table, and upserts a daily
summary with Delta `MERGE` (run twice to prove idempotency).

```bash
make delta-demo
```

See [docs/lakehouse-delta.md](docs/lakehouse-delta.md) for the Delta vs Iceberg
decision and merge semantics.

## Phase 2 add-ons — data quality (P2-04)

Soda Core checks on the curated Postgres schema plus native ClickHouse checks;
results are merged into JSON + HTML reports.

```bash
make quality-run
open quality/target/quality-report.html
```

See [docs/data-quality-soda.md](docs/data-quality-soda.md) for details.

## Phase 2 add-ons — Redis cache + rate limiting (P2-05)

- **Cache**: hot read endpoints (`/api/v1/summary`, `/api/v1/top/*`) are cached
  in Redis for `CACHE_TTL_SECONDS` (default 10s).
- **Rate limit**: read endpoints are limited per client IP to
  `RATE_LIMIT_MAX_REQUESTS` per `RATE_LIMIT_WINDOW_SECONDS` (default 60/60s);
  breaches return `429` with a `Retry-After` header.
- Redis is optional at runtime: if it is unreachable the cache degrades to a
  no-op and the limiter fails open, so the read-only API keeps working.

## Phase 2 add-ons — go_ops CLI (P2-06)

Read-only Go control-plane CLI for the API (`health`, `topics`, `freshness`,
`status`), pure Go standard library. Built/tested in a pinned `golang` Docker
image so no local Go toolchain is needed.

```bash
make go-ops-test    # unit tests in golang:1.24.3-alpine
make go-ops-build   # build go_ops/bin/go_ops
```

See [go_ops/README.md](go_ops/README.md) for usage.

## Phase 2 add-ons — Ansible (P2-07)

Provisioning playbooks in `ansible/`: a runnable local-demo playbook (checks
Docker/colima and ensures local services are up) plus a reference playbook for
preparing a Linux EKS bastion.

```bash
make ansible-check     # local playbook, --check mode
make ansible-provision # ensure local services are running
```

See [ansible/README.md](ansible/README.md).

## Phase 2 add-ons — ai_assistant (P2-08)

FastAPI assistant that explains alerts, generates **read-only** SQL against an
allowlisted table set, and runs z-score anomaly detection on Prometheus
metrics. Default LLM provider is **mock** (no API key, public-repo safe); an
OpenAI-compatible provider can be enabled via env vars.

```bash
make ai-assistant-up
curl -s http://localhost:8090/health
```

See [docs/ai-assistant.md](docs/ai-assistant.md) and
[ai_assistant/README.md](ai_assistant/README.md).

## Phase 3 add-ons — kind (P3-01)

Local Kubernetes deployment of the core subset (PostgreSQL, ClickHouse, Redis,
Kafka KRaft, API) plus a Spark verification Job. Manifests live in `k8s/`
(kustomize).

```bash
make kind-up          # create the kind cluster (control-plane + worker)
make kind-load        # load locally-built api/spark images into kind
make kind-apply       # apply the clickstream manifests (namespace + core services)
make kind-topics      # create clicks.raw / clicks.dlq (kafka-init Job)
make kind-spark-check # run the Spark (Delta) verification Job
make kind-down        # delete the cluster
```

Verified on 2026-09-06: all core pods `Running/Ready`, API `/ready` returns
postgres/clickhouse/kafka all `true`, and the Spark Job completes with an
idempotent Delta write (60 events). Notes and fixes that came out of the kind
bring-up are documented in `docs/kind-bring-up.md`.

## Phase 3 add-ons — Helm + cert-manager (P3-03)

The same core subset is packaged as a values-driven Helm chart at
`k8s/helm/clickstream` (PostgreSQL, ClickHouse, Redis, Kafka KRaft, API,
`kafka-init` Job, optional self-signed ClusterIssuer).

```bash
make helm-lint                 # helm lint
make helm-install-cert-manager # install cert-manager v1.16.3
make helm-up                   # helm upgrade --install into namespace clickstream
kubectl -n clickstream get pods
```

Verified on 2026-09-06 on kind: release `deployed`, all core pods
`Running/Ready`, `kafka-init` Job `Completed` (topics auto-created), API
`/ready` = all checks `true`, and ClusterIssuer `clickstream-selfsigned`
`Ready=True`.

## Phase 3 add-ons — CI/CD: GHCR images (P3-04)

`.github/workflows/build-images.yml` builds and pushes producer/api/spark/etl/
ai-assistant images to **GHCR** on `main` (tagged `sha-<commit>` + `latest`),
ignores doc-only changes, and cancels superseded runs. The optional EKS deploy
step exists but is disabled (`if: false`) until Phase 3 EKS is ready.

```bash
# Manual trigger from the GitHub UI: Actions → "Build & push images (GHCR)"
# Required when enabling the deploy step:
#   secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
#   vars:    AWS_REGION / EKS_CLUSTER_NAME
```

See [docs/ghcr-ci.md](docs/ghcr-ci.md).

## Phase 3 add-ons — Terraform EKS (P3-02)

`terraform/` defines the EKS platform: VPC (2 AZs + NAT) and EKS cluster with a
managed node group (`t3.medium`, 1–2 nodes), using pinned community modules.

```bash
make eks-init       # terraform init (downloads providers/modules)
make eks-validate   # terraform validate
make eks-plan       # terraform plan (needs AWS creds; no changes)
# make eks-apply    # GATED: only with explicit approval (EKS costs money)
make eks-destroy    # teardown everything
```

Code + `validate` are complete (2026-09-06). `plan`/`apply` are gated on AWS
credentials and explicit user approval. See
[terraform/README.md](terraform/README.md).

## Phase 3 add-ons — teardown & cost control (P3-06)

EKS is destroyed as a first-class action so demo costs never linger:

```bash
make eks-plan-destroy   # preview what destroy would remove
make eks-destroy        # terraform destroy -auto-approve
make kind-down          # delete the local kind cluster
make eks-destroy-all    # EKS + kind teardown
```

See [docs/cost-control.md](docs/cost-control.md) for the cost rules and the
post-demo checklist.
