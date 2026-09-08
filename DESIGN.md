# Real-Time Clickstream Streaming Pipeline — Design Document

Status: v0.3 (Phase 1 + Phase 2 complete) · Target: Data Engineer roles (portfolio, resume-oriented)

## 1. Goal

Build a production-shaped, resume-worthy real-time clickstream pipeline from the ground up that demonstrates:
Apache Kafka, stream processing (Spark Structured Streaming + optional Flink), ETL / batch processing,
SQL analytics, REST/JSON APIs, Kubernetes (kind → EKS), monitoring (Prometheus/Grafana), testing (TDD),
and CI/CD. Every component is small enough to run on a laptop first, then deploy to AWS EKS.

## 2. Architecture

```
 Simulator / Producer (Python)                 Batch Loader (ETL, Python/Spark)
        |  writes click events (JSON)                |  reads raw -> validates -> curated
        v                                            v
 +------------------+   topics: clicks.raw        +------------------+
 |   Apache Kafka   | ------------>  Stream jobs  |                  |
 | (local: KRaft;   |   clicks.enriched          |  Spark Structured |
 |  optional: MSK)  |   clicks.dlq (dead-letter) |  Streaming (win-  |
 +------------------+                            |  dowed agg)       |
        ^                                        |  optional: Flink  |
        |                                        +---------+---------+
        |                                                  |
        | writes                                             v
        |                                        +---------------------+
        |                                        |   Sinks: PostgreSQL |
        +-- API (FastAPI: status / query / ws)   |   (curated tables)  |
             (REST JSON, optional gRPC/WebSocket)+---------------------+
                                                          ^
 Monitoring: Prometheus + Grafana (Kafka lag, Spark/Flink, JVM, API, DLQ)
 Deployment: Docker Compose (local) -> kind (local K8s) -> EKS (Terraform)
 CI/CD: GitHub Actions (lint + unit/integration tests + docker build)
```

## 3. Components

| Component | Language/Framework | Responsibility |
|---|---|---|
| `producer/` | Python | Simulates click events (user, page, device, ts); produces to `clicks.raw`; supports backpressure & batching |
| `spark_jobs/` | PySpark (optional Scala) | Structured Streaming: windowed aggregations (page views, users, latency) → curated tables |
| `flink_jobs/` | Flink (PyFlink or Java) | Optional second engine: stateful sessionization / anomaly detection (JD requires Flink) |
| `etl/` | Python/Spark | Batch ETL: raw → curated, validation, dedup, idempotent upserts |
| `api/` | FastAPI | REST status/query endpoints (JSON); optional gRPC + WebSocket live-lag stream |
| `schemas/` | JSON / Avro (Schema Registry) | Event schemas + versioning |
| `k8s/` | kind manifests | Local Kubernetes deployment (Deployments, Services, ConfigMaps) |
| `terraform/` | Terraform + eksctl | AWS EKS cluster + optional MSK + OpenSearch |
| `monitoring/` | Prometheus/Grafana | Scrape configs + dashboards (Kafka lag, throughput, DLQ, JVM) |
| `tests/` | pytest | Unit + integration tests (TDD) |
| `sinks/clickhouse` | ClickHouse | OLAP sink for aggregated analytics; SQL queries (JD: ClickHouse/Druid) |
| `ai_assistant/` | Python/FastAPI + LLM | Pipeline-ops assistant: explain alerts, generate read-only SQL against curated data (JD: AI-powered capabilities) |
| `redis` | Redis | Caching / rate limiting / session state for the API (JD: Redis/caching) |
| `go_ops/` | Go | Ops CLI / control-plane service: health checks, topic status (JD: Go + infra automation) |
| `ansible/` | Ansible | Provisioning playbooks (demo nodes / EKS bastion); complements Terraform |

## 4. Event Schema

```json
{
  "event_id": "uuid",
  "user_id": "u-123",
  "session_id": "s-456",
  "ts": "2026-08-27T15:00:00Z",
  "page": "/home",
  "device": "mobile",
  "campaign_id": "c-7",
  "region": "us-west"
}
```

Optional Avro: `schemas/click.avsc` + Confluent Schema Registry for schema evolution.

## 5. Tech Stack Coverage Matrix (verification against saved job descriptions)

| JD requirement / mentioned stack | Project module | Status |
|---|---|---|
| Apache Kafka (topics, partitions, consumer groups, offsets, lag, DLQ) | `producer/`, `spark_jobs/`, `kafka/` (compose), `monitoring/` | Core |
| Flink (stateful/stateless) | `flink_jobs/` | Core (v2) |
| Spark | `spark_jobs/` | Core |
| SQL | `api/` + `sql/` (Postgres analytics) | Core |
| JVM languages (Kotlin/Scala/Java) | `jvm/` Java consumer + Scala Spark job | Optional |
| Python | `producer/`, `api/`, `etl/`, `tests/` | Core |
| REST / JSON | `api/` (FastAPI, JSON) | Core |
| gRPC | `api/grpc/` | Optional |
| WebSockets | `api/ws/` (live lag stream) | Optional |
| Avro / Protobuf | `schemas/` + Schema Registry | Optional |
| ETL / batch processing | `etl/` + `spark_jobs/batch/` | Core |
| Stream processing / real-time pipelines | Kafka + Spark/Flink jobs | Core |
| TDD / OOP / functional | `tests/` (pytest first), typed code | Core |
| Git / CI-CD | `.github/workflows/ci.yml` | Core |
| Monitoring / performance analysis | `monitoring/` (lag, throughput, DLQ, JVM) | Core |
| Docker / Kubernetes | `docker-compose.yml`, `k8s/` (kind) | Core |
| EKS | `terraform/` (EKS module) | Core (deploy) |
| AWS MSK | `terraform/msk/` (managed Kafka migration path) | Optional |
| AWS OpenSearch | `sinks/opensearch/` | Optional |
| S3 (semi/unstructured) | `sinks/s3/` raw archive | Optional |
| ClickHouse / Druid (OLAP) | `sinks/clickhouse` (ClickHouse); Druid optional | Core (ClickHouse) / Optional (Druid) |
| AI-powered platform capabilities | `ai_assistant/` | Core (v1) |
| Redis (caching) | `redis` compose service | Core (v1) |
| Go (infra automation / control plane) | `go_ops/` | Core (v1) |
| Ansible (config management) | `ansible/` | Core (v1) |

## 6. Deployment Paths

1. **Local (fast loop):** `docker compose up -d` → Kafka (KRaft), Spark, Postgres, Prometheus, Grafana, API.
2. **Local K8s:** `make kind-up` → `kubectl apply -f k8s/` (same images, validates manifests).
3. **AWS EKS:** `terraform/` (VPC, EKS, node group, ALB) → deploy via kubectl/Helm. **Teardown is mandatory after testing** — see `make eks-destroy` / `make eks-destroy-all` and `docs/cost-control.md` (P3-06).
4. **Optional managed:** `terraform/msk/` (MSK instead of self-hosted Kafka), OpenSearch sink.

## 7. Reliability & Monitoring

- Kafka: acks=all, idempotent producer, consumer groups with lag tracking, dead-letter topic (`clicks.dlq`).
- Spark: exactly-once-ish semantics (checkpointing, idempotent sinks), watermarking, windowed aggregation.
- ETL: idempotent upserts, schema validation, retries with backoff.
- API: structured logging, Prometheus metrics, health endpoints.
- Dashboards: Kafka consumer lag, produce/consume throughput, DLQ count, API latency, JVM metrics.

## 8. Testing & CI/CD

- Unit tests (pytest) for producer serialization, ETL validation, API endpoints.
- Integration tests against local Kafka/Postgres (docker compose, testcontainers-style).
- GitHub Actions: lint → unit → integration → build & push images → (optional) deploy to EKS.

## 9. Milestones & Learning Checklist

**Week 1 — Kafka + Spark core**
- [ ] Kafka concepts: topic/partition/offset/consumer group, acks, idempotence, retention, DLQ
- [ ] Producer (Python) with batching + retries
- [ ] Spark Structured Streaming windowed aggregation → Postgres
- [ ] Kafka lag monitoring (kafka-exporter + Grafana)

**Week 2 — Containerization + K8s + EKS + CI**
- [ ] Dockerize all components
- [ ] kind local cluster + manifests
- [ ] Terraform EKS (or eksctl), deploy, verify, teardown
- [ ] GitHub Actions CI/CD
- [ ] README with architecture + **measured metrics** (throughput, latency, lag, cost)

## 10. Resume Metrics to Capture (be honest, from your own runs)

- Produce/consume throughput (events/sec) under load
- End-to-end latency p50/p95
- Peak consumer lag and recovery time
- Spark window processing time vs event time
- EKS monthly cost for the test period

## 11. Repository Structure

```
clickstream/
├── DESIGN.md
├── README.md
├── Makefile
├── docker-compose.yml
├── producer/          # Python click simulator
├── spark_jobs/        # Structured Streaming + batch
├── flink_jobs/        # optional Flink jobs
├── etl/               # batch ETL/validation
├── api/               # FastAPI (REST/JSON, optional gRPC/WS)
├── schemas/           # JSON/Avro event schemas
├── k8s/               # kind manifests
├── terraform/         # EKS (+ optional MSK/OpenSearch)
├── monitoring/        # Prometheus config + Grafana dashboards
├── tests/             # pytest unit + integration
├── ai_assistant/      # LLM-based pipeline-ops assistant
├── go_ops/            # Go ops CLI / control-plane service
├── ansible/           # provisioning playbooks (Terraform complement)
├── sinks/             # Postgres / ClickHouse / OpenSearch / S3 adapters
└── docs/              # decisions, learning notes
```

## 12. As-Built — Phase 1 Measured Metrics

The Phase 1 local core pipeline (P1-01…P1-17) is merged on `main` and verified
end-to-end locally. Real measured values from a ~53-minute steady run
(macOS + colima/Docker, `make up`, producer ~100 events/s):

| Metric | Measured value |
|---|---|
| Producer throughput | ~100.0 events/s, 0 failed (~319k events in ~53 min) |
| End-to-end freshness | 0 s (latest 1-min curated window is the current minute) |
| DLQ depth | 0 |
| API p50/p95 (client, n=50) | summary 25.6/32.9 ms · events/recent 46.9/73.1 ms · top/pages 10.4/14.6 ms |
| API p50/p95 (Prometheus histogram) | summary 21/46 ms · events/recent 42/90 ms · top/pages 8.5/22 ms |
| Storage after ~53 min | Postgres `curated.page_views_1m` 22,754 rows · ClickHouse `olap.clicks` 319,759 events |

Measured 2026-09-02 06:54 UTC (= 2026-09-01 23:54 PDT). These are honest
reference points from our own run — re-measure on your machine before quoting
them (see `docs/demo-checklist.md` for reproduction steps).

## 13. As-Built — Phase 2 Add-ons

Phase 2 (Linear M2) adds data-engineering depth on top of the Phase 1 core.
Every add-on runs locally and was verified on 2026-09-04:

| Ticket | Add-on | Where | Verified result |
|---|---|---|---|
| P2-01 | Airflow DAGs | `etl/dags/` | webserver+scheduler up; freshness check + daily rollup DAG run |
| P2-02 | dbt models/tests | `dbt/` | 4 models built; 15 data tests pass |
| P2-03 | Delta Lake (local) | `spark_jobs/delta_lakehouse.py` | 60 events written; MERGE idempotent |
| P2-04 | Data quality (Soda + native CH) | `quality/` | 11 checks pass; JSON+HTML reports |
| P2-05 | Redis cache + rate limit | `api/` | cache keys written; 429 after limit |
| P2-06 | go_ops CLI | `go_ops/` | unit tests pass; live `topics`/`status` |
| P2-07 | Ansible playbooks | `ansible/` | local provision playbook run (ok) |
| P2-08 | ai_assistant | `ai_assistant/` | explain-alert / generate-sql / anomalies endpoints live |

Architecture note: these add-ons sit beside the Phase 1 core rather than
replacing it — Airflow/dbt work on the curated Postgres layer, Delta adds a
local lakehouse write path from Spark, Soda/quality validate both warehouses,
Redis hardens the API, `go_ops` is an ops CLI, Ansible prepares deployment
nodes, and `ai_assistant` is an LLM-powered ops layer (mock by default).
