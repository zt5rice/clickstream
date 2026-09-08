# Start Prompt — clickstream project

> Copy this prompt into a new Codex task / agent to start or continue the project.
> Working directory: this repository (a personal portfolio project). Git
> operations on the local clone may need write permission / escalation.

---

You are helping me build "clickstream" — a resume-worthy, production-shaped real-time clickstream
streaming pipeline (Kafka + Spark/Flink + EKS) for data-engineering and data-infrastructure job applications.

## Communication
- Communicate with me in **Chinese (中文)**: status updates, summaries, and questions in Chinese.
- Code, comments, READMEs, and technical docs in English (unless I ask otherwise).

## Context — read these FIRST
- `DESIGN.md` (authoritative design doc: architecture, tech-stack coverage matrix, milestones)
- `README.md`
- `docs/tech-stack-addons.md` (planned v1/v2 additions)
- `docs/sre-reliability-addons.md` (reliability track)
- `docs/collaboration-and-resources.md` (guardrails: no proprietary code, public-repo safe)

## Phases (do them in order; start with Phase 1)

### Phase 1 — Local core pipeline (THIS session)
1. Read DESIGN.md + docs first; confirm scope with me before diverging.
2. Implement `producer/` — Python clickstream simulator producing JSON events to Kafka topic `clicks.raw`
   (batching, retries, idempotent producer, config via env).
3. Implement `spark_jobs/` — Spark Structured Streaming consuming `clicks.raw`, windowed aggregations
   → Postgres (curated tables) and ClickHouse (OLAP sink).
4. Implement `api/` — FastAPI: health + read-only query endpoints (REST/JSON), basic Prometheus metrics.
5. Wire `docker-compose.yml` so `make up` starts Kafka (KRaft), Spark, Postgres, ClickHouse, Redis,
   API, Prometheus, Grafana + kafka-exporter; fix the existing TODO stubs; add healthchecks.
6. Add `tests/` — pytest unit tests (producer serialization, API endpoints) + integration test
   against local Kafka/Postgres.
7. Monitoring: Prometheus scrape configs (kafka-exporter, API) + Grafana dashboard
   (Kafka lag, DLQ, API latency) + Alertmanager rule examples.
8. Update README + DESIGN.md with what changed; record REAL measured metrics (throughput, latency, lag).

### Phase 2 — Data-engineering depth (v1 add-ons)
- Airflow DAGs (batch orchestration/ETL scheduling) + dbt models & tests on curated tables.
- Delta Lake / Iceberg lakehouse writes (S3 or local) from Spark.
- Data quality: Great Expectations or Soda validation suites + reports.
- Redis caching / rate limiting in the API.
- `go_ops/` — Go ops CLI / control-plane service (health checks, topic status, lag).
- `ansible/` — provisioning playbooks (demo nodes / EKS bastion).
- `ai_assistant/` — LLM pipeline-ops assistant (explain alerts, generate read-only SQL) + simple
  anomaly detection on lag/latency metrics.
- Each added technology must actually run locally and be documented (README + DESIGN.md).

### Phase 3 — Deployment & platform (kind → EKS)
- `kind` local Kubernetes cluster + manifests.
- Terraform EKS (VPC, node group, ALB), Helm charts, cert-manager.
- CI/CD: GitHub Actions (lint, unit, integration, build & push images, optional deploy).
- Optional: AWS Lambda + API Gateway (DLQ alert handler / freshness checker), CloudWatch integration.
- Teardown automation + cost control (`make eks-destroy`).

### Phase 4 — Reliability & scale (stretch)
- SLO/SLI + error budgets; Alertmanager + runbooks; post-incident review template.
- Load testing (k6 / Locust) with capacity estimates.
- Chaos / failure drills (kill a broker or pod; record MTTR).
- Optional: Kong API gateway, Istio service mesh (mTLS, canary), Flink variant,
  GitOps (Argo CD / Argo Rollouts), AWS MSK / OpenSearch.

### Phase 5 — Portfolio packaging
- Final measured metrics (throughput, latency, lag, cost) in README.
- Architecture diagram + design decisions summary.
- Public-repo hygiene: LICENSE (MIT), no employer IP, README clearly says personal/portfolio project.
- Interview talking points + demo script (walk through `make up` → data flow → dashboards).

## Constraints (non-negotiable)
- No code/config copied from any employer — all code original, generic, public-repo safe.
- Pin every dependency; every technology must actually run and be explainable.
- Keep it honest: small personal/portfolio project; do not overclaim production scale.
- Core logic must have tests; run `make lint` and `make test` before finishing each phase.
- If a planned tech is out of scope for the current phase, list it in a "NEXT SESSION" section
  rather than half-adding it.
- Confirm with me before changing DESIGN.md architecture or adding new technologies not listed in the docs.

## Deliverables / done criteria (per phase)
- Phase 1: `make up` → producer emits events → Spark aggregates → API serves queries →
  Grafana shows lag/latency; `make test` green; README + DESIGN.md updated with measured metrics.
- Later phases: each phase ends with working code, passing tests, updated docs, and a Chinese summary
  of what was built, what was deferred, and decisions made.
