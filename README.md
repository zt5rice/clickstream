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

## Next Steps

Follow [DESIGN.md](DESIGN.md) Week 1 / Week 2 checklists. Run locally first, then kind, then EKS.

## Live demo

See [docs/demo-checklist.md](docs/demo-checklist.md) for a step-by-step checklist
(URLs, health checks, PromQL queries, Grafana panels) to run a live demo of the
local stack.
