# Tech Stack Add-ons for Data Infrastructure Roles

Status: v0.1 · Purpose: input for the phased project plan (which technologies to add,
where they fit, and how much effort each takes).

## 1. High-value additions (frequent in Data Infra JDs + strong fit for this project)

| Tech | Why it is common | Where it fits in this project | Effort |
|---|---|---|---|
| Apache Airflow / Dagster | Nearly all data-infra roles require orchestration / ETL scheduling; Airflow already used in nanotrack | `etl/` → `dags/`: DAGs that schedule batch loads and validation | M |
| dbt | Transform layer is near-standard in current data-engineering JDs (dbt models / SQL / tests) | `dbt/`: SQL models + tests on top of curated Postgres tables | M |
| Delta Lake / Iceberg | Lakehouse table formats are the biggest current trend (S3 + Iceberg/Delta) | `spark_jobs/`: write to S3/local with Delta or Iceberg tables | M |
| Helm | Standard packaging for EKS deployments (JDs often ask for Helm charts) | `k8s/helm/`: package the k8s manifests as a chart | M |
| Data quality: Great Expectations / Soda | JDs frequently require data quality / validation; extends the existing data-quality story | `etl/` or `quality/`: validation suites + reports | M |
| OpenTelemetry | Observability standard (trace + metrics); aligns with existing distributed-tracing experience | `monitoring/`: instrument API / Spark, ship traces to Jaeger | M |

## 2. v2 additions (optional, add when v1 is stable)

| Tech | Notes | Effort |
|---|---|---|
| Trino | Federated / interactive SQL query engine; common in JDs | L |
| Debezium (CDC) | Change-data-capture ingestion; common in real-time pipelines | M |
| Argo CD (GitOps) | Kubernetes deployment trend; starting to appear in JDs | M |
| ClickHouse | Analytical columnar store; occasional JD ask; alternative OpenSearch sink | L |
| Pulumi / CloudFormation | IaC alternatives (Terraform is enough; pick one only if a JD asks) | S |

## 3. Ansible / Jenkins notes

- **Jenkins:** still appears in JDs but mostly for legacy systems. Add a `Jenkinsfile` as an
  alternative CI (resume already lists "Jenkins/GitHub CI/CD") — useful as evidence, not as the main path.
- **Ansible:** similar situation (config management / legacy). Add one simple playbook
  (e.g., provision demo-node dependencies or prepare an EKS bastion) to prove working knowledge.

## 4. Pitfalls

1. Do not overstuff the project: 3–4 technologies done deeply beats 10 touched superficially.
2. Keep the honesty rule: every added technology must actually be run in this project and be
   explainable in interviews (e.g., Helm `values`, dbt `test`, Delta `merge` semantics).
3. Do not bring any Intuit-internal material into the repo (see `collaboration-and-resources.md`).

## 5. Suggested phasing

- **v1 core (recommended):** Apache Airflow + dbt + Delta Lake/Iceberg + data quality (Great Expectations/Soda)
- **v2 stretch:** Trino, Debezium, Argo CD / GitOps
- **Nice-to-have:** Jenkinsfile, Ansible playbook

## 6. Landing

- Update `DESIGN.md` with an "Extended Stack Add-ons" section, or keep this document as the
  reference and link it from DESIGN.md.
- Update `docker-compose.yml` and the directory structure as components are added
  (`dags/`, `dbt/`, `quality/`, `k8s/helm/`, `monitoring/jaeger/`, etc.).

## 7. Additions from Staff Software Engineer – Data (R-124982, San Diego)

Fit note: this role requires 8+ years (a stretch today), but it is in San Diego and its stack is highly
relevant. The quick wins below are worth adding to the project.

### Quick wins (add to v1)

| Tech | Why | Where it fits | Effort |
|---|---|---|---|
| **ClickHouse** | JD explicitly requires ClickHouse / Apache Druid (OLAP) | Add as an OLAP sink: docker-compose service + Spark/Flink writes aggregated data; run SQL analytics against it | S–M (quick) |
| **AI-powered platform capability** | JD requires AI/ML tooling to automate workflows / improve data quality; reuses existing NL2SQL / GraphAtlas / MCP experience | `api/` or `ai_assistant/`: LLM-based pipeline-ops assistant (explain a Kafka-lag alert, generate SQL against curated tables) | M (reuses existing stack) |

### Not quick (keep optional / v2+)

| Tech | Notes | Effort |
|---|---|---|
| **Apache Druid** | Heavy JVM OLAP (coordinator/broker/historical); ClickHouse covers the JD requirement more cheaply | L |
| **AWS MSK** | Quick to test with an existing AWS account but costs money; keep optional | M (cost) |
| **Databricks** | Lakehouse; Iceberg/Delta in the project already demonstrates the same concepts | L |

### Already covered by this project
- Spark Structured Streaming, Kafka, Iceberg/Delta (lakehouse), FastAPI REST/JSON, ETL/batch,
  Prometheus/Grafana, EKS, Terraform, GitHub Actions — all already in DESIGN.md / this document.

## 8. Additions from Senior Software Engineer (Platform Data Reliability & Automation) (R-124371, San Diego)

Fit note: 6+ years with 3+ years Go + IaC is a stretch today, but the reliability/automation track is
highly relevant; the quick wins below also strengthen other targets (e.g., Apple's Golang backend role).

### Quick wins (add to v1)

| Tech | Why | Where it fits | Effort |
|---|---|---|---|
| **Redis** | JD explicitly requires caching (Redis) | Add `redis` to docker-compose; use for API result caching / rate limiting / session state | S |
| **Go component** | JD requires Go + Go for infra automation/control plane; also covers Apple Golang roles | `go_ops/`: small Go CLI (health checks, topic status) or a tiny control-plane API | S–M |
| **Ansible** | JD requires Terraform + Ansible; promote from nice-to-have to a real component | `ansible/`: playbook to provision demo nodes or bootstrap the EKS bastion | S–M |
| **AI anomaly detection** | JD: anomaly detection, predictive scaling, automated remediation | Extend `ai_assistant/` with simple statistical anomaly detection on Kafka lag / latency metrics | M |

### Not quick / optional

| Tech | Notes | Effort |
|---|---|---|
| Cassandra / Aerospike | Heavy NoSQL platforms; Redis covers the caching story for this project | L |
| AWS DynamoDB / ElastiCache / GCP | Managed services; cost + account required; optional | M (cost) |
| Internal developer platform / self-service | JD mentions it; significant scope | L |
| Linux internals / networking / storage | Conceptual depth — study alongside the project, not a component | — |

### Already covered by this project
- Terraform (EKS), Kubernetes, Kafka (self-hosted + optional MSK), SLO/SLI + observability
  (`sre-reliability-addons.md`), OpenTelemetry, AI-powered automation (`ai_assistant/`).

## 9. Additions from Senior Production Operations Engineer (R-125020, San Diego)

Fit note: 5+ years + 3+ years AWS Java/API ops is a stretch on years, but the on-call / incident /
AWS / AI-assisted workflows story overlaps strongly with existing experience. Quick wins below.

### Quick wins (add to v1)

| Tech | Why | Where it fits | Effort |
|---|---|---|---|
| **AWS Lambda + API Gateway** | JD explicitly lists Lambda / API Gateway; serverless component is cheap (free tier) | `lambda/`: e.g., DLQ alert handler or data-freshness checker; deployed via Terraform; API Gateway in front of the API | S–M |
| **AWS CloudWatch** | JD lists CloudWatch for observability | Export key alerts/metrics to CloudWatch (or use CloudWatch for Lambda); complements Prometheus/Grafana | S |
| **Java service** | JD requires Java/API services on AWS (and JVM coverage for other JDs) | `jvm/` or `api/java/`: small Java (Spring Boot or plain) status/ops service | S–M |
| **MySQL** | JD lists SQL/MySQL; trivial addition to prove multi-RDBMS | docker-compose `mysql` service (optional; Postgres already covers SQL) | S |

### Not quick / optional

| Tech | Notes | Effort |
|---|---|---|
| AWS DynamoDB / ElastiCache / Fargate | Managed services; cost + account required | M (cost) |
| Datadog / BigPanda / ServiceNow / JIRA | Commercial tooling; cover via interview story (on-call, incident workflows) rather than project | — |
| Snowflake / Oracle | Commercial data platforms; account + cost | L |
| Payment / commerce / fraud domain | Experience gap; not a project item | — |

### Already covered by this project
- On-call / incident response / RCA (SRE add-ons doc), Prometheus/Grafana observability, Docker/K8s/EKS,
  Terraform, Python/Go automation, AI-assisted workflows (`ai_assistant/` + existing Codex/Claude/Cursor experience).
