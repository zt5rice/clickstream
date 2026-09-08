# Design decisions & trade-offs

Status: consolidated for Phase 5 (P5-02), 2026-09-08.

## 1. Two parallel sinks: Postgres (curated) + ClickHouse (OLAP)
- **Why**: curated relational aggregates for SQL/BI-style queries and API
  serving; ClickHouse for raw events + fast OLAP aggregations.
- **Trade-off**: two write paths to keep consistent; demo keeps them aligned by
  writing from the same Spark micro-batch.

## 2. Freshness instead of broker "consumer lag"
- Spark manages offsets via checkpoints (`assign()`), so no broker consumer
  group exists — `kafka_consumergroup_lag` is meaningless here.
- **Decision**: expose `pipeline_freshness_seconds` (age of the latest curated
  window) as the pipeline SLI and Grafana panel.

## 3. Idempotent sinks for at-least-once semantics
- Spark guarantees at-least-once delivery per checkpoint; sinks are idempotent:
  Postgres `ON CONFLICT` upsert + ClickHouse `ReplacingMergeTree` (+`FINAL` on
  read). We claim "idempotent/at-least-once", not exactly-once end-to-end.

## 4. Delta (local) over Iceberg for the lakehouse demo
- Delta works out-of-the-box with Spark SQL (`DeltaSparkSessionExtension` +
  `DeltaCatalog`), no extra catalog service on a laptop. Iceberg noted as a
  multi-engine future option.

## 5. Soda Core over Great Expectations
- Lighter: YAML checks + SQL, machine-readable JSON we render into HTML.
- ClickHouse has no official Soda adapter on PyPI → native
  `clickhouse-connect` checks merged into the same report.

## 6. Redis cache + rate limiter fail open
- Read endpoints cached with TTL; per-client fixed-window limiter returns 429.
- Redis being down **fails open** (API stays available) — documented, and the
  load test showed the limiter tripping at 60 req/min/IP as designed.

## 7. Airflow freshness DAG fails on stale data
- The DAG raises when curated data is behind budget — a failure is the alert,
  so a missed window is visible instead of silently passing.

## 8. dbt daily mart omits "users"
- Summing per-window `approx_count_distinct` would double-count daily uniques;
  the mart exposes exact `views` + `window_count` instead.

## 9. Deployment: compose → kind/Helm → EKS, all with teardown discipline
- Local compose for fast loop; kind+Helm validates manifests; real EKS ran once
  with SPOT + single NAT and was destroyed the same session (cost <$2).
- EKS gotchas recorded in `docs/eks-run-2026-09-07.md`: Access Entry auth, EBS
  CSI/StorageClass, Postgres mount path, Kafka `fsGroup`.

## 10. MTTR measurement uses Pod UID
- "Pod named X is Ready" can report a false 0s when a StatefulSet recreates the
  pod instantly; the drill waits for a **different UID** to be Ready.

## 11. Public-repo hygiene
- All code original/synthetic; no employer data/IP; GHCR packages public for
  EKS pulls; MIT license + "personal/portfolio" statement in README.
