# kind bring-up notes (P3-01)

Status: implemented/verified locally on 2026-09-06 (kind 0.33, kubectl 1.37).

## What runs on kind

Core subset: PostgreSQL 16, ClickHouse 24.8, Redis 7, Kafka (KRaft single node,
`bitnamilegacy/kafka:3.7`), FastAPI API, plus a `kafka-init` Job (topics) and a
`spark-check` Job (local Delta demo via Spark). Manifests in `k8s/`, applied
with `kubectl apply -k k8s/`.

## Problems found & fixed during bring-up

1. **Kubernetes injects Service env vars that clobber API settings.** The API
   `Settings` uses plain names like `POSTGRES_PORT`, which K8s injects as
   `tcp://10.96.x.x:5432`, causing pydantic int parsing to fail. Fix: provide
   explicit numeric ports in the `api-config` ConfigMap and set
   `enableServiceLinks: false`.
2. **Background Kafka lag collector could crash the API.** When Kafka is still
   starting, `KafkaConsumer` bootstrap raises and previously brought the whole
   app down. Fix: wrap the collector loop so it logs and keeps the API alive.
3. **Readiness/liveness probes fired too early.** The API took a moment to
   start, so zero-delay probes caused restart loops. Fix: add
   `initialDelaySeconds` + larger `failureThreshold`.
4. **Kafka KRaft single-node could not register with its own controller.**
   Broker and controller run in one process; pointing the controller quorum at
   the Service DNS (`kafka:9093`) deadlocks because Services only get endpoints
   once the pod is Ready. Fix: use `127.0.0.1:9093` for
   `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS`.
5. **Heavy probe commands restarted a healthy Kafka.** `kafka-topics.sh --list`
   is slow (>10s), so liveness kept killing the container. Fix: lightweight TCP
   probe against port 9092.

## Verification

- `kubectl -n clickstream get pods` → core pods `Running`/`Ready`, Jobs
  `Completed`.
- API `/ready` → `{"status":"ready","checks":{"postgres":true,
  "clickhouse":true,"kafka":true}}`.
- `spark-check` Job logs →
  `DELTA_DEMO_RESULT {..., "merge_idempotent": true}`.
