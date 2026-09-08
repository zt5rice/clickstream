# Runbooks (P4-02)

Status: implemented 2026-09-07 (Phase 4, M4). Pair with the alert rules in
`monitoring/alerts.yml` and the SLO policy in
`docs/slo-sli-error-budget.md`.

## Runbook template (use for every alert)

1. **Alert name / severity**
2. **What it means** — one-paragraph, plain language
3. **Check first** — the 2–3 dashboards/queries to confirm
4. **Likely causes** — ordered list
5. **Actions** — numbered steps; stop when fixed
6. **Escalation** — when to page/roll back
7. **Post-incident** — link to the PIR template (`docs/pir-template.md`)

---

## R1 — API unhealthy / not ready

- **Alert**: health/readiness check fails, or 5xx rate high.
- **Check first**: `docker compose ps` (local) or
  `kubectl -n clickstream get pods` (kind/EKS); `curl /ready`.
- **Likely causes**: Postgres/ClickHouse/Kafka down; bad env/config; image
  change.
- **Actions**:
  1. Confirm which backend check fails (`/ready` JSON).
  2. Restart that backend first (`docker compose up -d <svc>` /
     `kubectl rollout restart deployment/<name>`).
  3. Check API logs for config/connectivity errors.
  4. If a recent image/deploy caused it, roll back.

## R2 — Pipeline freshness stale (freshness check fails)

- **Alert**: `clickstream_freshness_check` fails, or
  `pipeline_freshness_seconds` grows.
- **Check first**: producer logs (rate ~100/s, failed=0); Spark driver logs.
- **Likely causes**: producer stopped; Kafka down; Spark job crashed/restarted;
  sink DB full/locked.
- **Actions**:
  1. `docker compose logs --tail=50 producer spark-submit`.
  2. Restart producer/spark (`docker compose restart producer spark-submit`).
  3. Verify a new 1-min window appears in Postgres/ClickHouse.

## R3 — DLQ non-empty

- **Alert**: `ClickstreamDLQNonEmpty`.
- **Check first**: `kafka_topic_end_offset{topic="clicks.dlq"}`; peek messages.
- **Likely causes**: malformed JSON/event from simulator changes or schema
  drift; parse bug.
- **Actions**:
  1. Sample a few DLQ messages (`kafka-console-consumer`).
  2. Fix producer/schema, then replay or drop test messages deliberately.

## R4 — Kafka unavailable

- **Check first**: container/pod status; `kafka-topics.sh --list`.
- **Actions**: restart Kafka (compose) or StatefulSet pod (kind/EKS); check
  listeners/quorum config (see `docs/kind-bring-up.md` for the KRaft gotchas).

## R5 — EKS resources left running (cost guard)

- **Check first**: `aws eks list-clusters`; `aws ec2 describe-volumes`.
- **Actions**: `make eks-destroy`; delete leftover dynamic EBS volumes; confirm
  empty. See `docs/cost-control.md`.

---

## On-call / demo-day notes

- Before any demo, run `make up` + the checks in `docs/demo-checklist.md`.
- After an EKS demo, teardown is **mandatory**: `make eks-destroy` + EBS check.
- Record every incident in the PIR template so patterns become visible.
