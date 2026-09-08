# M4 Results — SLO snapshot, capacity, MTTR (P4-10)

Status: closeout of the M4 core scope, 2026-09-08.

## 1. SLO compliance snapshot

Honest framing: these are **short-run measurements** from the local/kind demo,
not a 30-day production history. Treat as a practised snapshot.

| SLO (from `docs/slo-sli-error-budget.md`) | Snapshot result | Meets? |
|---|---|---|
| API availability ≥ 99.5% | 100% during k6 run (0 errors / 3,227 reqs) and drills | ✅ (snapshot) |
| API p95 ≤ 300 ms | 180.6 ms @ 20 VUs (local stack) | ✅ |
| Freshness ≤ 3 min | ~0 s during steady local runs; not measured continuously | ⚠️ partial |
| DLQ = 0 | 0 (no error injection in these runs) | ✅ |

## 2. Load / capacity

- k6 read-API test (20 VUs, ~70s): **3,227 requests ≈ 46 req/s**;
  p50/p90/p95 = **12.5 / 128 / 180.6 ms**; 0% errors.
- First run tripped the per-IP rate limiter (60 req/min → 429s) — protection
  works as designed; load tests simulate distinct clients.
- Capacity thinking: local single-process uvicorn is the first bottleneck; in
  EKS, scale API replicas behind a load balancer and watch Postgres/ClickHouse.
  Full EKS load test = deferred (optional).

## 3. MTTR (kind chaos drills)

| Drill | MTTR |
|---|---|
| Delete API pod (`deployment/api`) | **21s** |
| Delete Kafka pod (`statefulset/kafka-0`) | **33s** |

Recovery verified: API `/ready` all-true after each drill. MTTR = time from
`kubectl delete` until a **new pod (different UID)** is Ready.

## 4. Delivered practices

- `docs/slo-sli-error-budget.md` (P4-01)
- `docs/runbooks.md` + `docs/pir-template.md` (P4-02)
- `scripts/load/k6/read-api.js` + `docs/load-testing-capacity.md` (P4-03)
- `scripts/chaos/measure_recovery.sh` + `docs/chaos-drills.md` (P4-04)

## 5. Gaps & next (honest)

- No long-window SLO history; freshness SLO needs a longer monitored run.
- EKS load test and EKS chaos drills not yet performed (deferred).
- Optional heavy items deferred: Kong, Istio, Flink variant, Argo GitOps,
  AWS MSK / OpenSearch.
- Recommendation for later: encode EKS Access Entry + EBS CSI/StorageClass into
  Terraform (P3-07 lesson) and run a monitored 7-day demo for freshness SLO.
