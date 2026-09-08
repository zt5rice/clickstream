# Chaos / failure drills (P4-04)

Status: implemented 2026-09-07 (Phase 4, M4) on the **kind** cluster
(helm release `clickstream`, all pods Ready before the drill).

## Method

1. Delete a workload (Deployment pod or StatefulSet pod) with `kubectl delete`.
2. Let Kubernetes self-heal (Deployment rollout / StatefulSet recreation).
3. Measure **MTTR = time from kill until Ready** with
   `scripts/chaos/measure_recovery.sh`.
4. Record results; verify dependent endpoints still work afterwards
   (API `/ready`).

Run with:

```bash
make chaos-drill-api     # delete API deployment -> rollout -> Ready
make chaos-drill-kafka   # delete kafka-0 pod -> recreate -> Ready
```

## Measured results (2026-09-07, kind)

| Drill | Target | MTTR |
|---|---|---|
| API pod deletion | `deployment/api` (pod `-l app=api`) | **21s** |
| Kafka pod deletion | `pod/kafka-0` | **33s** |

Recovery verified after each drill: API `/ready` =
`{"postgres":true,"clickhouse":true,"kafka":true}`.

The helper measures time from `kubectl delete` until the **new** pod (different
UID) is Ready — naive polls can measure the old terminating pod and report a
false `0s` (we hit that while building the drill).

## Interpretation

- Deployment/StatefulSet controllers give us self-healing for free; MTTR here
  is mostly image-start + readiness time, not manual intervention.
- Kafka restart depends on KRaft metadata recovery + topic creation already
  done (kafka-init). Real MTTR on EKS would include node scheduling.
- These drills protect the freshness SLO (R2 runbook): if Kafka/API goes down,
  freshness stalls until recovery — watch `pipeline_freshness_seconds`.

## Safety notes

- Run drills only against **kind / throwaway demo** stacks, not real traffic.
- Keep the EKS teardown discipline separate (`docs/cost-control.md`).
- Pair every drill with the PIR template (`docs/pir-template.md`).
