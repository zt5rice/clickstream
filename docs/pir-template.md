# Post-Incident Review (PIR) Template (P4-02)

Status: implemented 2026-09-07 (Phase 4, M4). Blameless by design — the goal is
to improve the system and the runbooks, not to blame people.

## Template

1. **Incident summary** (what happened, impact, duration)
2. **Timeline** (UTC; detection → mitigation → resolution)
3. **Severity & SLO impact** (which SLO/error budget was hit)
4. **Detection** (alert? dashboard? manual?)
5. **Root cause** (technical, with evidence)
6. **Contributing factors** (process/config/env gaps)
7. **Mitigation & resolution** (what actually fixed it)
8. **Follow-up actions**
   - [ ] code/config fix (owner)
   - [ ] runbook update (owner)
   - [ ] test/drill added (owner)
9. **What went well / what went wrong**
10. **Review date & participants**

---

## Worked example — EKS bring-up issues (2026-09-07)

Used as a deliberate practice drill for the real EKS run (P3-07):

1. **Summary**: EKS cluster deployed but core pods could not start: 401 on
   kubectl, PVCs Pending, Postgres init failure, Kafka PVC permission denied.
2. **Timeline**: apply (T+0) → auth fix (T+18m) → storage fix (T+30m) →
   postgres/kafka fixes (T+40m) → all Ready (T+45m).
3. **SLO impact**: none (demo infra, no live traffic); "time-to-deploy" was the
   real cost.
4. **Detection**: manual `kubectl get nodes/pods` + logs.
5. **Root causes**:
   - no EKS access entry for the creating IAM user;
   - EBS CSI / default StorageClass not provisioned;
   - Postgres image vs raw EBS mount (`lost+found`);
   - Bitnami Kafka non-root vs PVC permissions.
6. **Contributing factors**: first EKS run; assumptions from kind/Compose
   didn't transfer (auth model, storage, mount semantics).
7. **Mitigation**: Access Entry + admin policy; EBS CSI addon + StorageClass
   annotation + PVC recreate; chart mount/fsGroup fixes.
8. **Follow-ups**:
   - [x] chart fixes merged (P3-07)
   - [ ] encode access entry + storage class into Terraform/chart
   - [ ] runbook R4/R5 entries (this file)
9. **Went well**: fast diagnosis per layer; clean teardown; cost <$2.
10. **Review**: 2026-09-07, solo portfolio project.
