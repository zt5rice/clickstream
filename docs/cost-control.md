# EKS cost control & teardown (P3-06)

Status: implemented 2026-09-07.

## Why this exists

EKS costs money the moment it exists (cluster + nodes + NAT), even when idle.
This project is a portfolio demo, so teardown is a first-class action, not an
afterthought.

## Rules

1. **Never leave EKS running overnight.** After any EKS verification run
   `make eks-destroy` immediately.
2. Default cluster keeps cost low: `node_desired_size=1`, `node_max_size=2`,
   `t3.medium`, single NAT gateway (`terraform/terraform.tfvars.example`).
3. Use `make eks-plan-destroy` first to review what will be removed.

## Commands

```bash
make eks-plan-destroy  # preview the destroy (no changes)
make eks-destroy       # terraform destroy -auto-approve
make kind-down         # delete the local kind cluster
make eks-destroy-all   # both of the above
```

## Checklist (run when done testing)

- [ ] `make eks-destroy` completed (no `Error`/`timeout` left over).
- [ ] `aws eks list-clusters --region us-west-2` shows no `clickstream-demo`.
- [ ] AWS Cost Explorer / billing alarm: no unexpected EKS spend.
- [ ] `make kind-down` (if the local kind cluster is no longer needed).
- [ ] `docker compose down` (if the local stack was started).

## Suggested guardrail (do this once, in AWS)

Create a **billing budget + alarm** (e.g., $10/month) in AWS Budgets that emails
you before demo costs exceed free-tier-ish levels. Terraform/AWS Budgets code is
deliberately not included so no account-wide resources are created implicitly.
