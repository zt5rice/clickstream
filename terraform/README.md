# Terraform EKS (P3-02)

Status: code written; `terraform init` + `terraform validate` pass. Real
`terraform plan`/`apply` require AWS credentials and explicit user approval
(P3-07 gating) because EKS/nodes incur cost.

## What it creates

- VPC (2 AZs, private/public subnets, NAT gateway, DNS) via
  `terraform-aws-modules/vpc` (pinned `~> 5.0`).
- EKS cluster + one managed node group (default `t3.medium`, 1–2 nodes) via
  `terraform-aws-modules/eks` (pinned `~> 20.0`).
- Cost-control defaults: single NAT gateway, min 1 node, `environment=demo`
  tags; teardown with `make eks-destroy`.

## Usage

```bash
make eks-init       # terraform init
make eks-validate   # terraform validate
make eks-plan       # terraform plan (needs AWS creds)
make eks-apply      # terraform apply (ONLY with explicit approval; costs $)
make eks-destroy    # teardown everything (run after demos!)
```

Credentials: standard AWS env vars (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) or `aws configure`.

## ALB controller (later, P3-07)

The `aws-load-balancer-controller` addon is intentionally not created yet. Once
the cluster exists:

1. Create the OIDC provider + controller IAM policy (AWS docs / eks module
   `enable_irsa`).
2. `helm repo add eks https://aws.github.io/eks-charts` and install the
   controller with `--set clusterName=clickstream-demo`.
3. Deploy services with `type: LoadBalancer` (ALB) or `Ingress` annotations.

## Cost guardrails

- `node_desired_size=1`, `node_max_size=2`, `t3.medium`, single NAT.
- Never leave the cluster running: `make eks-destroy` after verification.
