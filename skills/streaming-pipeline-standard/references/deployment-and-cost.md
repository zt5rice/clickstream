# Deployment and cost reference

## The three-rung path

| Rung | Purpose | Why not skip it |
|---|---|---|
| Local compose | Fast inner loop | Seconds to start; no cluster to wait on |
| kind / Helm | Validate manifests and chart logic | Catches the YAML and chart errors before you pay for a cluster |
| Real managed cluster | Prove it runs in the target environment | Cloud-specific failures (IAM, storage classes, networking) only appear here |

Each rung exists because the next one is slower and more expensive. Climbing them in order
means the expensive rung is only used to answer questions the cheap rungs cannot.

## Managed cluster: cost discipline

A managed cluster bills the moment it exists — control plane, nodes, and NAT — including
while idle. Treat teardown as part of the procedure, not a cleanup step.

**Rules**

1. Never leave the cluster running overnight.
2. Destroy in the same session as the verification run.
3. Preview the removal before removing anything.
4. Keep the default footprint small: minimum node count, smallest viable instance type,
   single NAT gateway.
5. Use spot capacity for demo runs; it is the difference between dollars and tens of
   dollars.

**Sequence**

```
preview destroy  →  run verification  →  destroy  →  confirm nothing is left billing
```

## Cloud-specific gotchas worth pre-solving

These are the failures that turn a 30-minute verification into a day. Check each before
the run:

| Area | Failure mode |
|---|---|
| Cluster auth | kubectl identity not mapped to a cluster access entry |
| Storage | No default StorageClass, or a CSI driver not installed — PVCs hang pending |
| Stateful workloads | Volume mount paths and filesystem group ownership differ from local |
| Networking | Load balancer / ingress add-ons not installed |
| Registry | Cluster cannot pull a private image without credentials |

## Public-repo hygiene

If the repo is public:

- Synthetic data only; no employer data, no production identifiers, no third-party IP.
- State clearly that it is a personal/portfolio project.
- Include a license.
- Scrub any provenance you would not want read aloud (see the provenance rule: attribute
  honestly rather than removing the attribution).

