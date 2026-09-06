# Helm + cert-manager (P3-03)

Status: implemented/verified on kind, 2026-09-06.

## Chart

`k8s/helm/clickstream` packages the core subset (PostgreSQL, ClickHouse, Redis,
Kafka KRaft single node, API) plus a `kafka-init` Job that waits for Kafka and
creates `clicks.raw` / `clicks.dlq`.

- `values.yaml` drives images, storage sizes, API env, Kafka cluster id, and
  whether to create a self-signed `ClusterIssuer`.
- The API template keeps the kind fixes: `enableServiceLinks: false`, explicit
  numeric ports, tolerant probes.
- Kafka keeps `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093` and a
  lightweight TCP probe (see `docs/kind-bring-up.md`).

## cert-manager

- Install cert-manager v1.16.3 (pinned) with `make helm-install-cert-manager`.
- Enable the issuer at install time:

```bash
helm upgrade --install clickstream k8s/helm/clickstream \
  --namespace clickstream --create-namespace \
  --set certManager.enabled=true
```

- The chart creates `ClusterIssuer clickstream-selfsigned` (`selfSigned: {}`).
  On EKS, swap this for an ACME/Let's Encrypt issuer (documented Phase 3 step).

## Verification

- `helm lint k8s/helm/clickstream` → 0 failures.
- `helm template` renders 14 kinds (plus ClusterIssuer when enabled).
- On kind: release `deployed`, all core pods Ready, `kafka-init` Completed,
  API `/ready` all-true, `ClusterIssuer` Ready=True.
