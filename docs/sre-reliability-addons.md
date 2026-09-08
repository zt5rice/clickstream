# SRE / Reliability Add-ons for the Clickstream Project

Status: v0.1 · Purpose: input for the phased project plan — reliability engineering
practices worth adding, based on a Site Reliability Engineer II job description.

Fit note: this SRE role requires 5+ years and Kong/Service Mesh experience, so it is a
lower-priority target today; the additions below still strengthen Data Engineer applications
(monitoring, performance, load testing are also in those JDs).

## A. Low cost / high value (1–2 days each) — recommended first

1. **SLO / SLI / Error Budget**
   - Define SLIs: availability, p95 latency, data freshness, DLQ rate.
   - Define SLOs and an error-budget policy; document in `docs/`.
2. **Alerting + Runbooks**
   - Alertmanager rules: Kafka lag > threshold, DLQ count, API p95 latency.
   - Standard runbook template + on-call documentation.
3. **Post-incident review template**
   - Template for blameless post-incident reviews; run one after a deliberate incident.
4. **Load testing (k6 / Locust)**
   - Generate load, measure throughput/latency, estimate capacity; capture real resume metrics.
5. **Chaos / failure drill**
   - Kill a Kafka broker or pod; verify recovery; record MTTR.

## B. Medium cost — hits the SRE JD core (2–4 weeks)

6. **API Gateway (Kong)**
   - Put Kong in front of the FastAPI service: routing, rate limiting, TLS.
7. **Service Mesh (Istio)**
   - mTLS between services, canary traffic shifting, retries/timeouts/circuit breaking.
8. **cert-manager**
   - TLS certificate lifecycle management on EKS.

## C. Stretch (v2+)

9. **Argo CD (GitOps) + Argo Rollouts** — safe canary / blue-green rollouts.
10. **PostgreSQL HA (Patroni) / Redis sharding** — distributed data storage at scale.
11. **Release engineering** — semantic versioning, SBOM, image provenance.

## Suggested phasing

- **v1 reliability:** SLO/SLI + alerting/runbooks + post-incident review + k6 load test + chaos drill
- **v2:** pick one of Kong or Istio and do it deeply
- **v3 (stretch):** Argo CD/GitOps, PG HA, release engineering
