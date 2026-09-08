# SLO / SLI / Error Budget (P4-01)

Status: implemented as documentation 2026-09-07 (Phase 4, M4).

> Honest framing: this is a **portfolio demo**, not production. The targets
> below are sensible demo defaults to practise SRE thinking — they are not a
> claim about a real production system.

## 1. SLIs (what we measure)

For this pipeline we track four service-level indicators:

| SLI | Definition | PromQL / source |
|---|---|---|
| API availability | 1 − (5xx requests / total requests) over the window | `sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))` |
| API latency p95 | 95th percentile of read-request latency | `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))` |
| Pipeline freshness | Age of the latest curated 1-min window | `pipeline_freshness_seconds` (0 = current) |
| DLQ rate | Messages landing in the dead-letter topic | `sum(rate(kafka_topic_partition_current_offset{topic="clicks.dlq"}[5m]))` and depth `sum(kafka_topic_end_offset{topic="clicks.dlq"})` |

Notes:
- Consumer-group "lag" is intentionally **not** an SLI for Spark (checkpointed
  `assign()` has no broker consumer group) — freshness replaces it.
- Availability/latency SLIs come from the FastAPI Prometheus instrumentation
  added in Phase 1 (P1-09).

## 2. SLOs (demo targets)

Measured over a **30-day rolling window** (for a real demo, use a shorter
window such as 7 days and state it).

| SLI | SLO |
|---|---|
| API availability | ≥ 99.5% |
| API p95 latency | ≤ 300 ms for ≥ 95% of 5-minute windows |
| Freshness | latest curated window ≤ 3 minutes old for ≥ 99% of the time |
| DLQ | 0 messages/day for ≥ 99.5% of days (demo: no injected errors) |

## 3. Error budget

- Error budget = 100% − SLO.
- For 99.5% availability over 30 days: budget = 0.5% ≈ **3.6 hours/month**.
- Policy: **no risky change** (image bump, chart change, config change) when
  remaining budget for the window is below ~30% of the monthly budget, unless
  it is a rollback/incident fix.
- Record budget consumption in the P4-10 closeout with real measured numbers.

## 4. Burn-rate alerts (example)

Alert when errors are burning budget 14× faster than allowed for 1h (fast burn)
or 2× for 6h (slow burn):

```yaml
- alert: APIErrorBudgetFastBurn
  expr: |
    sum(rate(http_requests_total{status=~"5.."}[1h]))
      / sum(rate(http_requests_total[1h]))
    > 0.07   # ~14x the 0.5% error budget
  labels:
    severity: page
  annotations:
    summary: "API error budget burning fast"
```

See `monitoring/alerts.yml` for the existing Phase-1 alert rules
(p95/DLQ) that pair with this budget policy.

## 5. What we intentionally do not claim

- No production traffic/scale numbers.
- No multi-week SLO history yet — targets are practised defaults to be refined
  after load testing (P4-03) and real runs.
- Availability SLI covers the read API only (the pipeline itself is surfaced
  via freshness/DLQ SLIs).

## 6. How to review

1. Re-run the demo stack, then evaluate the four PromQL SLIs over a short
   window and record numbers in P4-10.
2. Practice an error-budget decision: e.g., budget nearly exhausted → defer a
   non-essential chart upgrade.
