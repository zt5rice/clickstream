# Reliability reference

## SLIs

Track four. Anything more is noise at this scale.

| SLI | Definition | Source |
|---|---|---|
| API availability | 1 − (5xx / total) over the window | `sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))` |
| API latency p95 | 95th percentile of read latency | `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))` |
| **Pipeline freshness** | Age of the newest complete curated window | `pipeline_freshness_seconds` (0 = current) |
| DLQ rate + depth | Rate of messages landing in the DLQ, and current depth | `rate(kafka_topic_partition_current_offset{topic="<dlq>"}[5m])`, `kafka_topic_end_offset{topic="<dlq>"}` |

### Why freshness replaces consumer-group lag

If the stream processor manages offsets through checkpoints (rather than joining a broker
consumer group), `kafka_consumergroup_lag` does not exist for it. Substituting broker lag
would mean monitoring a series that is always empty — which reads as "healthy". Freshness
measures the thing users actually experience: how stale is the newest data.

## SLOs and error budget

State the window explicitly. A 30-day window is conventional; for a demo, use 7 days and
say so.

| SLI | Target |
|---|---|
| Availability | ≥ 99.5% |
| Latency p95 | ≤ 300 ms for ≥ 95% of 5-minute windows |
| Freshness | newest curated window ≤ 3 minutes old ≥ 99% of the time |
| DLQ | 0 messages/day ≥ 99.5% of days |

**Error budget** = 100% − SLO. At 99.5% over 30 days that is 0.5% ≈ **3.6 hours/month**.

**Budget policy.** No risky change while remaining budget is below ~30% of the window
budget, unless the change is a rollback or an incident fix. The point is to make
"we're burning budget, stop shipping features" a rule rather than a judgement call
made under pressure.

## Burn-rate alerts

Multi-window burn-rate alerting catches both fast and slow degradation without paging on
every blip.

| Alert | Condition | Meaning |
|---|---|---|
| Fast burn | 14× budget consumption over 1 h | Something is badly broken now |
| Slow burn | 2× over 6 h | Slow leak; investigate this week |

## Runbooks

Every alert links to a runbook that answers, in order:

1. What the user-visible symptom is.
2. How to confirm the alert is real (the exact query or dashboard).
3. Mitigation steps, cheapest first.
4. How to verify recovery.
5. What to capture for the post-incident review.

An alert without a runbook is a notification, not an operational capability.

## Blameless post-incident review

Template:

| Section | Content |
|---|---|
| Impact | Who was affected, for how long, measured |
| Timeline | Timestamps in UTC, from first signal to resolution |
| Detection | How we found out — and how long that took |
| Root cause | Mechanism, not blame |
| Contributing factors | What made it possible or worse |
| What went well | Keep the things that worked |
| Action items | Owners and dates; each is a ticket, not a promise |

## Chaos drills

Verify recovery empirically rather than assuming it. Measure MTTR with a **Pod UID
comparison**: "pod named X is Ready" can report a false 0 s when a StatefulSet recreates
the pod instantly, so the drill waits for a *different* UID to become Ready.

Record each drill as: fault injected → detection time → recovery time → unexpected
behaviour.

