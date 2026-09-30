---
name: streaming-pipeline-standard
description: Build, change, or review a data pipeline against a fixed set of production defaults - Kafka ingestion, streaming aggregation, idempotent sinks, freshness SLOs, quality gates, and teardown discipline. Use when adding a component to a pipeline, choosing between streaming / micro-batch / batch / declarative-transform paradigms, defining or changing reliability settings, deciding whether a change is safe to ship, or reviewing someone else's pipeline design. Use before shipping any change that touches the data path or a published SLO.
---

# Data Pipeline Standard

## Overview

Opinionated defaults for data pipelines, so the same questions get the same answers
every time instead of being re-litigated per project. Each rule is written down because
the naive alternative failed once in a way that was hard to see.

These are **demo-grade defaults**: defensible for a portfolio system or a small internal
service, and a reasonable position to argue from. They are not a claim about any
employer's production system.

## When to use

- Adding or changing a stage in a pipeline.
- Choosing a processing paradigm or a sink strategy.
- Defining SLIs/SLOs, error budgets, or alert thresholds.
- Deciding whether a change is safe to ship right now.
- Reviewing a pipeline design or an incident follow-up.

## The rules

### Paradigms - pick deliberately

1. **Choose the paradigm from the consumer's latency requirement, not from the data's
   shape.** Streaming (seconds) for continuous event processing; batch or micro-batch with
   an idempotent merge (minutes to hours) when replay and backfill matter more than
   latency; an orchestrator (hours) for dependency scheduling, freshness gates and
   rollups; a declarative SQL transform layer for the last mile.
2. **Put the last mile in a declarative transform layer.** That is where tests are
   cheapest and where a model change is reviewable by someone who cannot read Spark.
3. **Name every seam, and record who owns the handoff.** A **seam** is where one engine's
   output becomes another engine's input *and the orchestration context changes*. At each
   seam answer three questions: is the write idempotent, can you replay across it, and who
   gets paged when it breaks?
4. **Never bridge heterogeneous control planes without a canonical model.** If the
   schedulers on either side of a seam keep metadata in incompatible shapes, a correct
   partial graph beats a confident wrong one - stop at the boundary and say so.

### Data path

5. **Producer:** idempotent, `acks=all`, retries with backoff. A producer that can
   duplicate is fine; a producer that can silently drop is not.
6. **Every stream has a dead-letter path.** Malformed records go to the DLQ and are
   counted; they never stall the main flow. DLQ depth is a monitored SLI.
7. **Sinks are idempotent, and you say so precisely.** Relational: `ON CONFLICT` upsert.
   OLAP: `ReplacingMergeTree` (with `FINAL` on read). Claim **"idempotent /
   at-least-once"** - never "exactly-once end-to-end".
8. **Windows use watermarks, and the lateness tolerance is stated.** An unstated
   watermark is an unstated correctness assumption.
9. **Two sinks by design:** a curated relational store (SQL/BI + API serving) and an OLAP
   store (raw events + fast aggregation). Write both **from the same micro-batch** so they
   stay aligned.

### Reliability

10. **The pipeline SLI is freshness - not broker consumer lag.** If the consumer manages
    offsets via checkpoints there is no broker consumer group, so group lag is
    meaningless and will read as permanently healthy.
11. **Caches and rate limiters fail open; freshness checks fail closed.** Losing a cache
    must not take the API down. Stale data must raise, not pass silently.
12. **SLO defaults:** availability >= 99.5%, p95 latency <= 300 ms, freshness <= 3 min,
    DLQ 0/day. Error budget = 0.5% (about 3.6 h/month at 99.5%).
13. **Burn-rate alerts:** 14x for 1 h (fast burn), 2x for 6 h (slow burn).
14. **Change policy:** no risky change (image bump, chart change, config change) while
    remaining error budget is below ~30% - unless it is a rollback or an incident fix.
15. **Measure MTTR by Pod UID, not pod name.** A StatefulSet can recreate a pod
    instantly and report a false 0 s. Wait for a *different* UID to become Ready.

### Quality gates

16. **Every dataset has independent tests.** Model-level tests plus an independent quality
    checker, merged into one report. A single test framework is a single point of blind
    spots.
17. **Never sum per-window distinct counts.** Daily uniques are not the sum of hourly
    uniques. Expose event counts plus window counts, or use an HLL with a stated error
    bound.
18. **The gate is one command:** lint + format check + tests. CI runs lint, unit,
    integration, build, then optionally deploy.

### Deployment, cost, and evidence

19. **Path is local compose, then kind/Helm, then a real cluster.** Compose for the fast
    loop, kind to validate manifests, the real cluster only to prove it runs there.
20. **Teardown is a first-class action, not an afterthought.** Never leave a managed
    cluster running overnight; preview the destroy before destroying it.
21. **Every published number must come from your own run and be reproducible.** No preset
    figures, no estimates presented as measurements.
22. **Synthetic data only.** No employer data, no production identifiers, no third-party IP.

## Known limitations

- Single-cluster, single-region. Multi-region failover is out of scope.
- These defaults assume at-least-once delivery plus idempotent writes. If a stage genuinely
  cannot be made idempotent, the framework needs extending before the rule applies.
- The SLO numbers are demo-grade; a real system sets them from user-visible expectations.

## References

- `references/paradigms-and-seams.md` - the four paradigms, when each wins, and how seams fail.
- `references/data-path.md` - component responsibilities, delivery semantics, sink patterns.
- `references/reliability.md` - SLI/SLO definitions, burn-rate math, runbooks, blameless PIR.
- `references/quality-gates.md` - test layering, declarative model tests, CI shape.
- `references/deployment-and-cost.md` - compose/kind/cluster path and teardown discipline.

