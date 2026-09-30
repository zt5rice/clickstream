# Paradigms and seams

## The four paradigms

| Paradigm | Typical engine | Latency | Wins when | Loses when |
|---|---|---|---|---|
| **Streaming** | Spark Structured Streaming, Flink | seconds | Continuous event processing; the consumer needs sub-minute freshness | State must be replayed; logic needs the whole history |
| **Batch / lakehouse** | Spark + Delta `MERGE` | minutes–hours | Replay and backfill matter; large idempotent upserts | Something needs the answer before the batch lands |
| **Orchestrated batch** | Airflow, Dagster, Step Functions | hours | Dependency scheduling, freshness gates, rollups, cross-system coordination | You need low latency, or the DAG becomes a hidden dependency of the hot path |
| **Declarative transform** | dbt, SQL models | on schedule | The last mile: business models, where tests are cheap and reviewers do not need to read Scala | Heavy compute, or stateful per-event logic |

### Choosing

1. **Start from the consumer's latency requirement.** "How stale can this be before
   someone is unhappy?" That single answer eliminates most of the table.
2. **Then ask about replay.** If you will need to backfill or recompute, a merge-based
   batch path is far easier to make correct than streaming state.
3. **Then ask who reviews it.** The last mile should be readable by the people who own the
   business definition. That usually means declarative SQL, not application code.

## Seams

> **A seam is where one engine's output becomes another engine's input, and the
> orchestration context changes.**

The first half of that definition is ordinary layering and is not especially dangerous.
The second half is what creates the trouble.

| Kind of seam | Example | Risk |
|---|---|---|
| **Within one orchestration context** | Streaming writes a curated table; the same orchestrator's transform layer and scheduled rollups read it | Manageable. One scheduler, one lineage graph, one replay story. |
| **Across orchestration contexts** | A job in scheduler A hands off to a topic or table consumed by scheduler B, where the metadata format also differs | Dangerous. Lineage truncates, replay semantics differ, and ownership of the handoff is usually unstated. |

### The three questions to answer at every seam

1. **Is the write idempotent?** If not, a retry on one side duplicates data on the other.
2. **Can you replay across it?** Replaying the producer must not double-count for the
   consumer. If it would, the seam needs a dedupe key or a versioned write.
3. **Who gets paged?** A seam with no owner is an outage waiting for a quiet weekend.

### Why you stop at a cross-context seam

When the schedulers on either side keep dependencies in incompatible shapes, stitching
them into one graph without a canonical model produces a graph that *looks* authoritative
and is not. The disciplined move is to terminate the traversal at the boundary and
declare the limitation, rather than emitting a confident wrong edge.

If you do need to bridge them, normalize every source into one canonical triple —
`(upstream dataset, process, downstream dataset)` — keep a `source` field on every edge so
disagreements stay visible, and apply precedence rules (observed beats declared, newer
beats older) instead of silently deduplicating.

