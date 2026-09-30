---
name: data-lineage-tracer
description: Traces downstream data lineage for a warehouse table and renders it as a Mermaid dependency graph. Use when asked "what depends on this table", "who reads this dataset", "what breaks if I change this column", or when estimating the blast radius of a schema change before a migration. Prefer this over guessing from naming conventions or asking the data team, and use it before any destructive change to a widely-read table.
---

# Data Lineage Tracer

## Overview

Given a fully-qualified dataset name, produce a downstream dependency graph.

Most lineage systems read **declared** dependencies (configs, orchestrator metadata).
This skill instead derives **observed** lineage: it finds the jobs that actually touched
a dataset's storage location in a recent window, then reads each job's own run log to
extract the tables that job read and wrote. Declaration drifts; runtime evidence does
not.

## When to use

- Before changing or dropping a column, to size the blast radius.
- When asked "who consumes this dataset?"
- When investigating why a downstream table stopped updating.
- When onboarding onto an unfamiliar part of the warehouse.

## Inputs

| Input | Required | Notes |
|---|---|---|
| `dataset` | yes | Fully-qualified name, e.g. `schema.table` |
| `depth` | no | Traversal depth limit. **Default 5.** |

## Workflow

1. **Resolve the dataset to its storage location.**
   Tables in a zone share a common path prefix, so the fully-qualified name maps
   deterministically to the location holding its run artifacts. No catalog lookup needed.

2. **Find candidate jobs.**
   Query the cloud audit log for read/write events on those paths, bounded to a
   **7-day lookback**. Seven days is the minimum window that guarantees at least one run
   for a weekly pipeline, while keeping event volume tractable.

3. **Extract edges from each job's own log.**
   Read the job's run log and pull the **analyzed SQL**. The analyzed plan contains
   fully-qualified table names — which makes extraction dramatically more reliable than
   regex over raw query text. Tables in read position are upstream; tables in write
   position are downstream.

4. **Traverse.**
   Breadth-first, with a visited set. Stop at `depth` (default 5).

5. **Render.**
   Emit a Mermaid graph. Encode job type as **node shape** so batch / streaming /
   incremental jobs are distinguishable at a glance without relying on colour.

## Rules

### 1. A self-loop on an incremental table is not a defect

Incremental datasets read their own prior state and write back to themselves
(`INSERT INTO t ... SELECT ... FROM t`, `MERGE INTO t USING t`). That self-edge is by
design. Only a **cross-job** cycle (A → B → C → A) indicates a genuine scheduling bug.
Distinguishing the two requires domain knowledge, not graph theory — always state which
one you are looking at.

### 2. Stay in the data plane

Traverse in one direction and **stop at interface boundaries** where the scheduling
system changes (for example an ETL job handing off to a streaming topic). Control-plane
dependencies live in several schedulers with inconsistent schemas. Merging them without a
canonical model produces a graph that looks authoritative but isn't — a correct partial
graph beats a confident wrong one.

### 3. The visited set must record the *shallowest* depth

A plain visited set prunes a node the second time it is reached. If a node is first
reached at depth 5 and later reachable at depth 2, marking it visited globally means its
descendants are never expanded — and they may fall inside the depth limit. Key the
visited set by the shallowest depth seen, and re-expand when a shorter path is found.

### 4. Emit pairwise edges, and say so

A job with 5 inputs and 3 outputs becomes 15 pairwise edges. That loses *which* input
fed *which* output. State this explicitly rather than letting the reader assume
attribution the data doesn't support.

### 5. Never silently deduplicate across sources

When two sources describe the same edge, keep both. Carry a `source` field on every edge
and surface disagreements instead of picking a winner. Precedence for display:
observed > declared, newer > older.

## Known limitations

State these before being asked:

- **Dynamic SQL is invisible.** Table names assembled at runtime never appear as
  literals in the analyzed plan.
- **Temp views / CTEs** must be resolved back to base tables, or the graph stops at an
  intermediate node.
- **Datasets slower than the lookback window don't appear.** A 7-day window covers
  weekly and faster; a monthly job will be missing.
- **Log delivery latency** means very recent runs may not be reflected yet.
- **Control-plane dependencies are out of scope** (see Rule 2).

## References

- `references/graph-traversal.md` — BFS, depth limits, cycle handling.
- `references/extraction.md` — reading analyzed SQL, temp-view resolution, edge cases.

