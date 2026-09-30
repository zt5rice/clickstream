# skills/ — Agent Skills for this pipeline

## What this is

Each subdirectory is an **Agent Skill**: a `SKILL.md` with YAML frontmatter (`name`,
`description`) plus optional `references/` files. The `description` is written as
**routing conditions**, so an agent can decide whether the skill applies *before* loading
the full text.

The purpose is to turn the standards this repo was built to — currently spread across
`DESIGN.md`, `docs/slo-sli-error-budget.md`, `docs/design-decisions.md`, and
`docs/cost-control.md` — into something an agent applies without being asked. Prose
documents have to be read and interpreted; a skill is picked up and followed.

## The skills

| Skill | Use when |
|---|---|
| [`streaming-pipeline-standard`](./streaming-pipeline-standard/SKILL.md) | Building, changing, or reviewing a pipeline; choosing between streaming / batch / orchestrated / declarative paradigms; defining reliability settings or SLOs; deciding whether a change is safe to ship |
| [`data-lineage-tracer`](./data-lineage-tracer/SKILL.md) | Answering "what depends on this table", sizing the blast radius of a schema change, or investigating why a downstream table stopped updating |

## How to use them

- **With an agent that supports skills** (Claude Code, Codex, and others that implement
  the `SKILL.md` progressive-disclosure convention): point it at this directory. Only the
  metadata is loaded up front; full instructions load when a task matches.
- **By hand:** read `SKILL.md` for the binding rules, then the `references/` file that
  matches your task. `SKILL.md` is deliberately short.

## Scope note

`streaming-pipeline-standard` describes the four paradigms **this repo actually uses** —
Spark Structured Streaming, Spark/Delta batch, Airflow-orchestrated batch, and dbt
transforms — and they all run **within a single orchestration context**.

It does **not** describe heterogeneous control planes, where different schedulers keep
lineage metadata in incompatible shapes. That seam class is exactly what motivates
`data-lineage-tracer`, and the two skills are complementary: one is about holding a
pipeline to a standard, the other about knowing what a change will break.

## Provenance

Both skills are original work.

- `streaming-pipeline-standard` is derived from this repository's own design documents
  and measured runs.
- `data-lineage-tracer` generalizes a lineage-traversal pattern the author built and
  operated; it contains no employer-identifying information, no proprietary schema, and
  no third-party code.

