# Data path reference

## Component responsibilities

| Stage | Responsibility | Must not |
|---|---|---|
| Producer | Emit well-formed events with a stable key; batch and retry | Silently drop events |
| Broker | Durable ordered log per partition | Be the system of record |
| Stream processor | Parse, validate, window, aggregate; route bad records to DLQ | Block the main path on a bad record |
| Curated sink (relational) | Serve SQL/BI reads and the read API | Be written non-idempotently |
| OLAP sink | Hold raw events and fast aggregations | Require exact distinct counts at read time |
| Read API | Serve reads only | Write to the curated store |
| Orchestrator | Freshness checks, rollups, backfills | Become a hidden dependency of the hot path |

## Delivery semantics

Be precise — this is the claim people most often overstate.

| Claim | Means |
|---|---|
| At-most-once | May lose messages. Acceptable only for metrics that tolerate loss. |
| At-least-once | May duplicate. **The realistic default.** |
| Exactly-once | Requires transactional coordination end to end. |

**Standard position:** at-least-once delivery + idempotent sinks. That combination gives
the *effect* of exactly-once writes without claiming transactional guarantees the system
does not provide.

### Making a sink idempotent

| Store | Pattern |
|---|---|
| PostgreSQL | `INSERT ... ON CONFLICT (key) DO UPDATE` |
| ClickHouse | `ReplacingMergeTree` + `FINAL` on read (or `OPTIMIZE` on a schedule) |
| Object storage | Write to a deterministic path derived from the key; overwrite is safe |
| Search index | Document `_id` derived from the event key |

## Windowing

- Always set a **watermark** and state the lateness tolerance. The tolerance is a
  correctness assumption, not a tuning knob.
- Choose the window from the consumer's question, not from the data's shape. If the
  consumer asks "what happened today", a 1-minute window plus a rollup beats a 24-hour
  window.
- Late data: decide explicitly whether it is dropped, allowed to update a closed window,
  or routed to a side path.

## Two sinks, one write

Curated (relational) and OLAP stores answer different questions — transactional reads and
API serving versus raw retention and fast aggregation. Keeping both is a deliberate
choice; the cost is a second write path to keep consistent.

**Keep them aligned by writing both from the same micro-batch.** If they diverge, you now
have two systems with different answers and no way to tell which is right.

## Aggregate correctness traps

- **Never sum per-window distinct counts.** Daily uniques ≠ sum of hourly uniques. Expose
  event counts and window counts, or use an HLL with a stated error bound.
- **Late-arriving events can reopen an aggregate.** Either accept updates (idempotent
  upsert) or declare the window final and drop late data — but choose on purpose.
- **Replays must be safe.** A backfill is a replay; if the sink is not idempotent, a
  backfill silently doubles your numbers.

