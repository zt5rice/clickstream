# Lakehouse: Delta Lake (local) — decision & semantics

Status: implemented in Phase 2 (P2-03, ZHA-142) as a local-only demo.

## What we built

`spark_jobs/delta_lakehouse.py` runs on the local Spark cluster and:

1. loads a deterministic sample fixture (`sample_data/clicks.sample.json`,
   60 events, seed 42),
2. writes the events to a Delta table (path from `DELTA_BASE_PATH`,
   default `/tmp/clickstream-delta/clicks`),
3. builds a daily page summary and upserts it into a second Delta table using
   **Delta `MERGE`**, run twice to prove idempotency,
4. reads both tables back and prints row counts.

No cloud account is required: everything runs on local disk inside the Spark
container (`make delta-demo`).

## Delta vs Iceberg — why Delta for this demo

- **Delta** ships as `delta-spark` and integrates with Spark SQL through
  `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
  `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
  No separate catalog/metastore service is needed for a laptop demo.
- It provides ACID transactions, schema enforcement/evolution, time travel and
  `MERGE`/`UPDATE`/`DELETE` — enough to demonstrate the lakehouse story.
- **Iceberg** is a strong, widely-used alternative (table format + catalog
  abstraction, good multi-engine story). We intentionally defer it: adding a
  second format to a small portfolio project would show breadth at the cost of
  depth. Iceberg remains a documented future option.

## Merge semantics in this demo

- The clicks table is effectively **append-only**: each event has a unique
  `event_id`. In a streaming version, Spark checkpoints guard against duplicate
  replays, so we do not dedupe clicks on write.
- The `daily_page_summary` table uses **`MERGE` on `(day, page)`**:
  `WHEN MATCHED THEN UPDATE` refreshes `views`; `WHEN NOT MATCHED THEN INSERT`
  adds new day/page rows. Running the merge twice yields identical row counts,
  which the job asserts and reports.

## How to run

```bash
make delta-demo
```

Expected output includes:

```text
DELTA_DEMO_RESULT {"sample_events":60, "clicks_rows_in_delta":60,
                   "summary_rows":N, ..., "merge_idempotent":true}
```
