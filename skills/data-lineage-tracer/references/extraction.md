# Edge extraction

## Read the analyzed SQL, not the raw text

The job run log contains the *analyzed* SQL plan, in which table names are already
resolved to fully-qualified form. This is the single most important extraction decision.

| Approach | Reliability |
|---|---|
| Regex over raw query text | Low — aliases, quoting, case, multi-line joins |
| Parse raw SQL yourself | Medium — you re-implement a dialect parser |
| **Read the analyzed plan from the log** | **High — the engine already did the work** |

## Which tables are upstream vs downstream

| Position | Meaning |
|---|---|
| Read position (`FROM`, `JOIN`, source of `MERGE`) | **Upstream** |
| Write position (`INSERT INTO`, `CREATE TABLE AS`, target of `MERGE`, overwrite target) | **Downstream** |

One job therefore yields a set of edges, not a single edge.

## Edge cases

### Dynamic SQL

Table names assembled at runtime —
`spark.sql(f"INSERT INTO {target} SELECT * FROM {source}")` — never appear as literals in
the analyzed plan.

**Mitigation:** none that is fully reliable from logs alone. This is a genuine blind spot;
declare it rather than hiding it. In environments where dynamic SQL is common, supplement
with catalog-level or orchestrator-level metadata and mark those edges with a lower
confidence.

### Temp views and CTEs

An intermediate node (`temp_view_x`) is not a real dataset. Resolve it back to base tables
before emitting edges, or the graph terminates at a node nobody can act on.

### Many-to-many jobs

5 inputs → 3 outputs produces 15 pairwise edges. This loses input→output attribution.
Emit pairwise edges (they are what the graph can prove) and state the limitation
explicitly.

### The same table read and written

This is the incremental self-dependency, and it is **provable from the log** rather than
inferred — the strongest form this evidence can take. See
`graph-traversal.md` for why it must not be reported as a cycle defect.

### Multiple jobs writing the same dataset

Normal in a layered warehouse (backfill job + daily job + repair job). Keep all producers
rather than picking one; the consumer cares about the union.

## Emitting the result

- Carry `source` and `observed_at` on every edge so provenance survives merging.
- Never silently deduplicate: if two sources disagree, surface both.
- Precedence for display: observed > declared, newer > older.

