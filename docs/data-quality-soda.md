# Data quality: Soda Core + native ClickHouse checks (P2-04)

Status: implemented in Phase 2 (P2-04, ZHA-143).

## What we built

`quality/` contains:

- `configuration.yml` — Soda data source for the curated Postgres (host port
  5433, demo-only `click`/`click` credentials).
- `checks/checks.yml` — Soda checks: freshness, row volume, missing columns,
  accepted values (`device`, `campaign_id`).
- `run_quality.py` — orchestrator that:
  1. runs Soda Core against Postgres,
  2. runs equivalent native checks against ClickHouse via `clickhouse-connect`
     (no official Soda ClickHouse adapter exists on PyPI),
  3. writes `quality/target/quality-results.json` and
     `quality/target/quality-report.html`,
  4. exits non-zero when any check fails (CI-friendly).

## Why Soda Core (not Great Expectations)

- Lighter to run and explain: YAML checks + SQL against the actual warehouse,
  no notebook/expectation-store scaffolding.
- Outputs machine-readable JSON that we turn into a small HTML report.

## How to run

```bash
make quality-run
open quality/target/quality-report.html
```

Freshness thresholds are intentionally generous (7 days) because the demo
pipeline is not kept running 24/7; in production the same checks would use
minutes.
