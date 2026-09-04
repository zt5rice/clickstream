"""Run data-quality checks and write JSON + HTML reports (P2-04).

- PostgreSQL checks run through Soda Core (``quality/checks/checks.yml``).
- ClickHouse has no Soda adapter on PyPI, so equivalent checks run natively via
  ``clickhouse-connect``. Both results are merged into one report.

Exit code is non-zero when any check fails.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import clickhouse_connect
from soda.scan import Scan

ROOT = Path(__file__).resolve().parent
TARGET = ROOT / "target"
CONFIG = ROOT / "configuration.yml"
CHECKS = ROOT / "checks" / "checks.yml"


def run_soda_postgres() -> list[dict]:
    """Run Soda scans against Postgres and return check rows."""
    scan = Scan()
    scan.add_configuration_yaml_str(CONFIG.read_text())
    scan.set_data_source_name("postgres_curated")
    scan.add_sodacl_yaml_file(str(CHECKS))
    scan.execute()

    rows: list[dict] = []
    results = scan.get_scan_results() or {}
    for entry in results.get("checks", []):
        diagnostics = entry.get("diagnostics") or {}
        rows.append(
            {
                "source": "postgres_curated",
                "table": entry.get("table"),
                "check": entry.get("name") or entry.get("definition"),
                "status": entry.get("outcome") or "error",
                "value": diagnostics.get("value"),
            }
        )
    if not rows:
        raise RuntimeError(f"soda produced no checks: {json.dumps(results)[:800]}")
    return rows


def run_clickhouse_checks() -> list[dict]:
    """Run native row-volume/freshness checks against ClickHouse OLAP tables."""
    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="click", password="click", database="olap"
    )
    rows: list[dict] = []
    checks = [
        ("olap.clicks", "select count() from olap.clicks FINAL", "row_count", ">", 0),
        ("olap.page_views_1m", "select count() from olap.page_views_1m FINAL", "row_count", ">", 0),
        (
            "olap.clicks",
            "select max(ts) from olap.clicks FINAL",
            "max_ts_not_null",
            "is_not_null",
            None,
        ),
    ]
    for table, sql, name, op, expected in checks:
        value = client.query(sql).result_rows[0][0]
        if op == "is_not_null":
            ok = value is not None
        elif op == ">":
            ok = value > expected
        else:
            ok = False
        rows.append(
            {
                "source": "clickhouse",
                "table": table,
                "check": name,
                "status": "pass" if ok else "fail",
                "value": value,
            }
        )
    return rows


def write_report(rows: list[dict]) -> int:
    """Write JSON + HTML reports; return the number of failed checks."""
    TARGET.mkdir(parents=True, exist_ok=True)
    failed = [r for r in rows if r.get("status") != "pass"]
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": rows,
        "total": len(rows),
        "failed": len(failed),
    }
    (TARGET / "quality-results.json").write_text(json.dumps(report, indent=2, default=str))

    badge = "PASS" if not failed else f"FAIL ({len(failed)})"
    trs = "\n".join(
        "<tr>"
        f"<td>{r['source']}</td><td>{r['table']}</td><td>{r['check']}</td>"
        f"<td>{r['status']}</td><td>{r['value']}</td></tr>"
        for r in rows
    )
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Clickstream data quality report</title>
<style>body{{font-family:system-ui,sans-serif;margin:2rem}}table{{border-collapse:collapse}}
td,th{{border:1px solid #ccc;padding:6px 10px}} .pass{{color:green}} .fail{{color:red}}</style>
</head><body><h1>Clickstream — data quality report</h1>
<p>Generated {report["generated_at"]} · {len(rows)} checks · <b>{badge}</b></p>
<table><tr><th>source</th><th>table</th><th>check</th><th>status</th><th>value</th></tr>
{trs}</table></body></html>"""
    (TARGET / "quality-report.html").write_text(html)
    return len(failed)


def main() -> int:
    rows = run_soda_postgres()
    rows.extend(run_clickhouse_checks())
    failed = write_report(rows)
    print(json.dumps({"total": len(rows), "failed": failed}, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
