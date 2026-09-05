"""Read-only SQL generation with a strict table allowlist (P2-08)."""

from __future__ import annotations

import re

ALLOWED_TABLES = {
    "curated.page_views_1m",
    "curated.campaign_stats_1m",
    "curated.daily_page_summary",
    "curated.daily_campaign_summary",
    "olap.clicks",
    "olap.page_views_1m",
}

FORBIDDEN_KEYWORDS = (
    "insert ",
    "update ",
    "delete ",
    "drop ",
    "alter ",
    "create ",
    "truncate ",
    "grant ",
    "copy ",
    "vacuum ",
)


def build_select_sql(table: str, columns: list[str] | None = None, limit: int = 100) -> str:
    """Build a safe SELECT against an allowlisted table."""
    if table not in ALLOWED_TABLES:
        raise ValueError(f"table {table!r} is not allowlisted")
    cols = ", ".join(columns) if columns else "*"
    return f"SELECT {cols} FROM {table} LIMIT {limit}"


def sanitize_sql(sql: str) -> str:
    """Validate that a generated SQL string is single-statement read-only."""
    stripped = sql.strip().rstrip(";").strip()
    lowered = " ".join(stripped.lower().split())
    if not re.match(r"^select\b", lowered):
        raise ValueError("only SELECT statements are allowed")
    if ";" in stripped:
        raise ValueError("multi-statement SQL is not allowed")
    if any(keyword in lowered for keyword in FORBIDDEN_KEYWORDS):
        raise ValueError("statement contains a forbidden keyword")
    if "--" in lowered or "/*" in lowered:
        raise ValueError("comments are not allowed")
    for table in re.findall(r"\bfrom\s+([a-z0-9_.]+)", lowered):
        if table not in ALLOWED_TABLES:
            raise ValueError(f"table {table!r} is not allowlisted")
    return stripped
