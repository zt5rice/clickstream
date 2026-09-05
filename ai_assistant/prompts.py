"""Prompt templates for the LLM assistant (P2-08)."""

from __future__ import annotations

from .sql import ALLOWED_TABLES


def explain_alert_prompt(alert_name: str, value: str, message: str) -> str:
    return (
        "You are a pipeline-ops assistant for a Kafka + Spark clickstream pipeline. "
        f"Explain the alert '{alert_name}' (value={value}) in 3 short bullets and "
        "suggest the first thing to check. Keep it concise and technical.\n"
        f"Alert context: {message}"
    )


def sql_prompt(question: str, table: str) -> str:
    allowed = ", ".join(sorted(ALLOWED_TABLES))
    return (
        "You generate read-only SQL for a clickstream analytics project. "
        f"Only these tables are allowed: {allowed}. "
        f"Return a single SELECT statement (no comments, no trailing semicolon).\n"
        f"Question: {question}\nPreferred table: {table}"
    )
