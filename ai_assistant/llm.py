"""Pluggable LLM provider with an offline mock fallback (P2-08).

The default provider is ``mock`` so the service runs with no API key and stays
public-repo safe. ``openai-compatible`` talks to any OpenAI-compatible
``/chat/completions`` endpoint when a key is configured.
"""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

from .config import Settings
from .prompts import explain_alert_prompt, sql_prompt
from .sql import build_select_sql

log = logging.getLogger("ai_assistant")


class LLM(Protocol):
    def explain_alert(self, alert_name: str, value: str, message: str) -> str: ...

    def generate_sql(self, table: str, question: str) -> str: ...


class MockLLM:
    """Deterministic offline provider used by default and in tests."""

    def explain_alert(self, alert_name: str, value: str, message: str) -> str:
        return (
            f"[mock] Alert '{alert_name}' (value={value}).\n"
            f"- Check the underlying metric source first: {message}\n"
            "- Verify the Spark job is healthy and checkpoints are progressing.\n"
            "- Open Grafana and confirm the panel trend before acting."
        )

    def generate_sql(self, table: str, question: str) -> str:
        return build_select_sql(table, limit=100)


class OpenAICompatibleLLM:
    """Minimal OpenAI-compatible chat completions client."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _chat(self, system: str, user: str) -> str:
        url = self.settings.llm_base_url.rstrip("/") + "/chat/completions"
        headers = {"Authorization": f"Bearer {self.settings.llm_api_key}"}
        payload = {
            "model": self.settings.llm_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        with httpx.Client(timeout=20) as client:
            resp = client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    def explain_alert(self, alert_name: str, value: str, message: str) -> str:
        return self._chat(
            "You are a concise pipeline-ops assistant.",
            explain_alert_prompt(alert_name, value, message),
        )

    def generate_sql(self, table: str, question: str) -> str:
        return self._chat(
            "You generate read-only SQL only.",
            sql_prompt(question, table),
        )


def get_llm(settings: Settings) -> LLM:
    if settings.llm_provider == "openai-compatible" and settings.llm_api_key:
        return OpenAICompatibleLLM(settings)
    if settings.llm_provider == "openai-compatible":
        log.warning("llm_api_key missing; falling back to mock provider")
    return MockLLM()
