"""Unit tests for Spark job JSON parsing (pure Python, no Spark required)."""

import json

import pytest

from spark_jobs.parsing import ParseError, parse_event, parse_message

VALID = {
    "event_id": "123e4567-e89b-42d3-a456-426614174000",
    "event_type": "page_view",
    "user_id": "u-0001",
    "session_id": "s-abcdef123456",
    "ts": "2026-08-27T15:00:00Z",
    "page": "/home",
    "device": "mobile",
    "region": "us-west",
    "campaign_id": "organic",
    "referrer": "google",
}


def test_parse_valid_event():
    assert parse_event(json.dumps(VALID)) == {key: str(value) for key, value in VALID.items()}


def test_parse_rejects_malformed_json():
    with pytest.raises(ParseError):
        parse_event("{not json")


def test_parse_rejects_non_object():
    with pytest.raises(ParseError):
        parse_event("[1, 2, 3]")


def test_parse_rejects_missing_field():
    data = {key: value for key, value in VALID.items() if key != "ts"}
    with pytest.raises(ParseError):
        parse_event(json.dumps(data))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "scroll"),
        ("device", "watch"),
        ("region", "mars"),
        ("campaign_id", "c-99"),
        ("referrer", "tiktok"),
        ("page", "/nope"),
        ("event_id", "not-a-uuid"),
        ("user_id", "alice"),
        ("session_id", "nope"),
        ("ts", "2026/08/27 15:00:00"),
    ],
)
def test_parse_rejects_invalid_value(field, value):
    data = dict(VALID)
    data[field] = value
    with pytest.raises(ParseError):
        parse_event(json.dumps(data))


def test_parse_accepts_product_page():
    data = dict(VALID, page="/product/42")
    assert parse_event(json.dumps(data))["page"] == "/product/42"


def test_parse_message_returns_event_on_success():
    result = parse_message(json.dumps(VALID))
    assert result["error"] is None
    assert result["event"] == {key: str(value) for key, value in VALID.items()}


def test_parse_message_returns_error_on_failure():
    result = parse_message("{bad json")
    assert result["event"] is None
    assert "invalid JSON" in result["error"]


def test_parse_message_rejects_empty():
    result = parse_message(None)
    assert result["event"] is None
    assert "empty" in result["error"]
