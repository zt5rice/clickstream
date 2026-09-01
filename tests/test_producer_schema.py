"""Unit tests for the ClickEvent schema."""

import pytest

from producer.schema import ClickEvent


def _event(**overrides) -> ClickEvent:
    base = {
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
    base.update(overrides)
    return ClickEvent(**base)


def test_to_dict_roundtrip():
    event = _event()
    assert ClickEvent.from_dict(event.to_dict()) == event


def test_to_json_contains_all_fields():
    payload = _event().to_json()
    for field in (
        "event_id",
        "event_type",
        "user_id",
        "session_id",
        "ts",
        "page",
        "device",
        "region",
        "campaign_id",
        "referrer",
    ):
        assert f'"{field}"' in payload


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "purchase"),
        ("device", "tablet"),
        ("region", "sa-east"),
        ("campaign_id", "c-8"),
        ("referrer", "email"),
    ],
)
def test_valid_enum_values_accepted(field, value):
    assert getattr(_event(**{field: value}), field) == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "scroll"),
        ("device", "watch"),
        ("region", "mars"),
        ("campaign_id", "c-99"),
        ("campaign_id", "unknown"),
        ("referrer", "tiktok"),
    ],
)
def test_invalid_enum_values_rejected(field, value):
    with pytest.raises(ValueError):
        _event(**{field: value})


def test_product_page_pattern_accepted():
    assert _event(page="/product/42").page == "/product/42"


@pytest.mark.parametrize("page", ["/product/0", "/product/abc", "/nope", "home"])
def test_invalid_page_rejected(page):
    with pytest.raises(ValueError):
        _event(page=page)


def test_invalid_event_id_rejected():
    with pytest.raises(ValueError):
        _event(event_id="not-a-uuid")


def test_invalid_ts_rejected():
    with pytest.raises(ValueError):
        _event(ts="2026/08/27 15:00:00")
