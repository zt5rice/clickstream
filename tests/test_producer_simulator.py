"""Unit tests for the deterministic clickstream simulator."""

import pytest

from producer.schema import (
    CAMPAIGN_IDS,
    DEVICES,
    EVENT_TYPES,
    PAGES,
    PRODUCT_PAGE_RE,
    REFERRERS,
    REGIONS,
)
from producer.simulator import (
    SESSION_MAX_EVENTS,
    SESSION_MIN_EVENTS,
    ClickstreamSimulator,
)


def test_fixed_seed_is_reproducible():
    a = ClickstreamSimulator(seed=42).generate(100)
    b = ClickstreamSimulator(seed=42).generate(100)
    assert [e.to_dict() for e in a] == [e.to_dict() for e in b]


def test_different_seeds_differ():
    a = ClickstreamSimulator(seed=1).generate(100)
    b = ClickstreamSimulator(seed=2).generate(100)
    assert [e.to_dict() for e in a] != [e.to_dict() for e in b]


def test_generate_count_and_empty():
    assert len(ClickstreamSimulator(seed=7).generate(500)) == 500
    assert ClickstreamSimulator(seed=7).generate(0) == []


def test_negative_count_rejected():
    with pytest.raises(ValueError):
        ClickstreamSimulator(seed=7).generate(-1)


def test_all_events_are_valid_schema():
    events = ClickstreamSimulator(seed=42).generate(500)
    for event in events:
        assert event.event_type in EVENT_TYPES
        assert event.device in DEVICES
        assert event.region in REGIONS
        assert event.campaign_id in CAMPAIGN_IDS
        assert event.referrer in REFERRERS
        assert event.page in PAGES or PRODUCT_PAGE_RE.match(event.page)


def test_timestamps_monotonically_increasing():
    events = ClickstreamSimulator(seed=42).generate(200)
    assert all(e2.ts >= e1.ts for e1, e2 in zip(events, events[1:], strict=False))


def test_session_grouping():
    events = ClickstreamSimulator(seed=42).generate(1000)
    sessions = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)
    # The final session may be truncated by generate(); drop it before asserting.
    sessions = dict(list(sessions.items())[:-1])
    for members in sessions.values():
        assert SESSION_MIN_EVENTS <= len(members) <= SESSION_MAX_EVENTS
        assert len({m.user_id for m in members}) == 1
        assert len({m.device for m in members}) == 1
        assert len({m.region for m in members}) == 1
        assert len({m.campaign_id for m in members}) == 1


def test_event_type_constraints():
    events = ClickstreamSimulator(seed=42).generate(2000)
    for event in events:
        if event.event_type == "purchase":
            assert event.page == "/checkout"
        if event.event_type == "add_to_cart":
            assert event.page in ("/cart", "/checkout") or PRODUCT_PAGE_RE.match(event.page)


def test_device_distribution_within_tolerance():
    events = ClickstreamSimulator(seed=42).generate(3000)
    mobile_ratio = sum(1 for e in events if e.device == "mobile") / len(events)
    assert 0.45 <= mobile_ratio <= 0.65


def test_region_distribution_within_tolerance():
    events = ClickstreamSimulator(seed=42).generate(3000)
    us_west_ratio = sum(1 for e in events if e.region == "us-west") / len(events)
    assert 0.30 <= us_west_ratio <= 0.50


def test_campaign_distribution_within_tolerance():
    events = ClickstreamSimulator(seed=42).generate(3000)
    organic_ratio = sum(1 for e in events if e.campaign_id == "organic") / len(events)
    assert 0.30 <= organic_ratio <= 0.50
