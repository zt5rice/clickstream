"""JSON parsing and validation for clickstream events.

Pure Python on purpose: this module has no Spark dependency, so the parsing
logic can be unit-tested without a JVM. ``parse_event`` is used by the
streaming job through a Spark UDF (and will drive DLQ routing in P1-07).
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from datetime import datetime

EVENT_TYPES = ("page_view", "click", "add_to_cart", "purchase")
PAGES = ("/home", "/search", "/product", "/cart", "/checkout", "/account", "/help")
DEVICES = ("mobile", "desktop", "tablet")
REGIONS = ("us-west", "us-east", "eu-west", "ap-southeast", "eu-central", "sa-east")
CAMPAIGN_IDS = tuple(f"c-{i}" for i in range(1, 9)) + ("organic",)
REFERRERS = ("google", "facebook", "email", "direct", "ads")

REQUIRED_FIELDS = (
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
)
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PRODUCT_PAGE_RE = re.compile(r"^/product/([1-9][0-9]*)$")


class ParseError(ValueError):
    """Raised when a raw event cannot be parsed or validated."""


def parse_event(raw: str) -> dict[str, str]:
    """Parse a raw JSON event string into a validated dict of strings."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, Mapping):
        raise ParseError("event must be a JSON object")
    missing = [field for field in REQUIRED_FIELDS if field not in data]
    if missing:
        raise ParseError(f"missing fields: {', '.join(missing)}")
    event = {field: str(data[field]) for field in REQUIRED_FIELDS}
    _validate(event)
    return event


def _validate(event: Mapping[str, str]) -> None:
    if event["event_type"] not in EVENT_TYPES:
        raise ParseError(f"invalid event_type: {event['event_type']!r}")
    if event["device"] not in DEVICES:
        raise ParseError(f"invalid device: {event['device']!r}")
    if event["region"] not in REGIONS:
        raise ParseError(f"invalid region: {event['region']!r}")
    if event["campaign_id"] not in CAMPAIGN_IDS:
        raise ParseError(f"invalid campaign_id: {event['campaign_id']!r}")
    if event["referrer"] not in REFERRERS:
        raise ParseError(f"invalid referrer: {event['referrer']!r}")
    if event["page"] not in PAGES and not PRODUCT_PAGE_RE.match(event["page"]):
        raise ParseError(f"invalid page: {event['page']!r}")
    try:
        uuid.UUID(event["event_id"])
    except ValueError as exc:
        raise ParseError(f"invalid event_id: {event['event_id']!r}") from exc
    if not event["user_id"].startswith("u-"):
        raise ParseError(f"invalid user_id: {event['user_id']!r}")
    if not event["session_id"].startswith("s-"):
        raise ParseError(f"invalid session_id: {event['session_id']!r}")
    try:
        datetime.strptime(event["ts"], TS_FORMAT)
    except ValueError as exc:
        raise ParseError(f"invalid ts: {event['ts']!r}") from exc
