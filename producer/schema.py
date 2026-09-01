"""ClickEvent schema: field definitions, validation, and (de)serialization."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

EVENT_TYPES = ("page_view", "click", "add_to_cart", "purchase")
PAGES = ("/home", "/search", "/product", "/cart", "/checkout", "/account", "/help")
DEVICES = ("mobile", "desktop", "tablet")
REGIONS = ("us-west", "us-east", "eu-west", "ap-southeast", "eu-central", "sa-east")
CAMPAIGN_IDS = tuple(f"c-{i}" for i in range(1, 9)) + ("organic",)
REFERRERS = ("google", "facebook", "email", "direct", "ads")

PRODUCT_PAGE_RE = re.compile(r"^/product/([1-9][0-9]*)$")
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True, slots=True)
class ClickEvent:
    """A single clickstream event produced by the simulator.

    Frozen and validated in ``__post_init__`` so an invalid event can never
    be constructed or serialized.
    """

    event_id: str
    event_type: str
    user_id: str
    session_id: str
    ts: str
    page: str
    device: str
    region: str
    campaign_id: str
    referrer: str

    def __post_init__(self) -> None:
        _validate_event(self)

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ClickEvent:
        return cls(
            event_id=str(data["event_id"]),
            event_type=str(data["event_type"]),
            user_id=str(data["user_id"]),
            session_id=str(data["session_id"]),
            ts=str(data["ts"]),
            page=str(data["page"]),
            device=str(data["device"]),
            region=str(data["region"]),
            campaign_id=str(data["campaign_id"]),
            referrer=str(data["referrer"]),
        )


def _validate_event(event: ClickEvent) -> None:
    if event.event_type not in EVENT_TYPES:
        raise ValueError(f"invalid event_type: {event.event_type!r}")
    if event.device not in DEVICES:
        raise ValueError(f"invalid device: {event.device!r}")
    if event.region not in REGIONS:
        raise ValueError(f"invalid region: {event.region!r}")
    if event.campaign_id not in CAMPAIGN_IDS:
        raise ValueError(f"invalid campaign_id: {event.campaign_id!r}")
    if event.referrer not in REFERRERS:
        raise ValueError(f"invalid referrer: {event.referrer!r}")
    if event.page not in PAGES and not PRODUCT_PAGE_RE.match(event.page):
        raise ValueError(f"invalid page: {event.page!r}")
    try:
        uuid.UUID(event.event_id)
    except ValueError as exc:
        raise ValueError(f"invalid event_id: {event.event_id!r}") from exc
    if not event.user_id.startswith("u-"):
        raise ValueError(f"invalid user_id: {event.user_id!r}")
    if not event.session_id.startswith("s-"):
        raise ValueError(f"invalid session_id: {event.session_id!r}")
    try:
        datetime.strptime(event.ts, TS_FORMAT)
    except ValueError as exc:
        raise ValueError(f"invalid ts: {event.ts!r}") from exc
