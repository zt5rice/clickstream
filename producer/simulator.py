"""Deterministic clickstream simulator with weighted, realistic distributions."""

from __future__ import annotations

import itertools
import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from .schema import PRODUCT_PAGE_RE, ClickEvent

USER_POOL_SIZE = 5000
PRODUCT_POOL_SIZE = 200
SESSION_MIN_EVENTS = 3
SESSION_MAX_EVENTS = 20

PAGE_WEIGHTS = {
    "/home": 25,
    "/search": 15,
    "/product": 30,
    "/cart": 10,
    "/checkout": 5,
    "/account": 8,
    "/help": 7,
}
DEVICE_WEIGHTS = {"mobile": 55, "desktop": 35, "tablet": 10}
REGION_WEIGHTS = {
    "us-west": 40,
    "us-east": 20,
    "eu-west": 15,
    "ap-southeast": 10,
    "eu-central": 10,
    "sa-east": 5,
}
CAMPAIGN_WEIGHTS: dict[str, float] = {"organic": 40}
for _i in range(1, 9):
    CAMPAIGN_WEIGHTS[f"c-{_i}"] = 7.5
REFERRER_WEIGHTS = {"google": 30, "direct": 25, "facebook": 15, "email": 15, "ads": 15}

# Simple funnel progression used to make session paths realistic:
# home -> product -> cart -> checkout.
_FUNNEL_NEXT = {
    "/home": "/product",
    "/search": "/product",
    "/product": "/cart",
    "/cart": "/checkout",
    "/checkout": "/checkout",
    "/account": "/home",
    "/help": "/home",
}


class ClickstreamSimulator:
    """Generates deterministic, reproducible clickstream events.

    The same ``seed`` always yields the same event sequence, including event
    IDs, session IDs, and timestamps, which keeps tests and sample data
    reproducible. Distributions follow ``PLAN.md`` section 8: long-tail users,
    weighted pages/devices/regions/campaigns/referrers, and session paths that
    tend to progress through the purchase funnel.
    """

    def __init__(
        self,
        seed: int | None = None,
        start_time: datetime | None = None,
        events_per_second: int = 100,
    ) -> None:
        if events_per_second <= 0:
            raise ValueError("events_per_second must be > 0")
        self._seed = seed
        self._rng = random.Random(seed)
        self._clock = start_time or datetime.now(UTC).replace(microsecond=0)
        self._user_weights = [1.0 / rank for rank in range(1, USER_POOL_SIZE + 1)]
        self._step_seconds = 1.0 / events_per_second

    @property
    def seed(self) -> int | None:
        return self._seed

    def generate(self, n: int) -> list[ClickEvent]:
        """Return exactly ``n`` events (possibly truncated mid-session)."""
        if n < 0:
            raise ValueError("n must be >= 0")
        return list(itertools.islice(self.iter_events(), n))

    def iter_events(self) -> Iterator[ClickEvent]:
        """Yield an unbounded stream of events, one session at a time."""
        while True:
            yield from self._generate_session()

    def _generate_session(self) -> Iterator[ClickEvent]:
        user_id = self._pick_user()
        session_id = f"s-{uuid.UUID(int=self._rng.getrandbits(128)).hex[:12]}"
        device = self._pick(DEVICE_WEIGHTS)
        region = self._pick(REGION_WEIGHTS)
        campaign_id = self._pick(CAMPAIGN_WEIGHTS)
        referrer = self._pick(REFERRER_WEIGHTS)
        page = self._first_page()
        session_len = self._rng.randint(SESSION_MIN_EVENTS, SESSION_MAX_EVENTS)
        for _ in range(session_len):
            event = ClickEvent(
                event_id=str(uuid.UUID(int=self._rng.getrandbits(128))),
                event_type=self._pick_event_type(page),
                user_id=user_id,
                session_id=session_id,
                ts=self._clock.strftime("%Y-%m-%dT%H:%M:%SZ"),
                page=page,
                device=device,
                region=region,
                campaign_id=campaign_id,
                referrer=referrer,
            )
            yield event
            # Advance simulated time at the production pace so event timestamps
            # stay close to wall-clock time (freshness/dashboards stay honest).
            self._clock += timedelta(seconds=self._step_seconds)
            page = self._next_page(page)

    def _pick(self, weights: dict[str, float]) -> str:
        (choice,) = self._rng.choices(list(weights), weights=list(weights.values()), k=1)
        return choice

    def _pick_user(self) -> str:
        (rank,) = self._rng.choices(range(1, USER_POOL_SIZE + 1), weights=self._user_weights, k=1)
        return f"u-{rank:04d}"

    def _first_page(self) -> str:
        roll = self._rng.random()
        if roll < 0.70:
            return "/home"
        if roll < 0.90:
            return f"/product/{self._rng.randint(1, PRODUCT_POOL_SIZE)}"
        return self._pick_and_expand_page()

    def _next_page(self, current: str) -> str:
        roll = self._rng.random()
        if roll < 0.50:
            return current
        if roll < 0.85:
            nxt = _FUNNEL_NEXT.get(self._page_category(current))
            return self._expand_page(nxt) if nxt else self._pick_and_expand_page()
        return self._pick_and_expand_page()

    def _pick_event_type(self, page: str) -> str:
        weights = {"page_view": 70, "click": 25}
        if page in ("/product", "/cart", "/checkout") or PRODUCT_PAGE_RE.match(page):
            weights["add_to_cart"] = 3
        if page == "/checkout":
            weights["purchase"] = 2
        return self._pick(weights)

    def _pick_and_expand_page(self) -> str:
        return self._expand_page(self._pick(PAGE_WEIGHTS))

    def _expand_page(self, page: str) -> str:
        if page == "/product":
            return f"/product/{self._rng.randint(1, PRODUCT_POOL_SIZE)}"
        return page

    @staticmethod
    def _page_category(page: str) -> str:
        return "/product" if PRODUCT_PAGE_RE.match(page) else page
