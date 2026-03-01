"""Polymarket CLOB orderbook helpers.

We use the public CLOB REST endpoint for orderbook snapshots:
GET {CLOB_URL}/book?token_id=...

Docs:
- https://docs.polymarket.com/api-reference/orderbook/get-order-book-summary

This module is intentionally dependency-light (requests only) so it can run in
Docker without extra websocket deps.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderBookSummary:
    token_id: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    min_order_size: float = 0.001
    tick_size: float = 0.01


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def fetch_orderbook_summary(*, clob_url: str, token_id: str, timeout: float = 3.0) -> Optional[OrderBookSummary]:
    """Fetch top-of-book snapshot for a token id.

    Returns None on failure.
    """

    if not token_id:
        return None
    if requests is None:
        return None

    base = (clob_url or 'https://clob.polymarket.com').rstrip('/')
    url = f'{base}/book'

    try:
        r = requests.get(url, params={'token_id': token_id}, timeout=float(timeout))
        if not r.ok:
            logger.debug('orderbook fetch failed: %s %s', r.status_code, r.text[:200])
            return None
        data = r.json() if r.text else {}
    except Exception as exc:
        logger.debug('orderbook fetch exception: %s', exc)
        return None

    if not isinstance(data, dict):
        return None

    bids_raw = data.get('bids') or []
    asks_raw = data.get('asks') or []
    bids: list[BookLevel] = []
    asks: list[BookLevel] = []
    if isinstance(bids_raw, list):
        for lvl in bids_raw[:50]:
            if isinstance(lvl, dict):
                bids.append(BookLevel(price=_to_f(lvl.get('price'), 0.0), size=_to_f(lvl.get('size'), 0.0)))
    if isinstance(asks_raw, list):
        for lvl in asks_raw[:50]:
            if isinstance(lvl, dict):
                asks.append(BookLevel(price=_to_f(lvl.get('price'), 0.0), size=_to_f(lvl.get('size'), 0.0)))

    # Ensure best prices are first.
    bids.sort(key=lambda x: x.price, reverse=True)
    asks.sort(key=lambda x: x.price)

    return OrderBookSummary(
        token_id=str(token_id),
        bids=bids,
        asks=asks,
        min_order_size=_to_f(data.get('min_order_size'), 0.001) or 0.001,
        tick_size=_to_f(data.get('tick_size'), 0.01) or 0.01,
    )


def best_ask(book: OrderBookSummary) -> Optional[BookLevel]:
    for lvl in (book.asks or []):
        if lvl.price > 0 and lvl.size > 0:
            return lvl
    return None


def best_bid(book: OrderBookSummary) -> Optional[BookLevel]:
    for lvl in (book.bids or []):
        if lvl.price > 0 and lvl.size > 0:
            return lvl
    return None


def depth_usdc_up_to_price(book: OrderBookSummary, price_cap: float) -> float:
    """Return cumulative ask-side notional (USDC) available up to *price_cap*.

    Orderbook levels are in "shares" (size) at a given price.
    Notional depth = sum(price * size) across asks with price <= price_cap.
    """

    try:
        cap = float(price_cap)
    except Exception:
        cap = 0.0

    if cap <= 0:
        return 0.0

    total = 0.0
    for lvl in (book.asks or []):
        if not lvl:
            continue
        p = float(lvl.price or 0.0)
        s = float(lvl.size or 0.0)
        if p <= 0 or s <= 0:
            continue
        if p > cap:
            break
        total += p * s
    return float(total)
