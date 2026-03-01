"""15-minute window math for Polymarket crypto Up/Down markets.

Each market covers a discrete 15-minute interval:
- Slug format: {asset}-updown-15m-{start_ts}  (e.g. btc-updown-15m-1771078200)
- start_ts is always a multiple of 900 (Unix seconds)
- Resolved by Chainlink oracle at the end of each window
- Supported assets: btc, eth, sol, xrp

This module is pure functions with no external dependencies.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

INTERVAL = 900  # seconds per window (15-minute markets)


@dataclass(frozen=True)
class Window:
    start_ts: int
    end_ts: int
    slug: str


def _align(ts: float) -> int:
    """Round a timestamp down to the nearest window boundary."""
    return int(ts // INTERVAL) * INTERVAL


def current_window(now: float | None = None, *, asset: str = 'btc') -> Window:
    """Return the window that contains *now* for the given asset."""
    t = now if now is not None else time.time()
    start = _align(t)
    a = asset.lower()
    return Window(start_ts=start, end_ts=start + INTERVAL, slug=f'{a}-updown-15m-{start}')


def next_window(now: float | None = None, *, asset: str = 'btc') -> Window:
    """Return the window after the current one for the given asset."""
    t = now if now is not None else time.time()
    start = _align(t) + INTERVAL
    a = asset.lower()
    return Window(start_ts=start, end_ts=start + INTERVAL, slug=f'{a}-updown-15m-{start}')


def seconds_remaining(now: float | None = None) -> int:
    """Seconds until the current window ends."""
    t = now if now is not None else time.time()
    w = current_window(t)
    return max(0, int(w.end_ts - t))


def seconds_into_window(now: float | None = None) -> int:
    """Seconds elapsed since the current window started."""
    t = now if now is not None else time.time()
    w = current_window(t)
    return max(0, int(t - w.start_ts))


def is_trade_time(lead_seconds: int = 30, now: float | None = None) -> bool:
    """True if we're inside the optimal trade-placement zone.

    The zone is [lead_seconds, lead_seconds + 60] seconds after window start.
    We wait *lead_seconds* for the market to appear on Gamma, then have a
    60-second placement window.
    """
    elapsed = seconds_into_window(now)
    return lead_seconds <= elapsed <= lead_seconds + 60


def is_snipe_time(lead_seconds: int = 10, now: float | None = None) -> bool:
    """True if we're in the late-entry snipe zone near window close.

    The snipe zone is the last *lead_seconds* of the window — e.g. the final
    10 seconds.  At this point the BTC price has already moved since window
    open, so direction is mostly locked in.
    """
    remaining = seconds_remaining(now)
    # remaining ∈ [0, lead_seconds] — we're in the last N seconds
    return 0 < remaining <= lead_seconds


def window_from_slug(slug: str) -> Window:
    """Parse a slug back into a Window object.

    >>> window_from_slug('btc-updown-5m-1771078200')
    Window(start_ts=1771078200, end_ts=1771079100, slug='btc-updown-15m-1771078200')
    """
    parts = slug.rsplit('-', 1)
    ts = int(parts[-1])
    return Window(start_ts=ts, end_ts=ts + INTERVAL, slug=slug)
