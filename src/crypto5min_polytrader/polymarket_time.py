"""Polymarket server-time alignment helpers.

Problem:
  Our 5-minute market slug selection is derived from wall-clock time:
    btc-updown-5m-{floor(epoch/300)*300}
  If the host clock drifts (Windows/WSL sleep, VPS NTP issues), the bot can
  place orders for the wrong window slug (often exactly one window behind).

Solution:
  Use Polymarket's public CLOB time endpoint (GET /time) to compute a small
  local->server offset and apply it when computing window slugs.

Design goals:
  - Fast: one lightweight GET on a slow cadence (cached)
  - Safe: if the endpoint is unavailable, fall back to local time
  - No strategy/guardrail changes: only time alignment
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Optional

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


logger = logging.getLogger(__name__)


_LOCK = threading.Lock()
_OFFSET_SEC: float = 0.0
_LAST_SYNC_TS: float = 0.0


def _getbool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or '').strip().lower()
    if not v:
        return bool(default)
    return v in {'1', 'true', 'yes', 'y', 'on'}


def _clob_url() -> str:
    url = (os.getenv('C5_POLY_CLOB_URL') or 'https://clob.polymarket.com').strip()
    url = url.rstrip('/')
    return url or 'https://clob.polymarket.com'


def _parse_server_time_seconds(payload: Any) -> Optional[float]:
    """Extract a Unix epoch seconds value from CLOB /time response.

    The exact response shape can vary (client/library versions), so we accept:
      - number (int/float)
      - numeric string
      - dict containing a numeric value under common keys
      - dict with a single numeric-ish value
    """

    if payload is None:
        return None

    # Raw number.
    if isinstance(payload, (int, float)):
        v = float(payload)
        return v if v > 0 else None

    # Numeric string.
    if isinstance(payload, str):
        s = payload.strip()
        if not s:
            return None
        try:
            v = float(s)
            return v if v > 0 else None
        except Exception:
            return None

    if isinstance(payload, dict):
        # Common key candidates.
        for k in (
            'server_time',
            'serverTime',
            'time',
            'timestamp',
            'ts',
            'epoch',
            'now',
            'current_time',
            'currentTime',
        ):
            if k in payload:
                v = _parse_server_time_seconds(payload.get(k))
                if v:
                    return v

        # Fallback: single-value dict.
        if len(payload) == 1:
            only_val = next(iter(payload.values()))
            v = _parse_server_time_seconds(only_val)
            if v:
                return v

        # Fallback: scan small dict.
        for _, v0 in list(payload.items())[:6]:
            v = _parse_server_time_seconds(v0)
            if v:
                return v

    return None


def _sync_offset_if_needed(*, ttl_seconds: float = 90.0) -> float:
    """Refresh and return the cached (server - local) offset in seconds."""

    global _OFFSET_SEC, _LAST_SYNC_TS

    if not _getbool('C5_POLY_TIME_SYNC_ENABLED', True):
        return 0.0

    now = time.time()
    # Fast path: fresh enough.
    if (now - _LAST_SYNC_TS) < float(ttl_seconds):
        return float(_OFFSET_SEC)

    if requests is None:
        return float(_OFFSET_SEC)

    with _LOCK:
        now = time.time()
        if (now - _LAST_SYNC_TS) < float(ttl_seconds):
            return float(_OFFSET_SEC)

        url = f'{_clob_url()}/time'
        # Use a short timeout to avoid blocking the trade loop.
        timeout_sec = 2.5

        t0 = time.time()
        try:
            resp = requests.get(url, timeout=timeout_sec)
            resp.raise_for_status()
            data = resp.json() if hasattr(resp, 'json') else None
        except Exception as exc:
            # Keep previous offset; just mark sync time so we don't spam.
            _LAST_SYNC_TS = time.time()
            logger.debug('CLOB time sync failed: %s', exc)
            return float(_OFFSET_SEC)
        t1 = time.time()

        server_ts = _parse_server_time_seconds(data)
        if not server_ts:
            _LAST_SYNC_TS = time.time()
            logger.debug('CLOB time sync: could not parse /time payload: %r', data)
            return float(_OFFSET_SEC)

        # Midpoint to reduce RTT bias.
        mid = (t0 + t1) / 2.0
        new_offset = float(server_ts) - float(mid)

        _OFFSET_SEC = new_offset
        _LAST_SYNC_TS = time.time()

        # Log only when drift is meaningfully large.
        if abs(new_offset) >= 1.0:
            logger.warning('Host clock offset vs Polymarket CLOB: %.3fs', new_offset)
        else:
            logger.debug('CLOB time offset: %.3fs', new_offset)

        return float(_OFFSET_SEC)


def get_time_offset_seconds() -> float:
    """Return best-effort (server - local) offset in seconds."""

    return float(_sync_offset_if_needed())


def polymarket_now() -> float:
    """Return a best-effort Polymarket-aligned wall-clock timestamp (epoch seconds)."""

    return time.time() + float(get_time_offset_seconds())
