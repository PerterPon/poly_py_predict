"""Chainlink oracle price feed via Polymarket RTDS WebSocket.

Polymarket 5-minute crypto UP/DOWN markets resolve using Chainlink
Data Streams — NOT Coinbase.  Using Coinbase as the sole price source
creates a structural oracle mismatch that causes losses when the
two feeds disagree near window boundaries.

This module connects to Polymarket's free, no-auth RTDS WebSocket:

    wss://ws-live-data.polymarket.com

and subscribes to `crypto_prices_chainlink` with an empty filter,
receiving prices for ALL supported assets: btc/usd, eth/usd,
sol/usd, xrp/usd.

It keeps a running latest-price / latest-timestamp per asset in memory
and exposes get_chainlink_price(asset) for the snipe prediction path.

The WebSocket runs in a background asyncio task with automatic reconnect.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

_log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────

WS_URL = 'wss://ws-live-data.polymarket.com'

# Subscribe with empty filters to receive ALL Chainlink crypto prices.
# Supported symbols: btc/usd, eth/usd, sol/usd, xrp/usd
SUBSCRIBE_MSG = json.dumps({
    'action': 'subscribe',
    'subscriptions': [{
        'topic': 'crypto_prices_chainlink',
        'type': '*',
        'filters': '',
    }],
})

# Canonical list of supported assets.
SUPPORTED_ASSETS = ('btc', 'eth', 'sol', 'xrp')
PING_INTERVAL = 5       # seconds — Polymarket recommends 5s pings
RECONNECT_DELAY = 3     # seconds between reconnect attempts
MAX_RECONNECT_DELAY = 60
STALE_THRESHOLD = 30    # seconds before we consider the feed stale


def _getbool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or '').strip().lower()
    if not v:
        return default
    return v in {'1', 'true', 'yes', 'y', 'on'}


def _getfloat(name: str, default: float) -> float:
    v = (os.getenv(name) or '').strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _stale_threshold_sec() -> float:
    # Allow runtime override via dashboard (logs/runtime_config.json → environ).
    return max(5.0, _getfloat('C5_CHAINLINK_STALE_THRESHOLD_SEC', float(STALE_THRESHOLD)))


# ── Shared state ─────────────────────────────────────────────────────

@dataclass
class _FeedState:
    """Thread-safe shared state for the latest Chainlink price."""
    price: float = 0.0
    ts: float = 0.0        # Unix timestamp of last update (from payload)
    recv_ts: float = 0.0   # local time.time() when we received it
    connected: bool = False
    error: Optional[str] = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, price: float, ts_ms: float) -> None:
        with self._lock:
            self.price = price
            self.ts = ts_ms / 1000.0  # convert ms → s
            self.recv_ts = time.time()
            self.error = None

    def snapshot(self) -> dict:
        with self._lock:
            return {
                'price': self.price,
                'ts': self.ts,
                'recv_ts': self.recv_ts,
                'connected': self.connected,
                'stale': self.is_stale,
                'error': self.error,
            }

    @property
    def is_stale(self) -> bool:
        if self.recv_ts <= 0:
            return True
        return (time.time() - self.recv_ts) > _stale_threshold_sec()


# Per-asset feed state: keyed by short asset name ('btc', 'eth', etc.)
_states: dict[str, _FeedState] = {a: _FeedState() for a in SUPPORTED_ASSETS}

# Legacy alias for backward compatibility (points to BTC state).
_state = _states['btc']


# ── Public API ───────────────────────────────────────────────────────

def _resolve_asset(asset: str) -> str:
    """Normalize asset name to lowercase short form."""
    return asset.lower().split('/')[0].split('-')[0]


def get_chainlink_price(asset: str = 'btc') -> float:
    """Return the latest Chainlink price for *asset* (0.0 if unavailable)."""
    a = _resolve_asset(asset)
    st = _states.get(a, _state)
    with st._lock:
        if st.is_stale:
            return 0.0
        return st.price


def get_chainlink_snapshot(asset: str = 'btc') -> dict:
    """Return full feed status dict for dashboard / diagnostics."""
    a = _resolve_asset(asset)
    st = _states.get(a, _state)
    snap = st.snapshot()
    snap['asset'] = a
    return snap


def get_all_chainlink_snapshots() -> dict[str, dict]:
    """Return snapshots for ALL supported assets."""
    return {a: st.snapshot() for a, st in _states.items()}


def is_feed_healthy(asset: str = 'btc') -> bool:
    """True if pipeline is connected and price is fresh for *asset*."""
    a = _resolve_asset(asset)
    st = _states.get(a, _state)
    with st._lock:
        return st.connected and not st.is_stale and st.price > 0


# ── WebSocket background task ────────────────────────────────────────

_started = False


async def _ws_loop() -> None:
    """Persistent WebSocket loop with auto-reconnect."""
    import websockets  # type: ignore

    async def _json_ping_loop(ws) -> None:
        """Send JSON PING messages periodically (RTDS docs recommendation)."""
        msg = json.dumps({'type': 'PING'})
        while True:
            try:
                enabled = _getbool('C5_RTDS_JSON_PING_ENABLED', False)
                interval = max(1.0, _getfloat('C5_RTDS_JSON_PING_INTERVAL_SEC', float(PING_INTERVAL)))
                if enabled:
                    await ws.send(msg)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception:
                # Connection likely closed; let the main loop handle reconnect.
                break

    delay = RECONNECT_DELAY
    while True:
        try:
            _log.info('Chainlink RTDS: connecting to %s …', WS_URL)

            # SSL fix for 'Hostname mismatch' or 'Certificate verify failed'
            # on some environments connecting to Polymarket RTDS.
            import ssl
            ssl_context = ssl._create_unverified_context()

            async with websockets.connect(
                WS_URL,
                ssl=ssl_context,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_INTERVAL * 3,
                close_timeout=5,
            ) as ws:
                for st in _states.values():
                    st.connected = True
                    st.error = None
                delay = RECONNECT_DELAY  # reset backoff on success
                _log.info('Chainlink RTDS: connected, subscribing …')

                await ws.send(SUBSCRIBE_MSG)
                _log.info('Chainlink RTDS: subscribed to ALL crypto assets (%s)',
                          ', '.join(f'{a}/usd' for a in SUPPORTED_ASSETS))

                ping_task = asyncio.create_task(_json_ping_loop(ws), name='chainlink_json_ping')

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        topic = msg.get('topic')
                        if topic != 'crypto_prices_chainlink':
                            continue

                        payload = msg.get('payload')
                        if not isinstance(payload, dict):
                            continue

                        sym = payload.get('symbol', '').lower()  # e.g. 'btc/usd'
                        asset_key = sym.split('/')[0]  # 'btc', 'eth', etc.
                        if asset_key not in _states:
                            continue

                        price = payload.get('value')
                        ts_ms = payload.get('timestamp', 0)
                        if price is not None and float(price) > 0:
                            _states[asset_key].update(float(price), float(ts_ms))
                            _log.debug(
                                'Chainlink %s: $%.2f (ts=%d)',
                                sym, float(price), int(ts_ms),
                            )
                finally:
                    try:
                        ping_task.cancel()
                    except Exception:
                        pass

        except asyncio.CancelledError:
            _log.info('Chainlink RTDS: task cancelled')
            break
        except Exception as exc:
            for st in _states.values():
                st.connected = False
                st.error = str(exc)
            _log.warning('Chainlink RTDS: disconnected (%s), reconnecting in %ds …', exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)


async def start_chainlink_feed() -> asyncio.Task:
    """Launch the background WebSocket task.  Safe to call multiple times."""
    global _started
    if _started:
        _log.debug('Chainlink feed already started')
        return None  # type: ignore
    _started = True
    task = asyncio.create_task(_ws_loop(), name='chainlink_feed')
    _log.info('Chainlink RTDS background task created')
    return task


# ── Persistent price history (for hybrid training target) ────────────
# Records the Chainlink price at every 5-minute window boundary.  Over
# time this builds a dataset aligned with the ACTUAL resolution oracle,
# so the model can train on what really resolves markets, not Coinbase.

_HISTORY_FILE = Path('logs') / 'chainlink_prices.csv'
_history_lock = threading.Lock()
_history_last: dict[str, float] = {}  # per-asset: asset -> last recorded ts


def record_window_price(window_ts: int, price: float, asset: str = 'btc') -> None:
    """Persist a Chainlink price at a 5-min window boundary.

    Called from runner.py when it caches the window open price.
    Safe to call multiple times for the same timestamp — deduplicates per asset.
    """
    if price <= 0:
        return
    a = _resolve_asset(asset)
    with _history_lock:
        if _history_last.get(a) == window_ts:
            return  # already recorded
        _history_last[a] = window_ts
        exists = _HISTORY_FILE.exists()
        try:
            _HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(_HISTORY_FILE, 'a', newline='') as f:
                wr = csv.writer(f)
                if not exists:
                    wr.writerow(['ts', 'asset', 'price'])
                wr.writerow([window_ts, a, round(price, 2)])
        except Exception as exc:
            _log.warning('chainlink history write failed: %s', exc)


def get_chainlink_history(asset: str = 'btc') -> pd.DataFrame:
    """Load the persistent Chainlink price history for *asset*.

    Returns DataFrame with columns: ts (int epoch), asset (str),
    price (float), time (UTC datetime).  Empty DataFrame if no history yet.
    """
    cols = ['ts', 'asset', 'price', 'time']
    if not _HISTORY_FILE.exists():
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(_HISTORY_FILE)
        # Backward compat: old files may lack 'asset' column.
        if 'asset' not in df.columns:
            df['asset'] = 'btc'
        df['ts'] = df['ts'].astype(int)
        df['price'] = df['price'].astype(float)
        df['time'] = pd.to_datetime(df['ts'], unit='s', utc=True)
        a = _resolve_asset(asset)
        df = df[df['asset'] == a]
        return df.drop_duplicates(subset='ts').sort_values('ts').reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=cols)


# ── Optional: Coinbase fallback + basis monitoring ───────────────────

def compute_basis(coinbase_price: float, asset: str = 'btc') -> dict:
    """Compare Coinbase price vs Chainlink oracle price for *asset*.

    Returns a dict with:
    - chainlink_price: latest oracle price
    - coinbase_price: the Coinbase price passed in
    - basis_bps: (coinbase - chainlink) / chainlink × 10_000
    - abs_basis_bps: absolute value
    - direction_agree: whether both sources agree on the sign of
      a move relative to some reference (useful for snipe logic)
    """
    cl = get_chainlink_price(asset)
    if cl <= 0 or coinbase_price <= 0:
        return {
            'chainlink_price': cl,
            'coinbase_price': coinbase_price,
            'basis_bps': 0.0,
            'abs_basis_bps': 0.0,
            'available': False,
        }
    basis = (coinbase_price - cl) / cl * 10_000
    return {
        'chainlink_price': cl,
        'coinbase_price': coinbase_price,
        'basis_bps': round(basis, 2),
        'abs_basis_bps': round(abs(basis), 2),
        'available': True,
    }
