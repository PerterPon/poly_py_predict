"""Gemini exchange candle feed — secondary price source.

Used to blend with Coinbase candles for a more robust signal.
Gemini's public market data API is completely free with no API key required.
Rate limit: 120 requests/minute (we'll use far fewer than this).

Symbol mapping: Coinbase uses 'BTC-USD', Gemini uses 'btcusd' (lowercase, no dash).
Supported symbols: BTC, ETH, SOL (XRP not available on Gemini).

Candle format returned: [timestamp_ms, open, high, low, close, volume]
We normalise to match the Coinbase DataFrame schema:
    time (datetime64, UTC), open, high, low, close, volume
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

_log = logging.getLogger(__name__)

# Coinbase symbol → Gemini symbol mapping
_SYMBOL_MAP: dict[str, str] = {
    'BTC-USD': 'btcusd',
    'ETH-USD': 'ethusd',
    'SOL-USD': 'solusd',
    # XRP is not listed on Gemini — no mapping
}

# Gemini granularity values accepted by their candle endpoint
_GRANULARITY_MAP: dict[int, str] = {
    60:   '1m',
    300:  '5m',
    900:  '15m',
    3600: '1hr',
    21600: '6hr',
    86400: '1day',
}

_BASE_URL = 'https://api.gemini.com/v2'


def _coinbase_to_gemini_symbol(symbol: str) -> Optional[str]:
    """Convert a Coinbase-style symbol (e.g. 'BTC-USD') to Gemini format ('btcusd')."""
    return _SYMBOL_MAP.get(symbol.upper())


def _granularity_to_period(granularity_seconds: int) -> Optional[str]:
    """Convert granularity in seconds to Gemini period string."""
    return _GRANULARITY_MAP.get(int(granularity_seconds))


def fetch_gemini_candles(
    symbol: str,
    granularity_seconds: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Fetch OHLCV candles from Gemini exchange.

    Parameters
    ----------
    symbol : str
        Coinbase-style symbol e.g. 'BTC-USD'.
    granularity_seconds : int
        Candle width in seconds (e.g. 900 for 15-min).
    start : datetime
        Start of the fetch window (UTC).
    end : datetime
        End of the fetch window (UTC).

    Returns
    -------
    pd.DataFrame
        Columns: time (UTC datetime), open, high, low, close, volume.
        Empty DataFrame if symbol not supported or fetch fails.
    """
    gemini_symbol = _coinbase_to_gemini_symbol(symbol)
    if gemini_symbol is None:
        _log.debug('data_gemini: symbol %s not supported on Gemini, skipping', symbol)
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

    period = _granularity_to_period(granularity_seconds)
    if period is None:
        _log.debug('data_gemini: granularity %ds not supported, skipping', granularity_seconds)
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

    url = f'{_BASE_URL}/candles/{gemini_symbol}/{period}'

    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        _log.warning('data_gemini: fetch failed for %s: %s', symbol, exc)
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

    if not data or not isinstance(data, list):
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

    # Gemini returns: [timestamp_ms, open, high, low, close, volume]
    df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)

    # Filter to requested window
    start_utc = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
    end_utc = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
    df = df[(df['time'] >= start_utc) & (df['time'] <= end_utc)]

    df = df.sort_values('time').drop_duplicates('time').reset_index(drop=True)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def blend_candles(
    coinbase_df: pd.DataFrame,
    gemini_df: pd.DataFrame,
    weight_coinbase: float = 0.6,
    weight_gemini: float = 0.4,
) -> pd.DataFrame:
    """Blend Coinbase and Gemini OHLCV candles into a single DataFrame.

    Candles are matched by timestamp. Where both sources have data for the
    same candle, OHLCV values are weighted-averaged. Where only one source
    has data, that source is used as-is (no data is discarded).

    Parameters
    ----------
    coinbase_df : pd.DataFrame
    gemini_df : pd.DataFrame
    weight_coinbase : float
        Weight for Coinbase values (default 0.6 — Coinbase is the primary feed
        and is used by Chainlink for resolution, so it gets higher weight).
    weight_gemini : float
        Weight for Gemini values (default 0.4).

    Returns
    -------
    pd.DataFrame
        Blended candles with same schema as input.
    """
    if gemini_df.empty:
        return coinbase_df
    if coinbase_df.empty:
        return gemini_df

    price_cols = ['open', 'high', 'low', 'close', 'volume']

    cb = coinbase_df.set_index('time')[price_cols]
    gm = gemini_df.set_index('time')[price_cols]

    # Rows present in both feeds → weighted average
    common = cb.index.intersection(gm.index)
    only_cb = cb.index.difference(gm.index)
    only_gm = gm.index.difference(cb.index)

    frames = []

    if len(common) > 0:
        blended = cb.loc[common] * weight_coinbase + gm.loc[common] * weight_gemini
        frames.append(blended)

    if len(only_cb) > 0:
        frames.append(cb.loc[only_cb])

    if len(only_gm) > 0:
        frames.append(gm.loc[only_gm])

    if not frames:
        return coinbase_df

    result = pd.concat(frames).sort_index().reset_index()
    result = result.rename(columns={'index': 'time'}) if 'index' in result.columns else result

    _log.debug(
        'data_gemini: blended %d common + %d cb-only + %d gemini-only candles',
        len(common), len(only_cb), len(only_gm),
    )
    return result


def is_symbol_supported(symbol: str) -> bool:
    """Return True if the symbol is available on Gemini."""
    return symbol.upper() in _SYMBOL_MAP
