from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import os


@dataclass(frozen=True)
class CoinbaseCandleSpec:
    symbol: str
    granularity_seconds: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fetch_candles(spec: CoinbaseCandleSpec, start: datetime, end: datetime) -> pd.DataFrame:
    base = 'https://api.exchange.coinbase.com'
    url = f'{base}/products/{spec.symbol}/candles'
    params = {
        'granularity': int(spec.granularity_seconds),
        'start': start.isoformat(),
        'end': end.isoformat(),
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
    if df.empty:
        return df
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    df = df.sort_values('time').drop_duplicates('time')
    return df


def load_or_fetch_candles(
    symbol: str,
    granularity_seconds: int,
    lookback_days: int,
    data_dir: str | Path = 'data',
    now: Optional[datetime] = None,
) -> pd.DataFrame:
    now = now or _utcnow()
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    cache_path = data_dir / f'candles_{symbol.replace("/", "-")}_{granularity_seconds}.csv'

    start = now - timedelta(days=int(lookback_days))
    chunk = timedelta(hours=12)

    existing = None
    if cache_path.exists():
        try:
            existing = pd.read_csv(cache_path)
            existing['time'] = pd.to_datetime(existing['time'], utc=True)
        except Exception:
            existing = None

    # Incremental fetch: if we have cached candles, only fetch the missing tail
    # (plus a small overlap) instead of refetching the full lookback window.
    incremental = (os.getenv('C5_COINBASE_INCREMENTAL_CANDLES', 'true') or 'true').strip().lower() in {
        '1', 'true', 'yes', 'y', 'on'
    }
    if incremental and existing is not None and not existing.empty:
        try:
            last_ts = pd.to_datetime(existing['time']).max()
            if pd.notna(last_ts):
                overlap = timedelta(seconds=int(granularity_seconds) * 2)
                inc_start = (last_ts.to_pydatetime() - overlap)
                # Never fetch outside the configured lookback window.
                if inc_start > start:
                    start = inc_start
        except Exception:
            pass

    spec = CoinbaseCandleSpec(symbol=symbol, granularity_seconds=granularity_seconds)

    frames: list[pd.DataFrame] = []
    t = start
    while t < now:
        t2 = min(t + chunk, now)
        try:
            df = fetch_candles(spec, t, t2)
        except Exception:
            df = pd.DataFrame(columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        if not df.empty:
            frames.append(df)
        t = t2

    fetched = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['time', 'low', 'high', 'open', 'close', 'volume']
    )

    merged = pd.concat([existing, fetched], ignore_index=True) if existing is not None else fetched

    if not merged.empty:
        merged = merged.sort_values('time').drop_duplicates('time')
        merged.to_csv(cache_path, index=False)
    return merged
