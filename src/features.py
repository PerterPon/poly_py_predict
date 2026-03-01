from __future__ import annotations

import numpy as np
import pandas as pd


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create simple, fast features for 5-min direction prediction."""

    if df.empty:
        return df

    out = df.copy()
    out = out.sort_values('time').reset_index(drop=True)

    close = out['close'].astype(float)
    high = out['high'].astype(float)
    low = out['low'].astype(float)
    volume = out['volume'].astype(float)

    out['ret_1'] = close.pct_change(1)
    out['ret_3'] = close.pct_change(3)
    out['ret_6'] = close.pct_change(6)

    out['hl_spread'] = (high - low) / close.replace(0, np.nan)
    out['range_ema'] = out['hl_spread'].ewm(span=20, adjust=False).mean()
    out['vol_ema'] = volume.ewm(span=20, adjust=False).mean()

    # RSI(14)
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.ewm(alpha=1 / 14, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / 14, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    out['rsi_14'] = 100 - (100 / (1 + rs))

    # Moving averages
    out['ema_12'] = close.ewm(span=12, adjust=False).mean()
    out['ema_26'] = close.ewm(span=26, adjust=False).mean()
    out['macd'] = out['ema_12'] - out['ema_26']
    out['macd_signal'] = out['macd'].ewm(span=9, adjust=False).mean()

    # Label: next candle direction
    out['future_close'] = close.shift(-1)
    out['y_up'] = (out['future_close'] > close).astype(int)

    # Drop rows that can’t be used
    out = out.dropna().reset_index(drop=True)
    return out
