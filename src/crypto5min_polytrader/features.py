from __future__ import annotations

import numpy as np
import pandas as pd


def add_features(df: pd.DataFrame, chainlink_prices: pd.DataFrame | None = None, for_prediction: bool = False) -> pd.DataFrame:
    """Compute technical features and target variable.

    Parameters
    ----------
    df : DataFrame
        Coinbase candle data with columns: time, open, high, low, close, volume.
    chainlink_prices : DataFrame, optional
        Chainlink oracle price history with columns: ts (epoch), price.
        When provided, the target variable y_up uses the Chainlink oracle
        price change instead of Coinbase close — this aligns training with
        the actual resolution oracle and eliminates the oracle mismatch.
    """
    if df.empty:
        return df

    out = df.copy()
    out = out.sort_values('time').reset_index(drop=True)

    close = out['close'].astype(float)
    high = out['high'].astype(float)
    low = out['low'].astype(float)
    volume = out['volume'].astype(float)
    opn = out['open'].astype(float) if 'open' in out.columns else close

    out['ret_1'] = close.pct_change(1)
    out['ret_3'] = close.pct_change(3)
    out['ret_6'] = close.pct_change(6)

    out['hl_spread'] = (high - low) / close.replace(0, np.nan)
    out['range_ema'] = out['hl_spread'].ewm(span=20, adjust=False).mean()
    out['vol_ema'] = volume.ewm(span=20, adjust=False).mean()

    # RSI
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.ewm(alpha=1 / 14, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / 14, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    out['rsi_14'] = 100 - (100 / (1 + rs))

    # MACD
    out['ema_12'] = close.ewm(span=12, adjust=False).mean()
    out['ema_26'] = close.ewm(span=26, adjust=False).mean()
    out['macd'] = out['ema_12'] - out['ema_26']
    out['macd_signal'] = out['macd'].ewm(span=9, adjust=False).mean()
    out['macd_hist'] = out['macd'] - out['macd_signal']

    # Bollinger Bands
    bb_sma = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    out['bb_upper'] = bb_sma + 2 * bb_std
    out['bb_lower'] = bb_sma - 2 * bb_std
    out['bb_width'] = (out['bb_upper'] - out['bb_lower']) / bb_sma.replace(0, np.nan)
    out['bb_pct'] = (close - out['bb_lower']) / (out['bb_upper'] - out['bb_lower']).replace(0, np.nan)

    # ATR (Average True Range)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out['atr_14'] = true_range.ewm(span=14, adjust=False).mean()
    out['atr_ratio'] = out['atr_14'] / close.replace(0, np.nan)

    # Stochastic %K / %D
    lo14 = low.rolling(14).min()
    hi14 = high.rolling(14).max()
    out['stoch_k'] = 100 * (close - lo14) / (hi14 - lo14).replace(0, np.nan)
    out['stoch_d'] = out['stoch_k'].rolling(3).mean()

    # Volume features
    out['vol_ratio'] = volume / out['vol_ema'].replace(0, np.nan)
    out['vwap'] = (volume * (high + low + close) / 3).cumsum() / volume.cumsum().replace(0, np.nan)
    out['vwap_dev'] = (close - out['vwap']) / out['vwap'].replace(0, np.nan)

    # Price-based
    out['body_ratio'] = (close - opn) / (high - low).replace(0, np.nan)

    # ── Research-driven features (Kaggle crypto-forecasting insights) ──

    # Longer-term returns (1h, 2h, 4h lookbacks at 5-min candles)
    out['ret_12'] = close.pct_change(12)     # 1-hour return
    out['ret_24'] = close.pct_change(24)     # 2-hour return
    out['ret_48'] = close.pct_change(48)     # 4-hour return

    # Momentum acceleration: difference between short and long returns
    out['mom_accel'] = out['ret_1'] - out['ret_6']

    # Rolling volatility at multiple windows
    out['rvol_6']  = close.pct_change(1).rolling(6).std()
    out['rvol_12'] = close.pct_change(1).rolling(12).std()
    out['rvol_24'] = close.pct_change(1).rolling(24).std()

    # Volatility ratio (short / long) — detects volatility regime shifts
    out['vol_regime'] = out['rvol_6'] / out['rvol_24'].replace(0, np.nan)

    # Volume momentum (rate of change)
    out['vol_roc'] = volume.pct_change(6)

    # Price position within recent range (where are we in the last N candles)
    out['pos_12'] = (close - close.rolling(12).min()) / \
                    (close.rolling(12).max() - close.rolling(12).min()).replace(0, np.nan)
    out['pos_48'] = (close - close.rolling(48).min()) / \
                    (close.rolling(48).max() - close.rolling(48).min()).replace(0, np.nan)

    # ADX-like trend strength (simplified: absolute directional movement)
    plus_dm = (high - high.shift(1)).clip(lower=0)
    minus_dm = (low.shift(1) - low).clip(lower=0)
    atr14 = true_range.ewm(span=14, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(span=14, adjust=False).mean() / atr14.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(span=14, adjust=False).mean() / atr14.replace(0, np.nan)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    out['adx'] = dx.ewm(span=14, adjust=False).mean()

    # On-balance volume slope (OBV trend)
    obv_sign = np.where(close > close.shift(1), 1, np.where(close < close.shift(1), -1, 0))
    out['obv'] = (volume * obv_sign).cumsum()
    out['obv_slope'] = out['obv'].diff(6) / out['obv'].rolling(6).mean().replace(0, np.nan)

    # ── Additional features from research papers ─────────────────────

    # Williams %R (14-period) — overbought/oversold oscillator
    out['williams_r'] = -100 * (hi14 - close) / (hi14 - lo14).replace(0, np.nan)

    # CCI — Commodity Channel Index (20-period)
    tp = (high + low + close) / 3
    tp_sma = tp.rolling(20).mean()
    tp_mad = tp.rolling(20).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    out['cci'] = (tp - tp_sma) / (0.015 * tp_mad).replace(0, np.nan)

    # Multi-timeframe RSI (short 6-period, long 24-period)
    # These detect divergence between fast & slow momentum regimes.
    for rsi_w in (6, 24):
        d = close.diff()
        u = d.clip(lower=0)
        dn = (-d).clip(lower=0)
        ru = u.ewm(alpha=1 / rsi_w, adjust=False).mean()
        rd = dn.ewm(alpha=1 / rsi_w, adjust=False).mean()
        out[f'rsi_{rsi_w}'] = 100 - (100 / (1 + ru / rd.replace(0, np.nan)))

    # RSI divergence — difference between price trend and RSI trend.
    # A bearish divergence (price up but RSI down) often precedes reversals.
    price_slope = close.diff(6) / close.shift(6).replace(0, np.nan)
    rsi_slope = out['rsi_14'].diff(6) / 100.0  # normalise
    out['rsi_divergence'] = price_slope - rsi_slope

    # MACD crossover binary (1 = bullish cross, -1 = bearish, 0 = none)
    macd_above = (out['macd'] > out['macd_signal']).astype(int)
    out['macd_cross'] = macd_above.diff().fillna(0).clip(-1, 1)

    # Distance from SMA-20, normalised by ATR — mean-reversion signal
    sma20 = close.rolling(20).mean()
    out['dist_sma20'] = (close - sma20) / out['atr_14'].replace(0, np.nan)

    # Time-of-day & day-of-week cyclical features
    if 'time' in out.columns:
        ts = pd.to_datetime(out['time'], utc=True, errors='coerce')
        hour = ts.dt.hour + ts.dt.minute / 60.0
        dow  = ts.dt.dayofweek.astype(float)
        out['hour_sin'] = np.sin(2 * np.pi * hour / 24)
        out['hour_cos'] = np.cos(2 * np.pi * hour / 24)
        out['dow_sin']  = np.sin(2 * np.pi * dow / 7)
        out['dow_cos']  = np.cos(2 * np.pi * dow / 7)

    # ── Target variable ────────────────────────────────────────────────
    # With deadband filter to eliminate noise-level moves.
    # Moves < 5 bps (0.05%) are essentially random; labelling them as
    # UP/DOWN just teaches the model noise and introduces directional bias.
    DEADBAND_BPS = 5  # basis points
    deadband = DEADBAND_BPS / 10_000

    # ---------- Hybrid target: prefer Chainlink oracle prices ----------
    # Polymarket 5-min markets resolve on Chainlink BTC/USD, NOT Coinbase.
    # When we have Chainlink price history, use it as the resolution oracle
    # to eliminate the structural oracle mismatch.
    _used_chainlink = False
    if chainlink_prices is not None and not chainlink_prices.empty and 'time' in out.columns:
        try:
            # Build a mapping: candle_epoch → chainlink_price at that boundary
            cl = chainlink_prices.copy()
            if 'ts' in cl.columns:
                cl = cl.set_index('ts')['price']
            elif 'time' in cl.columns:
                cl['_ts'] = pd.to_datetime(cl['time'], utc=True).astype(int) // 10**9
                cl = cl.set_index('_ts')['price']
            else:
                cl = None

            if cl is not None and len(cl) >= 10:
                # Convert candle timestamps to epoch seconds
                candle_ts = pd.to_datetime(out['time'], utc=True).astype(int) // 10**9
                # Look up Chainlink price at T and T+granularity (next boundary)
                granularity = 300  # 5-min default
                cl_at_t = candle_ts.map(cl).astype(float)
                cl_at_t1 = (candle_ts + granularity).map(cl).astype(float)

                # Only use Chainlink where we have BOTH prices
                has_both = cl_at_t.notna() & cl_at_t1.notna()
                if has_both.sum() >= 10:
                    cl_ret = (cl_at_t1 - cl_at_t) / cl_at_t.replace(0, np.nan)
                    out['future_close'] = cl_at_t1  # for reference
                    out['y_up'] = np.where(
                        has_both,
                        np.where(cl_ret > deadband, 1,
                                 np.where(cl_ret < -deadband, 0, np.nan)),
                        np.nan,  # no oracle data → drop row
                    )
                    _used_chainlink = True
        except Exception:
            pass  # fall through to Coinbase target

    if not _used_chainlink:
        # Fallback: Coinbase close (original behaviour)
        out['future_close'] = close.shift(-1)
        future_ret = (out['future_close'] - close) / close.replace(0, np.nan)
        # Rows inside the deadband get NaN → dropped by dropna() below.
        out['y_up'] = np.where(
            future_ret > deadband, 1,
            np.where(future_ret < -deadband, 0, np.nan)
        )

    if for_prediction:
        # Don't drop rows based on y_up/future_close — we don't need targets for prediction
        if 'y_up' in out.columns:
            out['y_up'] = out['y_up'].fillna(0.5)
        if 'future_close' in out.columns:
            out['future_close'] = out['future_close'].fillna(out['future_close'].shift(1))
        feature_cols = [c for c in out.columns if c not in ('y_up', 'future_close')]
        out = out.dropna(subset=feature_cols).reset_index(drop=True)
    else:
        out = out.dropna().reset_index(drop=True)
    return out
