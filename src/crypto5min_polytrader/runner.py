from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from .config import C5Config
from .data_coinbase import load_or_fetch_candles, fetch_candles, CoinbaseCandleSpec
from .features import add_features
from .model import FitResult, fit_logistic, predict_proba, SEQ_LEN
from .paper import backtest
from .persistence import JsonStore


STATE_STORE = JsonStore(Path('logs') / 'state.json')


# ── Spot-price helper (lightweight Coinbase ticker) ──────────────────
_spot_cache: dict[str, dict] = {}  # per-symbol: {'price': float, 'ts': float}


def _fetch_spot_price(symbol: str) -> float:
    """Return the live spot price from Coinbase. Cached per symbol for 5 s."""
    import time as _time
    import requests as _req

    now = _time.time()
    sc = _spot_cache.get(symbol, {'price': 0.0, 'ts': 0.0})
    if now - sc['ts'] < 5 and sc['price'] > 0:
        return sc['price']
    try:
        r = _req.get(
            f'https://api.coinbase.com/v2/prices/{symbol}/spot',
            timeout=4,
        )
        p = float(r.json()['data']['amount'])
        _spot_cache[symbol] = {'price': p, 'ts': now}
        return p
    except Exception:
        return 0.0


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _walk_forward_probs(
    df: pd.DataFrame,
    *,
    min_train: int = 300,
    refit_every: int = 12,
    max_train: int = 2000,
) -> list[float | None]:
    """Compute out-of-sample-ish probabilities for backtesting.

    We refit periodically (every `refit_every` rows) using a rolling training window
    (up to `max_train` most recent rows). This is not a perfect execution-grade
    walk-forward simulation, but it avoids fully in-sample evaluation.

    NOTE: Always uses fast logistic regression for backtesting. CNN-LSTM is too
    expensive for hundreds of refits (each takes ~30s on CPU).
    """

    probs: list[float | None] = []
    fit: object | None = None
    last_fit_at: int | None = None

    n = int(len(df))
    min_train = max(10, int(min_train))
    refit_every = max(1, int(refit_every))
    max_train = max(min_train, int(max_train))

    for i in range(n):
        if i < min_train:
            probs.append(None)
            continue

        if fit is None or last_fit_at is None or (i - last_fit_at) >= refit_every:
            start = max(0, i - max_train)
            fit = fit_logistic(df.iloc[start:i].copy(), force_logistic=True, fast=True)
            last_fit_at = i

        probs.append(predict_proba(fit, df.iloc[i]))

    return probs


def run_once(cfg: C5Config) -> dict:
    import logging as _logging
    _log = _logging.getLogger(__name__)

    # Try the configured lookback first, then fall back to shorter windows for
    # pairs (e.g. SOL-USD) that don't have deep Coinbase candle history.
    # BTC/ETH will succeed on the first attempt and use the full lookback.
    _fallback_days = [cfg.lookback_days, 14, 7, 3]
    candles = None
    used_lookback = cfg.lookback_days
    for _days in _fallback_days:
        _c = load_or_fetch_candles(
            symbol=cfg.symbol,
            granularity_seconds=cfg.granularity_seconds,
            lookback_days=_days,
            data_dir='data',
        )
        if _c is not None and len(_c) >= 60:
            candles = _c
            used_lookback = _days
            if _days < cfg.lookback_days:
                _log.info(
                    'run_once [%s]: full %d-day lookback insufficient; '
                    'using %d-day fallback (%d candles)',
                    cfg.symbol, cfg.lookback_days, _days, len(_c),
                )
            break
    if candles is None or candles.empty:
        return {'ts': utcnow_iso(), 'symbol': cfg.symbol, 'status': 'not_enough_data'}

    # ── Blend training candles with Gemini (free secondary feed) ─────────
    try:
        from .data_gemini import fetch_gemini_candles, blend_candles, is_symbol_supported
        from datetime import datetime, timezone
        if is_symbol_supported(cfg.symbol) and candles is not None and not candles.empty:
            _train_start = candles['time'].min().to_pydatetime()
            _train_end = candles['time'].max().to_pydatetime()
            _gem_train = fetch_gemini_candles(
                cfg.symbol, cfg.granularity_seconds, _train_start, _train_end
            )
            if not _gem_train.empty:
                candles = blend_candles(candles, _gem_train)
                _log.debug('run_once [%s]: blended Gemini candles into training set', cfg.symbol)
    except Exception as _gem_exc:
        _log.debug('run_once [%s]: Gemini training blend skipped: %s', cfg.symbol, _gem_exc)

    # Load Chainlink price history for hybrid training target.
    try:
        from .chainlink_feed import get_chainlink_history
        _asset_key = cfg.symbol.split('-')[0].lower() if cfg.symbol else 'btc'
        cl_hist = get_chainlink_history(_asset_key)
        if cl_hist.empty or len(cl_hist) < 20:
            cl_hist = None
    except Exception:
        cl_hist = None

    feats = add_features(candles, chainlink_prices=cl_hist)
    if feats.empty or len(feats) < 50:
        return {'ts': utcnow_iso(), 'symbol': cfg.symbol, 'status': 'not_enough_data'}

    train: pd.DataFrame = feats.iloc[:-1].copy()

    fit = fit_logistic(train)
    # For CNN-LSTM: pass the last seq_len rows; for logistic: pass last row.
    if hasattr(fit, 'backend') and fit.backend == 'cnn_lstm':
        tail = feats.iloc[-fit.seq_len:] if len(feats) >= fit.seq_len else feats
        p_up = predict_proba(fit, tail)
    else:
        p_up = predict_proba(fit, feats.iloc[-1])
    direction = 'UP' if p_up >= cfg.direction_threshold else 'DOWN'
    confidence = p_up if direction == 'UP' else 1.0 - p_up
    strong = confidence >= cfg.confidence_threshold

    # Walk-forward-ish probabilities for a more realistic paper backtest.
    # Note: this is still an approximation (candle closes + simple costs).
    probs = _walk_forward_probs(
        train,
        min_train=max(300, min(1000, int(len(train) * 0.2))),
        refit_every=12,  # 12 candles = 1 hour at 5-min granularity
        max_train=2000,
    )
    bt_df = train[['time', 'close']].copy()
    bt_df['p_up'] = probs
    bt_df = bt_df.dropna(subset=['p_up']).reset_index(drop=True)

    bt = backtest(
        df=bt_df,
        proba_col='p_up',
        threshold=cfg.confidence_threshold,
        starting_cash=cfg.paper_starting_cash,
        position_fraction=cfg.paper_position_fraction,
        fee_bps=cfg.paper_fee_bps,
        slippage_bps=cfg.paper_slippage_bps,
    )

    bt['method'] = 'walk_forward_refit'
    bt['refit_every_candles'] = 12
    bt['max_train_rows'] = 2000

    return {
        'ts': utcnow_iso(),
        'symbol': cfg.symbol,
        'status': 'ok',
        'direction': direction,
        'p_up': p_up,
        'confidence': confidence,
        'strong': strong,
        'price': float(feats.iloc[-1]['close']),
        'backtest': bt,
        'fit': fit,
    }


def predict_latest(cfg: C5Config, fit: FitResult) -> dict:
    """Fast prediction using a cached FitResult. Fetches only recent candles.

    When Chainlink price data is available for the current window, blends
    the model's prediction with the live oracle delta for a stronger
    ensemble signal.  The ensemble weight is configurable via
    C5_ENSEMBLE_WEIGHT (0.0 = pure delta, 1.0 = pure model, default 0.6).
    """
    import logging
    import math
    _log = logging.getLogger(__name__)

    now = datetime.now(timezone.utc)
    spec = CoinbaseCandleSpec(symbol=cfg.symbol, granularity_seconds=cfg.granularity_seconds)

    # Fetch enough candles for CNN-LSTM sequence window + feature warm-up.
    # Features need ~48+ candles for ret_48/pos_48; use at least 65 raw candles.
    # Lookback is calculated from actual granularity_seconds so it works for
    # both 5-min and 15-min variants without hardcoded assumptions.
    seq = getattr(fit, 'seq_len', 1)
    # Need at least 60 candles after feature warm-up (ret_48, pos_48 need 48+).
    # Use actual granularity_seconds rather than the old hardcoded 5-min assumption.
    candle_minutes = cfg.granularity_seconds / 60
    min_candles = max(seq + 40, 65)  # 65 ensures ret_48/pos_48 have valid rows
    lookback_hours = max(4, (min_candles * candle_minutes / 60) + 1)
    start = now - timedelta(hours=lookback_hours)
    try:
        candles = fetch_candles(spec, start, now)
    except Exception as exc:
        _log.warning('predict_latest: fetch_candles failed: %s', exc)
        return {'status': 'fetch_error'}

    # ── Blend with Gemini exchange candles (free secondary feed) ──────────
    # Gemini supports BTC, ETH, SOL (not XRP). When available, we blend
    # 60% Coinbase + 40% Gemini for a more robust price signal. Falls back
    # to Coinbase-only silently if Gemini is unavailable or unsupported.
    try:
        from .data_gemini import fetch_gemini_candles, blend_candles, is_symbol_supported
        if is_symbol_supported(cfg.symbol):
            gemini_candles = fetch_gemini_candles(cfg.symbol, cfg.granularity_seconds, start, now)
            if not gemini_candles.empty:
                candles = blend_candles(candles, gemini_candles)
                _log.debug('predict_latest: blended Coinbase+Gemini candles for %s', cfg.symbol)
    except Exception as _gem_exc:
        _log.debug('predict_latest: Gemini blend skipped: %s', _gem_exc)

    _log.info('predict_latest: fetched %d candles over %.1fh', len(candles), lookback_hours)

    # Grab the current candle's close BEFORE add_features (which drops the
    # last row because future_close = shift(-1) produces NaN).
    candle_price = float(candles.iloc[-1]['close']) if not candles.empty else 0.0

    # Best-effort live spot price from Coinbase ticker (more current than
    # the last completed candle close). Falls back to candle close.
    current_price = _fetch_spot_price(cfg.symbol) or candle_price

    # Append a duplicate of the last candle so add_features' shift(-1) doesn't
    # create NaN on the *actual* latest candle.  The fake row gets NaN in
    # future_close instead and is dropped by dropna(), leaving the real current
    # candle as feats.iloc[-1] with valid features.
    extended = pd.concat([candles, candles.iloc[[-1]]], ignore_index=True)

    # for_prediction=True skips y_up/deadband dropna — we only need feature columns
    feats = add_features(extended, chainlink_prices=None, for_prediction=True)
    if feats.empty or len(feats) < 10:
        _log.warning('predict_latest: only %d rows after features (need ≥10)', len(feats))
        return {'status': 'not_enough_data'}

    # For CNN-LSTM: pass last seq_len rows as DataFrame.
    # For logistic: pass just the latest row (Series).
    if hasattr(fit, 'backend') and fit.backend == 'cnn_lstm':
        tail = feats.iloc[-fit.seq_len:] if len(feats) >= fit.seq_len else feats
        p_up = predict_proba(fit, tail)
    else:
        latest = feats.iloc[-1]
        p_up = predict_proba(fit, latest)

    # ── Ensemble: blend model + live Chainlink delta ─────────────────
    # When the Chainlink feed is live and we have a window open price,
    # incorporate the observed price movement into the prediction.
    # This combines the model's predictive power with the oracle's
    # real-time directional information — the same data that resolves
    # the market.
    ensemble_source = 'model_only'
    ensemble_weight = getattr(cfg, 'ensemble_weight', 0.6)
    try:
        from .chainlink_feed import get_chainlink_price, get_chainlink_history
        from .window import current_window as _cw_inner
        import time as _t

        # Derive asset from symbol (e.g. 'SOL-USD' -> 'sol')
        _asset_key = (cfg.symbol or 'BTC-USD').split('-')[0].lower()
        _win = _cw_inner(now=_t.time(), asset=_asset_key)

        cl_now = get_chainlink_price(_asset_key)

        # Look up window-open price — try in-memory cache first (fastest),
        # then fall back to the persisted CSV so a fresh container or a second
        # instance sees the same value as the first.
        _key = (_asset_key, _win.start_ts)
        cl_open = _chainlink_window_open_cache.get(_key)
        if cl_open is None:
            try:
                _hist = get_chainlink_history(_asset_key)
                _row = _hist[_hist['ts'] == _win.start_ts]
                if not _row.empty:
                    cl_open = float(_row.iloc[-1]['price'])
                    # Warm the in-memory cache so subsequent calls are fast
                    _chainlink_window_open_cache[_key] = cl_open
            except Exception:
                cl_open = None

        if cl_now > 0 and cl_open and cl_open > 0:
            delta_pct = (cl_now - cl_open) / cl_open * 100
            # Convert delta to probability (same sigmoid as snipe)
            delta_p_up = 0.5 + 0.5 * math.tanh(delta_pct / 0.15)
            # Blend: weight × model + (1-weight) × delta
            p_up_raw = p_up
            p_up = ensemble_weight * p_up + (1.0 - ensemble_weight) * delta_p_up
            ensemble_source = 'model+chainlink'
            _log.info(
                'predict_latest: ensemble blend asset=%s model=%.4f delta_p=%.4f '
                '(delta=%.4f%%) → blended=%.4f (weight=%.2f)',
                _asset_key, p_up_raw, delta_p_up, delta_pct, p_up, ensemble_weight,
            )
    except Exception as exc:
        _log.debug('predict_latest: ensemble blend skipped: %s', exc)

    direction = 'UP' if p_up >= cfg.direction_threshold else 'DOWN'
    confidence = p_up if direction == 'UP' else 1.0 - p_up
    strong = confidence >= cfg.confidence_threshold

    return {
        'status': 'ok',
        'direction': direction,
        'p_up': p_up,
        'confidence': confidence,
        'strong': strong,
        'price': current_price,
        'ensemble_source': ensemble_source,
    }


def _sanitize_for_json(obj):
    """Recursively replace NaN/Inf floats with None so json.dumps never
    writes invalid JSON (which json.loads would then reject, wiping state)."""
    if isinstance(obj, float):
        import math
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def save_state(state: dict) -> None:
    STATE_STORE.save(_sanitize_for_json(state))


# ── Snipe (late-entry) prediction based on window delta ──────────────

# Per-window Chainlink price cache: maps (asset, window_start_ts) → open price.
# Populated the first time we see a Chainlink price after window start.
_chainlink_window_open_cache: dict[tuple[str, int], float] = {}
_CHAINLINK_CACHE_MAX = 50  # keep last N entries to avoid unbounded growth


def _record_chainlink_window_open(window_start_ts: int, asset: str = 'btc') -> float | None:
    """Cache and return the Chainlink price at window open for *asset*.

    Called once per window per asset, early in the window, to lock in the
    open price that Chainlink will use for resolution comparison.
    Also persists the price to the Chainlink history CSV for future training.
    """
    from .chainlink_feed import get_chainlink_price, record_window_price
    a = asset.lower()
    key = (a, window_start_ts)
    if key in _chainlink_window_open_cache:
        return _chainlink_window_open_cache[key]
    cl = get_chainlink_price(a)
    if cl <= 0:
        return None
    _chainlink_window_open_cache[key] = cl
    # Persist to disk for hybrid training target.
    record_window_price(window_start_ts, cl, asset=a)
    # Evict old entries.
    while len(_chainlink_window_open_cache) > _CHAINLINK_CACHE_MAX:
        oldest = min(_chainlink_window_open_cache)
        del _chainlink_window_open_cache[oldest]
    return cl


def predict_snipe(cfg: C5Config, window, asset: str = 'btc') -> dict:
    """Late-entry prediction using the live price delta since window open.

    **Oracle-aligned** — uses the Chainlink oracle price for *asset* from
    Polymarket's RTDS WebSocket as the PRIMARY source.  This is the exact
    same data that Polymarket uses to resolve 5-minute UP/DOWN markets,
    eliminating the oracle mismatch that caused false signals when Coinbase
    diverged.

    Falls back to Coinbase if the Chainlink feed is unavailable.

    Instead of predicting the future from historical TA features, this
    observes *what already happened* during the current 5-minute window:
    it compares the current oracle price to the price at window open.

    If the asset already moved significantly (> snipe_min_delta_pct) from
    the window open price, it's extremely likely to stay in that direction
    through resolution.
    """
    import logging
    import math
    import time as _time
    from datetime import datetime, timezone, timedelta
    from .data_coinbase import fetch_candles, CoinbaseCandleSpec
    from .chainlink_feed import get_chainlink_price, is_feed_healthy, compute_basis

    _log = logging.getLogger(__name__)
    a = asset.lower()

    # ── 1. Get the current live spot price (prefer Chainlink) ────────
    oracle_source = 'chainlink'
    spot = get_chainlink_price(a)
    if spot <= 0:
        # Fallback to Coinbase spot.
        oracle_source = 'coinbase_fallback'
        spot = _fetch_spot_price(cfg.symbol)
        _log.warning('predict_snipe[%s]: Chainlink unavailable, falling back to Coinbase spot=%.2f', a, spot)
    if spot <= 0:
        _log.warning('predict_snipe[%s]: spot price unavailable from all sources', a)
        return {'status': 'spot_error', 'reason_code': 'no_spot_price', 'asset': a}

    # ── 2. Get the window open price (prefer Chainlink cache) ────────
    key = (a, window.start_ts)
    window_open = _chainlink_window_open_cache.get(key)
    open_source = 'chainlink' if window_open else None

    # If we don't have a Chainlink open, try to record one now.
    if window_open is None:
        cl_open = _record_chainlink_window_open(window.start_ts, asset=a)
        if cl_open and cl_open > 0:
            window_open = cl_open
            open_source = 'chainlink_late'

    # Final fallback: Coinbase candle open.
    if window_open is None or window_open <= 0:
        open_source = 'coinbase_candle'
        try:
            spec = CoinbaseCandleSpec(
                symbol=cfg.symbol,
                granularity_seconds=cfg.granularity_seconds,
            )
            win_start_dt = datetime.fromtimestamp(window.start_ts, tz=timezone.utc)
            candles = fetch_candles(
                spec,
                win_start_dt - timedelta(seconds=cfg.granularity_seconds),
                datetime.now(timezone.utc),
            )
        except Exception as exc:
            _log.warning('predict_snipe: candle fetch failed: %s', exc)
            return {'status': 'fetch_error', 'reason_code': 'candle_fetch_failed'}

        if candles.empty:
            _log.warning('predict_snipe: no candles returned')
            return {'status': 'no_candles', 'reason_code': 'no_coinbase_candles'}

        # Find the candle whose time matches the window start.
        window_open = None
        for _, row in candles.iterrows():
            try:
                row_ts = int(datetime.fromisoformat(str(row['time']).replace('Z', '+00:00')).timestamp())
            except Exception:
                continue
            if row_ts == window.start_ts:
                window_open = float(row['open'])
                break
        if window_open is None:
            window_open = float(candles.iloc[-1]['open'])
            _log.info('predict_snipe: exact window candle not found, using latest open=%.2f', window_open)

    if not window_open or window_open <= 0:
        return {'status': 'bad_open_price', 'reason_code': 'zero_or_negative_open'}

    # ── 3. Compute basis (Coinbase vs Chainlink divergence) ──────────
    coinbase_spot = _fetch_spot_price(cfg.symbol)
    basis = compute_basis(coinbase_spot, asset=a)

    # ── 4. Compute window delta using oracle price ───────────────────
    delta_pct = (spot - window_open) / window_open * 100  # e.g. 0.05 = 0.05%
    abs_delta = abs(delta_pct)

    _log.info(
        'predict_snipe: source=%s open_source=%s window_open=%.2f spot=%.2f '
        'delta=%.4f%% threshold=%.4f%% basis=%.1fbps',
        oracle_source, open_source, window_open, spot, delta_pct,
        cfg.snipe_min_delta_pct,
        basis.get('basis_bps', 0),
    )

    # ── 5. If delta is below threshold, no strong signal — skip ──────
    if abs_delta < cfg.snipe_min_delta_pct:
        return {
            'status': 'below_threshold',
            'reason_code': 'delta_below_threshold',
            'asset': a,
            'delta_pct': delta_pct,
            'window_open': window_open,
            'spot': spot,
            'oracle_source': oracle_source,
            'open_source': open_source,
            'basis': basis,
        }

    # ── 6. Direction & confidence ────────────────────────────────────
    direction = 'UP' if delta_pct > 0 else 'DOWN'

    # Sigmoid-like scaling: confidence = 0.5 + 0.5 * tanh(abs_delta / 0.15)
    #    At delta=0.02% → confidence ~0.57
    #    At delta=0.10% → confidence ~0.82
    #    At delta=0.30% → confidence ~0.95 (capped)
    raw_conf = 0.5 + 0.5 * math.tanh(abs_delta / 0.15)
    confidence = min(raw_conf, 0.98)  # cap at 98%

    p_up = confidence if direction == 'UP' else 1.0 - confidence

    return {
        'status': 'ok',
        'asset': a,
        'direction': direction,
        'p_up': p_up,
        'confidence': confidence,
        'strong': True,
        'price': spot,
        'snipe': True,
        'window_open': window_open,
        'delta_pct': delta_pct,
        'oracle_source': oracle_source,
        'open_source': open_source,
        'basis': basis,
    }
