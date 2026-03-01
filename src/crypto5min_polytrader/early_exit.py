"""Early exit manager for open Polymarket positions.

Uses a TRAILING STOP for take-profit and a FIXED FLOOR for stop-loss.

How it works:
  - Every ~30s the monitor fetches the live bid for each open position.
  - It tracks the highest bid seen since entry (the "peak").
  - Take-profit fires when the bid DROPS back from the peak by trail_pct%.
    e.g. trail_pct=15: bid ran to $0.90, trail stop = $0.90 * 0.85 = $0.765
    If bid falls to $0.765 -> sell, locking in most of the gain.
  - Stop-loss fires when bid falls below entry by sl_pct% (hard floor).
    e.g. sl_pct=50, entry=$0.52 -> sell if bid <= $0.26.
  - The trailing stop only activates once bid has risen by at least
    trail_activate_pct% above entry (default 10%) to avoid thrashing.

Configured via runtime overrides (dashboard) or .env:
  C5_EARLY_EXIT_ENABLED        = true/false  (default: false)
  C5_EARLY_EXIT_TRAIL_PCT      = 15.0        (trail drop %, default 15)
  C5_EARLY_EXIT_TRAIL_ACT_PCT  = 5.0         (min gain before trail activates, default 5)
  C5_EARLY_EXIT_SL_PCT         = 25.0        (hard stop loss %, default 25)

Peak tracking is persisted to logs/early_exit_peaks.json so it survives restarts.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

from .persistence import JsonStore

logger = logging.getLogger(__name__)

TRADES_STORE = JsonStore(Path('logs') / 'poly_trades.json')
PEAKS_STORE  = JsonStore(Path('logs') / 'early_exit_peaks.json')


def _getfloat(key: str, default: float) -> float:
    try:
        return float(os.getenv(key) or default)
    except Exception:
        return default


def _getbool(key: str, default: bool = False) -> bool:
    v = (os.getenv(key) or '').strip().lower()
    if v in ('1', 'true', 'yes'):
        return True
    if v in ('0', 'false', 'no'):
        return False
    return default


def _is_filled(trade: dict) -> bool:
    status = str(trade.get('order_status') or '').strip().lower()
    if status in ('filled', 'matched'):
        return True
    resp = trade.get('response')
    if isinstance(resp, dict):
        if str(resp.get('status') or '').lower() == 'matched':
            return True
        try:
            if float(resp.get('takingAmount') or 0) > 0:
                return True
        except (ValueError, TypeError):
            pass
    try:
        if float(trade.get('filled_size') or 0) > 0:
            return True
    except (ValueError, TypeError):
        pass
    if trade.get('redeem_status') == 'success':
        return True
    return False


def _open_positions(trades: list[dict]) -> list[dict]:
    out = []
    for t in trades:
        if not isinstance(t, dict):
            continue
        if t.get('dry_run'):
            continue
        resolved = t.get('resolved') or ''
        if resolved in ('win', 'loss', 'win_unfilled', 'loss_unfilled'):
            continue
        if t.get('early_exit'):
            continue
        if not _is_filled(t):
            continue
        if not t.get('token_id'):
            continue
        try:
            entry = float(t.get('price') or 0)
            if entry <= 0:
                continue
        except (ValueError, TypeError):
            continue
        try:
            shares = float(t.get('filled_size') or t.get('size') or 0)
            if shares <= 0:
                continue
        except (ValueError, TypeError):
            continue
        out.append(t)
    return out


def _fetch_bid(clob_url: str, token_id: str) -> float | None:
    try:
        from .polymarket_orderbook import fetch_orderbook_summary, best_bid as _best_bid
        book = fetch_orderbook_summary(clob_url=clob_url, token_id=token_id, timeout=3.0)
        if book is None:
            return None
        top_bid = _best_bid(book)
        if top_bid is None or top_bid.price <= 0:
            return None
        return float(top_bid.price)
    except Exception as exc:
        logger.debug('early_exit: bid fetch failed for %s: %s', token_id[:12], exc)
        return None


def _place_sell(client: Any, token_id: str, shares: float, bid_price: float, dry_run: bool = False) -> dict:
    if dry_run:
        return {'status': 'dry_run', 'skipped': True}
    try:
        from py_clob_client.order_builder.constants import SELL  # type: ignore
        from py_clob_client.clob_types import OrderArgs  # type: ignore
        sell_price = round(float(bid_price), 4)
        sell_size  = round(float(shares), 4)
        order = client.create_order(OrderArgs(
            price=sell_price,
            size=sell_size,
            side=SELL,
            token_id=token_id,
        ))
        resp = client.post_order(order)
        return {'status': 'ok', 'response': resp, 'sell_price': sell_price, 'sell_size': sell_size}
    except Exception as exc:
        logger.warning('early_exit: SELL order failed: %s', exc)
        return {'status': 'error', 'error': str(exc)}


def _load_peaks() -> dict:
    raw = PEAKS_STORE.load(default={}) or {}
    return raw if isinstance(raw, dict) else {}


def _save_peaks(peaks: dict) -> None:
    PEAKS_STORE.save(peaks)


def check_early_exits(
    *,
    client: Any,
    clob_url: str,
    dry_run: bool = False,
    overrides: dict | None = None,
) -> list[dict]:

    def _ovr(key: str, default: float) -> float:
        if overrides and key in overrides:
            try:
                return float(overrides[key])
            except Exception:
                pass
        return _getfloat(key, default)

    def _ovr_bool(key: str, default: bool) -> bool:
        if overrides and key in overrides:
            v = str(overrides[key]).strip().lower()
            if v in ('1', 'true', 'yes'):
                return True
            if v in ('0', 'false', 'no'):
                return False
        return _getbool(key, default)

    enabled = _ovr_bool('C5_EARLY_EXIT_ENABLED', False)
    if not enabled:
        return []

    trail_pct     = _ovr('C5_EARLY_EXIT_TRAIL_PCT',     15.0)
    trail_act_pct = _ovr('C5_EARLY_EXIT_TRAIL_ACT_PCT', 5.0)
    sl_pct        = _ovr('C5_EARLY_EXIT_SL_PCT',        25.0)

    now       = time.time()
    trades    = _load_trades()
    peaks     = _load_peaks()
    positions = _open_positions(trades)

    if not positions:
        return []

    results        = []
    trades_updated = False
    peaks_updated  = False

    for trade in positions:
        token_id  = str(trade.get('token_id') or '')
        entry     = float(trade.get('price') or 0)
        shares    = float(trade.get('filled_size') or trade.get('size') or 0)
        direction = str(trade.get('direction') or '').upper()
        slug      = trade.get('window_slug', '?')

        if entry <= 0 or shares <= 0 or not token_id:
            continue

        bid = _fetch_bid(clob_url, token_id)
        if bid is None or bid <= 0:
            continue

        # Update peak
        prev_peak = float(peaks.get(token_id) or entry)
        peak = max(prev_peak, bid)
        if peak != prev_peak:
            peaks[token_id] = round(peak, 6)
            peaks_updated = True

        pnl_pct       = (bid - entry) / entry * 100.0
        peak_gain_pct = (peak - entry) / entry * 100.0
        trail_stop    = peak * (1.0 - trail_pct / 100.0)
        sl_floor      = entry * (1.0 - sl_pct / 100.0)
        trail_active  = peak_gain_pct >= trail_act_pct

        trigger = None
        if bid <= sl_floor:
            trigger = 'stop_loss'
        elif trail_active and bid <= trail_stop:
            trigger = 'trailing_stop'

        logger.info(
            'early_exit check: %s %s entry=%.4f bid=%.4f peak=%.4f '
            'pnl=%.1f%% peak_gain=%.1f%% trail_stop=%.4f sl=%.4f trail_active=%s -> %s',
            slug, direction, entry, bid, peak,
            pnl_pct, peak_gain_pct, trail_stop, sl_floor, trail_active,
            trigger or 'hold',
        )

        row = {
            'slug':          slug,
            'direction':     direction,
            'entry':         entry,
            'bid':           bid,
            'peak':          round(peak, 4),
            'pnl_pct':       round(pnl_pct, 2),
            'peak_gain_pct': round(peak_gain_pct, 2),
            'trail_stop':    round(trail_stop, 4),
            'sl_floor':      round(sl_floor, 4),
            'trail_active':  trail_active,
            'trigger':       trigger,
            'success':       False,
            'dry_run':       dry_run,
        }

        if trigger is None:
            results.append(row)
            continue

        sell_result = _place_sell(client, token_id, shares, bid, dry_run=dry_run)
        success     = sell_result.get('status') in ('ok', 'dry_run')
        row['success'] = success

        if success:
            trade['early_exit']          = True
            trade['early_exit_trigger']  = trigger
            trade['early_exit_bid']      = round(bid, 4)
            trade['early_exit_peak']     = round(peak, 4)
            trade['early_exit_pnl_pct']  = round(pnl_pct, 2)
            trade['early_exit_ts']       = int(now)
            trade['early_exit_response'] = sell_result.get('response')
            trade['resolved'] = 'win' if trigger == 'trailing_stop' else 'loss'
            trades_updated = True
            peaks.pop(token_id, None)
            peaks_updated = True

            logger.info(
                'early_exit: EXITED %s %s bid=%.4f peak=%.4f pnl=%.1f%% trigger=%s dry_run=%s',
                slug, direction, bid, peak, pnl_pct, trigger, dry_run,
            )

        results.append(row)

    if trades_updated:
        TRADES_STORE.save(trades)
    if peaks_updated:
        _save_peaks(peaks)

    return results


def _load_trades() -> list[dict]:
    trades = TRADES_STORE.load(default=[]) or []
    if not isinstance(trades, list):
        return []
    return [t for t in trades if isinstance(t, dict)]


def load_stats() -> dict:
    trades  = _load_trades()
    exits   = [t for t in trades if t.get('early_exit')]
    tp      = sum(1 for t in exits if t.get('early_exit_trigger') == 'trailing_stop')
    sl      = sum(1 for t in exits if t.get('early_exit_trigger') == 'stop_loss')
    avg_pnl = 0.0
    if exits:
        pnls    = [float(t.get('early_exit_pnl_pct') or 0) for t in exits]
        avg_pnl = round(sum(pnls) / len(pnls), 2)
    return {
        'total':         len(exits),
        'trailing_stop': tp,
        'stop_loss':     sl,
        'avg_pnl_pct':   avg_pnl,
    }
