"""Win/loss resolution for 5-minute BTC Up/Down markets.

Polls Gamma API to check whether past trades resolved as wins or losses,
based on the market's winning outcome vs. the direction we traded.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .persistence import JsonStore

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

logger = logging.getLogger(__name__)

TRADES_STORE = JsonStore(Path('logs') / 'poly_trades.json')


def _is_order_filled(trade: dict) -> bool:
    """Return True if the trade order was actually filled on the CLOB.

    Ghost trades (order canceled before fill, market resolved before fill)
    should NOT count as real wins/losses since no position was ever held.
    """
    status = str(trade.get('order_status') or '').strip().lower()
    if status in ('filled', 'matched'):
        return True
    # Check response for fill signals:
    # - takingAmount > 0 means shares were received
    # - status='matched' means the CLOB matched the order on post
    resp = trade.get('response')
    if isinstance(resp, dict):
        resp_status = str(resp.get('status') or '').strip().lower()
        if resp_status == 'matched':
            return True
        try:
            taking = float(resp.get('takingAmount') or 0)
            if taking > 0:
                return True
        except (ValueError, TypeError):
            pass
    # Check filled_size from reconciliation.
    try:
        if float(trade.get('filled_size') or 0) > 0:
            return True
    except (ValueError, TypeError):
        pass
    return False


def _fetch_market(gamma_url: str, slug: str) -> dict | None:
    if requests is None:
        return None
    try:
        r = requests.get(f'{gamma_url}/markets', params={'slug': slug}, timeout=15)
        r.raise_for_status()
        markets = r.json() or []
        if not markets:
            return None
        return markets[0] if isinstance(markets, list) else markets
    except Exception:
        return None


def _parse_json_list(val: Any) -> list[str]:
    import json
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val]
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except Exception:
            pass
    return []


def _winning_outcome(market: dict) -> str | None:
    """Return the winning outcome label if the market has resolved, else None."""

    if not market.get('closed', False):
        return None

    outcomes = _parse_json_list(market.get('outcomes'))
    # Gamma uses "winnerOutcome" or per-outcome price settling to 1.0.
    winner = market.get('winnerOutcome')
    if winner and str(winner).strip():
        return str(winner).strip()

    # Fallback: check if any outcome price settled to ~1.0.
    prices = _parse_json_list(market.get('outcomePrices'))
    if prices and outcomes and len(prices) == len(outcomes):
        for i, p in enumerate(prices):
            try:
                if float(p) >= 0.95:
                    return outcomes[i]
            except (ValueError, TypeError):
                pass

    return None


def check_resolutions(gamma_url: str = 'https://gamma-api.polymarket.com') -> int:
    """Poll Gamma for resolved 5-min markets and update trade records.

    Returns the number of trades newly resolved.
    """

    trades = TRADES_STORE.load(default=[]) or []
    if not isinstance(trades, list):
        return 0

    resolved_count = 0
    checked = 0

    # Iterate newest-first so recent pending trades get priority over old
    # ghost trades (win_unfilled) that will never change.
    for trade in reversed(trades):
        if not isinstance(trade, dict):
            continue
        slug = trade.get('window_slug')
        if not slug:
            continue
        existing_resolved = trade.get('resolved')
        # Resolutions are typically one-shot, but we *do* allow upgrading
        # ghost states (win_unfilled/loss_unfilled) if reconciliation later
        # shows the order actually filled.
        if existing_resolved and existing_resolved not in ('win_unfilled', 'loss_unfilled'):
            continue

        # For win_unfilled/loss_unfilled: only re-check if the order has
        # since been detected as filled (otherwise they'll never change
        # and just waste API calls + eat into the check budget).
        if existing_resolved in ('win_unfilled', 'loss_unfilled'):
            if not _is_order_filled(trade):
                continue

        if checked >= 50:
            break
        checked += 1

        market = _fetch_market(gamma_url, slug)
        if market is None:
            continue

        winner = _winning_outcome(market)
        if winner is None:
            continue

        direction = str(trade.get('direction', '')).strip().upper()
        # Compare: we bought "Up" or "Down"; winner is the resolved outcome.
        traded_outcome = 'Up' if direction == 'UP' else 'Down'

        prediction_correct = traded_outcome.lower() == winner.lower()
        # A successful on-chain redemption proves the order was filled,
        # even if filled_size was never reconciled back to the record.
        redeemed = trade.get('redeem_status') == 'success'
        filled = _is_order_filled(trade) or redeemed

        if prediction_correct:
            new_resolved = 'win' if filled else 'win_unfilled'
        else:
            new_resolved = 'loss' if filled else 'loss_unfilled'

        # If it's already resolved and the outcome doesn't change, don't bump
        # counters or write timestamps.
        if existing_resolved == new_resolved:
            continue

        trade['resolved'] = new_resolved

        if not filled:
            logger.info(
                'Trade %s resolved as %s (order_status=%s, no position held)',
                trade.get('window_slug', '?'),
                trade['resolved'],
                trade.get('order_status', '?'),
            )
        elif existing_resolved in ('win_unfilled', 'loss_unfilled'):
            logger.info(
                'Trade %s upgraded from %s to %s (fill detected)',
                trade.get('window_slug', '?'),
                existing_resolved,
                trade['resolved'],
            )

        trade['resolved_outcome'] = winner
        try:
            import time as _time

            trade['resolved_ts'] = int(_time.time())
        except Exception:
            pass
        resolved_count += 1

    if resolved_count > 0:
        TRADES_STORE.save(trades)

    return resolved_count


def load_stats() -> dict:
    """Return win/loss/pending stats from the trades log."""

    trades = TRADES_STORE.load(default=[]) or []
    if not isinstance(trades, list):
        return {'wins': 0, 'losses': 0, 'pending': 0, 'win_rate': 0.0, 'total': 0}

    wins = 0
    losses = 0
    pending = 0
    wins_unfilled = 0
    losses_unfilled = 0
    recent: list[str] = []  # last 20 results

    for t in trades:
        if not isinstance(t, dict):
            continue
        if not t.get('window_slug'):
            continue
        r = t.get('resolved')
        if r == 'win':
            wins += 1
            recent.append('win')
        elif r == 'win_unfilled':
            wins_unfilled += 1
            recent.append('ghost')
        elif r == 'loss':
            losses += 1
            recent.append('loss')
        elif r == 'loss_unfilled':
            losses_unfilled += 1
            recent.append('ghost')
        else:
            pending += 1
            recent.append('pending')

    # Real win rate: only filled trades count.
    total_filled = wins + losses
    win_rate = (wins / total_filled * 100) if total_filled > 0 else 0.0

    # Total including ghosts (for backward compat).
    total = wins + losses + wins_unfilled + losses_unfilled

    return {
        'wins': wins,
        'losses': losses,
        'pending': pending,
        'wins_unfilled': wins_unfilled,
        'losses_unfilled': losses_unfilled,
        'total': total,
        'total_filled': total_filled,
        'win_rate': round(win_rate, 1),
        'recent': recent[-20:],
    }
