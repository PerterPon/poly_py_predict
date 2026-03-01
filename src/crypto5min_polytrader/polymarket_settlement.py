"""Best-effort post-resolution cleanup (API-only).

Important: this is NOT on-chain redemption/claim.

What it does:
- Detect resolved trades (win/loss) from `logs/poly_trades.json`.
- If we still appear to hold the resolved outcome token, attempt to close it via
  CLOB SELL orders (same mechanism as the existing "sell all" ops).

Why:
- The repo currently doesn't implement on-chain redeem/claim.
- Closing residual exposure helps keep bankroll sane and avoids holding dead
  tokens indefinitely.

This module is designed to be safe:
- Disabled by default in paper mode (it becomes a no-op).
- In dry-run it only returns planned orders.
- Retries are rate-limited so we don't spam the CLOB.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

from .persistence import JsonStore
from .polymarket_ops import close_positions_by_token_ids_from_env

logger = logging.getLogger(__name__)

TRADES_STORE = JsonStore(Path('logs') / 'poly_trades.json')
OPS_STORE = JsonStore(Path('logs') / 'poly_ops.json')


def _getenv(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _getbool(name: str, default: bool = False) -> bool:
    v = _getenv(name)
    if not v:
        return default
    return v.lower() in {'1', 'true', 'yes', 'y', 'on'}


def _getint(name: str, default: int) -> int:
    v = _getenv(name)
    if not v:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def process_resolved_trades(*, dry_run: bool) -> dict[str, Any]:
    """Attempt to close any remaining exposure for resolved trades.

    Returns a small summary dict.
    """

    if not _getbool('C5_POLY_AUTO_CLOSE_RESOLVED', True):
        return {'ok': True, 'skipped': True, 'reason': 'auto_close_disabled'}

    now = int(time.time())
    retry_minutes = max(5, _getint('C5_POLY_SETTLE_RETRY_MINUTES', 60))
    retry_sec = retry_minutes * 60
    max_trades = max(1, _getint('C5_POLY_MAX_SETTLE_TRADES_PER_RUN', 10))

    loaded = TRADES_STORE.load(default=[]) or []
    if not isinstance(loaded, list):
        return {'ok': True, 'skipped': True, 'reason': 'no_trades'}

    trades: list[dict] = [t for t in loaded if isinstance(t, dict)]

    # Pick resolved trades that have token_id and either never attempted
    # settlement, or are past the retry window.
    token_ids: set[str] = set()
    selected_idx: list[int] = []

    for idx, t in enumerate(trades):
        res = t.get('resolved')
        if res not in {'win', 'loss'}:
            continue
        tid = str(t.get('token_id') or '').strip()
        if not tid:
            continue
        last = int(_to_f(t.get('settlement_attempted_ts') or 0, 0.0))
        if last and (now - last) < retry_sec:
            continue
        token_ids.add(tid)
        selected_idx.append(idx)
        if len(selected_idx) >= max_trades:
            break

    if not token_ids:
        return {'ok': True, 'skipped': True, 'reason': 'nothing_to_settle'}

    # Attempt close (SELL) for these token ids.
    result = close_positions_by_token_ids_from_env(token_ids=token_ids, dry_run=dry_run)

    # Mark attempts on the selected trades.
    for idx in selected_idx:
        try:
            trades[idx]['settlement_attempted_ts'] = now
            trades[idx]['settlement_dry_run'] = bool(dry_run)
            # Keep this small; store only high-level outcome.
            trades[idx]['settlement_result'] = {
                'ok': bool(result.get('ok', False)),
                'orders_attempted': int(result.get('orders_attempted', 0) or 0),
                'orders_planned': int(len(result.get('orders_planned', []) or [])) if isinstance(result.get('orders_planned'), list) else 0,
            }
        except Exception:
            pass

    TRADES_STORE.save(trades)

    # Also append an ops log entry so operators can see it without digging.
    try:
        ops = OPS_STORE.load(default=[]) or []
        if not isinstance(ops, list):
            ops = []
        ops.append({'ts': now, 'action': 'settle_close_resolved', 'dry_run': bool(dry_run), 'token_ids': sorted(token_ids)[:25], 'result': result})
        ops = ops[-200:]
        OPS_STORE.save(ops)
    except Exception:
        pass

    return {'ok': True, 'dry_run': bool(dry_run), 'token_ids': len(token_ids), 'result': result}
