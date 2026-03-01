"""Order reconciliation for Polymarket CLOB trades.

Why this exists:
- Posting an order is not the same as getting filled.
- For a sellable product we need a persistent, debuggable notion of order status.

This module is best-effort and intentionally conservative:
- Only runs in live mode (enabled and not dry_run)
- Only touches recent trades
- Can auto-cancel stale orders after a timeout

It updates `logs/poly_trades.json` in-place.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .persistence import JsonStore
from .polymarket_exec import PolyExecConfig

logger = logging.getLogger(__name__)

TRADES_STORE = JsonStore(Path('logs') / 'poly_trades.json')


_TERMINAL_STATUSES = {
    'filled',
    'canceled',
    'cancelled',
    'expired',
    'rejected',
    'failed',
}


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _get_str(d: Any, *keys: str) -> str:
    if not isinstance(d, dict):
        return ''
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ''


def _get_any(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _normalize_status(raw: str) -> str:
    s = (raw or '').strip().lower()
    if not s:
        return 'unknown'
    # common aliases
    if s in {'cancelled'}:
        return 'canceled'
    if s in {'matched', 'complete', 'completed'}:
        return 'filled'
    if s in {'open', 'live', 'active'}:
        return 'open'
    if s in {'partial', 'partially_filled', 'partially-filled', 'partial_fill'}:
        return 'partial'
    return s


def _extract_filled_size(order: Any, fallback: float = 0.0) -> float:
    if not isinstance(order, dict):
        return float(fallback)
    v = _get_any(
        order,
        'filled_size',
        'filledSize',
        'filled',
        'executed_size',
        'executedSize',
        'matchedSize',
        'sizeMatched',
        'matched_size',
        'size_matched',
        'matched',
    )
    if v is not None:
        return max(0.0, _to_f(v, float(fallback)))
    # Some APIs only give remaining size.
    remaining = _get_any(order, 'remaining_size', 'remainingSize', 'sizeRemaining')
    total = _get_any(order, 'size', 'original_size', 'originalSize')
    if remaining is not None and total is not None:
        rem = max(0.0, _to_f(remaining, 0.0))
        tot = max(0.0, _to_f(total, 0.0))
        if tot > 0:
            return max(0.0, tot - rem)
    return float(fallback)


def _extract_avg_fill_price(order: Any, fallback: float = 0.0) -> float:
    if not isinstance(order, dict):
        return float(fallback)
    v = _get_any(order, 'avg_fill_price', 'avgFillPrice', 'average_price', 'averagePrice', 'avg_price', 'avgPrice')
    if v is None:
        return float(fallback)
    px = _to_f(v, float(fallback))
    if px <= 0:
        return float(fallback)
    return float(px)


@dataclass
class ReconcileResult:
    ok: bool
    scanned: int
    updated: int
    canceled: int
    errors: int
    message: str = ''

    def as_dict(self) -> dict[str, Any]:
        return {
            'ok': self.ok,
            'scanned': self.scanned,
            'updated': self.updated,
            'canceled': self.canceled,
            'errors': self.errors,
            'message': self.message,
        }


def reconcile_recent_orders(
    cfg: PolyExecConfig,
    *,
    max_trades: int = 50,
    max_age_hours: float = 24.0,
) -> ReconcileResult:
    """Reconcile order status for recent trades.

    Updates trades in `logs/poly_trades.json`.
    """

    if not (cfg.enabled and (not cfg.dry_run)):
        return ReconcileResult(ok=True, scanned=0, updated=0, canceled=0, errors=0, message='not_live_mode')

    if not cfg.private_key:
        return ReconcileResult(ok=False, scanned=0, updated=0, canceled=0, errors=1, message='missing_private_key')

    try:
        from py_clob_client.client import ClobClient  # type: ignore
        from py_clob_client.constants import POLYGON  # type: ignore
    except Exception as e:  # pragma: no cover
        return ReconcileResult(ok=False, scanned=0, updated=0, canceled=0, errors=1, message=f'missing_py_clob_client: {e}')

    client = ClobClient(
        cfg.clob_url,
        key=cfg.private_key,
        chain_id=POLYGON,
        signature_type=cfg.signature_type,
        funder=cfg.funder,
    )
    from ._clob_auth import derive_api_creds_with_retry
    derive_api_creds_with_retry(client)

    now = int(time.time())
    max_age_sec = int(max_age_hours * 3600)

    loaded = TRADES_STORE.load(default=[]) or []
    if not isinstance(loaded, list):
        return ReconcileResult(ok=True, scanned=0, updated=0, canceled=0, errors=0, message='no_trades')

    trades: list[dict] = [t for t in loaded if isinstance(t, dict)]

    scanned = 0
    updated = 0
    canceled = 0
    errors = 0
    any_change = False

    # Newest-first scan.
    for t in reversed(trades):
        if scanned >= int(max_trades):
            break
        if t.get('dry_run'):
            continue
        order_id = str(t.get('order_id') or '').strip()
        if not order_id:
            continue

        placed_ts = int(_to_f(t.get('placed_ts') or t.get('ts') or 0, 0.0))
        if placed_ts and (now - placed_ts) > max_age_sec:
            continue

        status = _normalize_status(str(t.get('order_status') or ''))
        # Allow re-querying 'filled' orders that still have filled_size=0
        # so we can extract fill data from the CLOB API.
        needs_fill_data = (status == 'filled' and _to_f(t.get('filled_size'), 0.0) <= 0)
        if status in _TERMINAL_STATUSES and not needs_fill_data:
            continue

        scanned += 1
        try:
            order = client.get_order(order_id)
        except Exception as e:
            errors += 1
            t['last_reconciled_ts'] = now
            t['order_status'] = 'unknown'
            t['reconcile_error'] = str(e)
            any_change = True
            continue

        raw_status = _get_str(order, 'status', 'state', 'order_status', 'orderStatus')
        new_status = _normalize_status(raw_status)

        prev_filled = _to_f(t.get('filled_size'), 0.0)
        new_filled = _extract_filled_size(order, fallback=prev_filled)
        new_avg_px = _extract_avg_fill_price(order, fallback=_to_f(t.get('avg_fill_price'), 0.0))

        # Fallback: some client/API variants don't include filled size on
        # get_order, but our stored post_order response often contains
        # takingAmount/makingAmount for immediate matches.
        if new_filled <= 0.0:
            resp = t.get('response')
            if isinstance(resp, dict):
                taking = _to_f(
                    resp.get('takingAmount')
                    or resp.get('takerAmount')
                    or resp.get('taking_amount')
                    or resp.get('taker_amount'),
                    0.0,
                )
                if taking > 0:
                    new_filled = float(taking)

        if new_avg_px <= 0.0:
            resp = t.get('response')
            if isinstance(resp, dict):
                taking = _to_f(
                    resp.get('takingAmount')
                    or resp.get('takerAmount')
                    or resp.get('taking_amount')
                    or resp.get('taker_amount'),
                    0.0,
                )
                making = _to_f(
                    resp.get('makingAmount')
                    or resp.get('makerAmount')
                    or resp.get('making_amount')
                    or resp.get('maker_amount'),
                    0.0,
                )
                if taking > 0 and making > 0:
                    px = float(making) / float(taking)
                    if 0.0 <= px <= 1.0:
                        new_avg_px = float(px)

        # ── Override: if the original post_order response had a fill signal
        # (takingAmount > 0 or status='matched'), the order DID fill on-chain
        # even if the CLOB later reports it as 'canceled'. This happens when
        # the order was fully consumed and the market resolved.
        resp_for_override = t.get('response')
        if new_status in ('canceled', 'cancelled', 'canceled_market_resolved') and isinstance(resp_for_override, dict):
            resp_taking = _to_f(
                resp_for_override.get('takingAmount')
                or resp_for_override.get('takerAmount')
                or resp_for_override.get('taking_amount'),
                0.0,
            )
            resp_st = str(resp_for_override.get('status') or '').strip().lower()
            if resp_taking > 0 or resp_st == 'matched':
                new_status = 'filled'
                if resp_taking > 0 and new_filled <= 0:
                    new_filled = float(resp_taking)
                logger.info(
                    'Reconcile override: order %s was canceled but response shows fill '
                    '(takingAmount=%.4f, status=%s) — marking as filled',
                    order_id, resp_taking, resp_st,
                )

        t['order_status'] = new_status
        t['filled_size'] = float(new_filled)
        t['avg_fill_price'] = float(new_avg_px)
        t['last_reconciled_ts'] = now
        t.pop('reconcile_error', None)

        # If still open/partial, consider auto-cancel by timeout.
        if cfg.auto_cancel_stale and new_status in {'open', 'partial', 'unknown'}:
            timeout = max(10, int(cfg.order_timeout_seconds or 0))
            if placed_ts and (now - placed_ts) > timeout:
                try:
                    client.cancel(order_id)
                    t['order_status'] = 'canceled'
                    t['cancel_reason'] = 'timeout'
                    canceled += 1
                except Exception as e:
                    # If cancel fails, keep status but record the error.
                    errors += 1
                    t['cancel_error'] = str(e)

        updated += 1
        any_change = True

    if any_change:
        # Save the whole list (bounded elsewhere); keep existing length.
        TRADES_STORE.save(trades)

    return ReconcileResult(ok=True, scanned=scanned, updated=updated, canceled=canceled, errors=errors)
