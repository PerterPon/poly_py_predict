"""Operator actions for Polymarket (advanced).

These endpoints are intentionally guarded:
- must be authenticated
- must be explicitly enabled by env flags
- support dry-run

Currently supported:
- close/sell all positions (best-effort)

Notes:
- We use Polymarket's public data-api to enumerate positions.
- Actual order placement uses py-clob-client.
- This is not a perfect 'market sell' implementation; CLOB uses limit orders.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

from .polymarket_account import derive_address, fetch_positions


def _getenv(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _getbool(name: str, default: bool = False) -> bool:
    v = _getenv(name)
    if not v:
        return default
    return v.lower() in {'1', 'true', 'yes', 'y', 'on'}


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _token_id_from_position(p: dict) -> Optional[str]:
    for k in (
        'clobTokenId',
        'clob_token_id',
        'clobTokenID',
        'tokenId',
        'token_id',
        'assetId',
        'asset_id',
        'asset',
    ):
        v = p.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def _size_from_position(p: dict) -> float:
    # Common candidate fields; data-api has changed over time.
    for k in (
        'size',
        'shares',
        'quantity',
        'positionSize',
        'position_size',
        'balance',
    ):
        v = p.get(k)
        if v is None:
            continue
        f = _to_f(v, 0.0)
        if f > 0:
            return f

    # Fallback: derive from value/price
    value = _to_f(p.get('currentValue'), 0.0)
    price = _to_f(p.get('currentPrice'), 0.0)
    if value > 0 and price > 0:
        return value / price
    return 0.0


def _price_for_sell(p: dict, *, slip: float = 0.02) -> float:
    # Try to use currentPrice if available; otherwise a conservative mid.
    px = _to_f(p.get('currentPrice'), 0.5)
    px = max(0.01, min(0.99, px))
    # To get filled, selling typically needs to be <= bestBid.
    # We don't always have bestBid from data-api, so we shave a bit.
    px = max(0.01, min(0.99, px * (1.0 - abs(float(slip)))))
    return float(px)


def _is_redeemable_position(p: dict) -> bool:
    """Return True if a position is already resolved and should be redeemed.

    In Polymarket fast markets, once the market resolves the CLOB orderbook can
    disappear ("orderbook does not exist"). In that case, attempting to SELL is
    the wrong operator action — you must redeem on-chain.

    data-api commonly includes:
      - redeemable: true
      - curPrice: 1
    """

    if not isinstance(p, dict):
        return False
    if bool(p.get('redeemable')):
        return True
    # Best-effort: treat "curPrice" == 1 as resolved.
    if _to_f(p.get('curPrice'), 0.0) >= 0.999:
        return True
    return False


def _plan_close_orders(*, positions: list[dict], max_positions: int, slip: float) -> dict:
    """Pure planning helper for close_all/close_by_token_ids.

    Returns:
      - active: list of active positions considered
      - planned: list of sell orders to attempt
      - redeemable: list of positions that should be redeemed (not sold)
    """

    active = [p for p in (positions or []) if _to_f((p or {}).get('currentValue'), 0.0) > 0.01]
    active = active[: int(max_positions)]

    planned: list[dict] = []
    redeemable: list[dict] = []

    for p in active:
        p = p or {}

        if _is_redeemable_position(p):
            redeemable.append(
                {
                    'condition_id': (p.get('conditionId') or p.get('condition_id')),
                    'token_id': _token_id_from_position(p),
                    'current_value': _to_f(p.get('currentValue'), 0.0),
                    'cur_price': _to_f(p.get('curPrice'), 0.0),
                    'market': p.get('market') or p.get('question') or p.get('title'),
                    'hint': 'redeem_on_chain',
                }
            )
            continue

        token_id = _token_id_from_position(p)
        if not token_id:
            continue
        size = _size_from_position(p)
        if size <= 0:
            continue

        # If data-api doesn't provide a currentPrice, we can't even form a
        # reasonable limit price. Skip with a hint.
        cur_px = _to_f(p.get('currentPrice'), 0.0)
        if cur_px <= 0:
            planned.append(
                {
                    'token_id': token_id,
                    'size': float(size),
                    'price': None,
                    'current_value': _to_f(p.get('currentValue'), 0.0),
                    'current_price': float(cur_px),
                    'market': p.get('market') or p.get('question') or p.get('title'),
                    'skipped': True,
                    'reason': 'missing_current_price',
                }
            )
            continue

        price = _price_for_sell(p, slip=slip)
        planned.append(
            {
                'token_id': token_id,
                'size': float(size),
                'price': float(price),
                'current_value': _to_f(p.get('currentValue'), 0.0),
                'current_price': float(cur_px),
                'market': p.get('market') or p.get('question') or p.get('title'),
            }
        )

    return {'active': active, 'planned': planned, 'redeemable': redeemable}


def close_all_positions_from_env(*, dry_run: bool, max_positions: int = 40, slip: float = 0.02) -> dict:
    """Attempt to close all active positions by placing SELL orders.

    Returns a summary dict suitable for logging.

    Requires:
    - C5_POLY_PRIVATE_KEY for live mode
    """

    started = time.time()
    pk = _getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return {'ok': False, 'error': 'missing_private_key'}

    sig_type = int(_to_f(_getenv('C5_POLY_SIGNATURE_TYPE', '0'), 0.0))
    funder = _getenv('C5_POLY_FUNDER_ADDRESS', '') or None
    clob_url = _getenv('C5_POLY_CLOB_URL', 'https://clob.polymarket.com')

    address = derive_address(pk)
    positions = fetch_positions(address)

    plan = _plan_close_orders(positions=positions, max_positions=max_positions, slip=slip)
    active = plan['active']
    planned = plan['planned']
    redeemable = plan['redeemable']

    if dry_run:
        return {
            'ok': True,
            'dry_run': True,
            'address': address,
            'positions_seen': len(positions),
            'active_positions': len(active),
            'orders_planned': planned,
            'redeemable_positions': redeemable,
            'hint': 'If redeemable_positions is non-empty, those markets are resolved; use Redeem (on-chain) instead of Sell.',
            'duration_ms': int((time.time() - started) * 1000),
        }

    # Live: place SELL limit orders
    try:
        from py_clob_client.client import ClobClient  # type: ignore
        from py_clob_client.constants import POLYGON  # type: ignore
        from py_clob_client.order_builder.constants import SELL  # type: ignore
        from py_clob_client.clob_types import OrderArgs  # type: ignore
    except Exception as e:
        return {'ok': False, 'error': f'missing_py_clob_client: {e}'}

    from ._clob_auth import derive_api_creds_with_retry
    client = ClobClient(clob_url, key=pk, chain_id=POLYGON, signature_type=sig_type, funder=funder)
    derive_api_creds_with_retry(client)

    results: list[dict] = []
    for o in planned:
        if isinstance(o, dict) and o.get('skipped'):
            results.append({'ok': False, **o, 'error': 'skipped'})
            continue
        if not isinstance(o, dict) or o.get('price') is None:
            results.append({'ok': False, **(o if isinstance(o, dict) else {}), 'error': 'missing_price'})
            continue
        try:
            signed = client.create_order(OrderArgs(price=float(o['price']), size=float(o['size']), side=SELL, token_id=str(o['token_id'])))
            resp = client.post_order(signed)
            results.append({'ok': True, **o, 'response': resp})
        except Exception as e:
            results.append({'ok': False, **o, 'error': str(e)})

    return {
        'ok': True,
        'dry_run': False,
        'address': address,
        'positions_seen': len(positions),
        'active_positions': len(active),
        'orders_attempted': len(planned),
        'redeemable_positions': redeemable,
        'hint': 'If redeemable_positions is non-empty, those markets are resolved; use Redeem (on-chain) instead of Sell.',
        'results': results,
        'duration_ms': int((time.time() - started) * 1000),
    }


def close_positions_by_token_ids_from_env(*, token_ids: set[str], dry_run: bool, slip: float = 0.02) -> dict:
    """Attempt to close positions for a specific set of token ids.

    This is used by the post-resolution cleanup loop.
    """

    started = time.time()
    token_ids = {str(x).strip() for x in (token_ids or set()) if str(x).strip()}
    if not token_ids:
        return {'ok': True, 'dry_run': bool(dry_run), 'orders_planned': [], 'orders_attempted': 0, 'duration_ms': 0}

    pk = _getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return {'ok': False, 'error': 'missing_private_key'}

    sig_type = int(_to_f(_getenv('C5_POLY_SIGNATURE_TYPE', '0'), 0.0))
    funder = _getenv('C5_POLY_FUNDER_ADDRESS', '') or None
    clob_url = _getenv('C5_POLY_CLOB_URL', 'https://clob.polymarket.com')

    address = derive_address(pk)
    positions = fetch_positions(address)

    # Plan with the shared helper so we consistently skip redeemable/resolved positions.
    plan = _plan_close_orders(positions=positions, max_positions=10_000, slip=slip)
    active = plan['active']
    redeemable = plan['redeemable']

    planned: list[dict] = []
    for p in active:
        tid = _token_id_from_position(p or {})
        if not tid or tid not in token_ids:
            continue
        if _is_redeemable_position(p or {}):
            # Settled positions cannot be sold via CLOB; redeem on-chain instead.
            continue
        size = _size_from_position(p or {})
        if size <= 0:
            continue
        if _to_f((p or {}).get('currentPrice'), 0.0) <= 0:
            continue
        price = _price_for_sell(p or {}, slip=slip)
        planned.append(
            {
                'token_id': tid,
                'size': float(size),
                'price': float(price),
                'current_value': _to_f(p.get('currentValue'), 0.0),
                'current_price': _to_f(p.get('currentPrice'), 0.0),
                'market': p.get('market') or p.get('question') or p.get('title'),
            }
        )

    if dry_run:
        return {
            'ok': True,
            'dry_run': True,
            'address': address,
            'positions_seen': len(positions),
            'active_positions': len(active),
            'target_token_ids': sorted(token_ids)[:25],
            'orders_planned': planned,
            'redeemable_positions': redeemable,
            'orders_attempted': 0,
            'duration_ms': int((time.time() - started) * 1000),
        }

    # Live: place SELL limit orders
    try:
        from py_clob_client.client import ClobClient  # type: ignore
        from py_clob_client.constants import POLYGON  # type: ignore
        from py_clob_client.order_builder.constants import SELL  # type: ignore
        from py_clob_client.clob_types import OrderArgs  # type: ignore
    except Exception as e:
        return {'ok': False, 'error': f'missing_py_clob_client: {e}'}

    from ._clob_auth import derive_api_creds_with_retry
    client = ClobClient(clob_url, key=pk, chain_id=POLYGON, signature_type=sig_type, funder=funder)
    derive_api_creds_with_retry(client)

    results: list[dict] = []
    for o in planned:
        try:
            signed = client.create_order(OrderArgs(price=float(o['price']), size=float(o['size']), side=SELL, token_id=str(o['token_id'])))
            resp = client.post_order(signed)
            results.append({'ok': True, **o, 'response': resp})
        except Exception as e:
            results.append({'ok': False, **o, 'error': str(e)})

    return {
        'ok': True,
        'dry_run': False,
        'address': address,
        'positions_seen': len(positions),
        'active_positions': len(active),
        'target_token_ids': sorted(token_ids)[:25],
        'orders_attempted': len(planned),
        'orders_planned': planned,
        'redeemable_positions': redeemable,
        'results': results,
        'duration_ms': int((time.time() - started) * 1000),
    }


def sell_all_enabled() -> bool:
    return _getbool('C5_POLY_SELL_ALL_ENABLED', False)
