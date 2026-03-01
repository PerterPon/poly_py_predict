"""On-chain redemption (claim) for resolved Polymarket positions (EOA-only).

Polymarket markets settle via the Conditional Tokens Framework (CTF). After a
market resolves, winners can be redeemed by calling:

  CTF.redeemPositions(collateralToken, parentCollectionId, conditionId, indexSets)

This is an on-chain Polygon transaction.

Important safety notes:
- EOA-only: we only support redemption from the address derived from
  C5_POLY_PRIVATE_KEY. If your positions are held by a proxy/Safe (funder),
  redemption requires a different execution path and is intentionally not
  implemented here.
- Redeem is ON by default (buyers expect winnings to be claimed automatically).
- We support dry-run planning and rate-limited retries.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

from .persistence import JsonStore

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

logger = logging.getLogger(__name__)

TRADES_STORE = JsonStore(Path('logs') / 'poly_trades.json')
OPS_STORE = JsonStore(Path('logs') / 'poly_ops.json')
_ORPHAN_ATTEMPTED_STORE = JsonStore(Path('logs') / 'orphan_redeemed.json')


CTF_ADDRESS_POLYGON = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'
USDC_E_POLYGON = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'

_CTF_ABI = [
    {
        'constant': False,
        'inputs': [
            {'name': 'collateralToken', 'type': 'address'},
            {'name': 'parentCollectionId', 'type': 'bytes32'},
            {'name': 'conditionId', 'type': 'bytes32'},
            {'name': 'indexSets', 'type': 'uint256[]'},
        ],
        'name': 'redeemPositions',
        'outputs': [],
        'type': 'function',
    }
]


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


def _fetch_market_by_slug(gamma_url: str, slug: str) -> dict | None:
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


def _normalize_bytes32(hex_or_bytes: Any) -> bytes:
    """Return a 32-byte value from a 0x-hex string or bytes."""

    if isinstance(hex_or_bytes, (bytes, bytearray)):
        b = bytes(hex_or_bytes)
        if len(b) == 32:
            return b
        if len(b) < 32:
            return b.rjust(32, b'\x00')
        return b[-32:]

    s = str(hex_or_bytes or '').strip()
    if not s:
        return b'\x00' * 32
    if s.startswith('0x'):
        s = s[2:]
    try:
        b = bytes.fromhex(s)
    except Exception:
        b = b''
    if len(b) == 32:
        return b
    if len(b) < 32:
        return b.rjust(32, b'\x00')
    return b[-32:]


def is_redeem_enabled() -> bool:
    # Default ON: users expect the bot to claim winnings automatically.
    # Can be disabled explicitly via env/overrides by setting to false.
    return _getbool('C5_POLY_AUTO_REDEEM_ENABLED', True)


def select_redeem_candidates_from_trades(trades: list[dict], *, max_trades: int, now: int, retry_sec: int) -> list[dict]:
    """Pure selection helper (unit-test friendly)."""

    out: list[dict] = []
    for idx, t in enumerate(trades or []):
        if len(out) >= int(max_trades):
            break
        if not isinstance(t, dict):
            continue
        if t.get('resolved') != 'win':
            continue
        # Skip ghost wins — order was never filled, no position to redeem.
        order_status = str(t.get('order_status') or '').strip().lower()
        if order_status in ('canceled', 'canceled_market_resolved', 'expired'):
            continue
        # Also skip if filled_size is explicitly zero and no CLOB fill signal.
        resp = t.get('response') or {}
        taking = 0.0
        if isinstance(resp, dict):
            try:
                taking = float(resp.get('takingAmount') or 0)
            except (ValueError, TypeError):
                pass
        filled = float(t.get('filled_size') or 0) if t.get('filled_size') else 0.0
        if order_status not in ('filled', 'matched', 'posted', 'open', 'partial', 'unknown', '') and filled <= 0 and taking <= 0:
            continue
        # Important: do NOT permanently skip just because a tx hash exists.
        # Redeem txs can be dropped/reorged/not found, so we allow retries
        # after the retry window elapses.
        if t.get('redeem_status') == 'success':
            continue
        # Trades marked "dropped" by reconciliation are immediately eligible
        # for retry — skip the normal time-based wait.
        if t.get('redeem_status') == 'dropped':
            cond = str(t.get('condition_id') or t.get('conditionId') or '').strip()
            slug = str(t.get('window_slug') or '').strip()
            out.append({'trade_index': idx, 'window_slug': slug, 'condition_id': cond})
            continue
        last_attempt = int(_to_f(t.get('redeem_attempted_ts') or 0, 0.0))
        last_submit = int(_to_f(t.get('redeem_submitted_ts') or 0, 0.0))
        last = max(last_attempt, last_submit)
        if last and (now - last) < int(retry_sec):
            continue
        cond = str(t.get('condition_id') or t.get('conditionId') or '').strip()
        slug = str(t.get('window_slug') or '').strip()
        out.append({'trade_index': idx, 'window_slug': slug, 'condition_id': cond})
    return out


def find_redeem_candidates(*,
                           max_trades: int = 10,
                           now_ts: Optional[int] = None,
                           retry_minutes: int = 10) -> dict[str, Any]:
    """Return redeem candidates from the trades log.

    Candidates are resolved wins and not yet redeemed (or retry window elapsed).
    Retry window defaults to 10 min to quickly recover from dropped txs on
    unreliable Polygon RPC nodes.
    """

    now = int(now_ts or time.time())
    retry_sec = max(5, int(retry_minutes)) * 60

    loaded = TRADES_STORE.load(default=[]) or []
    if not isinstance(loaded, list):
        return {'ok': True, 'candidates': [], 'skipped': True, 'reason': 'no_trades'}

    trades: list[dict] = [t for t in loaded if isinstance(t, dict)]
    out = select_redeem_candidates_from_trades(trades, max_trades=max_trades, now=now, retry_sec=retry_sec)
    return {'ok': True, 'candidates': out, 'count': len(out)}


def redeem_positions_for_trade(*,
                               trade: dict,
                               dry_run: bool,
                               is_retry: bool = False,
                               gamma_url: str = 'https://gamma-api.polymarket.com',
                               rpc_url: Optional[str] = None,
                               usdc_address: str = USDC_E_POLYGON,
                               ctf_address: str = CTF_ADDRESS_POLYGON) -> dict[str, Any]:
    """Redeem winnings for a single trade record.

    Returns a result dict. Does not mutate the trade in-place.
    When *is_retry* is True, bumps gas price by 30 % to improve inclusion
    odds after a previously dropped tx.
    """

    pk = _getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return {'ok': False, 'error': 'missing_private_key'}

    # EOA-only guard.
    if _getenv('C5_POLY_FUNDER_ADDRESS', ''):
        return {'ok': False, 'error': 'proxy_wallet_not_supported'}
    if _getint('C5_POLY_SIGNATURE_TYPE', 0) != 0:
        return {'ok': False, 'error': 'signature_type_not_supported'}

    # Resolve conditionId if missing.
    condition_id = str(trade.get('condition_id') or trade.get('conditionId') or '').strip()
    if not condition_id:
        slug = str(trade.get('window_slug') or '').strip()
        if slug:
            m = _fetch_market_by_slug(gamma_url, slug)
            if isinstance(m, dict):
                condition_id = str(m.get('conditionId') or '').strip()
    if not condition_id:
        return {'ok': False, 'error': 'missing_condition_id'}

    # RPC
    rpc = (rpc_url or _getenv('C5_POLYGON_RPC', '') or 'https://polygon-bor-rpc.publicnode.com').strip()

    if dry_run:
        return {
            'ok': True,
            'dry_run': True,
            'condition_id': condition_id,
            'ctf_address': ctf_address,
            'collateral': usdc_address,
            'rpc_url': rpc,
            'index_sets': [1, 2],
        }

    # Live tx.
    from eth_account import Account  # type: ignore
    from web3 import Web3  # type: ignore

    w3 = Web3(Web3.HTTPProvider(rpc))
    acct = Account.from_key(pk)
    from_addr = acct.address

    contract = w3.eth.contract(address=Web3.to_checksum_address(ctf_address), abi=_CTF_ABI)
    # Use pending nonce so we don't accidentally try to reuse a nonce when a
    # previous redeem tx is still pending (common cause of
    # "replacement transaction underpriced" errors).
    try:
        nonce = w3.eth.get_transaction_count(from_addr, 'pending')
    except Exception:
        nonce = w3.eth.get_transaction_count(from_addr)

    parent = b'\x00' * 32
    cond_b32 = _normalize_bytes32(condition_id)
    # Bump gas price by 30 % on retries to help dropped txs get included.
    base_gas_price = w3.eth.gas_price
    gas_price = int(base_gas_price * 1.3) if is_retry else base_gas_price
    tx = contract.functions.redeemPositions(
        Web3.to_checksum_address(usdc_address),
        parent,
        cond_b32,
        [1, 2],
    ).build_transaction({
        'from': from_addr,
        'nonce': nonce,
        'gasPrice': gas_price,
        'chainId': 137,
    })

    # Estimate gas; if estimate fails, keep a conservative ceiling.
    try:
        tx['gas'] = int(w3.eth.estimate_gas(tx) * 1.20)
    except Exception:
        tx['gas'] = int(tx.get('gas') or 350000)

    signed = w3.eth.account.sign_transaction(tx, private_key=pk)
    raw_tx = getattr(signed, 'rawTransaction', None) or getattr(signed, 'raw_transaction', None)
    if raw_tx is None:
        return {'ok': False, 'error': 'sign_failed'}

    try:
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
    except Exception as e:
        msg = str(e)
        if 'replacement transaction underpriced' in msg.lower():
            return {
                'ok': False,
                'error': 'replacement_transaction_underpriced',
                'detail': msg,
                'hint': 'A previous tx may still be pending. Wait a bit or increase gas / retry later.',
            }
        return {'ok': False, 'error': 'send_raw_transaction_failed', 'detail': msg}

    return {
        'ok': True,
        'dry_run': False,
        'condition_id': condition_id,
        'from': from_addr,
        'tx_hash': tx_hash.hex(),
        'ctf_address': ctf_address,
        'collateral': usdc_address,
    }


def _load_orphan_attempted() -> set[str]:
    """Load set of orphan condition_ids already attempted for redeem."""
    try:
        data = _ORPHAN_ATTEMPTED_STORE.load(default=[]) or []
        if isinstance(data, list):
            return {str(x) for x in data if x}
    except Exception:
        pass
    return set()


def _save_orphan_attempted(cid: str) -> None:
    """Append a condition_id to the orphan-attempted cache."""
    try:
        attempted = _load_orphan_attempted()
        attempted.add(cid)
        # Keep max 500 entries to avoid unbounded growth.
        items = sorted(attempted)[-500:]
        _ORPHAN_ATTEMPTED_STORE.save(items)
    except Exception:
        pass


def _find_orphan_redeemable_positions(known_condition_ids: set[str]) -> list[dict]:
    """Scan Polymarket data API for redeemable positions not in the trades log.

    These are "orphan" positions — won positions on-chain that the bot has no
    trade record for (e.g. from a previous session, a reinstall, or manual
    trading).  Without this, they'd sit unredeemed forever.

    Returns a list of dicts with 'condition_id' suitable for
    ``redeem_positions_for_trade``.
    """

    pk = _getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return []

    # EOA guard — orphan redeem only works for EOA wallets.
    if _getenv('C5_POLY_FUNDER_ADDRESS', ''):
        return []

    try:
        from .polymarket_account import derive_address, fetch_positions
        address = derive_address(pk)
        positions = fetch_positions(address)
    except Exception as exc:
        logger.debug('orphan redeem: failed to fetch positions: %s', exc)
        return []

    already_attempted = _load_orphan_attempted()

    orphans: list[dict] = []
    for p in (positions or []):
        if not isinstance(p, dict):
            continue
        # Only consider actually redeemable positions (curPrice==1 or redeemable flag).
        redeemable = bool(p.get('redeemable'))
        try:
            cur_price = float(p.get('curPrice', 0))
        except (ValueError, TypeError):
            cur_price = 0.0
        if not redeemable and cur_price < 0.999:
            continue
        try:
            value = float(p.get('currentValue', 0))
        except (ValueError, TypeError):
            value = 0.0
        if value < 0.01:
            continue

        cond = str(p.get('conditionId') or p.get('condition_id') or '').strip()
        if not cond:
            continue
        if cond in known_condition_ids:
            continue  # Already tracked in trades log — not an orphan.
        if cond in already_attempted:
            continue  # Already attempted redeem — don't waste gas.

        orphans.append({
            'condition_id': cond,
            'current_value': value,
            'market': p.get('market') or p.get('question') or p.get('title') or '',
        })

    return orphans


def process_auto_redeem(*, dry_run: bool) -> dict[str, Any]:
    """Auto redeem loop entrypoint.

    Scans recent resolved wins and sends redeem txs (or returns a dry-run plan).
    Mutates the trades log to record attempts/tx hashes.

    Also discovers "orphan" redeemable positions from the data API that have no
    trade record and redeems those too — this covers positions from prior
    sessions, reinstalls, or manual trades.
    """

    if not is_redeem_enabled():
        return {'ok': True, 'skipped': True, 'reason': 'redeem_disabled'}

    now = int(time.time())
    retry_minutes = max(5, _getint('C5_POLY_REDEEM_RETRY_MINUTES', 10))
    max_trades = max(1, _getint('C5_POLY_MAX_REDEEM_TRADES_PER_RUN', 5))

    cand = find_redeem_candidates(max_trades=max_trades, now_ts=now, retry_minutes=retry_minutes)
    items = cand.get('candidates') or []

    loaded = TRADES_STORE.load(default=[]) or []
    if not isinstance(loaded, list):
        loaded = []
    trades: list[dict] = [t for t in loaded if isinstance(t, dict)]

    results: list[dict] = []
    for it in items:
        idx = int(it.get('trade_index', -1))
        if idx < 0 or idx >= len(trades):
            continue
        trade = trades[idx]
        # Detect retry: trade had a previous attempt that didn't succeed.
        is_retry = bool(trade.get('redeem_tx_hash') and trade.get('redeem_status') != 'success')
        try:
            res = redeem_positions_for_trade(trade=trade, dry_run=dry_run, is_retry=is_retry)
        except Exception as e:
            # Never raise from the auto loop — turn into a per-trade failure.
            res = {'ok': False, 'error': 'redeem_exception', 'detail': str(e)}
        results.append({'trade_index': idx, 'window_slug': trade.get('window_slug'), **res})
        # Mark attempt.
        try:
            trade['redeem_attempted_ts'] = now
            trade['redeem_dry_run'] = bool(dry_run)
            if res.get('ok') and (not dry_run) and res.get('tx_hash'):
                trade['redeem_tx_hash'] = res.get('tx_hash')
                trade['redeem_status'] = 'submitted'
                trade['redeem_submitted_ts'] = now
            elif res.get('ok') and dry_run:
                trade['redeem_status'] = 'planned'
            else:
                trade['redeem_status'] = 'error'
                trade['redeem_error'] = res.get('error')
            # Also store condition id for future runs.
            if res.get('condition_id'):
                trade['condition_id'] = res.get('condition_id')
        except Exception:
            pass

    TRADES_STORE.save(trades)

    # ── Orphan redeem: positions on-chain with no trade record ──────
    orphan_results: list[dict] = []
    try:
        # Collect all condition IDs that the trades log knows about.
        known_cids = set()
        for t in trades:
            cid = str(t.get('condition_id') or t.get('conditionId') or '').strip()
            if cid:
                known_cids.add(cid)

        orphans = _find_orphan_redeemable_positions(known_cids)
        for orph in orphans[:max_trades]:
            try:
                res = redeem_positions_for_trade(
                    trade={'condition_id': orph['condition_id']},
                    dry_run=dry_run,
                )
            except Exception as e:
                res = {'ok': False, 'error': 'orphan_redeem_exception', 'detail': str(e)}
            orphan_results.append({
                'orphan': True,
                'condition_id': orph.get('condition_id'),
                'current_value': orph.get('current_value'),
                'market': orph.get('market'),
                **res,
            })
            if res.get('ok') and res.get('tx_hash'):
                logger.info('orphan redeem: redeemed %s ($%.2f) tx=%s',
                            orph.get('condition_id', '?')[:16], orph.get('current_value', 0), res['tx_hash'][:16])
                # Cache this condition_id so we don't re-submit and waste gas.
                _save_orphan_attempted(orph.get('condition_id', ''))
            elif res.get('ok'):
                # Dry-run or no tx_hash — still cache to avoid repeated attempts.
                _save_orphan_attempted(orph.get('condition_id', ''))
    except Exception as exc:
        logger.warning('orphan redeem scan failed: %s', exc)

    all_results = results + orphan_results

    # Ops log entry.
    try:
        ops = OPS_STORE.load(default=[]) or []
        if not isinstance(ops, list):
            ops = []
        ops.append({'ts': now, 'action': 'redeem_positions', 'dry_run': bool(dry_run),
                    'results': all_results[-20:], 'orphans': len(orphan_results)})
        ops = ops[-200:]
        OPS_STORE.save(ops)
    except Exception:
        pass

    total = len(all_results)
    if total == 0:
        return {'ok': True, 'skipped': True, 'reason': 'nothing_to_redeem'}

    return {'ok': True, 'dry_run': bool(dry_run), 'count': total,
            'results': results, 'orphan_results': orphan_results}


def reconcile_redeem_txs(*, max_trades: int = 25) -> dict[str, Any]:
    """Best-effort: mark submitted redeem txs as success/failure once mined."""

    if not is_redeem_enabled():
        return {'ok': True, 'skipped': True, 'reason': 'redeem_disabled'}

    # EOA-only guard.
    if _getenv('C5_POLY_FUNDER_ADDRESS', ''):
        return {'ok': True, 'skipped': True, 'reason': 'proxy_wallet_not_supported'}
    if _getint('C5_POLY_SIGNATURE_TYPE', 0) != 0:
        return {'ok': True, 'skipped': True, 'reason': 'signature_type_not_supported'}

    rpc = (_getenv('C5_POLYGON_RPC', '') or 'https://polygon-bor-rpc.publicnode.com').strip()

    from web3 import Web3  # type: ignore

    w3 = Web3(Web3.HTTPProvider(rpc))

    loaded = TRADES_STORE.load(default=[]) or []
    if not isinstance(loaded, list):
        return {'ok': False, 'error': 'trades_not_list'}

    trades: list[dict] = [t for t in loaded if isinstance(t, dict)]
    now = int(time.time())
    checked = 0
    updated = 0
    results: list[dict] = []

    # Iterate newest-first.
    for t in reversed(trades):
        if checked >= int(max_trades):
            break
        if t.get('redeem_status') != 'submitted':
            continue
        txh = str(t.get('redeem_tx_hash') or '').strip()
        if not txh:
            continue
        # Normalize: Web3 expects a 0x-prefixed hash.
        if not txh.startswith('0x'):
            txh = '0x' + txh
        txh = txh.lower()
        checked += 1
        try:
            receipt = w3.eth.get_transaction_receipt(txh)
        except Exception:
            receipt = None

        if not receipt:
            # If the tx has been "submitted" for more than 10 min with no
            # on-chain receipt, the RPC likely dropped it.  Mark it as
            # "dropped" so the next auto-redeem pass retries immediately
            # instead of waiting for the full retry window.
            submitted_ts = int(_to_f(t.get('redeem_submitted_ts') or 0, 0.0))
            drop_threshold = _getint('C5_POLY_REDEEM_DROP_SECONDS', 600)  # 10 min
            if submitted_ts and (now - submitted_ts) > drop_threshold:
                t['redeem_status'] = 'dropped'
                t['redeem_dropped_ts'] = now
                updated += 1
                results.append({'tx_hash': txh, 'status': 'dropped',
                                'age_sec': now - submitted_ts})
                logger.info('redeem tx %s marked dropped after %d s',
                            txh[:18], now - submitted_ts)
            else:
                results.append({'tx_hash': txh, 'status': 'pending'})
            continue

        try:
            status = int(getattr(receipt, 'status', None) or receipt.get('status', 0))
        except Exception:
            status = 0

        if status == 1:
            t['redeem_status'] = 'success'
            t['redeemed_ts'] = now
            updated += 1
            results.append({'tx_hash': txh, 'status': 'success'})
        else:
            t['redeem_status'] = 'failed'
            t['redeem_failed_ts'] = now
            updated += 1
            results.append({'tx_hash': txh, 'status': 'failed'})

    if updated:
        TRADES_STORE.save(trades)
        try:
            ops = OPS_STORE.load(default=[]) or []
            if not isinstance(ops, list):
                ops = []
            ops.append({'ts': now, 'action': 'redeem_reconcile', 'checked': checked, 'updated': updated, 'results': results[-25:]})
            ops = ops[-200:]
            OPS_STORE.save(ops)
        except Exception:
            pass

    return {'ok': True, 'checked': checked, 'updated': updated, 'results': results}
