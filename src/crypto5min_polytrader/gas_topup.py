"""Native gas top-up helper (Polygon).

Goal: help the bot "look after" the user by keeping enough native gas token
(POL/MATIC) to:
- place Polymarket on-chain transactions (redeem, withdraw)
- avoid confusing failures when gas is low

IMPORTANT REALITY CHECK
- Swapping USDC -> native gas token still requires *some* gas already.
  If the wallet has ~0 gas, no on-chain action can fix it. The user must
  manually send a small amount of gas token first.

Implementation uses 0x Swap API v2 (AllowanceHolder) which returns an unsigned
transaction (`quote.transaction`) you can sign + broadcast.

Docs (retrieved 2026-02-15):
- https://0x.org/docs/0x-swap-api/guides/swap-tokens-with-0x-swap-api
- https://0x.org/docs/api (Swap → AllowanceHolder quote)

Env (non-secret):
- C5_NATIVE_GAS_SYMBOL=POL
- C5_NATIVE_GAS_MIN=0.15
- C5_GAS_TOPUP_ENABLED=false
- C5_GAS_TOPUP_TARGET_NATIVE=1.0
- C5_GAS_TOPUP_MAX_USDC=5

Env (secret-ish):
- C5_ZEROX_API_KEY=...

"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests  # type: ignore


logger = logging.getLogger(__name__)

POLYGON_CHAIN_ID = 137
USDC_E_POLYGON = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'

# 0x uses a special sentinel address to represent the chain native token.
# (FAQ references: 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE)
ZEROX_NATIVE_TOKEN = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'

# AllowanceHolder spender is returned by the API as allowanceTarget or
# issues.allowance.spender.

_ERC20_ABI = [
    {
        'constant': True,
        'inputs': [{'name': '_owner', 'type': 'address'}, {'name': '_spender', 'type': 'address'}],
        'name': 'allowance',
        'outputs': [{'name': 'remaining', 'type': 'uint256'}],
        'type': 'function',
    },
    {
        'constant': False,
        'inputs': [{'name': '_spender', 'type': 'address'}, {'name': '_value', 'type': 'uint256'}],
        'name': 'approve',
        'outputs': [{'name': 'success', 'type': 'bool'}],
        'type': 'function',
    },
    {
        'constant': True,
        'inputs': [],
        'name': 'decimals',
        'outputs': [{'name': '', 'type': 'uint8'}],
        'type': 'function',
    },
]


def _getenv(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _getbool(name: str, default: bool = False) -> bool:
    v = _getenv(name)
    if not v:
        return default
    return v.lower() in {'1', 'true', 'yes', 'y', 'on'}


def _getfloat(name: str, default: float) -> float:
    v = _getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _now() -> int:
    return int(time.time())


def _zero_x_headers() -> dict[str, str]:
    key = _getenv('C5_ZEROX_API_KEY', '')
    return {
        '0x-api-key': key,
        '0x-version': 'v2',
    }


def _zero_x_key_present() -> bool:
    return bool(_getenv('C5_ZEROX_API_KEY', ''))


def _polygon_rpc() -> str:
    return _getenv('C5_POLYGON_RPC', 'https://polygon-bor-rpc.publicnode.com')


def _native_symbol() -> str:
    return _getenv('C5_NATIVE_GAS_SYMBOL', 'POL') or 'POL'


def _native_min() -> float:
    return _getfloat('C5_NATIVE_GAS_MIN', 0.15)


def _topup_enabled() -> bool:
    return _getbool('C5_GAS_TOPUP_ENABLED', False)


def _topup_target() -> float:
    return max(0.0, _getfloat('C5_GAS_TOPUP_TARGET_NATIVE', 1.0))


def _topup_max_usdc() -> float:
    return max(0.0, _getfloat('C5_GAS_TOPUP_MAX_USDC', 5.0))


def _zero_x_base_url() -> str:
    return _getenv('C5_ZEROX_BASE_URL', 'https://api.0x.org')


def preview_topup(*, from_address: str, current_native: float) -> dict[str, Any]:
    """Return a preview quote to top-up native gas token using USDC.

    Does not send any on-chain transactions.
    """

    sym = _native_symbol()
    target = _topup_target()
    max_usdc = _topup_max_usdc()

    if current_native >= target:
        return {
            'ok': True,
            'skipped': True,
            'reason': 'already_sufficient',
            'native_symbol': sym,
            'current_native': float(current_native),
            'target_native': float(target),
        }

    if not _zero_x_key_present():
        return {
            'ok': False,
            'error': 'missing_0x_api_key',
            'hint': f'One-click gas top-up is not set up. Easiest fix: send ~$1–$3 worth of {sym} (POL/MATIC) to your bot wallet. This tool is optional/advanced.',
            'native_symbol': sym,
            'current_native': float(current_native),
            'target_native': float(target),
        }

    need = max(0.0, float(target) - float(current_native))
    if need <= 0:
        return {'ok': True, 'skipped': True, 'reason': 'nothing_to_buy'}

    # 0x Swap API v2 AllowanceHolder endpoints require `sellAmount`.
    # We therefore sell up to the configured USDC cap and see how much native
    # gas token we can buy. If the quote doesn't buy enough, we return a clear
    # error instructing the user to increase the cap or top up manually.
    sell_amount_raw = int(float(max_usdc) * 1_000_000.0)
    if sell_amount_raw <= 0:
        return {
            'ok': False,
            'error': 'max_usdc_too_low',
            'hint': 'Max spend (USDC) is set to 0. Increase it or top up gas manually.',
            'max_usdc': float(max_usdc),
            'native_symbol': sym,
            'current_native': float(current_native),
            'target_native': float(target),
        }

    params = {
        'chainId': str(POLYGON_CHAIN_ID),
        'sellToken': USDC_E_POLYGON,
        'buyToken': _getenv('C5_ZEROX_NATIVE_TOKEN', ZEROX_NATIVE_TOKEN),
        'sellAmount': str(sell_amount_raw),
        'taker': from_address,
        'slippageBps': str(int(_getfloat('C5_ZEROX_SLIPPAGE_BPS', 150.0))),
    }

    url = _zero_x_base_url().rstrip('/') + '/swap/allowance-holder/quote'
    r = requests.get(url, params=params, headers=_zero_x_headers(), timeout=20)
    try:
        data = r.json()
    except Exception:
        data = {'raw': r.text}

    if not r.ok:
        return {
            'ok': False,
            'error': 'quote_failed',
            'status': int(r.status_code),
            'detail': data,
            'native_symbol': sym,
            'current_native': float(current_native),
            'target_native': float(target),
        }

    tx = (data or {}).get('transaction') if isinstance(data, dict) else None
    allowance_target = (data or {}).get('allowanceTarget') if isinstance(data, dict) else None
    issues = (data or {}).get('issues') if isinstance(data, dict) else None

    # Best-effort: estimate USDC spend (base units, 6 decimals on Polygon).
    est_usdc = float(sell_amount_raw) / 1_000_000.0

    # Best-effort: estimate native bought from buyAmount (wei).
    buy_amount_out = None
    est_native = None
    try:
        buy_amount_out = int((data or {}).get('buyAmount') or 0)
        if buy_amount_out > 0:
            est_native = float(buy_amount_out) / 1e18
    except Exception:
        buy_amount_out = None
        est_native = None

    # If the USDC cap is too low to reach the target, return a clear message.
    if est_native is not None and float(est_native) + float(current_native) < float(target):
        return {
            'ok': False,
            'error': 'max_usdc_too_low_for_target',
            'hint': (
                f'This quote would only buy ~{est_native:.4f} {sym} (cap ${est_usdc:.2f} USDC). '
                f'Increase Max spend (USDC) or top up {sym} manually.'
            ),
            'estimated_usdc': float(est_usdc),
            'estimated_native': float(est_native),
            'max_usdc': float(max_usdc),
            'native_symbol': sym,
            'current_native': float(current_native),
            'target_native': float(target),
        }

    return {
        'ok': True,
        'native_symbol': sym,
        'current_native': float(current_native),
        'target_native': float(target),
        'need_native': float(need),
        'estimated_usdc': float(est_usdc),
        'estimated_native': float(est_native) if est_native is not None else None,
        'sellAmount': int(sell_amount_raw),
        'allowanceTarget': allowance_target,
        'issues': issues,
        'tx': tx,
    }


def execute_topup(*, private_key: str, from_address: str, current_native: float) -> dict[str, Any]:
    """Execute a gas top-up: (optional) approve USDC then submit swap tx."""

    prev = preview_topup(from_address=from_address, current_native=current_native)
    if not prev.get('ok'):
        return prev
    if prev.get('skipped'):
        return prev

    tx = prev.get('tx')
    if not isinstance(tx, dict):
        return {'ok': False, 'error': 'missing_tx', 'detail': prev}

    # If gas is extremely low, warn that we might not even be able to approve/swap.
    min_gas_for_swap = _getfloat('C5_GAS_TOPUP_MIN_NATIVE_FOR_SWAP', 0.02)
    if float(current_native) < float(min_gas_for_swap):
        return {
            'ok': False,
            'error': 'insufficient_gas_to_swap',
            'hint': f'Wallet has {current_native:.4f} {_native_symbol()}, which may be too low to approve/swap. Send a small amount manually first.',
            'min_native_for_swap': float(min_gas_for_swap),
        }

    from web3 import Web3  # type: ignore
    from eth_account import Account  # type: ignore

    rpc = _polygon_rpc()
    w3 = Web3(Web3.HTTPProvider(rpc))

    acct = Account.from_key(private_key)
    if acct.address.lower() != from_address.lower():
        return {'ok': False, 'error': 'address_mismatch'}

    approve_hash = None

    # Check allowance if API indicates one is needed.
    allowance_needed = None
    spender = None
    try:
        issues = prev.get('issues') or {}
        if isinstance(issues, dict) and issues.get('allowance'):
            allowance_needed = issues.get('allowance')
    except Exception:
        allowance_needed = None

    try:
        if isinstance(allowance_needed, dict):
            spender = allowance_needed.get('spender')
        if not spender:
            spender = prev.get('allowanceTarget')
    except Exception:
        spender = None

    if spender:
        # Query USDC allowance
        token = w3.eth.contract(address=Web3.to_checksum_address(USDC_E_POLYGON), abi=_ERC20_ABI)
        try:
            remaining = int(token.functions.allowance(Web3.to_checksum_address(from_address), Web3.to_checksum_address(spender)).call())
        except Exception:
            remaining = 0

        sell_amount_raw = 0
        try:
            sell_amount_raw = int(prev.get('sellAmount') or 0)
        except Exception:
            sell_amount_raw = 0

        # Approve max if 0x indicates allowance issue OR current allowance doesn't cover the quote.
        if allowance_needed is not None or (sell_amount_raw and remaining < sell_amount_raw):
            max_uint = (1 << 256) - 1
            try:
                nonce = w3.eth.get_transaction_count(from_address, 'pending')
            except Exception:
                nonce = w3.eth.get_transaction_count(from_address)
            appr = token.functions.approve(Web3.to_checksum_address(spender), int(max_uint)).build_transaction({
                'from': from_address,
                'nonce': nonce,
                'gasPrice': w3.eth.gas_price,
                'chainId': POLYGON_CHAIN_ID,
            })
            try:
                appr['gas'] = int(w3.eth.estimate_gas(appr) * 1.2)
            except Exception:
                appr['gas'] = int(appr.get('gas') or 120000)

            signed = w3.eth.account.sign_transaction(appr, private_key=private_key)
            raw = getattr(signed, 'rawTransaction', None) or getattr(signed, 'raw_transaction', None)
            if raw is None:
                return {'ok': False, 'error': 'approve_sign_failed'}
            approve_hash = w3.eth.send_raw_transaction(raw).hex()
            # Wait for approval so swap doesn't immediately revert.
            try:
                w3.eth.wait_for_transaction_receipt(approve_hash, timeout=120, poll_latency=2)
            except Exception:
                pass

    # Submit swap tx.
    try:
        to = tx.get('to')
        data = tx.get('data')
        value = int(tx.get('value') or 0)
    except Exception:
        return {'ok': False, 'error': 'bad_tx_format', 'tx': tx}

    if not (to and data):
        return {'ok': False, 'error': 'bad_tx_missing_fields', 'tx': tx}

    try:
        nonce = w3.eth.get_transaction_count(from_address, 'pending')
    except Exception:
        nonce = w3.eth.get_transaction_count(from_address)

    send_tx = {
        'from': from_address,
        'to': Web3.to_checksum_address(to),
        'data': data,
        'value': int(value),
        'nonce': nonce,
        'chainId': POLYGON_CHAIN_ID,
        'gasPrice': int(w3.eth.gas_price),
    }

    # Use API gas hint if present.
    try:
        gas_hint = int(tx.get('gas') or 0)
        if gas_hint > 0:
            send_tx['gas'] = int(gas_hint)
    except Exception:
        pass

    if 'gas' not in send_tx:
        try:
            send_tx['gas'] = int(w3.eth.estimate_gas(send_tx) * 1.2)
        except Exception:
            send_tx['gas'] = 350000

    signed = w3.eth.account.sign_transaction(send_tx, private_key=private_key)
    raw = getattr(signed, 'rawTransaction', None) or getattr(signed, 'raw_transaction', None)
    if raw is None:
        return {'ok': False, 'error': 'swap_sign_failed'}

    try:
        swap_hash = w3.eth.send_raw_transaction(raw).hex()
    except Exception as e:
        msg = str(e)
        return {'ok': False, 'error': 'swap_send_failed', 'detail': msg, 'approve_tx_hash': approve_hash}

    return {
        'ok': True,
        'approve_tx_hash': approve_hash,
        'swap_tx_hash': swap_hash,
        'native_symbol': _native_symbol(),
        'target_native': float(_topup_target()),
        'max_usdc': float(_topup_max_usdc()),
    }
