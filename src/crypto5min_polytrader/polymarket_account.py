"""Polymarket account snapshot helpers (optional).

Used to display simple PnL-like metrics on the dashboard:
- CLOB collateral balance (USDC.e)
- Positions value / cost basis / unrealized PnL (from data-api)

This is best-effort telemetry for operator convenience.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from .persistence import JsonStore


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _getenv(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


@dataclass(frozen=True)
class PolyAccountSnapshot:
    ts: float
    address: str
    clob_balance_usdc: float
    positions_value_usdc: float
    cost_basis_usdc: float
    unrealized_pnl_usdc: float
    active_positions: int
    native_gas_balance: float
    native_gas_symbol: str

    def as_dict(self) -> dict:
        return {
            'ts': self.ts,
            'address': self.address,
            'clob_balance_usdc': self.clob_balance_usdc,
            'positions_value_usdc': self.positions_value_usdc,
            'cost_basis_usdc': self.cost_basis_usdc,
            'unrealized_pnl_usdc': self.unrealized_pnl_usdc,
            'active_positions': self.active_positions,
            'native_gas_balance': self.native_gas_balance,
            'native_gas_symbol': self.native_gas_symbol,
            'total_equity_usdc': self.clob_balance_usdc + self.positions_value_usdc,
        }


def native_gas_balance(address: str, *, rpc_url: str) -> float:
    """Return the native gas token balance for an address (Polygon PoS).

    Note: Polygon has historically used MATIC as the native gas token; newer
    docs/ecosystem may refer to POL. We treat this as the native coin on chainId
    137 and only display a symbol for the user.
    """

    from web3 import Web3  # type: ignore

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    wei = int(w3.eth.get_balance(Web3.to_checksum_address(address)))
    return float(w3.from_wei(wei, 'ether'))


def derive_address(private_key: str) -> str:
    from eth_account import Account  # type: ignore

    acc = Account.from_key(private_key)
    return str(acc.address)


def fetch_positions(address: str, *, limit: int = 1000) -> list[dict]:
    import requests  # type: ignore

    url = f'https://data-api.polymarket.com/positions?user={address}&limit={limit}'
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json() or []
    return data if isinstance(data, list) else []


def summarize_positions(positions: list[dict], *, active_min_usdc: float = 0.01) -> dict:
    """Summarize positions list into dashboard-friendly metrics.

    IMPORTANT: data-api may return historical/closed positions. For an
    *unrealized* PnL view, we only consider positions with meaningful current
    value ("active" positions).
    """

    active = [p for p in (positions or []) if _to_f(p.get('currentValue', 0.0)) > float(active_min_usdc)]
    positions_value = sum(_to_f(p.get('currentValue', 0.0)) for p in active)
    cost_basis = sum(_to_f(p.get('initialValue', 0.0)) for p in active)
    return {
        'positions_value_usdc': float(positions_value),
        'cost_basis_usdc': float(cost_basis),
        'unrealized_pnl_usdc': float(positions_value - cost_basis),
        'active_positions': int(len(active)),
    }


def clob_balance_usdc(private_key: str, *, signature_type: int = 0, funder: Optional[str] = None, clob_url: str = 'https://clob.polymarket.com') -> float:
    # Lazy imports so the rest of the product works without these deps in paper-only contexts.
    from py_clob_client.client import ClobClient  # type: ignore
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType  # type: ignore
    from py_clob_client.constants import POLYGON  # type: ignore

    from ._clob_auth import derive_api_creds_with_retry
    client = ClobClient(clob_url, key=private_key, chain_id=POLYGON, signature_type=signature_type, funder=funder)
    derive_api_creds_with_retry(client)

    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    resp = client.get_balance_allowance(params)
    raw = resp.get('balance', 0) if isinstance(resp, dict) else resp
    # Polymarket USDC.e is 6 decimals.
    return _to_f(raw, 0.0) / 1_000_000.0


def snapshot_from_env() -> Optional[PolyAccountSnapshot]:
    """Build a snapshot from env vars.

    Requires C5_POLY_PRIVATE_KEY. (We derive address from it.)
    """

    pk = _getenv('C5_POLY_PRIVATE_KEY', '')
    if not pk:
        return None

    sig_type = 0
    try:
        sig_type = int(_getenv('C5_POLY_SIGNATURE_TYPE', '0') or '0')
    except Exception:
        sig_type = 0

    funder = _getenv('C5_POLY_FUNDER_ADDRESS', '') or None
    clob_url = _getenv('C5_POLY_CLOB_URL', 'https://clob.polymarket.com')
    rpc_url = _getenv('C5_POLYGON_RPC', 'https://polygon-bor-rpc.publicnode.com')
    gas_sym = _getenv('C5_NATIVE_GAS_SYMBOL', 'POL')

    address = derive_address(pk)

    # Positions (active only for unrealized PnL)
    positions = fetch_positions(address)
    summ = summarize_positions(positions, active_min_usdc=0.01)
    positions_value = float(summ.get('positions_value_usdc', 0.0))
    cost_basis = float(summ.get('cost_basis_usdc', 0.0))
    unreal = float(summ.get('unrealized_pnl_usdc', 0.0))
    active_count = int(summ.get('active_positions', 0))

    # CLOB balance (best-effort)
    try:
        bal = clob_balance_usdc(pk, signature_type=sig_type, funder=funder, clob_url=clob_url)
    except Exception:
        bal = 0.0

    # Native gas balance (best-effort)
    try:
        gas_bal = native_gas_balance(address, rpc_url=rpc_url)
    except Exception:
        gas_bal = 0.0

    return PolyAccountSnapshot(
        ts=time.time(),
        address=address,
        clob_balance_usdc=float(bal),
        positions_value_usdc=float(positions_value),
        cost_basis_usdc=float(cost_basis),
        unrealized_pnl_usdc=float(unreal),
        active_positions=int(active_count),
        native_gas_balance=float(gas_bal),
        native_gas_symbol=str(gas_sym or 'POL'),
    )


def append_equity_point(path: str, snapshot: PolyAccountSnapshot, *, max_points: int = 5000) -> None:
    """Append a time series point to a JSON file (bounded)."""

    store = JsonStore(path)
    point = {
        'ts': int(snapshot.ts),
        'equity': snapshot.clob_balance_usdc + snapshot.positions_value_usdc,
        'clob': snapshot.clob_balance_usdc,
        'positions': snapshot.positions_value_usdc,
        'pnl': snapshot.unrealized_pnl_usdc,
    }

    loaded = store.load(default=[])
    series: list[dict] = []
    if isinstance(loaded, list):
        series = [x for x in loaded if isinstance(x, dict)]

    series.append(point)
    if len(series) > int(max_points):
        series = series[-int(max_points):]

    store.save(series)
