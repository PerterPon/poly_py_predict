"""Withdrawal helper (EOA-only).

⚠️ WARNING
This module sends on-chain transactions.

We only support a simple ERC20 transfer from the EOA that owns the private key.
If the user's funds are in a proxy/safe wallet (common on Polymarket), this will
NOT work, and we intentionally do not attempt to execute Safe transactions.

Env:
- C5_WITHDRAW_ENABLED=true
- C5_WITHDRAW_TO_ADDRESS=0x...
- C5_WITHDRAW_MAX_USDC=100

Polymarket USDC.e on Polygon: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


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
    except ValueError:
        return default


@dataclass(frozen=True)
class WithdrawConfig:
    enabled: bool
    to_address: str
    max_usdc: float
    rpc_url: str
    usdc_address: str

    @classmethod
    def from_env(cls) -> 'WithdrawConfig':
        return cls(
            enabled=_getbool('C5_WITHDRAW_ENABLED', False),
            to_address=_getenv('C5_WITHDRAW_TO_ADDRESS', ''),
            max_usdc=_getfloat('C5_WITHDRAW_MAX_USDC', 100.0),
            rpc_url=_getenv('C5_POLYGON_RPC', 'https://polygon-bor-rpc.publicnode.com'),
            usdc_address=_getenv('C5_WITHDRAW_USDC_ADDRESS', '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'),
        )


_USDC_ABI = [
    {
        'constant': True,
        'inputs': [{'name': '_owner', 'type': 'address'}],
        'name': 'balanceOf',
        'outputs': [{'name': 'balance', 'type': 'uint256'}],
        'type': 'function',
    },
    {
        'constant': True,
        'inputs': [],
        'name': 'decimals',
        'outputs': [{'name': '', 'type': 'uint8'}],
        'type': 'function',
    },
    {
        'constant': False,
        'inputs': [{'name': '_to', 'type': 'address'}, {'name': '_value', 'type': 'uint256'}],
        'name': 'transfer',
        'outputs': [{'name': '', 'type': 'bool'}],
        'type': 'function',
    },
]


def send_usdc(private_key: str, *, to_address: str, amount_usdc: float, rpc_url: str, usdc_address: str) -> str:
    """Send USDC.e via ERC20 transfer. Returns tx hash."""

    from eth_account import Account  # type: ignore
    from web3 import Web3  # type: ignore

    if amount_usdc <= 0:
        raise ValueError('amount_usdc must be > 0')

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    acct = Account.from_key(private_key)
    from_addr = acct.address

    contract = w3.eth.contract(address=Web3.to_checksum_address(usdc_address), abi=_USDC_ABI)
    decimals = int(contract.functions.decimals().call())
    amount_raw = int(float(amount_usdc) * (10 ** decimals))

    nonce = w3.eth.get_transaction_count(from_addr)
    tx = contract.functions.transfer(Web3.to_checksum_address(to_address), amount_raw).build_transaction(
        {
            'from': from_addr,
            'nonce': nonce,
            'gas': 120000,
            'gasPrice': w3.eth.gas_price,
            'chainId': 137,
        }
    )

    signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
    raw_tx = getattr(signed, 'rawTransaction', None) or getattr(signed, 'raw_transaction', None)
    if raw_tx is None:
        raise RuntimeError('SignedTransaction missing raw tx bytes')

    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    return tx_hash.hex()


def usdc_balance(address: str, *, rpc_url: str, usdc_address: str) -> float:
    """Return the USDC.e balance for an address."""

    from web3 import Web3  # type: ignore

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    contract = w3.eth.contract(address=Web3.to_checksum_address(usdc_address), abi=_USDC_ABI)
    decimals = int(contract.functions.decimals().call())
    bal_raw = int(contract.functions.balanceOf(Web3.to_checksum_address(address)).call())
    return float(bal_raw) / float(10**decimals)


def send_usdc_all(private_key: str, *, to_address: str, rpc_url: str, usdc_address: str, max_usdc: float) -> dict:
    """Send as much USDC.e as possible up to max_usdc.

    Returns: {'sent_usdc': float, 'tx_hash': str}
    """

    from eth_account import Account  # type: ignore

    acct = Account.from_key(private_key)
    bal = usdc_balance(acct.address, rpc_url=rpc_url, usdc_address=usdc_address)
    amt = min(float(bal), float(max_usdc))
    if amt <= 0:
        raise ValueError('No USDC balance to withdraw')
    tx = send_usdc(private_key, to_address=to_address, amount_usdc=amt, rpc_url=rpc_url, usdc_address=usdc_address)
    return {'sent_usdc': float(amt), 'tx_hash': tx}
