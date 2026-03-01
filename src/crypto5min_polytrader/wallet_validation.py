from __future__ import annotations

import re
from typing import Iterable, Optional


_EVM_ADDRESS_RE = re.compile(r'^0x[a-fA-F0-9]{40}$')
_PRIVATE_KEY_RE = re.compile(r'^0x[a-fA-F0-9]{64}$')


WALLET_ERROR_MESSAGES: dict[str, str] = {
    'private_key_required': 'Please enter your wallet private key.',
    'invalid_key': 'Invalid private key. Paste the key from reveal.magic.link/polymarket.',
    'funder_required': 'Proxy wallet address is required for Email/Google and MetaMask proxy accounts.',
    'invalid_funder_address': 'Invalid proxy wallet address. It must be a 0x address (42 chars).',
    'funder_looks_like_private_key': 'You entered a private key where a wallet address is required.',
    'funder_equals_eoa': 'Proxy wallet address cannot be the same as your private-key wallet address.',
}


def is_evm_address(value: str) -> bool:
    return bool(_EVM_ADDRESS_RE.fullmatch((value or '').strip()))


def looks_like_private_key(value: str) -> bool:
    return bool(_PRIVATE_KEY_RE.fullmatch((value or '').strip()))


def wallet_error_message(code: str) -> str:
    key = (code or '').strip()
    if not key:
        return ''
    return WALLET_ERROR_MESSAGES.get(key, key.replace('_', ' '))


def resolve_wallet_signature_and_funder(
    wallet_type: str,
    funder_address: str,
    *,
    derived_eoa: Optional[str] = None,
) -> tuple[str, Optional[str], bool, Optional[str]]:
    """Return (sig_type, funder, clear_funder, error_code)."""

    wt = (wallet_type or 'metamask').strip().lower()
    funder = (funder_address or '').strip()
    eoa = (derived_eoa or '').strip().lower()

    if wt in ('email', 'google', 'email_google'):
        sig_type = '1'
    elif wt == 'metamask_proxy':
        sig_type = '2'
    else:
        # EOA path should always clear stale funder from previous proxy configs.
        return ('0', None, True, None)

    if not funder:
        return (sig_type, None, False, 'funder_required')
    if looks_like_private_key(funder):
        return (sig_type, None, False, 'funder_looks_like_private_key')
    if not is_evm_address(funder):
        return (sig_type, None, False, 'invalid_funder_address')
    if eoa and funder.lower() == eoa:
        return (sig_type, None, False, 'funder_equals_eoa')

    return (sig_type, funder, False, None)


def patch_env_lines(
    raw_lines: list[str],
    patch_keys: dict[str, str],
    *,
    delete_keys: Optional[Iterable[str]] = None,
) -> list[str]:
    """Patch env-style lines by replacing/inserting keys and deleting selected keys."""

    delete = {k for k in (delete_keys or []) if k}
    updated: set[str] = set()
    new_lines: list[str] = []

    for ln in raw_lines:
        key = ln.split('=', 1)[0].strip() if '=' in ln else None
        if not key:
            new_lines.append(ln)
            continue
        if key in delete:
            continue
        if key in patch_keys:
            new_lines.append(f'{key}={patch_keys[key]}')
            updated.add(key)
            continue
        new_lines.append(ln)

    for key, value in patch_keys.items():
        if key in delete:
            continue
        if key not in updated:
            new_lines.append(f'{key}={value}')

    return new_lines
