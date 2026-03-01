"""
derive_l2_keys.py
-----------------
Standalone utility that takes a Polymarket private key and derives:
  - The wallet address (EOA)
  - The Polymarket CLOB L2 API credentials (apiKey, secret, passphrase)
  - The wallet type / sig type (0 = EOA, 1 = POLY_PROXY, 2 = GNOSIS_SAFE)
  - The proxy / funder address (for Email/Google accounts)

This runs LOCALLY — nothing is ever sent to an external server.
The private key is used only to sign the EIP-712 message that Polymarket
needs to issue the L2 credentials.

Usage (standalone):
    python derive_l2_keys.py

Usage (imported):
    from derive_l2_keys import derive_keys
    result = derive_keys("0x<private_key>")

Output shape:
    {
        "address": "0x...",
        "sig_type": 0,          # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE
        "funder_address": "0x...",  # same as address for pure EOA wallets
        "api_key": "...",
        "api_secret": "...",
        "api_passphrase": "...",
        "polymarket_profile_url": "https://polymarket.com/profile/0x..."
    }
"""

from __future__ import annotations

import json
import sys
import time
from typing import Optional

try:
    import requests
    from eth_account import Account
    from eth_account.messages import encode_defunct
    from eth_account.signers.local import LocalAccount
except ImportError as exc:
    sys.exit(
        f"Missing dependency: {exc}\n"
        "Install with: pip install requests eth-account"
    )

# ---------------------------------------------------------------------------
# Polymarket CLOB endpoints
# ---------------------------------------------------------------------------
CLOB_BASE = "https://clob.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"

# EIP-712 domain + message used by Polymarket for L2 key derivation
_CLOB_DOMAIN = {
    "name": "ClobAuthDomain",
    "version": "1",
    "chainId": 137,          # Polygon mainnet
}
_CLOB_TYPES = {
    "ClobAuth": [
        {"name": "address",   "type": "address"},
        {"name": "timestamp", "type": "string"},
        {"name": "nonce",     "type": "uint256"},
        {"name": "message",   "type": "string"},
    ]
}


def _sign_l1(account: LocalAccount, nonce: int = 0) -> dict:
    """
    Sign the Polymarket CLOB L1 auth message with the given account.
    Returns the headers dict needed to call /auth/api-key.
    """
    timestamp = str(int(time.time()))
    message = {
        "address":   account.address,
        "timestamp": timestamp,
        "nonce":     nonce,
        "message":   "This message attests that I control the given wallet",
    }
    # eth_account structured data signing
    from eth_account.structured_data.structured_data_coder import encode_structured_data  # noqa
    from eth_account._utils.structured_data.hashing import hash_domain, hash_message      # noqa

    # Build the structured data payload
    structured = {
        "types": {
            "EIP712Domain": [
                {"name": "name",    "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "ClobAuth": _CLOB_TYPES["ClobAuth"],
        },
        "primaryType": "ClobAuth",
        "domain": _CLOB_DOMAIN,
        "message": message,
    }

    signed = account.sign_typed_data(
        domain_data=_CLOB_DOMAIN,
        message_types=_CLOB_TYPES,
        message_data=message,
    )
    sig = signed.signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig

    return {
        "POLY_ADDRESS":   account.address,
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": timestamp,
        "POLY_NONCE":     str(nonce),
    }


def _fetch_l2_keys(l1_headers: dict) -> Optional[dict]:
    """
    Call POST /auth/api-key with L1 headers to create or derive L2 credentials.
    Returns {"apiKey": ..., "secret": ..., "passphrase": ...} or None on error.
    """
    url = f"{CLOB_BASE}/auth/api-key"
    try:
        r = requests.post(url, headers=l1_headers, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def _fetch_profile(address: str) -> Optional[dict]:
    """
    Query Polymarket public-profile API for the given address.
    Returns the profile dict or None.
    """
    url = "https://gamma-api.polymarket.com/public-profile"
    try:
        r = requests.get(url, params={"address": address}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and data:
                return data
    except Exception:
        pass
    return None


def _detect_sig_type(address: str, profile: Optional[dict]) -> tuple[int, str]:
    """
    Determine the CLOB signature type and funder address.

    Returns (sig_type, funder_address):
      0 = plain EOA (MetaMask direct) — funder == address
      1 = POLY_PROXY (Email/Google via Magic) — funder is the proxy
      2 = GNOSIS_SAFE (MetaMask + Polymarket proxy) — funder is the proxy
    """
    if profile is None:
        # Cannot determine — default to EOA
        return (0, address)

    proxy = profile.get("proxyWallet") or profile.get("proxy_wallet")
    wallet_type = (profile.get("walletType") or profile.get("wallet_type") or "").lower()

    if not proxy or proxy.lower() == address.lower():
        # Pure EOA — no separate proxy
        return (0, address)

    if "magic" in wallet_type or "email" in wallet_type or "google" in wallet_type:
        return (1, proxy)

    # MetaMask but with a proxy/safe
    return (2, proxy)


def derive_keys(private_key: str) -> dict:
    """
    Main entry point.

    Given a hex private key (with or without 0x prefix), derive all
    Polymarket trading credentials and return them as a dict.

    Nothing is stored; nothing is sent to any server except the Polymarket
    CLOB API to fetch the L2 keys (which requires a signature, not the raw key).
    """
    pk = private_key.strip()
    if not pk.startswith("0x"):
        pk = "0x" + pk

    try:
        account: LocalAccount = Account.from_key(pk)
    except Exception as e:
        return {"error": f"Invalid private key: {e}"}

    address = account.address

    # Step 1: Fetch on-chain profile to determine wallet type
    print(f"  → Fetching Polymarket profile for {address} …", flush=True)
    profile = _fetch_profile(address)
    sig_type, funder_address = _detect_sig_type(address, profile)

    sig_type_label = {0: "EOA (MetaMask/direct)", 1: "POLY_PROXY (Email/Google)", 2: "GNOSIS_SAFE"}
    print(f"  → Detected sig type: {sig_type} — {sig_type_label.get(sig_type, 'unknown')}", flush=True)
    print(f"  → Funder address: {funder_address}", flush=True)

    # Step 2: Sign L1 message and fetch L2 credentials
    print("  → Signing L1 message …", flush=True)
    l1_headers = _sign_l1(account, nonce=0)

    print("  → Fetching L2 API credentials …", flush=True)
    l2 = _fetch_l2_keys(l1_headers)

    if not l2 or "error" in l2:
        return {
            "error":          l2.get("error", "Failed to fetch L2 credentials") if l2 else "No response",
            "address":        address,
            "sig_type":       sig_type,
            "funder_address": funder_address,
        }

    return {
        "address":              address,
        "sig_type":             sig_type,
        "funder_address":       funder_address,
        "api_key":              l2.get("apiKey", ""),
        "api_secret":           l2.get("secret", ""),
        "api_passphrase":       l2.get("passphrase", ""),
        "polymarket_profile_url": f"https://polymarket.com/profile/{address}",
    }


# ---------------------------------------------------------------------------
# Flask API endpoint helper — call this from web.py / dashboard.py
# ---------------------------------------------------------------------------
def make_flask_route(app):
    """
    Register a POST /api/derive-keys route on an existing Flask app.

    Request body (JSON):  {"private_key": "0x..."}
    Response (JSON):
        {
            "ok": true,
            "address": "0x...",
            "sig_type": 0,
            "funder_address": "0x...",
            "api_key": "...",
            "api_secret": "...",
            "api_passphrase": "..."
        }

    This endpoint is intentionally stateless — it does NOT store the private key.
    The caller (browser wizard) receives the credentials and saves them locally
    to the bot's config via the normal /setup/save route.
    """
    from flask import request, jsonify

    @app.route("/api/derive-keys", methods=["POST"])
    def api_derive_keys():
        data = request.get_json(silent=True) or {}
        pk   = (data.get("private_key") or "").strip()
        if not pk:
            return jsonify({"ok": False, "error": "No private_key provided"}), 400

        result = derive_keys(pk)
        if "error" in result:
            return jsonify({"ok": False, **result}), 400

        return jsonify({"ok": True, **result})

    return app


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import getpass

    print("=" * 60)
    print("  Polymarket L2 Key Derivation Tool")
    print("  Keys stay on your machine — nothing is uploaded.")
    print("=" * 60)
    print()

    pk = getpass.getpass("Paste your Polymarket private key (0x...): ").strip()

    print()
    print("Deriving credentials …")
    result = derive_keys(pk)

    print()
    if "error" in result:
        print(f"ERROR: {result['error']}")
        sys.exit(1)

    print("=" * 60)
    print("  SUCCESS — copy these into your bot config:")
    print("=" * 60)
    print(json.dumps(result, indent=2))
    print()
    print("TIP: You can discard your private key now if you only")
    print("     use these L2 keys for trading. The L2 keys can only")
    print("     place/cancel orders — they cannot transfer funds.")
