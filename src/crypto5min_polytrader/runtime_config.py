"""Runtime config overrides for operator controls.

We treat `config/.env` as the long-lived, secret-bearing config source.
Operator tweaks from the dashboard (symbols toggles, execution flags, risk knobs)
are stored as JSON in `logs/runtime_config.json`.

Why:
- Atomic writes via JsonStore
- Avoid rewriting .env on every click
- Avoid race conditions with the background loop's load_dotenv

Only a small allowlist of keys can be overridden.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any

from .persistence import JsonStore

logger = logging.getLogger(__name__)

RUNTIME_CONFIG_PATH = Path('logs') / 'runtime_config.json'
RUNTIME_STORE = JsonStore(RUNTIME_CONFIG_PATH)

# Only these env vars can be overridden at runtime from the dashboard.
ALLOWED_KEYS: set[str] = {
    # Symbols / model
    'C5_SYMBOL',
    'C5_SYMBOLS',
    'C5_CONFIDENCE_THRESHOLD',

    # Execution mode + Polymarket knobs
    'C5_MODE',
    'C5_POLY_DRY_RUN',
    'C5_POLY_MARKET_QUERY',
    'C5_POLY_OUTCOME_UP',
    'C5_POLY_OUTCOME_DOWN',
    'C5_POLY_MAX_USDC_PER_TRADE',
    'C5_POLY_BET_MODE',
    'C5_POLY_BET_PERCENT',
    'C5_POLY_KELLY_FRACTION',
    'C5_POLY_KELLY_MIN_PCT',
    'C5_POLY_SNIPE_BET_MULTIPLIER',
    'C5_POLY_HIGH_RISK_MODE',
    'C5_POLY_EXPERT_MODE',
    'C5_POLY_COOLDOWN_SECONDS',
    'C5_POLY_USE_5MIN_SLUG',
    'C5_POLY_TRADE_LEAD_SECONDS',

    # Complement arbitrage (safe to toggle; no secrets)
    'C5_POLY_ARB_ENABLED',
    'C5_POLY_ARB_MIN_EDGE_CENTS',
    'C5_POLY_ARB_TAKER_FEE_BPS',
    'C5_POLY_ARB_SLIPPAGE_BPS',

    # Snipe mode (late-entry window-delta strategy)
    'C5_SNIPE_ENABLED',
    'C5_SNIPE_LEAD_SECONDS',
    'C5_SNIPE_MIN_DELTA_PCT',

    # Delta-first strategy (v0.5.0) — snipe-only with delta-based pricing
    'C5_DELTA_FIRST',
    'C5_DELTA_PRICING',
    'C5_DELTA_PRICE_T1',
    'C5_DELTA_PRICE_T2',
    'C5_DELTA_PRICE_T3',
    'C5_DELTA_PRICE_T4',
    'C5_DELTA_PRICE_T5',

    # Risk rails — circuit breakers for capital protection
    'C5_RISK_DAILY_LOSS_PCT',
    'C5_RISK_CONSEC_LOSS_LIMIT',
    'C5_RISK_UNFILLED_RATIO',
    'C5_RISK_UNFILLED_LOOKBACK',
    'C5_RISK_AUTO_RESUME_MINUTES',

    # Edge gate — minimum edge (p - P) for directional trades
    'C5_POLY_EDGE_MIN',

    # Execution price-source behavior
    'C5_POLY_ASK_MODE',

    # Thin orderbook guard (prevents win_unfilled on empty books)
    'C5_POLY_REQUIRE_BOOK_DEPTH',
    'C5_POLY_BOOK_DEPTH_MULT',
    'C5_POLY_MIN_BOOK_USDC',

    # Early exit — take profit / stop loss before window resolution
    'C5_EARLY_EXIT_ENABLED',
    'C5_EARLY_EXIT_TRAIL_PCT',
    'C5_EARLY_EXIT_TRAIL_ACT_PCT',
    'C5_EARLY_EXIT_SL_PCT',

    # RTDS / Chainlink feed health
    'C5_RTDS_JSON_PING_ENABLED',
    'C5_RTDS_JSON_PING_INTERVAL_SEC',
    'C5_CHAINLINK_STALE_THRESHOLD_SEC',

    # Data fetching performance
    'C5_COINBASE_INCREMENTAL_CANDLES',

        # On-chain redeem/claim (EOA-only)
        'C5_POLY_AUTO_REDEEM_ENABLED',
        'C5_POLY_REDEEM_RETRY_MINUTES',
        'C5_POLY_MAX_REDEEM_TRADES_PER_RUN',

    # Gas / wallet care (no secrets)
    'C5_NATIVE_GAS_SYMBOL',
    'C5_NATIVE_GAS_MIN',
    'C5_GAS_TOPUP_ENABLED',
    'C5_GAS_TOPUP_TARGET_NATIVE',
    'C5_GAS_TOPUP_MAX_USDC',
    # Advanced (still safe to override)
    'C5_POLY_SIGNATURE_TYPE',
    'C5_POLY_FUNDER_ADDRESS',
    'C5_POLY_GAMMA_URL',
    'C5_POLY_CLOB_URL',

    # Ensemble / smart-signal tuning
    'C5_ENSEMBLE_WEIGHT',
    'C5_QUIET_HOURS_UTC',

    # Market quality (CLOB orderbook health)
    'C5_MQ_MAX_SPREAD_BPS',
    'C5_MQ_MIN_DEPTH_USDC',
    'C5_MQ_DEPTH_CAP_BPS',
    'C5_MQ_EDGE_SPREAD_MULT',

    # Per-asset tuning (JSON)
    'C5_PER_ASSET_TUNING_JSON',
}

# ── Warning thresholds (advisory only — never blocks a save) ─────────
# When a value crosses these thresholds we emit a log WARNING so the
# operator can see it in the terminal panel, but the value is always
# stored as-is.  Operators have full freedom to set whatever they want.
_WARN_THRESHOLDS: dict[str, list[tuple[str, float, str]]] = {
    # key: [(comparison, threshold, message), ...]
    'C5_CONFIDENCE_THRESHOLD': [
        ('lt', 0.52, 'below 0.52 is near coin-flip territory — most trades will have no edge'),
    ],
    'C5_POLY_EDGE_MIN': [
        ('eq', 0.0, 'edge gate disabled — the bot will trade even when model edge is negative'),
    ],
    'C5_POLY_BET_PERCENT': [
        ('gt', 40.0, 'risking >40% per trade is extremely aggressive — consider 10-20%'),
    ],
    'C5_RISK_DAILY_LOSS_PCT': [
        ('gt', 50.0, 'daily loss limit above 50% provides almost no protection'),
    ],
    'C5_RISK_CONSEC_LOSS_LIMIT': [
        ('eq', 0.0, 'consecutive loss limit disabled — no circuit breaker on losing streaks'),
    ],
}

_lock = threading.RLock()


def _warn_if_dangerous(key: str, value: Any) -> Any:
    """Emit a log warning if *value* is in a risky range.  Always returns
    value unchanged — the operator's choice is respected.
    """
    checks = _WARN_THRESHOLDS.get(key)
    if not checks:
        return value
    try:
        v = float(value)
    except (TypeError, ValueError):
        return value
    for cmp, thresh, msg in checks:
        triggered = False
        if cmp == 'lt' and v < thresh:
            triggered = True
        elif cmp == 'gt' and v > thresh:
            triggered = True
        elif cmp == 'eq' and v == thresh:
            triggered = True
        if triggered:
            logger.warning('⚠️  %s = %s — %s', key, value, msg)
    return value


def load_overrides() -> dict[str, Any]:
    """Return stored overrides (may be empty)."""

    with _lock:
        raw = RUNTIME_STORE.load(default={}) or {}
        return raw if isinstance(raw, dict) else {}


def save_overrides(data: dict[str, Any]) -> None:
    with _lock:
        cleaned: dict[str, Any] = {}
        for k, v in (data or {}).items():
            if k in ALLOWED_KEYS:
                _warn_if_dangerous(k, v)
                cleaned[k] = v
        RUNTIME_STORE.save(cleaned)


def update_overrides(patch: dict[str, Any]) -> dict[str, Any]:
    """Merge patch into overrides.

    - Keys not in the allowlist are ignored.
    - Values of None or '' remove the override.

    Returns the updated overrides.
    """

    with _lock:
        cur = RUNTIME_STORE.load(default={}) or {}
        if not isinstance(cur, dict):
            cur = {}

        for k, v in (patch or {}).items():
            if k not in ALLOWED_KEYS:
                continue
            if v is None:
                cur.pop(k, None)
                continue
            if isinstance(v, str) and v.strip() == '':
                cur.pop(k, None)
                continue
            _warn_if_dangerous(k, v)
            cur[k] = v

        cleaned: dict[str, Any] = {}
        for k, v in (cur or {}).items():
            if k in ALLOWED_KEYS:
                cleaned[k] = v

        RUNTIME_STORE.save(cleaned)
        return dict(cleaned)


def apply_overrides_to_environ(overrides: dict[str, Any]) -> None:
    """Apply overrides into process env.

    We set/replace allowed keys. This is used by the background loop and request
    handlers right before calling *.from_env().
    """

    if not overrides:
        return

    for k, v in overrides.items():
        if k not in ALLOWED_KEYS:
            continue
        if v is None:
            os.environ.pop(k, None)
            continue
        # Always store as strings in environ.
        os.environ[k] = str(v)
