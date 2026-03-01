"""Polymarket execution (optional) for Crypto5min PolyTrader.

This is intentionally conservative:
- Disabled by default (paper mode)
- Requires explicit env vars to enable live trading
- Places at most one trade per 5-min window (slug-based dedup)

Market discovery uses Polymarket's Gamma Markets API.
CLOB trading uses `py-clob-client`.

Refs:
- Gamma GET /markets: https://docs.polymarket.com/developers/gamma-markets-api/get-markets
- CLOB auth: https://docs.polymarket.com/developers/CLOB/authentication
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from .persistence import JsonStore
from .window import Window

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

logger = logging.getLogger(__name__)


def _getenv(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _getfloat(name: str, default: float) -> float:
    v = _getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _getint(name: str, default: int) -> int:
    v = _getenv(name)
    if not v:
        return default
    try:
        return int(float(v))
    except ValueError:
        return default


def _getbool(name: str, default: bool = False) -> bool:
    v = _getenv(name)
    if not v:
        return default
    return v.lower() in {'1', 'true', 'yes', 'y', 'on'}


def _clamp01(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
    except Exception:
        f = float(default)
    if f != f:  # NaN
        return float(default)
    return max(0.0, min(1.0, float(f)))


def _kelly_fraction(*, p: float, P: float) -> float:
    """Return the full-Kelly fraction for a binary contract at price P.

    We use the simplified prediction-market form:

      F = (p - P) / (1 - P)

    where:
      - p is our estimated win probability (0..1)
      - P is the market price / implied probability (0..1)

    We clamp p and P to [0,1] and never return negative sizing.
    """

    p = _clamp01(p, 0.0)
    P = _clamp01(P, 0.0)

    # No edge (or bad inputs): don't bet.
    if p <= P:
        return 0.0
    # Avoid divide-by-zero / pathological near-$1 pricing.
    if P >= 0.999:
        return 0.0

    F = (p - P) / (1.0 - P)
    if F != F:  # NaN
        return 0.0
    return max(0.0, float(F))


def _parse_json_list(val: Any) -> list[str]:
    """Gamma sometimes returns list fields as JSON strings."""

    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val]
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except Exception:
            pass
    return []


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _extract_order_id(resp: Any) -> str:
    """Best-effort extraction of order id from py-clob-client responses.

    The CLOB client has changed response shapes over time; we normalize to a
    stable string so later reconciliation can query/cancel.
    """

    # If it already looks like an id.
    if resp is None:
        return ''
    if isinstance(resp, str):
        s = resp.strip()
        # Might be raw JSON.
        if s.startswith('{') and s.endswith('}'):
            try:
                import json as _json

                return _extract_order_id(_json.loads(s))
            except Exception:
                return s
        return s

    if isinstance(resp, dict):
        # Common keys.
        for k in (
            'order_id',
            'orderId',
            'orderID',
            'id',
            'order',
        ):
            v = resp.get(k)
            if isinstance(v, (str, int)) and str(v).strip():
                # If nested under 'order', recurse.
                if k == 'order' and isinstance(v, dict):
                    continue
                return str(v).strip()

        # Nested order dict.
        nested = resp.get('order')
        if isinstance(nested, dict):
            got = _extract_order_id(nested)
            if got:
                return got

        # Generic recursive search (small depth).
        for k, v in resp.items():
            if isinstance(v, dict):
                got = _extract_order_id(v)
                if got:
                    return got
            if isinstance(v, list):
                for item in v[:3]:
                    got = _extract_order_id(item)
                    if got:
                        return got
            if isinstance(v, (str, int)):
                lk = str(k).lower()
                if 'order' in lk and 'id' in lk and str(v).strip():
                    return str(v).strip()

    return ''


def _extract_fill_from_response(resp: Any) -> tuple[float, float]:
    """Best-effort extraction of (filled_size, avg_fill_price) from CLOB responses.

    The py-clob-client `post_order` response often includes:
      - takingAmount: shares received (size filled)
      - makingAmount: USDC paid

    When both are present, avg_fill_price ≈ makingAmount / takingAmount.
    Returns (0.0, 0.0) when fill info is not present.
    """

    if not isinstance(resp, dict):
        return 0.0, 0.0

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

    filled_size = max(0.0, float(taking))
    avg_px = 0.0
    if filled_size > 0 and making > 0:
        avg_px = float(making) / float(filled_size)

    # Guard against obviously bad prices.
    if not (0.0 <= avg_px <= 1.0):
        avg_px = 0.0

    return float(filled_size), float(avg_px)


def _normalize_trade_record(trade: dict) -> dict:
    """Ensure a trade record has stable keys for later reconciliation.

    This is a forward-compatible, non-breaking schema evolution helper.
    """

    out = dict(trade or {})
    now = int(time.time())

    # Keep legacy 'ts' semantics (placement time). Only fill if missing.
    if 'ts' not in out:
        out['ts'] = now
    if 'placed_ts' not in out:
        out['placed_ts'] = int(_to_f(out.get('ts'), now))

    # Normalize order id if present in response.
    if not out.get('order_id'):
        resp = out.get('response')
        oid = _extract_order_id(resp)
        if oid:
            out['order_id'] = oid

    # Status and fill fields.
    out.setdefault('order_status', 'dry_run' if out.get('dry_run') else 'unknown')
    out.setdefault('filled_size', 0.0)
    out.setdefault('avg_fill_price', 0.0)
    out.setdefault('fees_usdc', 0.0)
    out.setdefault('last_reconciled_ts', 0)
    out.setdefault('cancel_reason', '')

    return out


def estimate_redeemed_profit_usdc(trade: dict) -> Optional[float]:
    """Estimate net USDC profit for a *winning* trade that has been redeemed.

    This is UI-facing and intentionally best-effort. It does not attempt to
    parse on-chain receipts.

    Assumptions:
    - Polymarket outcome shares redeem at ~$1.00 per share when the trade wins.
    - Cost basis can be approximated from filled_size * avg_fill_price, and
      falls back to the originally requested spend (usdc).

    Returns None when the estimate is not reliable / not applicable.
    """

    if not isinstance(trade, dict):
        return None

    # Don't show a "profit" number for simulated trades.
    if trade.get('dry_run'):
        return None

    # Shares: prefer filled size, then requested size, then infer from usdc/price.
    filled_size = _to_f(trade.get('filled_size'), 0.0)
    size = _to_f(trade.get('size'), 0.0)
    usdc = _to_f(trade.get('usdc'), 0.0)
    placed_price = _to_f(trade.get('price'), 0.0)
    avg_fill_price = _to_f(trade.get('avg_fill_price'), 0.0)
    fees_usdc = _to_f(trade.get('fees_usdc'), 0.0)

    shares = 0.0
    if filled_size > 0:
        shares = filled_size
    elif size > 0:
        shares = size
    elif usdc > 0 and placed_price > 0:
        shares = usdc / placed_price

    if shares <= 0:
        return None

    # Cost basis.
    cost_usdc = 0.0
    fill_price = avg_fill_price if avg_fill_price > 0 else (placed_price if placed_price > 0 else 0.0)
    if filled_size > 0 and fill_price > 0:
        cost_usdc = filled_size * fill_price
    elif usdc > 0:
        # Fallback: treat the requested spend as the cost basis.
        cost_usdc = usdc
    elif fill_price > 0:
        cost_usdc = shares * fill_price

    if cost_usdc <= 0:
        return None

    payout_usdc = shares * 1.0
    profit = payout_usdc - cost_usdc - max(0.0, fees_usdc)

    # Guard against obviously broken numbers.
    if not (-10_000.0 <= profit <= 10_000.0):
        return None

    return float(profit)


@dataclass(frozen=True)
class PolyExecConfig:
    enabled: bool
    dry_run: bool

    gamma_url: str
    clob_url: str

    private_key: str
    signature_type: int
    funder: Optional[str]

    market_query: str
    outcome_up: str
    outcome_down: str

    max_usdc_per_trade: float
    bet_mode: str            # 'fixed' | 'percent' | 'kelly'
    bet_percent: float       # 0.0–100.0 (% of CLOB balance to risk per trade)
    kelly_fraction: float    # 0.0–1.0 (fractional Kelly multiplier; e.g. 0.25)
    cooldown_seconds: int

    # Order lifecycle controls (live mode only)
    auto_cancel_stale: bool
    order_timeout_seconds: int

    # Risk rails
    high_risk_mode: bool
    expert_mode: bool

    # Complement arbitrage (binary bundle: buy UP+DOWN when sum(asks) < 1)
    arb_enabled: bool
    arb_min_edge_cents: float
    arb_taker_fee_bps: float
    arb_slippage_bps: float

    use_5min_slug: bool

    # Snipe-mode bet multiplier: snipe trades are higher-confidence (80–95%)
    # so we size them more aggressively than ML-prediction trades.
    snipe_bet_multiplier: float   # e.g. 2.0 = snipe bets 2× normal size

    @classmethod
    def from_env(cls) -> 'PolyExecConfig':
        enabled = _getenv('C5_MODE', 'paper').lower() in {'polymarket', 'live', 'pm'}

        high_risk = _getbool('C5_POLY_HIGH_RISK_MODE', False)
        expert = _getbool('C5_POLY_EXPERT_MODE', False)
        bet_pct = _getfloat('C5_POLY_BET_PERCENT', 5.0)
        bet_pct = max(0.0, min(100.0, float(bet_pct)))
        # Default safety rail: cap bet percent unless explicitly unlocked.
        # - normal: 10%
        # - high-risk: 50%
        # - expert/unsafe: 100%
        bet_pct_cap = 100.0 if expert else (50.0 if high_risk else 10.0)
        bet_pct = min(bet_pct, bet_pct_cap)

        return cls(
            enabled=enabled,
            dry_run=_getbool('C5_POLY_DRY_RUN', True),
            gamma_url=_getenv('C5_POLY_GAMMA_URL', 'https://gamma-api.polymarket.com'),
            clob_url=_getenv('C5_POLY_CLOB_URL', 'https://clob.polymarket.com'),
            private_key=_getenv('C5_POLY_PRIVATE_KEY', ''),
            signature_type=_getint('C5_POLY_SIGNATURE_TYPE', 0),
            funder=_getenv('C5_POLY_FUNDER_ADDRESS', '') or None,
            market_query=_getenv('C5_POLY_MARKET_QUERY', 'Bitcoin Up or Down'),
            outcome_up=_getenv('C5_POLY_OUTCOME_UP', 'Up'),
            outcome_down=_getenv('C5_POLY_OUTCOME_DOWN', 'Down'),
            max_usdc_per_trade=_getfloat('C5_POLY_MAX_USDC_PER_TRADE', 5.0),
            bet_mode=_getenv('C5_POLY_BET_MODE', 'fixed').lower(),
            bet_percent=bet_pct,
            kelly_fraction=_clamp01(_getfloat('C5_POLY_KELLY_FRACTION', 0.25), 0.25),
            cooldown_seconds=_getint('C5_POLY_COOLDOWN_SECONDS', 60 * 5),
            use_5min_slug=_getbool('C5_POLY_USE_5MIN_SLUG', True),

            auto_cancel_stale=_getbool('C5_POLY_AUTO_CANCEL_STALE', True),
            order_timeout_seconds=_getint('C5_POLY_ORDER_TIMEOUT_SEC', 120),
            high_risk_mode=high_risk,
            expert_mode=expert,

            arb_enabled=_getbool('C5_POLY_ARB_ENABLED', False),
            arb_min_edge_cents=_getfloat('C5_POLY_ARB_MIN_EDGE_CENTS', 1.0),
            arb_taker_fee_bps=_getfloat('C5_POLY_ARB_TAKER_FEE_BPS', 0.0),
            arb_slippage_bps=_getfloat('C5_POLY_ARB_SLIPPAGE_BPS', 0.0),

            snipe_bet_multiplier=max(1.0, _getfloat('C5_POLY_SNIPE_BET_MULTIPLIER', 2.0)),
        )


class PolyExecutor:
    def __init__(self, cfg: PolyExecConfig):
        self.cfg = cfg
        self._last_trade_path = os.path.join('logs', 'poly_last_trade.json')
        self._trades_path = os.path.join('logs', 'poly_trades.json')

        self._last_trade_store = JsonStore(self._last_trade_path)
        self._trades_store = JsonStore(self._trades_path)

        self._client = None
        if self.cfg.enabled and not self.cfg.dry_run:
            self._client = self._init_client()

        # Risk manager — circuit breakers for capital protection.
        from .risk_rails import RiskConfig, RiskManager
        self._risk_mgr = RiskManager(RiskConfig.from_env())

    def _init_client(self):
        if not self.cfg.private_key:
            raise RuntimeError('C5_POLY_PRIVATE_KEY is required for Polymarket execution')

        # Lazy import so paper mode has zero extra dependency surface.
        from py_clob_client.client import ClobClient  # type: ignore
        from py_clob_client.constants import POLYGON  # type: ignore

        client = ClobClient(
            self.cfg.clob_url,
            key=self.cfg.private_key,
            chain_id=POLYGON,
            signature_type=self.cfg.signature_type,
            funder=self.cfg.funder,
        )
        from ._clob_auth import derive_api_creds_with_retry
        derive_api_creds_with_retry(client)
        return client

    # ------------------------------------------------------------------
    # Dedup: one trade per 5-min window
    # ------------------------------------------------------------------

    def _already_traded_window(self, slug: str) -> bool:
        """Check if we already placed a trade for this window slug."""
        try:
            raw = self._last_trade_store.load(default={}) or {}
            if not isinstance(raw, dict):
                return False
            return raw.get('window_slug') == slug
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Legacy cooldown (used when use_5min_slug=False)
    # ------------------------------------------------------------------

    def _cooldown_ok(self) -> bool:
        try:
            raw = self._last_trade_store.load(default={}) or {}
            if not isinstance(raw, dict) or not raw:
                return True
            ts = float(raw.get('ts', 0))
            return (time.time() - ts) >= float(self.cfg.cooldown_seconds)
        except Exception:
            return True

    def _mark_trade(self, payload: dict) -> None:
        payload = _normalize_trade_record(payload)

        self._last_trade_store.save(payload)

        # Append to rolling trades log (bounded)
        try:
            trades: list[dict] = []
            loaded = self._trades_store.load(default=[])
            if isinstance(loaded, list):
                trades = [x for x in loaded if isinstance(x, dict)]
            trades.append(_normalize_trade_record(payload))
            trades = trades[-500:]
            self._trades_store.save(trades)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Market discovery
    # ------------------------------------------------------------------

    def _find_market_by_slug(self, slug: str) -> dict | None:
        """Query Gamma API by exact slug. Returns market dict or None."""
        if requests is None:
            raise RuntimeError('requests is required for Gamma API market discovery')
        r = requests.get(
            f'{self.cfg.gamma_url}/markets',
            params={'slug': slug},
            timeout=15,
        )
        r.raise_for_status()
        markets = r.json() or []
        if not markets:
            return None
        # Gamma returns a list; take the first (should be exactly one).
        return markets[0] if isinstance(markets, list) else markets

    def _find_market(self) -> dict | None:
        """Legacy: text-search discovery across active markets."""
        if requests is None:
            raise RuntimeError('requests is required for Gamma API market discovery')
        params = {
            'active': 'true',
            'closed': 'false',
            'limit': 200,
            'offset': 0,
        }
        r = requests.get(f'{self.cfg.gamma_url}/markets', params=params, timeout=15)
        r.raise_for_status()
        markets = r.json() or []

        q = self.cfg.market_query.lower().strip()
        candidates = [m for m in markets if q in str(m.get('question', '')).lower()]
        if not candidates:
            return None

        def key(m: dict) -> tuple:
            accepting = bool(m.get('acceptingOrders', True))
            end = str(m.get('endDate') or '')
            return (0 if accepting else 1, end)

        candidates.sort(key=key)
        return candidates[0]

    def _token_for_direction(self, market: dict, direction: str) -> str | None:
        outcomes = _parse_json_list(market.get('outcomes'))
        token_ids = _parse_json_list(market.get('clobTokenIds'))
        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            return None

        target = self.cfg.outcome_up if direction.upper() == 'UP' else self.cfg.outcome_down
        for i, name in enumerate(outcomes):
            if str(name).strip().lower() == str(target).strip().lower():
                return str(token_ids[i])
        return None

    def _token_for_label(self, market: dict, label: str) -> str | None:
        outcomes = _parse_json_list(market.get('outcomes'))
        token_ids = _parse_json_list(market.get('clobTokenIds'))
        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            return None

        target = str(label or '').strip().lower()
        if not target:
            return None

        for i, name in enumerate(outcomes):
            if str(name).strip().lower() == target:
                return str(token_ids[i])
        return None

    def _tokens_for_complement(self, market: dict) -> tuple[str | None, str | None]:
        """Return (up_token_id, down_token_id) for the configured outcome labels."""

        up = self._token_for_label(market, self.cfg.outcome_up)
        dn = self._token_for_label(market, self.cfg.outcome_down)
        return up, dn

    # ------------------------------------------------------------------
    # Order placement (shared between trade_window and maybe_trade)
    # ------------------------------------------------------------------

    def _place_order(
        self,
        *,
        market: dict,
        direction: str,
        confidence: float,
        window_slug: str | None = None,
        snipe: bool = False,
        delta_pct: float = 0.0,
        # Optional per-asset overrides (from dashboard) — avoids mutating env.
        edge_min_override: float | None = None,
        mq_max_spread_bps_override: float | None = None,
        mq_min_depth_usdc_override: float | None = None,
        mq_depth_cap_bps_override: float | None = None,
        mq_edge_spread_mult_override: float | None = None,
        asset: str | None = None,
    ) -> dict:
        """Build and place (or dry-run) an order. Returns a result dict."""

        def _ovr(name: str, fallback: float) -> float:
            """Apply per-asset overrides, then runtime_config overrides, then fallback."""
            try:
                if name == 'C5_POLY_EDGE_MIN' and edge_min_override is not None:
                    return float(edge_min_override)
                if name == 'C5_MQ_MAX_SPREAD_BPS' and mq_max_spread_bps_override is not None:
                    return float(mq_max_spread_bps_override)
                if name == 'C5_MQ_MIN_DEPTH_USDC' and mq_min_depth_usdc_override is not None:
                    return float(mq_min_depth_usdc_override)
                if name == 'C5_MQ_DEPTH_CAP_BPS' and mq_depth_cap_bps_override is not None:
                    return float(mq_depth_cap_bps_override)
                if name == 'C5_MQ_EDGE_SPREAD_MULT' and mq_edge_spread_mult_override is not None:
                    return float(mq_edge_spread_mult_override)
            except Exception:
                return float(fallback)
            # Fall through to runtime_config overrides (dashboard saves)
            try:
                from . import runtime_config as _rc
                _rt_ovr = _rc.load_overrides()
                if _rt_ovr and name in _rt_ovr:
                    return float(_rt_ovr[name])
            except Exception:
                pass
            return float(fallback)

        token_id = self._token_for_direction(market, direction)
        if not token_id:
            return {'enabled': True, 'skipped': True, 'reason': 'no_matching_outcome',
                    'reason_code': 'no_matching_outcome'}

        # ── Risk rails check ──
        # Run circuit breakers BEFORE any sizing/pricing work.
        try:
            risk_verdict = self._risk_mgr.check()
            if not risk_verdict.allowed:
                logger.warning(
                    'RISK BLOCK: %s — %s',
                    risk_verdict.reason_code, risk_verdict.detail,
                )
                return {
                    'enabled': True, 'skipped': True,
                    'reason': risk_verdict.reason_code,
                    'reason_code': risk_verdict.reason_code,
                    'risk': risk_verdict.as_dict(),
                }
        except Exception as exc:
            logger.debug('risk check failed (proceeding): %s', exc)

        def to_f(x: Any, d: float) -> float:
            try:
                return float(x)
            except Exception:
                return d

        # ── Price discovery: prefer LIVE orderbook, fall back to Gamma ──
        # The Gamma API bestAsk is often stale by seconds or more, leading to
        # limit orders that sit unfilled until the market resolves.  Fetching
        # the live CLOB orderbook gives us the real-time ask price.
        live_ask = None
        live_book = None
        live_best_ask = None
        mq_info: dict[str, Any] | None = None
        try:
            from .polymarket_orderbook import (
                fetch_orderbook_summary,
                best_ask as _best_ask_fn,
                best_bid as _best_bid_fn,
                depth_usdc_up_to_price as _depth_usdc_up_to_price,
            )
            book = fetch_orderbook_summary(
                clob_url=self.cfg.clob_url, token_id=str(token_id), timeout=3.0,
            )
            if book is not None:
                live_book = book
                top = _best_ask_fn(book)
                top_bid = _best_bid_fn(book)
                if top is not None and top.price > 0:
                    live_best_ask = top
                    live_ask = float(top.price)
                    logger.info('CLOB live ask for %s: $%.4f', direction, live_ask)

                    # ── Market quality metrics (spread + depth) ─────────────
                    # Used for operator visibility and optional gating.
                    # Gating is controlled by env vars (defaults are permissive).
                    try:
                        ask_px = float(top.price)
                        bid_px = float(top_bid.price) if (top_bid is not None and top_bid.price > 0) else 0.0
                        mid = (ask_px + bid_px) / 2.0 if (ask_px > 0 and bid_px > 0) else 0.0
                        spread_bps = ((ask_px - bid_px) / mid * 10000.0) if mid > 0 else 0.0
                        depth_cap_bps = _ovr('C5_MQ_DEPTH_CAP_BPS', _getfloat('C5_MQ_DEPTH_CAP_BPS', 30.0))
                        cap_px = ask_px * (1.0 + max(0.0, depth_cap_bps) / 10000.0)
                        depth_usdc = float(_depth_usdc_up_to_price(book, cap_px))
                        mq_info = {
                            'ask': round(ask_px, 4),
                            'bid': round(bid_px, 4),
                            'mid': round(mid, 4),
                            'spread_bps': round(float(spread_bps), 2),
                            'depth_cap_bps': round(float(depth_cap_bps), 2),
                            'depth_usdc': round(float(depth_usdc), 2),
                        }
                    except Exception:
                        mq_info = None
                    # ── Thin book guard ──────────────────────────────────────
                    # Skip if available liquidity at best ask is below threshold.
                    # Prevents placing orders that can never fill (canceled_market_resolved).
                    min_book_usdc = _ovr('C5_POLY_MIN_BOOK_USDC', _getfloat('C5_POLY_MIN_BOOK_USDC', 8.0))
                    if min_book_usdc > 0:
                        book_depth_usdc = top.size * top.price
                        if book_depth_usdc < min_book_usdc:
                            logger.info(
                                'THIN BOOK: depth=$%.2f USDC (need $%.2f) for %s @ $%.4f — skipping',
                                book_depth_usdc, min_book_usdc, direction, live_ask,
                            )
                            return {
                                'enabled': True,
                                'skipped': True,
                                'reason': 'thin_book',
                                'market_quality': mq_info,
                                'book_depth_usdc': round(book_depth_usdc, 4),
                                'min_book_usdc': min_book_usdc,
                                'direction': direction,
                                'live_ask': live_ask,
                            }
        except Exception as exc:
            logger.debug('orderbook fetch failed, falling back to Gamma: %s', exc)

        gamma_ask = to_f(market.get('bestAsk'), 0.55)

        ask_mode = _getenv('C5_POLY_ASK_MODE', 'prefer_live').lower()
        if ask_mode not in {'prefer_live', 'legacy_max'}:
            ask_mode = 'prefer_live'

        if ask_mode == 'legacy_max':
            # Legacy behavior: choose the higher ask. This was introduced to
            # avoid underpricing when Gamma is stale-low, but can also cause
            # missed fills if Gamma is stale-high.
            base_ask = max(live_ask or 0.0, gamma_ask)
        else:
            # Default: prefer the live orderbook ask when available.
            base_ask = float(live_ask) if (live_ask is not None and live_ask > 0) else gamma_ask

        if live_ask is not None and gamma_ask > 0 and abs(live_ask - gamma_ask) > 0.01:
            logger.info(
                'Ask source (%s): live=$%.4f gamma=$%.4f delta=$%.4f base=$%.4f',
                ask_mode, live_ask, gamma_ask, live_ask - gamma_ask, base_ask,
            )

        # ── Market quality gate (optional) ───────────────────────────
        # If the live orderbook is too wide or too thin, skip the trade.
        # This reduces unfilled orders and avoids paying through bad spreads.
        if mq_info is not None and not snipe:
            max_spread_bps = _ovr('C5_MQ_MAX_SPREAD_BPS', _getfloat('C5_MQ_MAX_SPREAD_BPS', 120.0))
            min_depth_usdc = _ovr('C5_MQ_MIN_DEPTH_USDC', _getfloat('C5_MQ_MIN_DEPTH_USDC', 15.0))
            try:
                sbps = float(mq_info.get('spread_bps') or 0.0)
                d_usdc = float(mq_info.get('depth_usdc') or 0.0)
            except Exception:
                sbps = 0.0
                d_usdc = 0.0
            if max_spread_bps > 0 and sbps > max_spread_bps:
                logger.info('MARKET QUALITY: spread %.2f bps > max %.2f bps — skipping', sbps, max_spread_bps)
                return {
                    'enabled': True,
                    'skipped': True,
                    'reason': 'market_quality_spread',
                    'reason_code': 'market_quality_skip',
                    'market_quality': {**mq_info, 'max_spread_bps': float(max_spread_bps), 'min_depth_usdc': float(min_depth_usdc)},
                }
            if min_depth_usdc > 0 and d_usdc < min_depth_usdc:
                logger.info('MARKET QUALITY: depth $%.2f < min $%.2f — skipping', d_usdc, min_depth_usdc)
                return {
                    'enabled': True,
                    'skipped': True,
                    'reason': 'market_quality_depth',
                    'reason_code': 'market_quality_skip',
                    'market_quality': {**mq_info, 'max_spread_bps': float(max_spread_bps), 'min_depth_usdc': float(min_depth_usdc)},
                }

        # ── Edge gate — require p - P ≥ threshold for directional trades ──
        # Skipped for snipe (uses its own delta threshold) and arb (uses spread).
        # p = model confidence, P = market ask price.
        edge_min = _ovr('C5_POLY_EDGE_MIN', _getfloat('C5_POLY_EDGE_MIN', 0.0))
        # Adaptive edge: widen required edge when spreads widen.
        if mq_info is not None and not snipe:
            try:
                sbps = float(mq_info.get('spread_bps') or 0.0)
            except Exception:
                sbps = 0.0
            spread_mult = _ovr('C5_MQ_EDGE_SPREAD_MULT', _getfloat('C5_MQ_EDGE_SPREAD_MULT', 0.10))
            if spread_mult > 0 and sbps > 0:
                # Convert spread bps to probability space (~price cents).
                # Example: 80 bps spread, mult 0.10 → +0.0080 edge requirement.
                edge_min = float(edge_min) + (sbps / 10000.0) * float(spread_mult)
        if edge_min > 0 and not snipe:
            p = _clamp01(confidence, 0.0)
            P = _clamp01(base_ask, 0.0)
            edge = p - P
            if edge < edge_min:
                logger.info(
                    'EDGE GATE: p=%.4f  P(ask)=%.4f  edge=%.4f < min=%.4f — skipping',
                    p, P, edge, edge_min,
                )
                return {
                    'enabled': True, 'skipped': True,
                    'reason': 'edge_too_small',
                    'reason_code': 'edge_gate_blocked',
                    'market_quality': mq_info,
                    'edge': {
                        'p': float(p), 'P': float(P),
                        'edge': round(edge, 4),
                        'min_required': edge_min,
                    },
                }

        # ── Delta-based pricing for snipe trades ──────────────────────
        # When delta_pricing is enabled and we have live delta info, cap    
        # the FOK price based on the delta magnitude.  This ensures we only
        # fill at prices that give acceptable risk/reward.
        #
        # Insight from Archetapp (most profitable open-source Polymarket bot):
        # "window delta is king" — price the trade based on how much BTC
        # already moved during the window, not the stale ask + huge buffer.
        #
        # Small delta  → cheap price cap → high ROI if we win, skip if market disagrees
        # Large delta  → higher cap      → still good ROI, higher fill probability
        # Extreme delta → near-market cap → almost certain win, low ROI per share
        #
        # The FOK order fills at the BEST AVAILABLE ask (not the cap),
        # so the cap just prevents overpaying.
        delta_pricing_enabled = _getbool('C5_DELTA_PRICING', True)
        abs_delta = abs(delta_pct)

        if snipe and delta_pricing_enabled and abs_delta > 0:
            # Delta-based price GATE (configurable via env)
            # Inspired by Archetapp's proven model.  Unlike a FOK cap, the
            # delta gate compares the *live ask* against the tier price.
            # If the market ask exceeds the tier → skip (too expensive for
            # the observed delta magnitude).  If the ask is at or below the
            # tier → execute at ask + buffer (standard FOK fill logic).
            #
            # Example: delta = 0.08%, tier = $0.82.
            #   ask = $0.75 → execute (ask $0.75 ≤ tier $0.82)
            #   ask = $0.90 → skip   (ask $0.90 > tier $0.82, ROI too low)
            #
            # Tiers:
            #   delta < 0.01%  → $0.52  (require cheap ask → 100% ROI)
            #   delta   0.02%  → $0.58  (72% ROI)
            #   delta   0.05%  → $0.68  (47% ROI)
            #   delta   0.10%  → $0.82  (22% ROI)
            #   delta   0.10%+ → $0.97  (fill at almost any market price)
            t1 = _getfloat('C5_DELTA_PRICE_T1', 0.52)    # delta < 0.01%
            t2 = _getfloat('C5_DELTA_PRICE_T2', 0.58)    # delta < 0.02%
            t3 = _getfloat('C5_DELTA_PRICE_T3', 0.68)    # delta < 0.05%
            t4 = _getfloat('C5_DELTA_PRICE_T4', 0.82)    # delta < 0.10%
            t5 = _getfloat('C5_DELTA_PRICE_T5', 0.97)    # delta >= 0.10%

            if abs_delta < 0.01:
                delta_cap = t1
            elif abs_delta < 0.02:
                delta_cap = t2
            elif abs_delta < 0.05:
                delta_cap = t3
            elif abs_delta < 0.10:
                delta_cap = t4
            else:
                delta_cap = t5

            # Gate check: if the live ask exceeds our delta tier, skip.
            # The tier represents the maximum we're willing to PAY for this
            # signal strength.  If the market is pricing higher, the
            # risk/reward isn't worth it.
            if base_ask > delta_cap:
                logger.info(
                    'DELTA GATE: delta=%.4f%% cap=$%.4f ask=$%.4f → too expensive, skipping',
                    abs_delta, delta_cap, base_ask,
                )
                return {
                    'enabled': True, 'skipped': True,
                    'reason': 'delta_gate_expensive',
                    'reason_code': 'delta_gate_blocked',
                    'delta_gate': {
                        'delta_pct': float(delta_pct),
                        'abs_delta': float(abs_delta),
                        'delta_cap': float(delta_cap),
                        'base_ask': float(base_ask),
                    },
                }

            # Passed the gate → execute at standard ask + buffer pricing.
            # The FOK order fills at the best available ask just like normal;
            # the gate simply prevented us from even trying on thin-margin setups.
            price_buffer = _getfloat('C5_POLY_PRICE_BUFFER', 0.40)
            price = max(0.01, min(0.99, base_ask + price_buffer))

            logger.info(
                'DELTA PRICING: delta=%.4f%% abs=%.4f%% gate=$%.4f ask=$%.4f → exec_price=$%.4f (PASSED)',
                delta_pct, abs_delta, delta_cap, base_ask, price,
            )
        else:
            # ── Legacy pricing for ML trades and non-delta snipes ──
            # Aggressive buffer ensures FOK fills on thin books.
            price_buffer = _getfloat('C5_POLY_PRICE_BUFFER', 0.40)

            if snipe:
                snipe_price_buffer = _getfloat('C5_POLY_SNIPE_PRICE_BUFFER', 0.40)
                price_buffer = max(price_buffer, snipe_price_buffer)

            logger.info(
                'FOK pricing: base_ask=$%.4f  buffer=$%.2f  final=$%.4f  snipe=%s',
                base_ask, price_buffer, min(0.99, base_ask + price_buffer), snipe,
            )

            price = max(0.01, min(0.99, base_ask + price_buffer))

        # Compute bet size.
        bet_mode = (self.cfg.bet_mode or 'fixed').strip().lower()
        if bet_mode not in {'fixed', 'percent', 'kelly'}:
            bet_mode = 'fixed'

        bal = None
        def _get_bal() -> float:
            nonlocal bal
            if bal is not None:
                return float(bal)
            from .polymarket_account import clob_balance_usdc as _clob_bal
            bal = float(
                _clob_bal(
                    self.cfg.private_key,
                    signature_type=self.cfg.signature_type,
                    funder=self.cfg.funder,
                    clob_url=self.cfg.clob_url,
                )
            )
            return float(bal)

        if bet_mode == 'percent' and self.cfg.bet_percent > 0:
            try:
                bal_val = _get_bal()
                usdc = max(0.0, bal_val * (float(self.cfg.bet_percent) / 100.0))
            except Exception:
                # Fallback to fixed if balance fetch fails.
                usdc = max(0.0, float(self.cfg.max_usdc_per_trade))
        elif bet_mode == 'kelly':
            # Fractional Kelly sizing (defaults to 1/4 Kelly).
            # Use raw market ask (base_ask) for edge calculation, NOT the
            # execution price (which includes the price buffer).  The buffer
            # is for crossing the spread — it shouldn't penalise edge detection.
            p = _clamp01(confidence, 0.0)
            P = _clamp01(base_ask, 0.0)
            full_F = _kelly_fraction(p=p, P=P)
            frac_k = _clamp01(self.cfg.kelly_fraction, 0.25)
            F = max(0.0, full_F * frac_k)

            if F <= 0.0:
                logger.info(
                    'kelly_no_edge: p=%.4f  P=%.4f  full_F=%.4f  frac=%.2f',
                    p, P, full_F, frac_k,
                )
                return {
                    'enabled': True,
                    'skipped': True,
                    'reason': 'kelly_no_edge',
                    'reason_code': 'kelly_no_edge',
                    'kelly': {
                        'p': float(p),
                        'P': float(P),
                        'full_F': float(full_F),
                        'fraction': float(frac_k),
                        'F': float(F),
                    },
                }

            try:
                bal_val = _get_bal()
                usdc = max(0.0, bal_val * float(F))
            except Exception:
                usdc = max(0.0, float(self.cfg.max_usdc_per_trade))

            # Kelly can produce very tiny bets when edge is small.
            # Apply a configurable minimum bet (default: 2% of balance).
            kelly_min_pct = _getfloat('C5_POLY_KELLY_MIN_PCT', 2.0)
            if kelly_min_pct > 0:
                try:
                    bal_val = _get_bal()
                    min_kelly_usdc = bal_val * (kelly_min_pct / 100.0)
                    if usdc < min_kelly_usdc:
                        logger.info(
                            'Kelly bet $%.2f below floor (%.1f%% = $%.2f), bumping',
                            usdc, kelly_min_pct, min_kelly_usdc,
                        )
                        usdc = min_kelly_usdc
                except Exception:
                    pass

            logger.info(
                'kelly_bet: p=%.4f  P(ask)=%.4f  exec_price=%.4f  '
                'full_F=%.4f  frac=%.2f  F=%.4f  usdc=$%.2f',
                p, P, price, full_F, frac_k, F, usdc,
            )
        else:
            usdc = max(0.0, float(self.cfg.max_usdc_per_trade))

        # ── Snipe bet multiplier ──
        # Snipe trades carry higher empirical accuracy (80–95%) than ML
        # prediction trades (~55%), so we boost their size.
        if snipe and self.cfg.snipe_bet_multiplier > 1.0:
            pre_snipe = usdc
            usdc *= self.cfg.snipe_bet_multiplier
            logger.info(
                'snipe_bet_boost: $%.2f × %.1f → $%.2f',
                pre_snipe, self.cfg.snipe_bet_multiplier, usdc,
            )

        # Safety rail: still cap at max_usdc_per_trade if set.
        if self.cfg.max_usdc_per_trade > 0:
            usdc = min(usdc, float(self.cfg.max_usdc_per_trade))

        # ── Enforce Polymarket minimum order size ──
        # Polymarket has no official minimum order size — the CLOB will
        # reject orders that are truly too small.  We use a 1-share
        # default (configurable) and only bump small bets upward when
        # the balance can cover it.
        min_shares = _getfloat('C5_POLY_MIN_SHARES', 1.0)
        min_usdc = min_shares * price
        if usdc < min_usdc:
            try:
                bal_val = _get_bal()
            except Exception:
                bal_val = 0.0
            if bal_val >= min_usdc:
                logger.info(
                    'Bumping bet $%.2f → $%.2f (minimum %d shares × $%.4f)',
                    usdc, min_usdc, int(min_shares), price,
                )
                usdc = min_usdc
            else:
                logger.warning(
                    'TRADE SKIPPED: balance $%.2f below minimum $%.2f '
                    '(%d shares × $%.4f). Need more USDC to trade.',
                    bal_val, min_usdc, int(min_shares), price,
                )
                return {
                    'enabled': True,
                    'skipped': True,
                    'reason': 'balance_below_minimum',
                    'balance': float(bal_val),
                    'min_usdc': float(min_usdc),
                }

        # ── Thin orderbook guard (v0.4.17+) ───────────────────────────
        # Prevent "ghost wins" / win_unfilled caused by orders that sit
        # unfilled until the market resolves due to insufficient liquidity.
        #
        # IMPORTANT: FOK orders are *amount-based* (USDC) with a worst-price cap.
        # We therefore check cumulative ask-side notional (sum(price*size))
        # up to the same rounded FOK cap we will actually send.
        require_depth = _getbool('C5_POLY_REQUIRE_BOOK_DEPTH', False)
        depth_mult = _getfloat('C5_POLY_BOOK_DEPTH_MULT', 1.10)
        if require_depth and live_book is not None:
            try:
                from .polymarket_orderbook import depth_usdc_up_to_price

                fok_amount = round(float(usdc), 2)
                fok_price = round(float(price), 4)
                available_usdc = float(depth_usdc_up_to_price(live_book, fok_price))
                required_usdc = float(fok_amount) * max(1.0, float(depth_mult))

                if available_usdc + 1e-9 < required_usdc:
                    logger.info(
                        'THIN ORDERBOOK: avail=$%.2f need=$%.2f (mult=%.2f) cap=$%.4f amount=$%.2f %s',
                        available_usdc, required_usdc, float(depth_mult), fok_price, fok_amount, direction,
                    )
                    return {
                        'enabled': True,
                        'skipped': True,
                        'reason': 'thin_orderbook',
                        'reason_code': 'thin_orderbook',
                        'orderbook': {
                            'available_usdc_up_to_cap': round(float(available_usdc), 6),
                            'required_usdc': round(float(required_usdc), 6),
                            'depth_mult': float(depth_mult),
                            'price_cap': float(fok_price),
                            'amount_usdc': float(fok_amount),
                            'best_ask_price': float(live_best_ask.price) if live_best_ask else None,
                            'best_ask_size': float(live_best_ask.size) if live_best_ask else None,
                        },
                        'direction': direction,
                        'token_id': str(token_id),
                    }
            except Exception as exc:
                # Fail-open: if the orderbook snapshot can't be evaluated,
                # proceed with normal execution to avoid halting trading.
                logger.debug('thin orderbook guard failed (proceeding): %s', exc)

        size = usdc / price if price > 0 else 0.0

        info: dict[str, Any] = {
            'enabled': True,
            'dry_run': self.cfg.dry_run,
            'asset': asset,
            'market_id': market.get('id'),
            'condition_id': market.get('conditionId'),
            'question': market.get('question'),
            'token_id': token_id,
            'direction': direction,
            'confidence': float(confidence),
            'price': float(price),
            'bet_mode': bet_mode,
            'usdc': float(usdc),
            'size': float(size),
        }
        if mq_info is not None:
            info['market_quality'] = mq_info
        if window_slug:
            info['window_slug'] = window_slug

        if self.cfg.dry_run:
            self._mark_trade({'dry_run': True, 'order_status': 'dry_run', **info})
            return {'trade': info, 'placed': False}

        if self._client is None:
            self._client = self._init_client()

        from py_clob_client.order_builder.constants import BUY  # type: ignore
        from py_clob_client.clob_types import OrderArgs  # type: ignore

        # ── FOK (Fill-or-Kill) market order for ALL trades ────────────
        # FOK orders fill instantly at the best price or get killed entirely.
        #   • No order left on the book → cannot become 'canceled_market_resolved'
        #   • No polling needed → the response is the final state
        #   • Fastest possible execution → maximises fill rate
        # GTC limit orders only used as fallback if FOK import/exception fails.
        # Ref: https://docs.polymarket.com/developers/CLOB/orders/create-order
        use_fok = True
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions  # type: ignore
        except ImportError:
            # Fall through to GTC path if py-clob-client is too old.
            logger.warning('py-clob-client missing MarketOrderArgs/OrderType; falling back to GTC')
            use_fok = False

        if use_fok:
            logger.info(
                'FOK market order: amount=$%.2f price=%.4f token=%s direction=%s snipe=%s',
                round(float(usdc), 2), round(float(price), 4),
                token_id[:12], direction, snipe,
            )
            try:
                # Polymarket CLOB API hard limit: makerAmount max 2 decimals,
                # takerAmount max 4 decimals (in human-readable USDC terms).
                # BTC 5-min markets use tick_size=0.001 which maps to
                # RoundConfig(amount=5) inside py-clob-client — producing
                # taker amounts with 5 decimals that the API rejects.
                # Force tick_size="0.01" → RoundConfig(amount=4) so taker
                # stays within the 4-decimal API limit.
                fok_amount = round(float(usdc), 2)
                fok_price = round(float(price), 4)
                mo = MarketOrderArgs(
                    token_id=token_id,
                    amount=fok_amount,
                    side=BUY,
                    price=fok_price,             # worst price hint
                    order_type=OrderType.FOK,
                )
                fok_opts = PartialCreateOrderOptions(tick_size="0.01")
                signed = self._client.create_market_order(mo, options=fok_opts)
                resp = self._client.post_order(signed, OrderType.FOK)
            except Exception as exc:
                logger.error('FOK order failed: %s — falling back to GTC', exc)
                resp = None

            filled = False
            if isinstance(resp, dict):
                resp_status = str(resp.get('status') or '').strip().lower()
                resp_taking = 0.0
                try:
                    resp_taking = float(resp.get('takingAmount') or 0)
                except (ValueError, TypeError):
                    pass
                if resp_status == 'matched' or resp_taking > 0:
                    filled = True
                    logger.info(
                        'FOK FILLED: status=%s takingAmount=%.4f snipe=%s',
                        resp_status, resp_taking, snipe,
                    )
                elif resp.get('success') is False:
                    err = resp.get('errorMsg', '')
                    logger.warning('FOK rejected: %s (status=%s)', err, resp_status)
                else:
                    logger.warning('FOK not filled: status=%s (book empty?)', resp_status)

            if resp is not None:
                order_id = _extract_order_id(resp)
                info['price'] = float(price)
                info['size'] = usdc / price if price > 0 else 0.0
                info['response'] = resp
                info['order_id'] = order_id or ''
                # FOK orders are killed immediately if not filled — they never
                # rest on the book, so 'posted' would create ghost trades that
                # reconciliation can never match.
                info['order_status'] = 'filled' if filled else 'canceled'
                if not filled:
                    info['cancel_reason'] = 'fok_not_filled'
                info['fill_attempts'] = 1
                info['order_type'] = 'FOK'

                if filled:
                    filled_size, avg_px = _extract_fill_from_response(resp)
                    if filled_size > 0:
                        info['filled_size'] = float(filled_size)
                    if avg_px > 0:
                        info['avg_fill_price'] = float(avg_px)
                self._mark_trade({'dry_run': False, **info})
                return {'trade': info, 'placed': True, 'filled': filled}

            # If FOK failed with an exception (resp is None), fall through to
            # the GTC path below as a resilient fallback.
            logger.info('FOK exception fallback → GTC path with aggressive pricing')

        # ── Place order with fill-or-retry loop (GTC limit) ─────────────
        # On these fast 5-min markets the book moves quickly.  If our limit
        # order doesn't fill within ~20 s we cancel and re-post at a higher
        # price.  Max 3 attempts (price bumps +$0.02 each).
        #
        # For snipe fallback (FOK failed): 1 attempt, 8 s max poll.
        if snipe:
            max_attempts = 1
            fill_wait_sec = _getint('C5_POLY_SNIPE_FILL_WAIT_SEC', 8)
        else:
            max_attempts = _getint('C5_POLY_FILL_MAX_ATTEMPTS', 3)
            fill_wait_sec = _getint('C5_POLY_FILL_WAIT_SEC', 20)
        retry_bump = _getfloat('C5_POLY_FILL_RETRY_BUMP', 0.05)
        current_price = float(price)
        final_resp = None
        final_order_id = ''
        final_order_info: Any = None
        filled = False

        for attempt in range(1, max_attempts + 1):
            current_size = usdc / current_price if current_price > 0 else 0.0
            # Re-enforce minimum shares after price bump (price goes up each
            # retry, but usdc stays the same, so size can drop below 5).
            if current_size < min_shares:
                current_size = min_shares

            signed = self._client.create_order(
                OrderArgs(price=current_price, size=current_size, side=BUY, token_id=token_id),
            )
            resp = self._client.post_order(signed)
            order_id = _extract_order_id(resp)
            logger.info(
                'Order attempt %d/%d: price=$%.4f size=%.2f order_id=%s',
                attempt, max_attempts, current_price, current_size, order_id,
            )

            # ── Check for immediate fill signals in the post_order response ──
            # The CLOB may return status='matched' and/or takingAmount > 0
            # even when an order_id is present. This means the order was
            # fully consumed on the book — no need to poll.
            if isinstance(resp, dict):
                resp_status = str(resp.get('status') or '').strip().lower()
                resp_taking = 0.0
                try:
                    resp_taking = float(resp.get('takingAmount') or 0)
                except (ValueError, TypeError):
                    pass

                if resp_status == 'matched' or resp_taking > 0:
                    final_resp = resp
                    final_order_id = order_id or ''
                    filled = True
                    logger.info(
                        'Immediate fill: status=%s takingAmount=%.4f order_id=%s',
                        resp_status, resp_taking, order_id,
                    )
                    break

            if not order_id:
                # Could not extract order id — no fill and no order to poll.
                final_resp = resp
                final_order_id = ''
                break

            # ── Poll for fill status ──
            final_resp = resp
            final_order_id = order_id
            poll_end = time.time() + fill_wait_sec

            while time.time() < poll_end:
                time.sleep(4)
                try:
                    order_info = self._client.get_order(order_id)
                    if not isinstance(order_info, dict):
                        continue
                    final_order_info = order_info
                    raw_st = str(
                        order_info.get('status')
                        or order_info.get('state')
                        or order_info.get('order_status')
                        or ''
                    ).strip().lower()

                    if raw_st in ('filled', 'matched', 'complete', 'completed'):
                        filled = True
                        logger.info('Order %s FILLED on attempt %d', order_id, attempt)
                        break
                    if raw_st in ('canceled', 'cancelled', 'expired', 'rejected', 'failed'):
                        # Already dead — no point polling further.
                        logger.info('Order %s status=%s on attempt %d', order_id, raw_st, attempt)
                        break
                except Exception as exc:
                    logger.debug('get_order poll error: %s', exc)

            if filled:
                break

            # ── Not filled — cancel and retry at higher price ──
            if attempt < max_attempts:
                try:
                    self._client.cancel(order_id)
                    logger.info('Canceled unfilled order %s, bumping price +$%.2f', order_id, retry_bump)
                except Exception as exc:
                    logger.warning('Cancel failed for %s: %s (will still retry)', order_id, exc)

                current_price = min(0.99, current_price + retry_bump)
            else:
                logger.warning(
                    'Order %s still unfilled after %d attempts (final price $%.4f)',
                    order_id, max_attempts, current_price,
                )

        # ── Record the final state ──
        info['price'] = float(current_price)
        info['size'] = usdc / current_price if current_price > 0 else 0.0
        info['response'] = final_resp
        info['order_id'] = final_order_id
        info['order_status'] = 'filled' if filled else 'posted'
        info['fill_attempts'] = attempt

        # Persist best-effort fill fields immediately so resolution/analytics
        # are accurate even if reconciliation lags or API shapes change.
        if filled:
            filled_size, avg_px = _extract_fill_from_response(final_resp)

            # If post_order response didn't include fill sizes, fall back to the
            # last polled order snapshot.
            if (filled_size <= 0 or avg_px <= 0) and isinstance(final_order_info, dict):
                # Size.
                if filled_size <= 0:
                    v = (
                        final_order_info.get('filledSize')
                        or final_order_info.get('filled_size')
                        or final_order_info.get('executedSize')
                        or final_order_info.get('executed_size')
                        or final_order_info.get('matchedSize')
                        or final_order_info.get('sizeMatched')
                        or final_order_info.get('size_matched')
                    )
                    filled_size = max(0.0, _to_f(v, 0.0))
                    if filled_size <= 0:
                        # Some APIs only provide remaining size.
                        remaining = (
                            final_order_info.get('remainingSize')
                            or final_order_info.get('remaining_size')
                            or final_order_info.get('sizeRemaining')
                        )
                        total = (
                            final_order_info.get('originalSize')
                            or final_order_info.get('original_size')
                            or final_order_info.get('size')
                        )
                        rem = max(0.0, _to_f(remaining, 0.0))
                        tot = max(0.0, _to_f(total, 0.0))
                        if tot > 0 and tot >= rem:
                            filled_size = max(0.0, tot - rem)

                # Avg fill price.
                if avg_px <= 0:
                    vpx = (
                        final_order_info.get('avgFillPrice')
                        or final_order_info.get('avg_fill_price')
                        or final_order_info.get('averagePrice')
                        or final_order_info.get('average_price')
                        or final_order_info.get('avgPrice')
                        or final_order_info.get('avg_price')
                    )
                    avg_px = max(0.0, _to_f(vpx, 0.0))
                    if not (0.0 <= avg_px <= 1.0):
                        avg_px = 0.0

            if filled_size > 0:
                info['filled_size'] = float(filled_size)
            if avg_px > 0:
                info['avg_fill_price'] = float(avg_px)
        self._mark_trade({'dry_run': False, **info})
        return {'trade': info, 'placed': True, 'filled': filled}

    # ------------------------------------------------------------------
    # 5-min window trading (primary path)
    # ------------------------------------------------------------------

    def trade_window(
        self,
        *,
        window: Window,
        direction: str,
        confidence: float,
        snipe: bool = False,
        delta_pct: float = 0.0,
        # Optional per-asset overrides
        edge_min_override: float | None = None,
        mq_max_spread_bps_override: float | None = None,
        mq_min_depth_usdc_override: float | None = None,
        mq_depth_cap_bps_override: float | None = None,
        mq_edge_spread_mult_override: float | None = None,
        asset: str | None = None,
    ) -> dict:
        """Place a trade for a specific 5-min window.

        One trade per window is enforced by checking the window slug against the
        last trade record.

        When *snipe=True*, the bet-sizing logic applies the snipe_bet_multiplier
        so higher-confidence snipe trades can be sized more aggressively.

        *delta_pct* (optional) is the live Chainlink delta percentage for the
        current window.  When delta_pricing is enabled (C5_DELTA_PRICING=true)
        and snipe=True, the FOK price cap is derived from the delta magnitude
        instead of ask + buffer, giving better risk/reward on smaller deltas.
        """
        if not self.cfg.enabled:
            return {'enabled': False}

        if self._already_traded_window(window.slug):
            return {'enabled': True, 'skipped': True, 'reason': 'already_traded_window'}

        market = self._find_market_by_slug(window.slug)
        if not market:
            logger.warning('No market found for slug %s', window.slug)
            return {'enabled': True, 'skipped': True, 'reason': 'no_market_for_slug'}

        return self._place_order(
            market=market,
            direction=direction,
            confidence=confidence,
            window_slug=window.slug,
            snipe=snipe,
            delta_pct=delta_pct,
            edge_min_override=edge_min_override,
            mq_max_spread_bps_override=mq_max_spread_bps_override,
            mq_min_depth_usdc_override=mq_min_depth_usdc_override,
            mq_depth_cap_bps_override=mq_depth_cap_bps_override,
            mq_edge_spread_mult_override=mq_edge_spread_mult_override,
            asset=asset,
        )

    def trade_window_arb(self, *, window: Window) -> dict:
        """Attempt complement arbitrage for a specific 5-min window.

        If `C5_POLY_ARB_ENABLED=true`, this trades the binary bundle:
        BUY UP + BUY DOWN when top-of-book asks sum to < $1 (net of configured
        fee/slippage buffers) and the edge meets the configured minimum.

        This method is separate from the directional `trade_window` path so the
        caller can choose priority/order (arb-first).
        """

        if not self.cfg.enabled:
            return {'enabled': False}
        if not self.cfg.arb_enabled:
            return {'enabled': True, 'skipped': True, 'reason': 'arb_disabled'}
        if self._already_traded_window(window.slug):
            return {'enabled': True, 'skipped': True, 'reason': 'already_traded_window'}

        market = self._find_market_by_slug(window.slug)
        if not market:
            logger.warning('No market found for slug %s', window.slug)
            return {'enabled': True, 'skipped': True, 'reason': 'no_market_for_slug'}

        up_tid, dn_tid = self._tokens_for_complement(market)
        if not up_tid or not dn_tid:
            return {'enabled': True, 'skipped': True, 'reason': 'missing_token_ids'}

        from .polymarket_orderbook import fetch_orderbook_summary, best_ask
        from .strategies.complement_arb import find_complement_arb

        up_book = fetch_orderbook_summary(clob_url=self.cfg.clob_url, token_id=str(up_tid), timeout=2.5)
        dn_book = fetch_orderbook_summary(clob_url=self.cfg.clob_url, token_id=str(dn_tid), timeout=2.5)
        if up_book is None or dn_book is None:
            return {'enabled': True, 'skipped': True, 'reason': 'orderbook_unavailable'}

        up_ask = best_ask(up_book)
        dn_ask = best_ask(dn_book)
        if up_ask is None or dn_ask is None:
            return {'enabled': True, 'skipped': True, 'reason': 'missing_best_ask'}

        min_os = max(float(up_book.min_order_size or 0.001), float(dn_book.min_order_size or 0.001), 0.001)
        max_spend = max(0.0, float(self.cfg.max_usdc_per_trade))
        plan = find_complement_arb(
            ask_up=float(up_ask.price),
            size_up=float(up_ask.size),
            ask_down=float(dn_ask.price),
            size_down=float(dn_ask.size),
            max_spend_usdc=max_spend,
            min_edge_usdc=max(0.0, float(self.cfg.arb_min_edge_cents)) / 100.0,
            taker_fee_bps=max(0.0, float(self.cfg.arb_taker_fee_bps)),
            slippage_bps=max(0.0, float(self.cfg.arb_slippage_bps)),
            min_order_size=min_os,
        )

        if not plan.ok:
            return {
                'enabled': True,
                'skipped': True,
                'reason': plan.reason,
                'arb': {
                    'ask_up': plan.ask_up,
                    'ask_down': plan.ask_down,
                    'sum_asks': plan.sum_asks,
                    'edge_per_share': plan.est_edge_usdc,
                    'min_edge_cents': float(self.cfg.arb_min_edge_cents),
                },
            }

        # Build two symmetric BUY orders.
        shares = float(plan.shares)
        price_up = float(plan.ask_up)
        price_dn = float(plan.ask_down)

        trade_common: dict[str, Any] = {
            'enabled': True,
            'dry_run': self.cfg.dry_run,
            'market_id': market.get('id'),
            'condition_id': market.get('conditionId'),
            'question': market.get('question'),
            'confidence': 1.0,
            'window_slug': window.slug,
            'arb': True,
            'arb_sum_asks': float(plan.sum_asks),
            'arb_edge_per_share': float(plan.est_edge_usdc),
            'arb_min_edge_cents': float(self.cfg.arb_min_edge_cents),
            'arb_spend_usdc': float(plan.spend_usdc),
            'arb_shares': float(shares),
        }

        t_up = {
            **trade_common,
            'token_id': str(up_tid),
            'direction': 'UP',
            'price': float(price_up),
            'size': float(shares),
            'usdc': float(shares * price_up),
        }
        t_dn = {
            **trade_common,
            'token_id': str(dn_tid),
            'direction': 'DOWN',
            'price': float(price_dn),
            'size': float(shares),
            'usdc': float(shares * price_dn),
        }

        if self.cfg.dry_run:
            self._mark_trade({'dry_run': True, 'order_status': 'dry_run', **t_up})
            self._mark_trade({'dry_run': True, 'order_status': 'dry_run', **t_dn})
            return {'trade': {'bundle': True, 'up': t_up, 'down': t_dn}, 'placed': False}

        if self._client is None:
            self._client = self._init_client()

        from py_clob_client.order_builder.constants import BUY  # type: ignore
        from py_clob_client.clob_types import OrderArgs  # type: ignore

        # Post orders back-to-back (best-effort low latency).
        signed_up = self._client.create_order(OrderArgs(price=price_up, size=shares, side=BUY, token_id=str(up_tid)))
        resp_up = self._client.post_order(signed_up)
        t_up['response'] = resp_up
        t_up['order_id'] = _extract_order_id(resp_up)
        t_up['order_status'] = 'posted'
        self._mark_trade({'dry_run': False, **t_up})

        signed_dn = self._client.create_order(OrderArgs(price=price_dn, size=shares, side=BUY, token_id=str(dn_tid)))
        resp_dn = self._client.post_order(signed_dn)
        t_dn['response'] = resp_dn
        t_dn['order_id'] = _extract_order_id(resp_dn)
        t_dn['order_status'] = 'posted'
        self._mark_trade({'dry_run': False, **t_dn})

        return {'trade': {'bundle': True, 'up': t_up, 'down': t_dn}, 'placed': True}

    # ------------------------------------------------------------------
    # Legacy trade (cooldown-based, text-search)
    # ------------------------------------------------------------------

    def maybe_trade(self, *, direction: str, confidence: float) -> dict:
        """Attempt a single Polymarket trade using the legacy text-search path.

        Returns a small dict suitable for logging into state.json.
        """

        if not self.cfg.enabled:
            return {'enabled': False}

        if not self._cooldown_ok():
            return {'enabled': True, 'skipped': True, 'reason': 'cooldown'}

        market = self._find_market()
        if not market:
            return {'enabled': True, 'skipped': True, 'reason': 'no_market_match'}

        return self._place_order(
            market=market,
            direction=direction,
            confidence=confidence,
        )
