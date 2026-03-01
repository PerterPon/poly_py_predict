from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Optional


def _getlist(name: str, default: str = '') -> list[str]:
    """Parse comma-separated env var into a list of non-empty strings."""

    raw = _getenv(name, default) or ''
    items = [x.strip() for x in raw.split(',')]
    return [x for x in items if x]


def _getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != '' else default


def _getfloat(name: str, default: float) -> float:
    v = _getenv(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _getint(name: str, default: int) -> int:
    v = _getenv(name)
    if v is None:
        return default
    try:
        return int(float(v))
    except ValueError:
        return default


@dataclass(frozen=True)
class C5Config:
    # Dashboard
    dashboard_password: str
    dashboard_host: str
    dashboard_port: int
    dashboard_public_port: int
    dashboard_allowed_ips: Optional[str]

    # Market
    symbol: str
    symbols: list[str]
    granularity_seconds: int
    lookback_days: int

    # Model
    retrain_minutes: int
    confidence_threshold: float
    direction_threshold: float    # p_up must exceed this to call UP (default 0.5)

    # Paper
    mode: str
    paper_starting_cash: float
    paper_fee_bps: float
    paper_slippage_bps: float
    paper_position_fraction: float

    # 5-min window trading
    trade_lead_seconds: int

    # Snipe mode — late-entry strategy based on window delta
    snipe_enabled: bool
    snipe_lead_seconds: int          # seconds before window close to fire (default 10)
    snipe_min_delta_pct: float       # minimum |delta| to trigger (default 0.02%)

    # Smart signal tuning
    ensemble_weight: float           # 0.0 = pure delta, 1.0 = pure model (default 0.6)
    quiet_hours_utc: str             # e.g. "20-04" — UTC hours to skip trading (default '')

    # Delta-first strategy: use live Chainlink delta as PRIMARY signal
    # instead of ML model.  Snipe becomes the only entry mechanism.
    # https://gist.github.com/Archetapp — validated by the most profitable
    # open-source Polymarket bot ($313 → $414k on BTC 5-min markets).
    delta_first: bool                # True = snipe-only (default), False = ML+snipe
    delta_pricing: bool              # True = delta-based price cap (default)

    # Ops
    log_level: str

    @classmethod
    def from_env(cls) -> 'C5Config':
        symbols = _getlist('C5_SYMBOLS', '')
        symbol = _getenv('C5_SYMBOL', 'BTC-USD') or 'BTC-USD'
        # Priority rules:
        # 1. If C5_SYMBOLS is explicitly set (e.g. from dashboard), use it as the
        #    full list — never inject C5_SYMBOL defaults on top of it.
        # 2. If only C5_SYMBOL is set, use that as the single symbol.
        # 3. Fall back to BTC-USD.
        _symbols_explicit = bool(os.getenv('C5_SYMBOLS', '').strip())
        if _symbols_explicit and symbols:
            # C5_SYMBOLS wins entirely — use as-is, set primary to first entry
            symbol = symbols[0]
        elif not symbols:
            symbols = [symbol]
        # Never inject extra symbols from defaults into an explicit list.
        return cls(
            dashboard_password=_getenv('C5_DASHBOARD_PASSWORD', '') or '',
            dashboard_host=_getenv('C5_DASHBOARD_HOST', '0.0.0.0') or '0.0.0.0',
            dashboard_port=_getint('C5_DASHBOARD_PORT', 8601),
            dashboard_public_port=_getint('C5_DASHBOARD_PUBLIC_PORT', 8602),
            dashboard_allowed_ips=_getenv('C5_DASHBOARD_ALLOWED_IPS', None),
            symbol=symbol,
            symbols=symbols,
            granularity_seconds=_getint('C5_GRANULARITY_SECONDS', 900),
            lookback_days=_getint('C5_LOOKBACK_DAYS', 30),
            retrain_minutes=_getint('C5_RETRAIN_MINUTES', 30),
            confidence_threshold=_getfloat('C5_CONFIDENCE_THRESHOLD', 0.55),
            direction_threshold=_getfloat('C5_DIRECTION_THRESHOLD', 0.5),
            trade_lead_seconds=_getint('C5_POLY_TRADE_LEAD_SECONDS', 30),
            snipe_enabled=_getenv('C5_SNIPE_ENABLED', 'true') in ('1', 'true', 'yes', 'on'),
            snipe_lead_seconds=_getint('C5_SNIPE_LEAD_SECONDS', 10),
            snipe_min_delta_pct=_getfloat('C5_SNIPE_MIN_DELTA_PCT', 0.02),
            ensemble_weight=_getfloat('C5_ENSEMBLE_WEIGHT', 0.6),
            quiet_hours_utc=_getenv('C5_QUIET_HOURS_UTC', '') or '',
            delta_first=_getenv('C5_DELTA_FIRST', 'true') in ('1', 'true', 'yes', 'on'),
            delta_pricing=_getenv('C5_DELTA_PRICING', 'true') in ('1', 'true', 'yes', 'on'),
            mode=_getenv('C5_MODE', 'paper') or 'paper',
            paper_starting_cash=_getfloat('C5_PAPER_STARTING_CASH', 10000.0),
            paper_fee_bps=_getfloat('C5_PAPER_FEE_BPS', 10.0),
            paper_slippage_bps=_getfloat('C5_PAPER_SLIPPAGE_BPS', 5.0),
            paper_position_fraction=_getfloat('C5_PAPER_POSITION_FRACTION', 1.0),
            log_level=_getenv('C5_LOG_LEVEL', 'INFO') or 'INFO',
        )

    def with_symbol(self, symbol: str) -> 'C5Config':
        """Return a new config that targets a single symbol."""

        return C5Config(
            dashboard_password=self.dashboard_password,
            dashboard_host=self.dashboard_host,
            dashboard_port=self.dashboard_port,
            dashboard_public_port=self.dashboard_public_port,
            dashboard_allowed_ips=self.dashboard_allowed_ips,
            symbol=symbol,
            symbols=[symbol],
            granularity_seconds=self.granularity_seconds,
            lookback_days=self.lookback_days,
            retrain_minutes=self.retrain_minutes,
            confidence_threshold=self.confidence_threshold,
            direction_threshold=self.direction_threshold,
            trade_lead_seconds=self.trade_lead_seconds,
            snipe_enabled=self.snipe_enabled,
            snipe_lead_seconds=self.snipe_lead_seconds,
            snipe_min_delta_pct=self.snipe_min_delta_pct,
            ensemble_weight=self.ensemble_weight,
            quiet_hours_utc=self.quiet_hours_utc,
            delta_first=self.delta_first,
            delta_pricing=self.delta_pricing,
            mode=self.mode,
            paper_starting_cash=self.paper_starting_cash,
            paper_fee_bps=self.paper_fee_bps,
            paper_slippage_bps=self.paper_slippage_bps,
            paper_position_fraction=self.paper_position_fraction,
            log_level=self.log_level,
        )
