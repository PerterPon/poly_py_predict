from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Optional


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
    symbol: str            # primary symbol (BTC-USD)
    assets: list[str]      # all enabled assets: ['BTC', 'ETH', 'SOL', 'XRP']
    granularity_seconds: int
    lookback_days: int

    # Model
    retrain_minutes: int
    confidence_threshold: float

    # Paper
    mode: str
    paper_starting_cash: float
    paper_fee_bps: float
    paper_slippage_bps: float
    paper_position_fraction: float

    # Ops
    log_level: str

    @classmethod
    def from_env(cls) -> 'C5Config':
        return cls(
            dashboard_password=_getenv('C5_DASHBOARD_PASSWORD', '') or '',
            dashboard_host=_getenv('C5_DASHBOARD_HOST', '0.0.0.0') or '0.0.0.0',
            dashboard_port=_getint('C5_DASHBOARD_PORT', 8601),
            dashboard_public_port=_getint('C5_DASHBOARD_PUBLIC_PORT', 8602),
            dashboard_allowed_ips=_getenv('C5_DASHBOARD_ALLOWED_IPS', None),
            symbol=_getenv('C5_SYMBOL', 'BTC-USD') or 'BTC-USD',
            granularity_seconds=_getint('C5_GRANULARITY_SECONDS', 300),
            lookback_days=_getint('C5_LOOKBACK_DAYS', 30),
            retrain_minutes=_getint('C5_RETRAIN_MINUTES', 60),
            confidence_threshold=_getfloat('C5_CONFIDENCE_THRESHOLD', 0.55),
            mode=_getenv('C5_MODE', 'paper') or 'paper',
            paper_starting_cash=_getfloat('C5_PAPER_STARTING_CASH', 10000.0),
            paper_fee_bps=_getfloat('C5_PAPER_FEE_BPS', 10.0),
            paper_slippage_bps=_getfloat('C5_PAPER_SLIPPAGE_BPS', 5.0),
            paper_position_fraction=_getfloat('C5_PAPER_POSITION_FRACTION', 1.0),
            log_level=_getenv('C5_LOG_LEVEL', 'INFO') or 'INFO',
        )
