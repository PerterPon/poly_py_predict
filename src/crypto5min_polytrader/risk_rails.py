"""Risk rails — automated circuit breakers for capital protection.

These rails run BEFORE each trade is placed and can veto the trade with
a structured reason code.  They protect against:

1. **Daily loss limit** — stop trading after losing X% of starting equity
   in a rolling 24-hour window.
2. **Consecutive loss limit** — pause after N consecutive losing trades.
3. **Unfilled ratio gate** — pause if too many recent orders went unfilled
   (indicates the bot is bidding at prices the market won't fill).

All limits are configurable via env vars with safe defaults.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .persistence import JsonStore

_log = logging.getLogger(__name__)

# Persistent state for risk tracking across restarts.
_RISK_STORE = JsonStore(Path('logs') / 'risk_state.json')


@dataclass
class RiskConfig:
    """Risk-rail settings, loaded from env vars."""

    # Daily drawdown limit (% of starting balance). 0 = disabled.
    daily_loss_limit_pct: float       # e.g. 10.0 = stop after 10% daily loss
    # Consecutive losing trades before auto-pause.  0 = disabled.
    consec_loss_limit: int            # e.g. 5 = pause after 5 losses in a row
    # Unfilled order ratio threshold (0.0-1.0).  0 = disabled.
    # If more than X% of the last N orders went unfilled, pause.
    unfilled_ratio_limit: float       # e.g. 0.5 = pause if >50% unfilled
    unfilled_lookback: int            # how many recent orders to check
    # Auto-resume after N minutes of pause (0 = stay paused until manual).
    auto_resume_minutes: int

    @classmethod
    def from_env(cls) -> 'RiskConfig':
        import os

        def _f(name: str, default: float) -> float:
            v = os.getenv(name, '')
            try:
                return float(v) if v.strip() else default
            except ValueError:
                return default

        def _i(name: str, default: int) -> int:
            v = os.getenv(name, '')
            try:
                return int(float(v)) if v.strip() else default
            except ValueError:
                return default

        return cls(
            # Defaults tuned for the 15m bot (Option B):
            # - pause after 3 consecutive losses
            # - auto-resume after 45 minutes
            # Operators can override from .env or the dashboard at any time.
            daily_loss_limit_pct=_f('C5_RISK_DAILY_LOSS_PCT', 10.0),
            consec_loss_limit=_i('C5_RISK_CONSEC_LOSS_LIMIT', 3),
            unfilled_ratio_limit=_f('C5_RISK_UNFILLED_RATIO', 0.5),
            unfilled_lookback=_i('C5_RISK_UNFILLED_LOOKBACK', 20),
            auto_resume_minutes=_i('C5_RISK_AUTO_RESUME_MINUTES', 45),
        )


@dataclass
class RiskVerdict:
    """Result of a risk check — whether to allow or block the trade."""
    allowed: bool
    reason_code: str        # e.g. 'ok', 'daily_loss_limit', 'consec_loss_limit'
    detail: str             # human-readable explanation
    metrics: dict           # raw numbers for logging/dashboard

    def as_dict(self) -> dict:
        return {
            'allowed': self.allowed,
            'reason_code': self.reason_code,
            'detail': self.detail,
            'metrics': self.metrics,
        }


class RiskManager:
    """Stateful risk manager that tracks losses and fill rates."""

    def __init__(self, cfg: RiskConfig):
        self.cfg = cfg
        self._state = self._load_state()

    def _load_state(self) -> dict:
        try:
            data = _RISK_STORE.load()
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {
            'trades': [],       # list of {ts, result, pnl_usdc}
            'paused_at': 0,     # Unix ts when risk pause triggered
            'pause_reason': '',
        }

    def _save_state(self) -> None:
        try:
            _RISK_STORE.save(self._state)
        except Exception as exc:
            _log.warning('risk_state save failed: %s', exc)

    # ── Recording outcomes ───────────────────────────────────────────

    def record_trade(self, *, result: str, pnl_usdc: float = 0.0,
                     filled: bool = True) -> None:
        """Record a trade outcome. result = 'win' | 'loss' | 'push'."""
        entry = {
            'ts': time.time(),
            'result': result,
            'pnl_usdc': round(pnl_usdc, 4),
            'filled': filled,
        }
        trades = self._state.get('trades', [])
        if not isinstance(trades, list):
            trades = []
        trades.append(entry)
        # Keep last 200 trades.
        if len(trades) > 200:
            trades = trades[-200:]
        self._state['trades'] = trades
        self._save_state()

    def record_unfilled(self) -> None:
        """Record that an order was placed but went unfilled."""
        self.record_trade(result='push', pnl_usdc=0.0, filled=False)

    # ── Pre-trade risk check ─────────────────────────────────────────

    def check(self, balance_usdc: float = 0.0) -> RiskVerdict:
        """Run all risk checks. Returns a verdict."""
        now = time.time()
        trades = self._state.get('trades', [])
        if not isinstance(trades, list):
            trades = []

        # ── Auto-resume from pause ──
        paused_at = self._state.get('paused_at', 0)
        if paused_at > 0 and self.cfg.auto_resume_minutes > 0:
            elapsed_min = (now - paused_at) / 60.0
            if elapsed_min >= self.cfg.auto_resume_minutes:
                _log.info(
                    'Risk auto-resume: paused %.1f min ago (limit=%d)',
                    elapsed_min, self.cfg.auto_resume_minutes,
                )
                self._state['paused_at'] = 0
                self._state['pause_reason'] = ''
                self._save_state()

        # If still in manual/risk pause, block.
        if self._state.get('paused_at', 0) > 0:
            return RiskVerdict(
                allowed=False,
                reason_code='risk_paused',
                detail=f"Risk pause active since {self._state.get('pause_reason', 'unknown')}",
                metrics={'paused_at': self._state['paused_at']},
            )

        # ── 1. Daily loss limit ──
        if self.cfg.daily_loss_limit_pct > 0 and balance_usdc > 0:
            cutoff = now - 86400  # 24 hours
            daily_pnl = sum(
                t.get('pnl_usdc', 0)
                for t in trades
                if isinstance(t, dict) and t.get('ts', 0) > cutoff
            )
            daily_loss_pct = abs(min(daily_pnl, 0)) / balance_usdc * 100
            if daily_loss_pct >= self.cfg.daily_loss_limit_pct:
                self._trigger_pause('daily_loss_limit')
                return RiskVerdict(
                    allowed=False,
                    reason_code='daily_loss_limit',
                    detail=f'24h loss {daily_loss_pct:.1f}% >= limit {self.cfg.daily_loss_limit_pct:.1f}%',
                    metrics={
                        'daily_pnl_usdc': round(daily_pnl, 2),
                        'daily_loss_pct': round(daily_loss_pct, 2),
                        'limit_pct': self.cfg.daily_loss_limit_pct,
                    },
                )

        # ── 2. Consecutive loss limit ──
        if self.cfg.consec_loss_limit > 0:
            recent = [t for t in trades if isinstance(t, dict)][-self.cfg.consec_loss_limit:]
            if len(recent) >= self.cfg.consec_loss_limit:
                all_losses = all(t.get('result') == 'loss' for t in recent)
                if all_losses:
                    self._trigger_pause('consec_loss_limit')
                    return RiskVerdict(
                        allowed=False,
                        reason_code='consec_loss_limit',
                        detail=f'{self.cfg.consec_loss_limit} consecutive losses',
                        metrics={
                            'consec_losses': self.cfg.consec_loss_limit,
                            'limit': self.cfg.consec_loss_limit,
                        },
                    )

        # ── 3. Unfilled ratio gate ──
        if self.cfg.unfilled_ratio_limit > 0 and self.cfg.unfilled_lookback > 0:
            recent = [t for t in trades if isinstance(t, dict)][-self.cfg.unfilled_lookback:]
            if len(recent) >= 5:  # need at least 5 data points
                unfilled_count = sum(1 for t in recent if not t.get('filled', True))
                ratio = unfilled_count / len(recent)
                if ratio >= self.cfg.unfilled_ratio_limit:
                    self._trigger_pause('unfilled_ratio')
                    return RiskVerdict(
                        allowed=False,
                        reason_code='unfilled_ratio',
                        detail=f'{unfilled_count}/{len(recent)} unfilled ({ratio:.0%}) >= {self.cfg.unfilled_ratio_limit:.0%}',
                        metrics={
                            'unfilled_count': unfilled_count,
                            'lookback': len(recent),
                            'ratio': round(ratio, 3),
                            'limit': self.cfg.unfilled_ratio_limit,
                        },
                    )

        return RiskVerdict(
            allowed=True,
            reason_code='ok',
            detail='All risk checks passed',
            metrics={},
        )

    def _trigger_pause(self, reason: str) -> None:
        self._state['paused_at'] = time.time()
        self._state['pause_reason'] = reason
        self._save_state()
        _log.warning('RISK PAUSE triggered: %s', reason)

    def clear_pause(self) -> None:
        """Manually clear a risk pause (e.g. from dashboard)."""
        self._state['paused_at'] = 0
        self._state['pause_reason'] = ''
        self._save_state()
        _log.info('Risk pause cleared manually')

    def status(self) -> dict:
        """Return current risk state for dashboard."""
        trades = self._state.get('trades', [])
        if not isinstance(trades, list):
            trades = []

        now = time.time()
        cutoff = now - 86400

        daily_trades = [t for t in trades if isinstance(t, dict) and t.get('ts', 0) > cutoff]
        daily_pnl = sum(t.get('pnl_usdc', 0) for t in daily_trades)
        daily_wins = sum(1 for t in daily_trades if t.get('result') == 'win')
        daily_losses = sum(1 for t in daily_trades if t.get('result') == 'loss')

        # Consecutive losses (from end)
        consec = 0
        for t in reversed(trades):
            if isinstance(t, dict) and t.get('result') == 'loss':
                consec += 1
            else:
                break

        return {
            'paused': self._state.get('paused_at', 0) > 0,
            'pause_reason': self._state.get('pause_reason', ''),
            'daily_pnl_usdc': round(daily_pnl, 2),
            'daily_wins': daily_wins,
            'daily_losses': daily_losses,
            'consec_losses': consec,
            'total_trades': len(trades),
        }
