from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class PaperState:
    cash: float
    position_base: float
    last_price: float
    equity: float
    trades: int


def init_state(starting_cash: float) -> PaperState:
    return PaperState(
        cash=float(starting_cash),
        position_base=0.0,
        last_price=0.0,
        equity=float(starting_cash),
        trades=0,
    )


def _apply_costs(price: float, side: str, fee_bps: float, slippage_bps: float) -> float:
    # Very simple cost model: slippage moves against you, fee increases effective cost.
    slip = slippage_bps / 10_000.0
    fee = fee_bps / 10_000.0
    if side == 'buy':
        return price * (1 + slip + fee)
    return price * (1 - slip - fee)


def step(
    state: PaperState,
    price: float,
    signal_up: bool,
    position_fraction: float,
    fee_bps: float,
    slippage_bps: float,
) -> PaperState:
    """A naive strategy: if signal_up -> fully invested, else -> all cash."""

    price = float(price)
    state.last_price = price

    target_invest = float(position_fraction) if signal_up else 0.0
    target_value = state.equity * target_invest
    current_value = state.position_base * price

    # buy
    if target_value > current_value + 1e-9:
        delta_value = target_value - current_value
        # Cap by available cash
        delta_value = min(delta_value, state.cash)
        if delta_value > 0:
            fill = _apply_costs(price, 'buy', fee_bps, slippage_bps)
            base_bought = delta_value / fill
            state.cash -= delta_value
            state.position_base += base_bought
            state.trades += 1

    # sell
    if target_value < current_value - 1e-9:
        delta_value = current_value - target_value
        if delta_value > 0 and state.position_base > 0:
            fill = _apply_costs(price, 'sell', fee_bps, slippage_bps)
            base_sold = min(state.position_base, delta_value / price)
            proceeds = base_sold * fill
            state.position_base -= base_sold
            state.cash += proceeds
            state.trades += 1

    state.equity = state.cash + state.position_base * price
    return state


def backtest(
    df: pd.DataFrame,
    proba_col: str,
    threshold: float,
    starting_cash: float,
    position_fraction: float,
    fee_bps: float,
    slippage_bps: float,
) -> dict:
    state = init_state(starting_cash)
    equity_curve = []

    for _, row in df.iterrows():
        price = float(row['close'])
        p = float(row[proba_col])
        signal_up = p >= float(threshold)
        step(state, price, signal_up, position_fraction, fee_bps, slippage_bps)
        equity_curve.append({'time': row['time'], 'equity': state.equity})

    eq = pd.DataFrame(equity_curve)
    if eq.empty:
        return {
            'final_equity': starting_cash,
            'return_pct': 0.0,
            'trades': 0,
            'equity_curve': [],
        }

    final_equity = float(eq['equity'].iloc[-1])
    return {
        'final_equity': final_equity,
        'return_pct': (final_equity / float(starting_cash) - 1.0) * 100.0,
        'trades': int(state.trades),
        'equity_curve': [
            {'time': str(t), 'equity': float(v)}
            for t, v in zip(eq['time'].astype(str).tolist(), eq['equity'].tolist())
        ],
    }
