"""Binary complement arbitrage (YES + NO < $1).

For binary markets with mutually exclusive outcomes, buying 1 share of each side
pays out exactly $1 at resolution. If we can buy both sides for < $1 (net of
fees/slippage), the bundle has locked-in profit.

This module is pure logic (no I/O) so it can be unit-tested.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ComplementArbPlan:
    ok: bool
    reason: str

    # Inputs / computed
    ask_up: float = 0.0
    ask_down: float = 0.0
    sum_asks: float = 0.0
    est_fee_usdc: float = 0.0
    est_slip_usdc: float = 0.0
    est_edge_usdc: float = 0.0

    # Execution sizing
    shares: float = 0.0
    spend_usdc: float = 0.0


def find_complement_arb(
    *,
    ask_up: float,
    size_up: float,
    ask_down: float,
    size_down: float,
    max_spend_usdc: float,
    min_edge_usdc: float,
    taker_fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    min_order_size: float = 0.001,
) -> ComplementArbPlan:
    """Decide whether to take a complement arb and size it.

    Notes:
    - `ask_*` are prices in [0,1]
    - `size_*` are available shares at the ask level
    - spend = shares * (ask_up + ask_down)
    - edge per share bundle = 1 - (ask_up + ask_down) - fees - slip
    """

    if ask_up <= 0 or ask_down <= 0:
        return ComplementArbPlan(ok=False, reason='missing_ask', ask_up=ask_up, ask_down=ask_down)

    sum_asks = float(ask_up) + float(ask_down)
    if sum_asks <= 0:
        return ComplementArbPlan(ok=False, reason='bad_prices', ask_up=ask_up, ask_down=ask_down, sum_asks=sum_asks)

    # Conservative fee/slippage approximations.
    # Polymarket fee schedules differ by market; we keep this configurable.
    fee = (taker_fee_bps / 10_000.0) * sum_asks
    slip = (slippage_bps / 10_000.0) * sum_asks

    edge_per_share = 1.0 - sum_asks - fee - slip
    if edge_per_share <= 0:
        return ComplementArbPlan(
            ok=False,
            reason='no_edge',
            ask_up=ask_up,
            ask_down=ask_down,
            sum_asks=sum_asks,
            est_fee_usdc=fee,
            est_slip_usdc=slip,
            est_edge_usdc=edge_per_share,
        )

    if edge_per_share < float(min_edge_usdc):
        return ComplementArbPlan(
            ok=False,
            reason='edge_below_min',
            ask_up=ask_up,
            ask_down=ask_down,
            sum_asks=sum_asks,
            est_fee_usdc=fee,
            est_slip_usdc=slip,
            est_edge_usdc=edge_per_share,
        )

    depth_shares = max(0.0, min(float(size_up), float(size_down)))
    if depth_shares <= 0:
        return ComplementArbPlan(ok=False, reason='no_depth', ask_up=ask_up, ask_down=ask_down, sum_asks=sum_asks)

    if max_spend_usdc <= 0:
        return ComplementArbPlan(ok=False, reason='max_spend_zero', ask_up=ask_up, ask_down=ask_down, sum_asks=sum_asks)

    # Size by spend cap and top-of-book depth.
    max_shares_by_spend = float(max_spend_usdc) / sum_asks
    shares = min(depth_shares, max_shares_by_spend)

    # Enforce min order size (per-leg, so we apply it to shares).
    shares = max(0.0, shares)
    if shares < float(min_order_size):
        return ComplementArbPlan(
            ok=False,
            reason='below_min_order_size',
            ask_up=ask_up,
            ask_down=ask_down,
            sum_asks=sum_asks,
            shares=shares,
        )

    spend = shares * sum_asks
    return ComplementArbPlan(
        ok=True,
        reason='ok',
        ask_up=ask_up,
        ask_down=ask_down,
        sum_asks=sum_asks,
        est_fee_usdc=fee,
        est_slip_usdc=slip,
        est_edge_usdc=edge_per_share,
        shares=shares,
        spend_usdc=spend,
    )
