"""Multi-asset TSMOM backtest engine — pure Python, offline only (T-25).

This module is the offline validation harness.  It must **never** be imported
by the daily Airflow/Dataflow pipeline (``orchestration/``, ``dataflow/``); the
production path reads versioned parameters from BigQuery and applies them in the
Beam gold stage (T-24).  The engine reuses the same signal and portfolio logic
as production but computes equity curves from a passed-in row dict rather than a
BigQuery query.

Timing contract — w(t) × r(t+1), never w(t) × r(t):
The signal for week t is formed from data through the Sunday that closes week t
(``compute_tsmom_rows``'s formation window is inclusive of week t, and so is the
``realized_vol_26w`` window in the T-21 view).  The book built from that signal
is traded on the Monday that opens week t+1 and earns week t+1's return.
``BacktestResult.dates`` therefore holds **return weeks**: the weights recorded
at each date were formed from the prior week's information.

Excess-return contract (resolved by T-21, documented here, not re-implemented):
``vw_asset_returns_weekly`` materialises two excess-return columns:

    excess_return      — simple: r_simple − (POW(1 + DFF/100, 7/365) − 1)
    excess_log_return  — log:    r_log    − LN(POW(1 + DFF/100, 7/365))

The engine:
  * feeds ``excess_log_return`` (additive per-asset) to ``compute_tsmom_rows``
    for the formation sum;
  * reads ``excess_return`` (simple) for cross-asset portfolio aggregation.

Log returns are NOT additive across assets (Jensen's gap grows with dispersion);
simple returns are.  The view already provides both — no internal conversion
is needed or wanted.

7/365 is deliberate: the Monday→Sunday window is 7 calendar days.  Using
5/252 would mismatch the cadence.  This pairing is also in the DDL comment
for ``vw_asset_returns_weekly`` (sql/DDL.sql).

Kim et al. (2016) isolation:
Using ``vol_scaling=False`` in ``TsmomParams`` *and* ``scheme=EQUAL_WEIGHT``
in ``PortfolioParams`` removes vol scaling from both layers and gives the true
no-vol-scaling baseline.  ``vol_scaling=False`` alone still feeds signal signs
to ``build_portfolio``, but if the scheme is ``inverse_vol``, portfolio weights
remain vol-proportional — vol scaling re-enters through the portfolio layer.
Document the required combination when calling the engine for Kim isolation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from backtest.costs import (
    CRYPTO_SYMBOLS,
    transaction_cost_return,
)
from dataflow.strategy.portfolio import PortfolioParams, build_portfolio
from dataflow.strategy.tsmom_signal import TsmomParams, compute_tsmom_rows

# ── Result containers ────────────────────────────────────────────────────────

@dataclass
class BacktestResult:
    """Output of :func:`run_backtest` for one parameter set.

    Attributes:
        dates: Calendar week-start dates of the **return weeks** that
            contributed to the equity curve (only rebalanced weeks; warm-up
            weeks are absent).  The weights recorded at each date were formed
            from the prior week's information (w(t) × r(t+1)).
        gross_returns: Pre-cost portfolio return for each week in ``dates``.
        net_returns: Post-cost portfolio return (``gross - costs``).
        turnover: Sum of absolute weight changes each week (``Σ|Δwᵢ|``).
        weights: Weight book held during each return week (after crypto-cap
            but before the next week's rebalance).
    """

    dates: list[date] = field(default_factory=list)
    gross_returns: list[float] = field(default_factory=list)
    net_returns: list[float] = field(default_factory=list)
    turnover: list[float] = field(default_factory=list)
    weights: list[dict[str, float]] = field(default_factory=list)

    # ── Derived convenience ──────────────────────────────────────────────────

    def equity_curve(self, start: float = 1.0) -> list[float]:
        """Compound the net-return series into an equity curve.

        Args:
            start: Initial portfolio value (default 1.0).

        Returns:
            Equity curve, same length as ``net_returns``, compounding simple
            returns: ``equity[t] = equity[t-1] × (1 + net_returns[t])``.
        """
        curve: list[float] = []
        v = start
        for r in self.net_returns:
            v *= (1.0 + r)
            curve.append(v)
        return curve


# ── Main entry point ─────────────────────────────────────────────────────────

def run_backtest(
    rows_by_symbol: dict[str, list[dict[str, Any]]],
    tsmom_params: TsmomParams,
    portfolio_params: PortfolioParams,
    *,
    with_crypto: bool = True,
    cost_multiplier: float = 1.0,
    roll_cost_bps_pa: dict[str, float] | None = None,
) -> BacktestResult:
    """Run the multi-asset TSMOM backtest over the supplied row data.

    Args:
        rows_by_symbol: Per-symbol row dicts, sorted ascending by
            ``week_start`` (a :class:`datetime.date` or ISO-string key).  Each
            dict must carry at minimum:

            - ``week_start`` — the Monday that opens the return period.
            - ``excess_log_return`` — additive log excess return (for T-22
              formation sum).
            - ``excess_return`` — simple excess return (for cross-asset
              portfolio aggregation; already deducted at the simple scale by
              the T-21 view).
            - ``realized_vol_26w`` — ex-ante annualised vol (may be ``None``
              during the 26-week warm-up of the T-21 estimator).

        tsmom_params: Signal parameters passed to
            :func:`~dataflow.strategy.tsmom_signal.compute_tsmom_rows`.
            ``vol_scaling=False`` + ``portfolio_params.scheme=EQUAL_WEIGHT``
            is the Kim et al. (2016) no-vol-scaling baseline.
        portfolio_params: Weighting scheme and crypto cap passed to
            :func:`~dataflow.strategy.portfolio.build_portfolio`.
        with_crypto: When ``False``, the ``BTCUSD`` leg is excluded from the
            portfolio (``build_portfolio(include_crypto=False)``).
        cost_multiplier: Scales ``ROUND_TRIP_BPS`` for every symbol — use the
            grid ``{1.0, 1.5, 2.0}`` to test cost sensitivity.  ``1.0`` is
            the base-case assumption; ``2.0`` models worst-case retail costs
            (160 bps round-trip for BTCUSD).
        roll_cost_bps_pa: Per-symbol annual roll cost in bps (sensitivity
            hook; ``None`` means all-zero, matching ``ROLL_COST_BPS_PA``
            defaults where DBC roll is already in the NAV).

    Returns:
        :class:`BacktestResult` with dates (return weeks), gross/net returns,
        turnover, and the weight book held during each rebalanced week.  The
        book earning each week's return was formed from the prior week's
        signal and volatility (w(t) × r(t+1) timing contract, see module
        docstring).
    """
    if not rows_by_symbol:
        return BacktestResult()

    # ── Pre-compute per-symbol signal rows ───────────────────────────────────
    #
    # compute_tsmom_rows reads excess_log_return (additive) for the formation
    # window sum and realized_vol_26w for the vol-scaling factor.  The result is
    # indexed by week_start so the main loop can look up each symbol's signal
    # for any date without re-walking the full series.

    signal_lookup: dict[str, dict[date, dict[str, Any]]] = {}
    return_lookup: dict[str, dict[date, float]] = {}

    for sym, rows in rows_by_symbol.items():
        if not rows:
            continue
        signal_rows = compute_tsmom_rows(rows, tsmom_params)
        sym_sig: dict[date, dict[str, Any]] = {}
        sym_ret: dict[date, float] = {}
        for orig_row, sig_row in zip(rows, signal_rows):
            wk = _to_date(orig_row["week_start"])
            sym_sig[wk] = {
                "signal": sig_row["signal"],
                "realized_vol_26w": orig_row.get("realized_vol_26w"),
            }
            exc = orig_row.get("excess_return")
            if _is_finite(exc):
                sym_ret[wk] = float(exc)
        signal_lookup[sym] = sym_sig
        return_lookup[sym] = sym_ret

    # ── Sorted union of all available dates ──────────────────────────────────
    all_dates: list[date] = sorted(
        {wk for sym_dates in signal_lookup.values() for wk in sym_dates}
    )

    # ── Main loop ─────────────────────────────────────────────────────────────
    #
    # Timing contract: w(t) × r(t+1).  The signal for week t closes on Sunday
    # night of week t; the book is traded on the Monday that opens week t+1 and
    # earns that week's return.  Both the signal sign and realized_vol_26w are
    # read from the signal week (the vol window includes week t, so it is fully
    # known when the position is sized).  The last date in all_dates forms a
    # book with no following return and is not recorded.  If dates are not
    # contiguous, the book simply trades on a stale (but past) signal — no
    # look-ahead either way.
    result = BacktestResult()
    prev_weights: dict[str, float] = {}

    for signal_wk, ret_wk in zip(all_dates, all_dates[1:]):
        # Build the cross-section from the signal week: one row per symbol that
        # has a live signal entry at that date.
        cross_section: list[dict[str, Any]] = []
        for sym, sig_map in signal_lookup.items():
            if signal_wk not in sig_map:
                continue
            sig_entry = sig_map[signal_wk]
            cross_section.append(
                {
                    "symbol": sym,
                    "signal": sig_entry["signal"],
                    "realized_vol_26w": sig_entry["realized_vol_26w"],
                    "is_crypto": sym in CRYPTO_SYMBOLS,
                }
            )

        # Delegate weighting to portfolio.py (reuses prod logic, T-23).
        weights = build_portfolio(
            cross_section,
            portfolio_params,
            include_crypto=with_crypto,
        )

        if not weights:
            # All instruments are in warm-up or flat — no rebalance.
            prev_weights = {}
            continue

        # ── Gross portfolio return (simple, cross-asset additive) ─────────────
        # Uses excess_return (simple column from T-21), not excess_log_return.
        # Log returns are not additive across assets.  The return is the NEXT
        # week's (ret_wk) — the week the book is actually held.
        gross_return = math.fsum(
            w * return_lookup.get(sym, {}).get(ret_wk, 0.0)
            for sym, w in weights.items()
        )

        # ── Transaction costs ─────────────────────────────────────────────────
        cost = 0.0
        total_turnover = 0.0
        for sym, w_after in weights.items():
            w_before = prev_weights.get(sym, 0.0)
            tc = transaction_cost_return(
                w_before, w_after, sym, cost_multiplier=cost_multiplier
            )
            cost += tc
            total_turnover += abs(w_after - w_before)

        # Include turnover from positions that were closed this week.
        for sym, w_before in prev_weights.items():
            if sym not in weights:
                tc = transaction_cost_return(
                    w_before, 0.0, sym, cost_multiplier=cost_multiplier
                )
                cost += tc
                total_turnover += abs(w_before)

        net_return = gross_return + cost  # cost is already ≤ 0

        # ── Record ────────────────────────────────────────────────────────────
        result.dates.append(ret_wk)
        result.gross_returns.append(gross_return)
        result.net_returns.append(net_return)
        result.turnover.append(total_turnover)
        result.weights.append(dict(weights))

        prev_weights = dict(weights)

    return result


# ── Internal helpers ──────────────────────────────────────────────────────────

def _to_date(value: Any) -> date:
    """Convert a week_start value to a :class:`datetime.date`.

    Accepts a ``date`` object or an ISO-format string (``YYYY-MM-DD``).
    """
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _is_finite(x: Any) -> bool:
    """True for a finite, non-NaN real number."""
    return isinstance(x, (int, float)) and math.isfinite(x)
