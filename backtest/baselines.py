"""Frozen benchmark baselines for multi-asset TSMOM comparison (T-26).

Three baselines, each neutralising exactly one dimension relative to TSMOM:

  TSH (Time-Series Historical) — sign of the mean excess log return over the
  training window.  Same vol scaling, scheme, and crypto cap as TSMOM.  The
  strictest benchmark: Huang et al. (2020 JFE) show that TSMOM cannot generate
  alpha over TSH (differential p ≈ 0.26).

  Vol-Scaled Buy-and-Hold — signal = +1 always.  Same vol scaling and
  portfolio construction as TSMOM; neutralises only the directional signal
  (Kim et al. 2016 JFM confound isolation).

  60/40 Passive — 30 % SPY + 30 % EFA + 20 % IEF + 20 % TLT, fixed weights,
  annual drift rebalance.  Pure passive benchmark.

Comparability notes
-------------------
Sharpe and Sortino are scale-invariant and comparable across all four strategies.
Max drawdown is NOT comparable between 60/40 and the vol-scaled strategies: 60/40
runs at its natural volatility (~10–12 %), while TSMOM/TSH/vol-BH target 10 % via
vol scaling — any apparent similarity is coincidental.

Weight drift (60/40 only)
-------------------------
Real portfolio weights evolve with total price return, not with the excess return.
``run_passive_baseline`` uses ``simple_return`` (= total price return) for drift and
``excess_return`` for gross return reporting so the Sharpe denominator is comparable
across strategies.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any

from backtest.costs import CRYPTO_SYMBOLS, transaction_cost_return
from backtest.engine import BacktestResult
from dataflow.strategy.portfolio import PortfolioParams, build_portfolio
from dataflow.strategy.tsmom_signal import tsmom_sign

# ── Public constants ──────────────────────────────────────────────────────────

PASSIVE_WEIGHTS: dict[str, float] = {
    "SPY": 0.30,
    "EFA": 0.30,
    "IEF": 0.20,
    "TLT": 0.20,
}


# ── Private helpers ───────────────────────────────────────────────────────────

def _to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _is_finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def _portfolio_return_and_cost(
    week_weights: dict[str, float],
    prev_weights: dict[str, float],
    week_rows: dict[str, dict],
    cost_multiplier: float,
) -> tuple[float, float, float]:
    """Gross return, net return, and turnover for one week.

    Args:
        week_weights: Portfolio weights to hold this week (gross-1).
        prev_weights: Weights held last week before any rebalance.
        week_rows: Row dict keyed by symbol; each entry must carry
            ``excess_return`` (simple excess return, same column as T-21 view).
        cost_multiplier: Scales ``ROUND_TRIP_BPS`` for cost sensitivity.

    Returns:
        ``(gross_return, net_return, turnover)``
    """
    gross = math.fsum(
        w * float(week_rows[sym]["excess_return"])
        for sym, w in week_weights.items()
        if sym in week_rows and _is_finite(week_rows[sym].get("excess_return"))
    )
    cost = 0.0
    turnover = 0.0
    for sym, w_after in week_weights.items():
        w_before = prev_weights.get(sym, 0.0)
        cost += transaction_cost_return(w_before, w_after, sym, cost_multiplier)
        turnover += abs(w_after - w_before)
    for sym, w_before in prev_weights.items():
        if sym not in week_weights:
            cost += transaction_cost_return(w_before, 0.0, sym, cost_multiplier)
            turnover += abs(w_before)
    return gross, gross + cost, turnover


# ── (a) TSH — Time-Series Historical ─────────────────────────────────────────

def run_tsh_baseline(
    rows_by_symbol: dict[str, list[dict]],
    train_dates: list[date],
    test_dates: list[date],
    portfolio_params: PortfolioParams,
    *,
    with_crypto: bool = True,
    cost_multiplier: float = 1.0,
) -> BacktestResult:
    """TSH benchmark: sign of the training-window mean excess log return.

    Signal formation uses the mean ``excess_log_return`` over ``train_dates``
    per symbol; the sign is fixed for the entire test window of the fold.  Vol
    scaling and portfolio construction are identical to TSMOM — only the signal
    formation horizon differs (entire training window vs 52-week trailing window).

    This is the strictest benchmark per Huang et al. (2020 JFE): if TSMOM does
    not beat TSH, the 52-week formation adds nothing over the historical mean direction.

    Args:
        rows_by_symbol: Per-symbol row dicts, sorted ascending by ``week_start``.
        train_dates: Dates forming the training window for signal formation.
        test_dates: Dates on which to run the backtest.
        portfolio_params: Vol scaling and portfolio scheme (identical to TSMOM).
        with_crypto: Include the BTCUSD sleeve when ``True``.
        cost_multiplier: Transaction cost sensitivity multiplier.

    Returns:
        :class:`~backtest.engine.BacktestResult` over ``test_dates``.
    """
    train_set = frozenset(train_dates)

    # Build date-keyed row lookup for all symbols.
    rows_lookup: dict[str, dict[date, dict]] = {}
    for sym, rows in rows_by_symbol.items():
        sym_map: dict[date, dict] = {}
        for row in rows:
            wk = _to_date(row["week_start"])
            sym_map[wk] = row
        rows_lookup[sym] = sym_map

    # Compute TSH signal: sign of mean excess_log_return over training window.
    tsh_signals: dict[str, int] = {}
    for sym, sym_map in rows_lookup.items():
        log_rets = [
            float(sym_map[d]["excess_log_return"])
            for d in sorted(train_set)
            if d in sym_map and _is_finite(sym_map[d].get("excess_log_return"))
        ]
        if not log_rets:
            continue
        mean_ret = math.fsum(log_rets) / len(log_rets)
        sig = tsmom_sign(mean_ret)
        if sig is not None and sig != 0:
            tsh_signals[sym] = sig

    # Run over test dates with the fixed TSH signal.
    result = BacktestResult()
    prev_weights: dict[str, float] = {}
    for wk in sorted(frozenset(test_dates)):
        cross_section: list[dict[str, Any]] = []
        for sym, sig in tsh_signals.items():
            sym_map = rows_lookup.get(sym, {})
            if wk not in sym_map:
                continue
            row = sym_map[wk]
            cross_section.append({
                "symbol": sym,
                "signal": sig,
                "realized_vol_26w": row.get("realized_vol_26w"),
                "is_crypto": sym in CRYPTO_SYMBOLS,
            })

        weights = build_portfolio(cross_section, portfolio_params, include_crypto=with_crypto)
        if not weights:
            prev_weights = {}
            continue

        week_rows = {
            sym: rows_lookup[sym][wk]
            for sym in weights
            if sym in rows_lookup and wk in rows_lookup[sym]
        }
        gross, net, tv = _portfolio_return_and_cost(weights, prev_weights, week_rows, cost_multiplier)

        result.dates.append(wk)
        result.gross_returns.append(gross)
        result.net_returns.append(net)
        result.turnover.append(tv)
        result.weights.append(dict(weights))
        prev_weights = dict(weights)

    return result


# ── (b) Vol-Scaled Buy-and-Hold ───────────────────────────────────────────────

def run_vol_bh_baseline(
    rows_by_symbol: dict[str, list[dict]],
    test_dates: list[date],
    portfolio_params: PortfolioParams,
    *,
    with_crypto: bool = True,
    cost_multiplier: float = 1.0,
) -> BacktestResult:
    """Vol-BH benchmark: always long (+1), same vol scaling as TSMOM.

    Signal = +1 for every instrument every week.  Vol scaling and portfolio
    construction are identical to TSMOM; only the signal direction is neutralised
    (Kim et al. 2016 JFM isolation: if vol-BH ≈ TSMOM, vol scaling drives returns,
    not momentum).

    Args:
        rows_by_symbol: Per-symbol row dicts, sorted ascending by ``week_start``.
        test_dates: Dates on which to run the backtest.
        portfolio_params: Vol scaling and portfolio scheme (identical to TSMOM).
        with_crypto: Include the BTCUSD sleeve when ``True``.
        cost_multiplier: Transaction cost sensitivity multiplier.

    Returns:
        :class:`~backtest.engine.BacktestResult` over ``test_dates``.
    """
    rows_lookup: dict[str, dict[date, dict]] = {}
    for sym, rows in rows_by_symbol.items():
        sym_map: dict[date, dict] = {}
        for row in rows:
            wk = _to_date(row["week_start"])
            sym_map[wk] = row
        rows_lookup[sym] = sym_map

    result = BacktestResult()
    prev_weights: dict[str, float] = {}
    for wk in sorted(frozenset(test_dates)):
        cross_section: list[dict[str, Any]] = []
        for sym, sym_map in rows_lookup.items():
            if wk not in sym_map:
                continue
            row = sym_map[wk]
            cross_section.append({
                "symbol": sym,
                "signal": 1,
                "realized_vol_26w": row.get("realized_vol_26w"),
                "is_crypto": sym in CRYPTO_SYMBOLS,
            })

        weights = build_portfolio(cross_section, portfolio_params, include_crypto=with_crypto)
        if not weights:
            prev_weights = {}
            continue

        week_rows = {
            sym: rows_lookup[sym][wk]
            for sym in weights
            if sym in rows_lookup and wk in rows_lookup[sym]
        }
        gross, net, tv = _portfolio_return_and_cost(weights, prev_weights, week_rows, cost_multiplier)

        result.dates.append(wk)
        result.gross_returns.append(gross)
        result.net_returns.append(net)
        result.turnover.append(tv)
        result.weights.append(dict(weights))
        prev_weights = dict(weights)

    return result


# ── (c) 60/40 Passive ────────────────────────────────────────────────────────

def run_passive_baseline(
    rows_by_symbol: dict[str, list[dict]],
    test_dates: list[date],
    *,
    cost_multiplier: float = 1.0,
    rebalance_weeks: int = 52,
) -> BacktestResult:
    """60/40 Passive benchmark: fixed weights with annual drift rebalance.

    Weight drift uses ``simple_return`` (total price return) so portfolio weights
    evolve as in a real portfolio.  Gross returns are reported using
    ``excess_return`` for comparability with vol-scaled strategies.

    Rebalance to :data:`PASSIVE_WEIGHTS` at week 0 (inception) and every
    ``rebalance_weeks`` thereafter.  Transaction costs are applied only at
    rebalance; no costs between rebalances (natural drift, no trades).

    Args:
        rows_by_symbol: Per-symbol row dicts; must carry ``simple_return`` and
            ``excess_return`` for the :data:`PASSIVE_WEIGHTS` symbols.
        test_dates: Dates on which to run the backtest.
        cost_multiplier: Transaction cost sensitivity multiplier.
        rebalance_weeks: Drift-rebalance frequency (52 = annual).

    Returns:
        :class:`~backtest.engine.BacktestResult` over ``test_dates``.
    """
    # Build lookup restricted to passive universe.
    rows_lookup: dict[str, dict[date, dict]] = {}
    for sym in PASSIVE_WEIGHTS:
        sym_map: dict[date, dict] = {}
        for row in rows_by_symbol.get(sym, []):
            wk = _to_date(row["week_start"])
            sym_map[wk] = row
        rows_lookup[sym] = sym_map

    sorted_dates = sorted(frozenset(test_dates))
    result = BacktestResult()
    prev_weights: dict[str, float] = {}
    weeks_since_rebalance = 0

    for i, wk in enumerate(sorted_dates):
        rebalancing = (i == 0) or (weeks_since_rebalance >= rebalance_weeks)

        if rebalancing:
            available = {s for s in PASSIVE_WEIGHTS if wk in rows_lookup.get(s, {})}
            if not available:
                continue
            total_target = sum(PASSIVE_WEIGHTS[s] for s in available)
            week_weights = {s: PASSIVE_WEIGHTS[s] / total_target for s in available}
            weeks_since_rebalance = 1  # count this week; next rebalance after rebalance_weeks more
        else:
            week_weights = dict(prev_weights)
            weeks_since_rebalance += 1
            if not week_weights:
                continue

        # Gross return uses excess_return.
        gross = 0.0
        for sym, w in week_weights.items():
            row = rows_lookup.get(sym, {}).get(wk)
            if row and _is_finite(row.get("excess_return")):
                gross += w * float(row["excess_return"])

        # Costs only at rebalance.
        if rebalancing:
            cost = 0.0
            turnover = 0.0
            for sym, w_after in week_weights.items():
                w_before = prev_weights.get(sym, 0.0)
                cost += transaction_cost_return(w_before, w_after, sym, cost_multiplier)
                turnover += abs(w_after - w_before)
            for sym, w_before in prev_weights.items():
                if sym not in week_weights:
                    cost += transaction_cost_return(w_before, 0.0, sym, cost_multiplier)
                    turnover += abs(w_before)
        else:
            cost = 0.0
            turnover = 0.0

        result.dates.append(wk)
        result.gross_returns.append(gross)
        result.net_returns.append(gross + cost)
        result.turnover.append(turnover)
        result.weights.append(dict(week_weights))

        # Drift weights for next week using simple_return (total price return).
        drifted: dict[str, float] = {}
        total = 0.0
        for sym, w in week_weights.items():
            row = rows_lookup.get(sym, {}).get(wk)
            sr = row.get("simple_return") if row else None
            growth = w * (1.0 + float(sr)) if _is_finite(sr) else w
            drifted[sym] = growth
            total += abs(growth)
        if total > 0:
            prev_weights = {s: v / total for s, v in drifted.items()}
        else:
            prev_weights = dict(week_weights)

    return result
