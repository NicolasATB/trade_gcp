"""Per-instrument-class transaction cost model for the T-25 backtest engine.

Costs are expressed in basis points (bps) on a round-trip basis.  All nine
symbols are mapped explicitly to an ``InstrumentClass`` via ``SYMBOL_CLASS``
— **not** by a string-suffix heuristic.  A heuristic like ``endswith('USD')``
would silently mis-classify BTCEUR or futures contracts at 8 bps instead of the
correct 80 bps, introducing a systematic bias in the most cost-sensitive number
in the verdict.

The symbols in ``SYMBOL_CLASS`` match those emitted by
``prod_trade_silver.vw_asset_returns_weekly`` (confirmed by ``SELECT DISTINCT
symbol`` on 2026-06-30): BTCUSD, DBC, EFA, FXY, GLD, IEF, SPY, TLT, UUP.
Crypto carries the currency pair; the eight ETFs use clean tickers.

Roll-cost note (DBC)
--------------------
DBC is a broad commodity ETF that wraps futures.  Its roll/contango cost is
**embedded in the NAV** and therefore already captured in the price (NAV)
stored in bronze/silver (T-18, T-21).  Adding an explicit roll cost on top would
double-count.  ``ROLL_COST_BPS_PA`` is therefore zero for all instruments by
default and exists only as a **sensitivity hook** — set it non-zero to measure
how much an isolated roll burden matters.

Basis for 80 bps BTCUSD
------------------------
80 bps round-trip = spot retail spread (~30 bps) + exchange fee (~40 bps) +
slippage estimate (~10 bps).  Source: Binance/Bitstamp maker/taker fee schedule
at the account tier of this project (< $50 k/month volume, 2024-2026).  The
``cost_multiplier`` grid {1.0, 1.5, 2.0} in ``run_backtest`` covers the
uncertainty range (80 → 120 → 160 bps round-trip).
"""

from __future__ import annotations

import math
from enum import Enum


class InstrumentClass(str, Enum):
    """Asset class of a frozen-universe instrument."""

    EQUITY_ETF = "equity_etf"
    BOND_ETF = "bond_etf"
    COMMODITY_ETF = "commodity_etf"
    FX_ETF = "fx_etf"
    CRYPTO = "crypto"


# Explicit mapping — single source of truth for class and universe membership.
# Symbols as emitted by vw_asset_returns_weekly.
SYMBOL_CLASS: dict[str, InstrumentClass] = {
    "SPY": InstrumentClass.EQUITY_ETF,
    "EFA": InstrumentClass.EQUITY_ETF,
    "IEF": InstrumentClass.BOND_ETF,
    "TLT": InstrumentClass.BOND_ETF,
    "GLD": InstrumentClass.COMMODITY_ETF,
    "DBC": InstrumentClass.COMMODITY_ETF,
    "UUP": InstrumentClass.FX_ETF,
    "FXY": InstrumentClass.FX_ETF,
    "BTCUSD": InstrumentClass.CRYPTO,  # view emits BTCUSD, not BTC
}

# Derived sets — keep these in sync with SYMBOL_CLASS (they ARE derived from it).
FULL_UNIVERSE: frozenset[str] = frozenset(SYMBOL_CLASS)
CRYPTO_SYMBOLS: frozenset[str] = frozenset(
    s for s, cls in SYMBOL_CLASS.items() if cls is InstrumentClass.CRYPTO
)

# Round-trip transaction costs (fee + bid-ask + slippage), in bps.
ROUND_TRIP_BPS: dict[InstrumentClass, float] = {
    InstrumentClass.EQUITY_ETF:    8.0,
    InstrumentClass.BOND_ETF:      8.0,
    InstrumentClass.COMMODITY_ETF: 12.0,
    InstrumentClass.FX_ETF:        10.0,
    InstrumentClass.CRYPTO:        80.0,  # see module docstring for basis
}

# Annual roll costs (sensitivity only; default 0 — DBC roll is in NAV already).
ROLL_COST_BPS_PA: dict[InstrumentClass, float] = {
    InstrumentClass.EQUITY_ETF:    0.0,
    InstrumentClass.BOND_ETF:      0.0,
    InstrumentClass.COMMODITY_ETF: 0.0,
    InstrumentClass.FX_ETF:        0.0,
    InstrumentClass.CRYPTO:        0.0,
}


def transaction_cost_return(
    w_before: float,
    w_after: float,
    symbol: str,
    cost_multiplier: float = 1.0,
) -> float:
    """Round-trip transaction cost as a fractional return (always ≤ 0).

    Applies the class-level ``ROUND_TRIP_BPS`` scaled by ``cost_multiplier``
    to the absolute weight change ``|w_after − w_before|``.

    Args:
        w_before: Portfolio weight before rebalancing (signed).
        w_after: Portfolio weight after rebalancing (signed).
        symbol: Instrument symbol; must be in ``SYMBOL_CLASS``.
        cost_multiplier: Sensitivity multiplier (1.0 / 1.5 / 2.0 grid).

    Returns:
        Cost as a fractional return (≤ 0; zero when no turnover).

    Raises:
        KeyError: If ``symbol`` is not in ``SYMBOL_CLASS``.
    """
    cls = SYMBOL_CLASS[symbol]
    bps = ROUND_TRIP_BPS[cls] * cost_multiplier
    turnover = abs(w_after - w_before)
    return -(bps / 10_000.0) * turnover


def roll_cost_return(
    weight: float,
    symbol: str,
    periods_per_year: int = 52,
) -> float:
    """Pro-rated weekly roll cost as a fractional return (always ≤ 0).

    This is a **sensitivity hook** — ``ROLL_COST_BPS_PA`` defaults to 0 for all
    classes.  Set non-zero values in ``ROLL_COST_BPS_PA`` to model an explicit
    roll burden (e.g. to test how much the embedded DBC roll costs).

    Args:
        weight: Current gross weight of the position (signed).
        symbol: Instrument symbol; must be in ``SYMBOL_CLASS``.
        periods_per_year: Cadence (52 for weekly).

    Returns:
        Weekly roll cost as a fractional return (≤ 0).
    """
    cls = SYMBOL_CLASS[symbol]
    annual_bps = ROLL_COST_BPS_PA[cls]
    if annual_bps == 0.0:
        return 0.0
    annual_rate = annual_bps / 10_000.0
    weekly_rate = math.pow(1.0 + annual_rate, 1.0 / periods_per_year) - 1.0
    return -weekly_rate * abs(weight)
