"""Frozen multi-asset universe for Strategy 3 (cross-asset TSMOM).

Single source of truth for the nine instruments frozen in T-19 (eight non-crypto
ETFs + the BTC crypto sleeve), used by the multi-asset bronze ingests
(``yahoo_etf_ingest`` / ``stooq_etf_ingest``) and by the T-19/T-20 coverage-gate
integration test. The universe is frozen **in code** (a constant, not env-var
switching) so the ingest, the gate and the docs read it from one place.

Per-class sourcing is fixed in ``ml_strategy_docs/strategy_3_analysis.md``: every
non-crypto class uses Yahoo Finance as the primary source and stooq as a competing
fallback (they compete by ``priority`` in the silver consolidation, T-21); BTC
reuses the existing spot ingest (Binance/Bitstamp), so it carries no ETF tickers.

The canonical ``symbol`` is the series, not a provider ticker; each provider's
ticker for that symbol is mapped explicitly (Yahoo ``SPY`` vs stooq ``spy.us``).
``inception`` is the documented first trading date from the T-19 coverage probe;
it is the lower bound the coverage gate checks against, never an ingest cut-off
(the data, not the date, decides — see strategy_3_analysis.md).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# Coverage-gate tiers (T-19). Tier A = core ETFs (parity required); Tier B = the
# BTC sleeve (documented diversification leg, no Tier-A parity required).
TIER_A = "A"
TIER_B = "B"


@dataclass(frozen=True)
class Instrument:
    """One frozen universe member.

    Attributes:
        symbol: Canonical series symbol (e.g. ``"SPY"``, ``"BTC"``).
        asset_class: One of equity / bonds / commodities / fx / crypto.
        tier: Coverage-gate tier (:data:`TIER_A` or :data:`TIER_B`).
        inception: Documented first trading date (T-19 probe); the gate's lower
            bound, not an ingest cut-off.
        yahoo_ticker: Yahoo Finance ticker, or ``None`` for non-ETF members.
        stooq_ticker: stooq ticker (lower-case ``*.us``), or ``None``.
    """

    symbol: str
    asset_class: str
    tier: str
    inception: date
    yahoo_ticker: str | None = None
    stooq_ticker: str | None = None


# Eight non-crypto ETFs (Tier A), sourced Yahoo (primary) + stooq (fallback).
ETF_UNIVERSE: tuple[Instrument, ...] = (
    Instrument("SPY", "equity", TIER_A, date(1993, 1, 29), "SPY", "spy.us"),
    Instrument("EFA", "equity", TIER_A, date(2001, 8, 27), "EFA", "efa.us"),
    Instrument("IEF", "bonds", TIER_A, date(2002, 7, 30), "IEF", "ief.us"),
    Instrument("TLT", "bonds", TIER_A, date(2002, 7, 30), "TLT", "tlt.us"),
    Instrument("GLD", "commodities", TIER_A, date(2004, 11, 18), "GLD", "gld.us"),
    Instrument("DBC", "commodities", TIER_A, date(2006, 2, 6), "DBC", "dbc.us"),
    Instrument("UUP", "fx", TIER_A, date(2007, 3, 1), "UUP", "uup.us"),
    Instrument("FXY", "fx", TIER_A, date(2007, 2, 13), "FXY", "fxy.us"),
)

# BTC crypto sleeve (Tier B). Sourced by the existing spot ingest, not the ETF
# providers, so it carries no Yahoo/stooq ticker; included here so the universe
# and the coverage gate see the full nine instruments in one place.
CRYPTO_UNIVERSE: tuple[Instrument, ...] = (
    Instrument("BTC", "crypto", TIER_B, date(2011, 8, 18)),
)

FULL_UNIVERSE: tuple[Instrument, ...] = ETF_UNIVERSE + CRYPTO_UNIVERSE
