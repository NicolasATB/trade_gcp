"""Tests for backtest.costs — transaction cost model and universe registry (T-25)."""

from __future__ import annotations

import pytest

from backtest.costs import (
    CRYPTO_SYMBOLS,
    FULL_UNIVERSE,
    ROUND_TRIP_BPS,
    SYMBOL_CLASS,
    InstrumentClass,
    roll_cost_return,
    transaction_cost_return,
)

# Ground-truth set from SELECT DISTINCT symbol FROM vw_asset_returns_weekly
# (confirmed 2026-06-30).  Any divergence means SYMBOL_CLASS is out of sync
# with the view that feeds the backtest engine.
EXPECTED_SYMBOLS = frozenset({
    "SPY", "EFA",        # equities
    "IEF", "TLT",        # bonds
    "GLD", "DBC",        # commodities
    "UUP", "FXY",        # FX
    "BTCUSD",            # crypto — view emits the currency pair, not "BTC"
})


class TestUniverseRegistry:
    def test_symbol_class_matches_view_universe(self):
        assert set(SYMBOL_CLASS.keys()) == EXPECTED_SYMBOLS, (
            "SYMBOL_CLASS diverges from vw_asset_returns_weekly.  "
            "Re-run SELECT DISTINCT symbol FROM vw_asset_returns_weekly and update."
        )

    def test_full_universe_derived_from_symbol_class(self):
        assert FULL_UNIVERSE == frozenset(SYMBOL_CLASS.keys())

    def test_crypto_symbols_derived_not_hardcoded(self):
        expected = frozenset(
            s for s, cls in SYMBOL_CLASS.items() if cls is InstrumentClass.CRYPTO
        )
        assert CRYPTO_SYMBOLS == expected

    def test_crypto_symbols_is_btcusd(self):
        # Only one crypto in the frozen universe; the symbol is BTCUSD (not BTC).
        assert CRYPTO_SYMBOLS == {"BTCUSD"}

    def test_btcusd_classified_as_crypto(self):
        assert SYMBOL_CLASS["BTCUSD"] is InstrumentClass.CRYPTO

    def test_all_classes_have_bps_entry(self):
        for cls in InstrumentClass:
            assert cls in ROUND_TRIP_BPS, f"{cls} missing from ROUND_TRIP_BPS"


class TestTransactionCostReturn:
    def test_zero_turnover_zero_cost(self):
        for sym in SYMBOL_CLASS:
            assert transaction_cost_return(0.5, 0.5, sym) == 0.0, sym
            assert transaction_cost_return(-0.3, -0.3, sym) == 0.0, sym

    def test_cost_is_nonpositive(self):
        for sym in SYMBOL_CLASS:
            cost = transaction_cost_return(0.0, 0.3, sym)
            assert cost <= 0.0, f"{sym}: expected cost <= 0, got {cost}"

    def test_cost_scales_with_turnover(self):
        c1 = transaction_cost_return(0.0, 0.1, "SPY")
        c2 = transaction_cost_return(0.0, 0.2, "SPY")
        assert abs(c2) == pytest.approx(2 * abs(c1), rel=1e-9)

    def test_crypto_cost_greater_than_equity_same_turnover(self):
        c_equity = transaction_cost_return(0.0, 0.5, "SPY")
        c_crypto = transaction_cost_return(0.0, 0.5, "BTCUSD")
        assert abs(c_crypto) > abs(c_equity)

    def test_unknown_symbol_raises_key_error(self):
        with pytest.raises(KeyError):
            transaction_cost_return(0.0, 0.1, "UNKNOWN")

    def test_symmetric_turnover(self):
        # Buy → sell should cost same as sell → buy (absolute turnover is same).
        c_buy = transaction_cost_return(0.0, 0.4, "IEF")
        c_sell = transaction_cost_return(0.4, 0.0, "IEF")
        assert c_buy == pytest.approx(c_sell, rel=1e-9)


class TestCostMultiplierGrid:
    """cost_multiplier={1.0, 1.5, 2.0} covers the base/moderate/worst-case cost range."""

    @pytest.mark.parametrize("mult", [1.0, 1.5, 2.0])
    def test_multiplier_scales_cost_linearly(self, mult):
        base = transaction_cost_return(0.0, 0.1, "BTCUSD", cost_multiplier=1.0)
        scaled = transaction_cost_return(0.0, 0.1, "BTCUSD", cost_multiplier=mult)
        assert scaled == pytest.approx(mult * base, rel=1e-9)

    def test_btcusd_2x_multiplier_yields_160bps(self):
        # 80 bps base × 2 = 160 bps round-trip.  Full unit weight (1.0) → 160/10000.
        cost = transaction_cost_return(0.0, 1.0, "BTCUSD", cost_multiplier=2.0)
        assert cost == pytest.approx(-160 / 10_000, rel=1e-9)

    def test_btcusd_1x_multiplier_yields_80bps(self):
        cost = transaction_cost_return(0.0, 1.0, "BTCUSD", cost_multiplier=1.0)
        assert cost == pytest.approx(-80 / 10_000, rel=1e-9)

    def test_cost_multiplier_monotone_decreasing(self):
        c1 = transaction_cost_return(0.0, 0.3, "BTCUSD", cost_multiplier=1.0)
        c15 = transaction_cost_return(0.0, 0.3, "BTCUSD", cost_multiplier=1.5)
        c2 = transaction_cost_return(0.0, 0.3, "BTCUSD", cost_multiplier=2.0)
        assert c1 >= c15 >= c2  # all ≤ 0; higher multiplier → more negative


class TestRollCostReturn:
    def test_default_roll_cost_is_zero_for_all_symbols(self):
        for sym in SYMBOL_CLASS:
            assert roll_cost_return(0.5, sym) == 0.0, sym

    def test_zero_weight_zero_roll_cost(self):
        assert roll_cost_return(0.0, "DBC") == 0.0
