"""Unit tests for the frozen Strategy 3 universe (``orchestration/ingest/strategy_3_universe.py``).

Guards the T-19 freeze: nine instruments across five classes, the eight ETFs in
Tier A with both provider tickers, BTC as the Tier-B crypto sleeve with no ETF
tickers. A change here should be a deliberate re-freeze, not an accident.
"""

from __future__ import annotations

from datetime import date

from orchestration.ingest.strategy_3_universe import (
    CRYPTO_UNIVERSE,
    ETF_UNIVERSE,
    FULL_UNIVERSE,
    TIER_A,
    TIER_B,
)


class TestFrozenUniverse:
    def test_nine_instruments(self):
        assert len(FULL_UNIVERSE) == 9
        assert len(ETF_UNIVERSE) == 8
        assert len(CRYPTO_UNIVERSE) == 1

    def test_expected_symbols(self):
        symbols = {i.symbol for i in FULL_UNIVERSE}
        assert symbols == {"SPY", "EFA", "IEF", "TLT", "GLD", "DBC", "UUP", "FXY", "BTC"}

    def test_five_asset_classes(self):
        assert {i.asset_class for i in FULL_UNIVERSE} == {
            "equity", "bonds", "commodities", "fx", "crypto",
        }

    def test_symbols_are_unique(self):
        symbols = [i.symbol for i in FULL_UNIVERSE]
        assert len(symbols) == len(set(symbols))

    def test_fxe_is_not_in_universe(self):
        # T-19 dropped FXE (≈ -UUP, redundant) in favour of FXY.
        assert "FXE" not in {i.symbol for i in FULL_UNIVERSE}


class TestEtfMembers:
    def test_all_tier_a(self):
        assert all(i.tier == TIER_A for i in ETF_UNIVERSE)

    def test_have_both_provider_tickers(self):
        for i in ETF_UNIVERSE:
            assert i.yahoo_ticker, f"{i.symbol} missing yahoo_ticker"
            assert i.stooq_ticker, f"{i.symbol} missing stooq_ticker"

    def test_yahoo_ticker_matches_symbol(self):
        # For these ETFs the Yahoo ticker is the canonical symbol.
        assert all(i.yahoo_ticker == i.symbol for i in ETF_UNIVERSE)

    def test_stooq_ticker_is_us_lowercase(self):
        for i in ETF_UNIVERSE:
            assert i.stooq_ticker == f"{i.symbol.lower()}.us"

    def test_inception_dates_are_real_dates(self):
        assert all(isinstance(i.inception, date) for i in ETF_UNIVERSE)

    def test_spy_is_the_oldest(self):
        oldest = min(ETF_UNIVERSE, key=lambda i: i.inception)
        assert oldest.symbol == "SPY"
        assert oldest.inception == date(1993, 1, 29)


class TestCryptoSleeve:
    def test_btc_is_tier_b_crypto(self):
        (btc,) = CRYPTO_UNIVERSE
        assert btc.symbol == "BTC"
        assert btc.asset_class == "crypto"
        assert btc.tier == TIER_B

    def test_btc_has_no_etf_tickers(self):
        # BTC is sourced by the existing spot ingest, not the ETF providers.
        (btc,) = CRYPTO_UNIVERSE
        assert btc.yahoo_ticker is None
        assert btc.stooq_ticker is None

    def test_btc_sets_the_common_portfolio_start(self):
        (btc,) = CRYPTO_UNIVERSE
        assert btc.inception == date(2011, 8, 18)


def test_instrument_is_frozen():
    inst = ETF_UNIVERSE[0]
    try:
        inst.symbol = "XXX"  # type: ignore[misc]
    except Exception as exc:
        assert isinstance(exc, AttributeError)
    else:  # pragma: no cover - dataclass(frozen=True) must reject mutation
        raise AssertionError("Instrument should be frozen")
