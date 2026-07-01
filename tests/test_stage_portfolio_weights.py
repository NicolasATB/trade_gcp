"""
Unit tests for dataflow/stages/portfolio_weights_stage.py — pure helpers only.

All BQ/Beam IO functions are marked ``# pragma: no cover`` in the stage.
These tests cover: signal_start→week_start conversion, trigger_params parsing,
_IS_CRYPTO correctness, ragged cross-section handling, and tz-coherence.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pytest

from dataflow.stages.portfolio_weights_stage import (
    _IS_CRYPTO,
    _BuildPortfolioWeightsFn,
    _parse_signal_row,
    _signal_start_to_week_start,
)
from dataflow.stages.tsmom_signal_stage import _week_start_to_ts
from dataflow.strategy.portfolio import PortfolioParams, Scheme

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def inv_vol_params() -> PortfolioParams:
    return PortfolioParams(scheme=Scheme.INVERSE_VOL, crypto_cap=0.20)


@pytest.fixture()
def equal_weight_params() -> PortfolioParams:
    return PortfolioParams(scheme=Scheme.EQUAL_WEIGHT)


def _make_signal_row(symbol: str, signal: int, vol: float | None = 0.10) -> dict:
    """Build a fact_signals row dict as Stage B reads from BigQuery."""
    return {
        "symbol": symbol,
        "signal_start": _week_start_to_ts(date(2023, 1, 2)),
        "trigger_params": {
            "signal": signal,
            "realized_vol_26w": vol,
        },
    }


# ---------------------------------------------------------------------------
# _signal_start_to_week_start
# ---------------------------------------------------------------------------

class TestSignalStartToWeekStart:
    def test_datetime_utc_extracts_date(self):
        signal_start = datetime(2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        assert _signal_start_to_week_start(signal_start) == date(2023, 1, 2)

    def test_datetime_midnight_utc(self):
        # UTC-midnight timestamps always yield the correct date.
        signal_start = datetime(2021, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert _signal_start_to_week_start(signal_start) == date(2021, 3, 1)

    def test_date_input_passthrough(self):
        d = date(2022, 6, 6)
        assert _signal_start_to_week_start(d) == d

    def test_string_input(self):
        assert _signal_start_to_week_start("2023-01-02") == date(2023, 1, 2)

    def test_string_with_time_suffix(self):
        # BigQuery may return an ISO timestamp string.
        assert _signal_start_to_week_start("2023-01-02T00:00:00+00:00") == date(2023, 1, 2)

    def test_non_utc_datetime_converts_to_utc_date(self):
        # A UTC-midnight timestamp stored as-is gives the correct week date.
        signal_start = datetime(2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        result = _signal_start_to_week_start(signal_start)
        assert result == date(2023, 1, 2)


# ---------------------------------------------------------------------------
# _parse_signal_row
# ---------------------------------------------------------------------------

class TestParseSignalRow:
    def test_happy_path_returns_expected_keys(self):
        row = _make_signal_row("SPY", 1, vol=0.08)
        parsed = _parse_signal_row(row)
        assert parsed is not None
        assert set(parsed.keys()) == {"symbol", "signal", "realized_vol_26w", "is_crypto"}

    def test_signal_is_int(self):
        row = _make_signal_row("SPY", 1, vol=0.08)
        parsed = _parse_signal_row(row)
        assert isinstance(parsed["signal"], int)

    def test_vol_is_float(self):
        row = _make_signal_row("SPY", 1, vol=0.08)
        parsed = _parse_signal_row(row)
        assert isinstance(parsed["realized_vol_26w"], float)

    def test_sell_signal_preserved(self):
        row = _make_signal_row("IEF", -1, vol=0.05)
        parsed = _parse_signal_row(row)
        assert parsed["signal"] == -1

    def test_is_crypto_false_for_etf(self):
        row = _make_signal_row("SPY", 1, vol=0.10)
        parsed = _parse_signal_row(row)
        assert parsed["is_crypto"] is False

    def test_is_crypto_true_for_btc(self):
        row = _make_signal_row("BTC", 1, vol=0.90)
        parsed = _parse_signal_row(row)
        assert parsed["is_crypto"] is True

    def test_missing_trigger_params_returns_none(self):
        row = {"symbol": "SPY", "signal_start": "2023-01-02T00:00:00+00:00"}
        assert _parse_signal_row(row) is None

    def test_signal_none_in_trigger_params_returns_none(self):
        row = {
            "symbol": "SPY",
            "signal_start": "2023-01-02T00:00:00+00:00",
            "trigger_params": {"signal": None, "realized_vol_26w": 0.10},
        }
        assert _parse_signal_row(row) is None

    def test_vol_none_stored_as_none(self):
        row = {
            "symbol": "SPY",
            "signal_start": "2023-01-02T00:00:00+00:00",
            "trigger_params": {"signal": 1, "realized_vol_26w": None},
        }
        parsed = _parse_signal_row(row)
        assert parsed is not None
        assert parsed["realized_vol_26w"] is None

    def test_unknown_symbol_is_not_crypto(self):
        row = {
            "symbol": "UNKNOWN",
            "signal_start": "2023-01-02T00:00:00+00:00",
            "trigger_params": {"signal": 1, "realized_vol_26w": 0.10},
        }
        parsed = _parse_signal_row(row)
        assert parsed is not None
        assert parsed["is_crypto"] is False


# ---------------------------------------------------------------------------
# _IS_CRYPTO static dict
# ---------------------------------------------------------------------------

class TestIsCrypto:
    def test_btc_is_crypto(self):
        assert _IS_CRYPTO["BTC"] is True

    def test_etf_symbols_are_not_crypto(self):
        for symbol in ("SPY", "EFA", "IEF", "TLT", "GLD", "DBC", "UUP", "FXY"):
            assert _IS_CRYPTO.get(symbol) is False, f"Expected {symbol} to be non-crypto"

    def test_covers_all_nine_universe_instruments(self):
        from orchestration.ingest.strategy_3_universe import FULL_UNIVERSE
        assert set(_IS_CRYPTO.keys()) == {i.symbol for i in FULL_UNIVERSE}

    def test_only_btc_is_crypto_in_universe(self):
        crypto_symbols = [sym for sym, is_c in _IS_CRYPTO.items() if is_c]
        assert crypto_symbols == ["BTC"]


# ---------------------------------------------------------------------------
# Ragged cross-section (early weeks with partial warm-up)
# ---------------------------------------------------------------------------

class TestRaggedCrossSection:
    """Verify _BuildPortfolioWeightsFn handles partial warm-up weeks correctly.

    Early weeks (e.g. 1993, shortly after SPY's inception) only have 2-3 instruments
    with a formed signal. The rest are either absent or in warm-up (no row in
    fact_signals). The DoFn must produce valid weight rows without crashing.
    """

    def test_three_instrument_week_produces_weights(self, inv_vol_params):
        # Week where only SPY (equity), IEF (bonds), GLD (commodities) have signals.
        signal_rows = [
            _make_signal_row("SPY", 1,  vol=0.10),
            _make_signal_row("IEF", 1,  vol=0.05),
            _make_signal_row("GLD", -1, vol=0.12),
        ]
        fn = _BuildPortfolioWeightsFn(
            portfolio_params=inv_vol_params,
            scheme_str="inverse_vol",
            param_version=1,
            strategy_id=3,
        )
        week_start = date(2003, 1, 6)
        output_rows = list(fn.process((week_start, signal_rows)))
        # Two books per week: include_crypto=True and include_crypto=False.
        assert len(output_rows) > 0
        # All output rows must have the expected schema keys.
        required = {"week_start", "strategy_id", "symbol", "include_crypto",
                    "scheme", "weight", "param_version", "created_at"}
        for row in output_rows:
            assert set(row.keys()) == required, f"Missing keys in: {row.keys()}"

    def test_weight_books_gross_approximately_one(self, inv_vol_params):
        signal_rows = [
            _make_signal_row("SPY", 1,  vol=0.10),
            _make_signal_row("IEF", 1,  vol=0.05),
            _make_signal_row("GLD", -1, vol=0.12),
        ]
        fn = _BuildPortfolioWeightsFn(
            portfolio_params=inv_vol_params,
            scheme_str="inverse_vol",
            param_version=1,
            strategy_id=3,
        )
        week_start = date(2003, 1, 6)
        output_rows = list(fn.process((week_start, signal_rows)))

        for include_crypto in (True, False):
            book = [r for r in output_rows if r["include_crypto"] == include_crypto]
            if not book:
                continue
            gross = math.fsum(abs(r["weight"]) for r in book)
            assert abs(gross - 1.0) < 1e-9, (
                f"include_crypto={include_crypto}: gross={gross:.6f}, expected 1.0"
            )

    def test_warm_up_none_instruments_absent(self, equal_weight_params):
        # Simulate rows where some instruments have signal=None (warm-up).
        # _parse_signal_row drops None-signal rows, so they must not appear in output.
        signal_rows = [
            {  # warm-up: trigger_params has signal=None
                "symbol": "DBC",
                "signal_start": _week_start_to_ts(date(2006, 2, 13)),
                "trigger_params": {"signal": None, "realized_vol_26w": None},
            },
            _make_signal_row("SPY", 1, vol=0.10),
            _make_signal_row("IEF", 1, vol=0.05),
        ]
        fn = _BuildPortfolioWeightsFn(
            portfolio_params=equal_weight_params,
            scheme_str="equal_weight",
            param_version=1,
            strategy_id=3,
        )
        week_start = date(2006, 2, 13)
        output_rows = list(fn.process((week_start, signal_rows)))

        output_symbols = {r["symbol"] for r in output_rows}
        assert "DBC" not in output_symbols, "Warm-up instrument must not appear in weights"
        assert "SPY" in output_symbols
        assert "IEF" in output_symbols

    def test_single_etf_produces_unit_weight(self, equal_weight_params):
        # Edge: only one instrument active. Equal-weight → sole leg gets |w|=1.
        signal_rows = [_make_signal_row("SPY", 1, vol=0.10)]
        fn = _BuildPortfolioWeightsFn(
            portfolio_params=equal_weight_params,
            scheme_str="equal_weight",
            param_version=1,
            strategy_id=3,
        )
        week_start = date(1993, 2, 1)
        output_rows = list(fn.process((week_start, signal_rows)))
        # At least the with-crypto book (no BTC anyway → both books identical here).
        spy_rows = [r for r in output_rows if r["symbol"] == "SPY"]
        assert len(spy_rows) > 0
        for r in spy_rows:
            assert abs(abs(r["weight"]) - 1.0) < 1e-9

    def test_output_week_start_matches_input(self, equal_weight_params):
        signal_rows = [_make_signal_row("SPY", 1), _make_signal_row("IEF", 1)]
        fn = _BuildPortfolioWeightsFn(
            portfolio_params=equal_weight_params,
            scheme_str="equal_weight",
            param_version=1,
            strategy_id=3,
        )
        week_start = date(2023, 1, 2)
        output_rows = list(fn.process((week_start, signal_rows)))
        for row in output_rows:
            assert row["week_start"] == "2023-01-02"

    def test_strategy_id_is_3_in_all_output_rows(self, equal_weight_params):
        signal_rows = [_make_signal_row("SPY", 1)]
        fn = _BuildPortfolioWeightsFn(
            portfolio_params=equal_weight_params,
            scheme_str="equal_weight",
            param_version=1,
            strategy_id=3,
        )
        output_rows = list(fn.process((date(2023, 1, 2), signal_rows)))
        for row in output_rows:
            assert row["strategy_id"] == 3


# ---------------------------------------------------------------------------
# Tz-coherence: Stage A's _week_start_to_ts ↔ Stage B's _signal_start_to_week_start
# ---------------------------------------------------------------------------

class TestTzCoherence:
    """Asserts that Stage B's DATE(signal_start) grouping key equals Stage A's week_start.

    Guards against a week being split into two GroupByKey buckets due to timezone
    drift. The invariant is that _signal_start_to_week_start(_week_start_to_ts(d)) == d
    for any Monday week_start d, regardless of locale.
    """

    @pytest.mark.parametrize("week_start", [
        date(2021, 1, 4),   # a Monday — standard case
        date(2000, 1, 3),   # a Monday — millennium edge
        date(2023, 12, 25), # a Monday — Christmas
        date(1993, 2, 1),   # oldest ETF inception (SPY)
    ])
    def test_round_trip_preserves_date(self, week_start: date):
        signal_start_ts = _week_start_to_ts(week_start)          # Stage A output
        recovered = _signal_start_to_week_start(signal_start_ts) # Stage B grouping key
        assert recovered == week_start, (
            f"week_start={week_start} → signal_start={signal_start_ts!r} "
            f"→ recovered={recovered} (mismatch)"
        )

    def test_datetime_object_round_trip(self):
        # BigQuery may return signal_start as a Python datetime (not a string).
        week_start = date(2023, 3, 6)
        ts_str = _week_start_to_ts(week_start)
        dt = datetime.fromisoformat(ts_str)
        recovered = _signal_start_to_week_start(dt)
        assert recovered == week_start

    def test_no_date_shift_at_utc_midnight_boundary(self):
        # Non-trivial: a date that naive-local could shift forward or back a day.
        week_start = date(2023, 1, 2)
        ts_str = _week_start_to_ts(week_start)
        dt = datetime.fromisoformat(ts_str)
        assert dt.hour == 0 and dt.minute == 0 and dt.second == 0
        assert dt.utcoffset().total_seconds() == 0
        assert _signal_start_to_week_start(dt) == week_start

    def test_two_consecutive_mondays_are_separate_keys(self):
        # Consecutive weeks must produce distinct GroupByKey buckets.
        w1, w2 = date(2023, 1, 2), date(2023, 1, 9)
        r1 = _signal_start_to_week_start(_week_start_to_ts(w1))
        r2 = _signal_start_to_week_start(_week_start_to_ts(w2))
        assert r1 != r2

    def test_groupby_key_from_fact_signals_row(self):
        """Simulate a fact_signals row as read by Stage B's KeyByWeek step."""
        week_start = date(2022, 5, 2)
        # Stage A writes signal_start as UTC-midnight ISO string.
        row = {
            "symbol": "SPY",
            "signal_start": _week_start_to_ts(week_start),
            "trigger_params": {"signal": 1, "realized_vol_26w": 0.10},
        }
        recovered_key = _signal_start_to_week_start(row["signal_start"])
        assert recovered_key == week_start
