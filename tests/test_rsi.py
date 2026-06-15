"""Unit tests for the pure Wilder-RSI logic in ``dataflow/stages/rsi.py``.

These lock the T-08 behaviour: the recursion is seeded at zero on the first
candle and continues identically in incremental mode, so re-running never
rewrites history (idempotency by design). No BigQuery/Beam involved.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from dataflow.stages.rsi import _rsi_value, compute_rsi_rows

# ---------------------------------------------------------------------------
# _rsi_value — the 0–100 transform with the zero-loss convention
# ---------------------------------------------------------------------------

class TestRsiValue:
    def test_pure_up_run_is_hundred(self):
        # avg_loss == 0 with a gain → conventional RSI 100 (a strong uptrend is
        # overbought, not oversold). Guards against the inverted-scale bug.
        assert _rsi_value(avg_gain=5.0, avg_loss=0.0) == 100.0

    def test_zero_seed_is_zero(self):
        # avg_gain == avg_loss == 0 (no movement / bootstrap seed) → RSI 0.
        assert _rsi_value(avg_gain=0.0, avg_loss=0.0) == 0.0

    def test_equal_gain_and_loss_is_fifty(self):
        assert _rsi_value(avg_gain=2.0, avg_loss=2.0) == pytest.approx(50.0)

    def test_zero_gain_positive_loss_is_zero(self):
        assert _rsi_value(avg_gain=0.0, avg_loss=3.0) == 0.0

    @pytest.mark.parametrize("avg_gain, avg_loss", [(1.0, 0.5), (4.0, 1.0), (3.3, 7.1)])
    def test_value_stays_within_0_100(self, avg_gain, avg_loss):
        assert 0.0 <= _rsi_value(avg_gain, avg_loss) <= 100.0


# ---------------------------------------------------------------------------
# compute_rsi_rows — bootstrap, incremental, and continuity
# ---------------------------------------------------------------------------

class TestComputeRsiRows:
    def test_empty_input_returns_empty(self):
        assert compute_rsi_rows([], None, "BTCUSD", "1d", 14) == []

    def test_bootstrap_emits_one_row_per_candle_from_the_first(self, make_ohlcv):
        rows = make_ohlcv([(date(2024, 1, 1), 10.0), (date(2024, 1, 2), 8.0)])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", 2)
        assert len(out) == 2  # bootstrap includes the seed candle

    def test_bootstrap_seed_candle_starts_at_zero(self, make_ohlcv):
        rows = make_ohlcv([(date(2024, 1, 1), 10.0)])
        (first,) = compute_rsi_rows(rows, None, "BTCUSD", "1d", 2)
        assert first["var_p_recursive"] == 0.0
        assert first["var_n_recursive"] == 0.0
        assert first["rsi"] is None  # warm-up row: state stored, no RSI published
        assert first["price_close"] == 10.0

    def test_worked_wilder_example_period_2(self, make_ohlcv):
        # Hand-computed Wilder recursion, rsi_period=2, prices 10 → 8 → 12:
        #   seed (10):            ag=0,   al=0     → warm-up, rsi=None
        #   step 8  (delta -2):   ag=0,   al=1.0   → warm-up, rsi=None
        #   step 12 (delta +4):   ag=2.0, al=0.5,  rs=4 → rsi=80 (first published)
        rows = make_ohlcv([
            (date(2024, 1, 1), 10.0),
            (date(2024, 1, 2), 8.0),
            (date(2024, 1, 3), 12.0),
        ])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", 2)
        assert out[0]["rsi"] is None
        assert out[1]["rsi"] is None
        assert out[1]["var_n_recursive"] == pytest.approx(1.0)
        assert out[2]["var_p_recursive"] == pytest.approx(2.0)
        assert out[2]["var_n_recursive"] == pytest.approx(0.5)
        assert out[2]["rsi"] == pytest.approx(80.0)

    def test_monotonic_rises_push_rsi_to_100(self, make_ohlcv):
        # An all-up series never accumulates a loss (avg_loss stays 0): every
        # post-warm-up candle has gains with no loss → RSI 100. RSI tracks price
        # direction (up → overbought), as the strategy expects.
        rows = make_ohlcv([(date(2024, 1, d), 10.0 + d) for d in range(1, 6)])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", 2)
        assert all(r["rsi"] is None for r in out[:2])      # warm-up (rsi_period=2)
        assert all(r["rsi"] == 100.0 for r in out[2:])

    def test_rows_sorted_internally(self, make_ohlcv):
        # Pass candles out of order; output must be chronological.
        rows = make_ohlcv([
            (date(2024, 1, 3), 12.0),
            (date(2024, 1, 1), 10.0),
            (date(2024, 1, 2), 8.0),
        ])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", 2)
        starts = [r["time_period_start"] for r in out]
        assert starts == sorted(starts)

    def test_incremental_continues_from_prior_state(self, make_ohlcv):
        # Prior state taken from the worked example's last row (close 12).
        prior = {"price_close": 12.0, "var_p_recursive": 2.0, "var_n_recursive": 0.5}
        new_rows = make_ohlcv([(date(2024, 1, 4), 10.0)])  # delta -2
        out = compute_rsi_rows(new_rows, prior, "BTCUSD", "1d", 2)
        # ag=(2*1+0)/2=1.0 ; al=(0.5*1+2)/2=1.25
        assert len(out) == 1
        assert out[0]["var_p_recursive"] == pytest.approx(1.0)
        assert out[0]["var_n_recursive"] == pytest.approx(1.25)

    def test_incremental_matches_full_bootstrap(self, make_ohlcv):
        """Continuity invariant: bootstrap(all) == bootstrap(prefix) then incremental(suffix).

        This is the idempotency guarantee — replaying the latest candle on top of
        stored state reproduces exactly what a full recompute would, so the MERGE
        never changes already-written history.
        """
        all_pairs = [
            (date(2024, 1, 1), 10.0),
            (date(2024, 1, 2), 8.0),
            (date(2024, 1, 3), 12.0),
            (date(2024, 1, 4), 10.0),
        ]
        full = compute_rsi_rows(make_ohlcv(all_pairs), None, "BTCUSD", "1d", 2)

        prefix = compute_rsi_rows(make_ohlcv(all_pairs[:3]), None, "BTCUSD", "1d", 2)
        last = prefix[-1]
        prior = {
            "price_close": last["price_close"],
            "var_p_recursive": last["var_p_recursive"],
            "var_n_recursive": last["var_n_recursive"],
        }
        incr = compute_rsi_rows(make_ohlcv(all_pairs[3:]), prior, "BTCUSD", "1d", 2)

        # The incrementally produced last row equals the full recompute's last row.
        for key in ("var_p_recursive", "var_n_recursive", "rsi", "price_close"):
            assert incr[-1][key] == pytest.approx(full[-1][key])

    def test_row_carries_metadata(self, make_ohlcv):
        rows = make_ohlcv([(date(2024, 1, 1), 10.0)])
        (row,) = compute_rsi_rows(rows, None, "ETHUSD", "1w", 14)
        assert row["symbol"] == "ETHUSD"
        assert row["temporality"] == "1w"
        assert row["rsi_period"] == 14
        # time_period_start serialised as ISO string (datetime.isoformat path).
        assert row["time_period_start"].startswith("2024-01-01T")

    def test_rsi_always_within_bounds_over_a_volatile_series(self, make_ohlcv):
        # Sanity bound on a mixed up/down series (not property-based — Hypothesis
        # is deferred as technical debt; final validation will compare against a
        # real API-downloaded series).
        prices = [100, 102, 99, 105, 95, 110, 108, 90, 120, 85]
        rows = make_ohlcv([(date(2024, 1, d + 1), p) for d, p in enumerate(prices)])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", 2)
        published = [r["rsi"] for r in out if r["rsi"] is not None]
        assert len(published) == len(prices) - 2  # warm-up rows excluded (rsi_period=2)
        assert all(0.0 <= rsi <= 100.0 and math.isfinite(rsi) for rsi in published)

    def test_bootstrap_warm_up_rows_have_null_rsi(self, make_ohlcv):
        # Business rule: the first `rsi_period` rows of a bootstrap are warm-up —
        # they persist the recursive state but publish rsi = None; the first
        # value appears on row index `rsi_period`.
        rows = make_ohlcv([(date(2024, 1, d), 100.0 + (-1) ** d * d) for d in range(1, 21)])
        period = 14
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", period)
        assert all(r["rsi"] is None for r in out[:period])
        assert all(r["rsi"] is not None for r in out[period:])
        # State is tracked even while rsi is unpublished.
        assert all(r["var_p_recursive"] is not None and r["var_n_recursive"] is not None
                   for r in out[:period])

    @pytest.mark.parametrize("period", [2, 5, 14])
    def test_first_published_rsi_is_exactly_at_index_rsi_period(self, make_ohlcv, period):
        # The warm-up boundary must track rsi_period, not a hard-coded 14:
        # indices [0, period) are NULL, index `period` publishes the first value.
        rows = make_ohlcv([(date(2024, 1, d), 100.0 + d) for d in range(1, period + 5)])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", period)
        null_indices = [i for i, r in enumerate(out) if r["rsi"] is None]
        assert null_indices == list(range(period))

    def test_bootstrap_shorter_than_period_is_all_warm_up(self, make_ohlcv):
        # A series shorter than rsi_period never leaves warm-up: every row is
        # emitted (state must persist for future increments) but none publishes
        # an RSI value.
        rows = make_ohlcv([(date(2024, 1, d), 100.0 + d) for d in range(1, 6)])
        out = compute_rsi_rows(rows, None, "BTCUSD", "1d", 14)
        assert len(out) == 5
        assert all(r["rsi"] is None for r in out)

    def test_incremental_rows_always_publish_rsi(self, make_ohlcv):
        # Incremental mode continues from stored state: the bootstrap already
        # covered the warm-up, so every new row carries a value.
        prior = {"price_close": 12.0, "var_p_recursive": 2.0, "var_n_recursive": 0.5}
        new_rows = make_ohlcv([(date(2024, 1, 4), 10.0), (date(2024, 1, 5), 11.0)])
        out = compute_rsi_rows(new_rows, prior, "BTCUSD", "1d", 2)
        assert all(r["rsi"] is not None for r in out)
