"""Unit tests for the signal strategy logic in ``dataflow/stages/signals.py``.

Covers the week bucketing, the BUY/SELL/NEUTRAL rules, the stateful weekly-trend
walk-forward, and the ``_ComputeSignalFn`` DoFn (including the T-08 fix: it must
emit ``trigger_params`` as a dict, not a JSON string). No BigQuery/Beam IO.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dataflow.stages.signals import (
    _ComputeSignalFn,
    _apply_signal,
    _compute_trend_state,
    _compute_trend_states,
    _week_start,
)


# ---------------------------------------------------------------------------
# _week_start — Monday-based (weeks run Monday→Sunday), must match BigQuery
# DATE_TRUNC(d, WEEK(MONDAY))
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "given, expected",
    [
        (date(2024, 1, 8), date(2024, 1, 8)),    # Monday → itself
        (date(2024, 1, 10), date(2024, 1, 8)),   # Wednesday → same week's Monday
        (date(2024, 1, 14), date(2024, 1, 8)),   # Sunday (week end) → same week's Monday
        (date(2024, 1, 15), date(2024, 1, 15)),  # next Monday → itself
    ],
)
def test_week_start_is_monday_based(given, expected):
    assert _week_start(given) == expected


# ---------------------------------------------------------------------------
# _apply_signal — BUY / SELL / NEUTRAL rules
# ---------------------------------------------------------------------------

class TestApplySignal:
    @pytest.mark.parametrize(
        "daily_rsi, in_trend, expected",
        [
            (25.0, True, "BUY"),       # in trend + oversold → BUY
            (80.0, True, "SELL"),      # in trend + overbought → SELL
            (50.0, True, "NEUTRAL"),   # in trend + mid-band → NEUTRAL
            (80.0, False, "SELL"),     # out of trend + overbought → SELL (flatten)
            (25.0, False, "NEUTRAL"),  # out of trend + oversold → no BUY
            (50.0, False, "NEUTRAL"),  # out of trend + mid-band → NEUTRAL
        ],
    )
    def test_signal_matrix(self, daily_rsi, in_trend, expected, strategy_params):
        assert _apply_signal(daily_rsi, in_trend, strategy_params) == expected

    def test_threshold_is_strict(self, strategy_params):
        # Exactly at oversold (30) is NOT < 30 → not a BUY.
        assert _apply_signal(30.0, True, strategy_params) == "NEUTRAL"
        # Exactly at overbought (70) is NOT > 70 → not a SELL.
        assert _apply_signal(70.0, True, strategy_params) == "NEUTRAL"


# ---------------------------------------------------------------------------
# _compute_trend_states — stateful weekly walk-forward
# ---------------------------------------------------------------------------

class TestComputeTrendStates:
    def test_empty_history_returns_empty(self):
        assert _compute_trend_states([], 40.0, 70.0) == {}

    def test_walk_forward_opens_and_closes_on_previous_week(self, make_weekly_rsi):
        # trend_start=40, trend_end=70. Transitions are driven by the PREVIOUS
        # week's RSI, so a week's own RSI never flips its own state. Weekly rows
        # are labelled by their Monday week start.
        history = make_weekly_rsi([
            (date(2024, 1, 1), 35.0),   # w0: seed → False (own 35 doesn't open w0)
            (date(2024, 1, 8), 50.0),   # w1: prev 35 <= 40 → opens → True
            (date(2024, 1, 15), 75.0),  # w2: prev 50, still in band → True
            (date(2024, 1, 22), 60.0),  # w3: prev 75 >= 70 → closes → False
            (date(2024, 1, 29), 50.0),  # w4: prev 60, stays closed → False
        ])
        states = _compute_trend_states(history, 40.0, 70.0)
        assert states == {
            date(2024, 1, 1): False,
            date(2024, 1, 8): True,
            date(2024, 1, 15): True,
            date(2024, 1, 22): False,
            date(2024, 1, 29): False,
        }

    def test_single_week_is_seeded_false(self, make_weekly_rsi):
        history = make_weekly_rsi([(date(2024, 1, 1), 20.0)])
        assert _compute_trend_states(history, 40.0, 70.0) == {date(2024, 1, 1): False}


class TestComputeTrendState:
    def test_returns_latest_week_state_open(self, make_weekly_rsi):
        history = make_weekly_rsi([
            (date(2024, 1, 1), 35.0),
            (date(2024, 1, 8), 50.0),  # latest week is in-trend
        ])
        assert _compute_trend_state(history, 40.0, 70.0) is True

    def test_empty_history_is_false(self):
        assert _compute_trend_state([], 40.0, 70.0) is False


# ---------------------------------------------------------------------------
# _ComputeSignalFn — the DoFn that joins a daily candle to its week's trend
# ---------------------------------------------------------------------------

class TestComputeSignalFn:
    def _run(self, fn, element):
        return list(fn.process(element))

    def test_emits_trigger_params_as_dict_not_string(self, strategy_params):
        # T-08 regression guard: the BigQuery column is JSON type, so the value
        # must be a dict; a json.dumps string would store an escaped string.
        states = {date(2024, 1, 1): True}
        fn = _ComputeSignalFn(trend_states=states, params=strategy_params, strategy_id=1)
        element = {"rsi": 25.0, "time_period_start": datetime(2024, 1, 3, tzinfo=timezone.utc)}
        (out,) = self._run(fn, element)
        assert isinstance(out["trigger_params"], dict)
        assert out["trigger_params"]["in_trend"] is True
        assert out["trigger_params"]["daily_rsi"] == pytest.approx(25.0)

    def test_in_trend_oversold_yields_buy(self, strategy_params):
        states = {date(2024, 1, 1): True}
        fn = _ComputeSignalFn(trend_states=states, params=strategy_params, strategy_id=1)
        element = {"rsi": 20.0, "time_period_start": datetime(2024, 1, 4, tzinfo=timezone.utc)}
        (out,) = self._run(fn, element)
        assert out["signal"] == "BUY"
        assert out["strategy_id"] == 1
        assert out["temporality"] == "1d"

    def test_trend_for_falls_back_to_most_recent_known_week(self, strategy_params):
        # The candle's own week has no weekly RSI yet (still-forming week); the
        # DoFn must fall back to the most recent earlier week via bisect.
        states = {date(2024, 1, 1): True}
        fn = _ComputeSignalFn(trend_states=states, params=strategy_params, strategy_id=1)
        # 2024-01-08 (Mon) → week of 2024-01-08, which is absent from `states`.
        element = {"rsi": 20.0, "time_period_start": datetime(2024, 1, 8, tzinfo=timezone.utc)}
        (out,) = self._run(fn, element)
        assert out["trigger_params"]["in_trend"] is True  # fell back to 2024-01-01
        assert out["signal"] == "BUY"

    def test_trend_for_no_earlier_week_defaults_false(self, strategy_params):
        # Candle precedes every known week → no fallback available → not in trend.
        states = {date(2024, 2, 5): True}
        fn = _ComputeSignalFn(trend_states=states, params=strategy_params, strategy_id=1)
        element = {"rsi": 20.0, "time_period_start": datetime(2024, 1, 3, tzinfo=timezone.utc)}
        (out,) = self._run(fn, element)
        assert out["trigger_params"]["in_trend"] is False
        assert out["signal"] == "NEUTRAL"  # oversold but out of trend → no BUY

    def test_respects_symbol_and_temporality_from_element(self, strategy_params):
        states = {date(2024, 1, 1): False}
        fn = _ComputeSignalFn(trend_states=states, params=strategy_params, strategy_id=2)
        element = {
            "rsi": 80.0,
            "time_period_start": datetime(2024, 1, 3, tzinfo=timezone.utc),
            "symbol": "ETHUSD",
            "temporality": "1d",
        }
        (out,) = self._run(fn, element)
        assert out["symbol"] == "ETHUSD"
        assert out["signal"] == "SELL"  # overbought, flatten even out of trend
