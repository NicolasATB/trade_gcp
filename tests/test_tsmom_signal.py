"""Unit tests for the pure TSMOM signal logic in ``dataflow/strategy/tsmom_signal.py``.

Locks the T-22 contract: signal = sign of the cumulative excess return over the
formation horizon; volatility scaling is a *separate, counted* factor (so the
unscaled signal and the scaled position are computable independently); warm-up
emits ``None``; excess return ≈ 0 is flat. No BigQuery/Beam IO.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dataflow.strategy.tsmom_signal import (
    TsmomParams,
    compute_tsmom_rows,
    formation_excess_return,
    tsmom_position,
    tsmom_sign,
    vol_scale,
)


def _utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


@pytest.fixture
def params() -> TsmomParams:
    """Small, hand-checkable params: 3-period formation, 10% vol target."""
    return TsmomParams(formation_horizon=3, vol_target=0.10, vol_lookback=4)


# ---------------------------------------------------------------------------
# TsmomParams — validation (no hidden defaults; reject nonsense up front)
# ---------------------------------------------------------------------------

class TestTsmomParams:
    def test_valid_params(self):
        p = TsmomParams(formation_horizon=52, vol_target=0.4, vol_lookback=13)
        assert p.periods_per_year == 52  # weekly cadence default
        assert p.max_leverage is None

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"formation_horizon": 0, "vol_target": 0.1, "vol_lookback": 4},
            {"formation_horizon": 3, "vol_target": 0.0, "vol_lookback": 4},
            {"formation_horizon": 3, "vol_target": -0.1, "vol_lookback": 4},
            {"formation_horizon": 3, "vol_target": 0.1, "vol_lookback": 0},
            {"formation_horizon": 3, "vol_target": 0.1, "vol_lookback": 4, "periods_per_year": 0},
            {"formation_horizon": 3, "vol_target": 0.1, "vol_lookback": 4, "max_leverage": 0},
        ],
    )
    def test_invalid_params_raise(self, kwargs):
        with pytest.raises(ValueError):
            TsmomParams(**kwargs)


# ---------------------------------------------------------------------------
# formation_excess_return — trailing cumulative, warm-up, guards
# ---------------------------------------------------------------------------

class TestFormationExcessReturn:
    def test_sums_trailing_window(self):
        # last 3 of [.., 0.01, -0.02, 0.03] → 0.02
        assert formation_excess_return([0.05, 0.01, -0.02, 0.03], 3) == pytest.approx(0.02)

    def test_uses_only_the_last_horizon(self):
        # a big early value must NOT leak into a 2-period window
        assert formation_excess_return([10.0, 0.01, 0.02], 2) == pytest.approx(0.03)

    def test_warmup_returns_none(self):
        assert formation_excess_return([0.01, 0.02], 3) is None

    def test_exact_horizon_length(self):
        assert formation_excess_return([0.01, 0.02, 0.03], 3) == pytest.approx(0.06)

    def test_invalid_horizon_raises(self):
        with pytest.raises(ValueError):
            formation_excess_return([0.01], 0)

    def test_non_finite_in_window_raises(self):
        with pytest.raises(ValueError):
            formation_excess_return([0.01, float("nan"), 0.02], 3)


# ---------------------------------------------------------------------------
# tsmom_sign — direction, the ≈0 flat case, warm-up passthrough
# ---------------------------------------------------------------------------

class TestTsmomSign:
    @pytest.mark.parametrize(
        "formation_ret, expected",
        [
            (0.05, 1),     # positive momentum → long
            (-0.05, -1),   # negative momentum → short
            (0.0, 0),      # exactly zero → flat
            (None, None),  # warm-up passes through
        ],
    )
    def test_sign_matrix(self, formation_ret, expected):
        assert tsmom_sign(formation_ret) == expected

    def test_dead_band_maps_small_to_flat(self):
        # excess return ≈ 0 within the band is flat; just outside keeps direction
        assert tsmom_sign(0.001, eps=0.005) == 0
        assert tsmom_sign(-0.001, eps=0.005) == 0
        assert tsmom_sign(0.01, eps=0.005) == 1
        assert tsmom_sign(-0.01, eps=0.005) == -1


# ---------------------------------------------------------------------------
# vol_scale — vol_target / realized_vol, cap, unusable vol
# ---------------------------------------------------------------------------

class TestVolScale:
    def test_scales_to_target(self, params):
        # 10% target / 20% realized → 0.5
        assert vol_scale(0.20, params) == pytest.approx(0.5)

    def test_low_vol_demands_more_leverage(self, params):
        # 10% target / 5% realized → 2.0
        assert vol_scale(0.05, params) == pytest.approx(2.0)

    def test_cap_limits_leverage(self):
        capped = TsmomParams(
            formation_horizon=3, vol_target=0.10, vol_lookback=4, max_leverage=1.5
        )
        assert vol_scale(0.05, capped) == pytest.approx(1.5)  # 2.0 capped to 1.5
        assert vol_scale(0.20, capped) == pytest.approx(0.5)  # below cap, untouched

    @pytest.mark.parametrize("bad_vol", [None, 0.0, -0.1, float("nan"), float("inf")])
    def test_unusable_vol_returns_none(self, bad_vol, params):
        assert vol_scale(bad_vol, params) is None


# ---------------------------------------------------------------------------
# tsmom_position — signal × scale, with the flat / warm-up / unsizable rules
# ---------------------------------------------------------------------------

class TestTsmomPosition:
    def test_long_is_scale(self):
        assert tsmom_position(1, 0.5) == pytest.approx(0.5)

    def test_short_is_negative_scale(self):
        assert tsmom_position(-1, 0.5) == pytest.approx(-0.5)

    def test_flat_is_zero_regardless_of_scale(self):
        assert tsmom_position(0, 0.5) == 0.0
        assert tsmom_position(0, None) == 0.0  # flat needs no sizing

    def test_warmup_is_none(self):
        assert tsmom_position(None, 0.5) is None

    def test_directional_without_scale_is_none(self):
        # a long/short that cannot be sized (no usable vol) is NULL, not unscaled
        assert tsmom_position(1, None) is None
        assert tsmom_position(-1, None) is None


# ---------------------------------------------------------------------------
# compute_tsmom_rows — the per-instrument walk
# ---------------------------------------------------------------------------

class TestComputeTsmomRows:
    def _series(self, triples):
        # mirror the T-21 view columns (week_start, excess_log_return,
        # realized_vol_26w) the driver reads by default
        return [
            {"week_start": _utc(d), "excess_log_return": er, "realized_vol_26w": rv}
            for d, er, rv in triples
        ]

    def test_warmup_rows_emit_none(self, params):
        rows = self._series(
            [
                (date(2024, 1, 1), 0.01, 0.20),
                (date(2024, 1, 8), 0.02, 0.20),  # still warm-up (need 3)
                (date(2024, 1, 15), 0.03, 0.20),  # first full window
            ]
        )
        out = compute_tsmom_rows(rows, params)
        # first formation_horizon - 1 = 2 rows are warm-up
        assert out[0]["signal"] is None and out[0]["position"] is None
        assert out[1]["signal"] is None and out[1]["position"] is None
        assert out[2]["formation_return"] == pytest.approx(0.06)
        assert out[2]["signal"] == 1
        assert out[2]["position"] == pytest.approx(0.5)  # 0.10 / 0.20

    def test_signal_and_position_are_separable(self, params):
        # the unscaled signal (+1) and the scaled position (sign × 0.10/0.05) are
        # distinct fields → enables the with/without-vol-scaling report
        rows = self._series(
            [
                (date(2024, 1, 1), 0.10, 0.05),
                (date(2024, 1, 8), 0.10, 0.05),
                (date(2024, 1, 15), 0.10, 0.05),
            ]
        )
        last = compute_tsmom_rows(rows, params)[-1]
        assert last["signal"] == 1
        assert last["vol_scale"] == pytest.approx(2.0)
        assert last["position"] == pytest.approx(2.0)

    def test_negative_momentum_goes_short(self, params):
        rows = self._series(
            [
                (date(2024, 1, 1), -0.04, 0.10),
                (date(2024, 1, 8), -0.03, 0.10),
                (date(2024, 1, 15), -0.02, 0.10),
            ]
        )
        last = compute_tsmom_rows(rows, params)[-1]
        assert last["formation_return"] == pytest.approx(-0.09)
        assert last["signal"] == -1
        assert last["position"] == pytest.approx(-1.0)  # -1 × (0.10 / 0.10)

    def test_zero_formation_is_flat(self, params):
        # binary-exact values so the sum is exactly 0.0 (0.05−0.03−0.02 leaves a
        # float epsilon — that ≈0 case is covered by the dead-band test instead)
        rows = self._series(
            [
                (date(2024, 1, 1), 0.5, 0.10),
                (date(2024, 1, 8), -0.25, 0.10),
                (date(2024, 1, 15), -0.25, 0.10),  # sum = 0.0 exactly
            ]
        )
        last = compute_tsmom_rows(rows, params)[-1]
        assert last["formation_return"] == 0.0
        assert last["signal"] == 0
        assert last["position"] == 0.0

    def test_unusable_vol_leaves_directional_position_none(self, params):
        rows = self._series(
            [
                (date(2024, 1, 1), 0.01, 0.10),
                (date(2024, 1, 8), 0.02, 0.10),
                (date(2024, 1, 15), 0.03, None),  # no vol → cannot size
            ]
        )
        last = compute_tsmom_rows(rows, params)[-1]
        assert last["signal"] == 1
        assert last["vol_scale"] is None
        assert last["position"] is None

    def test_no_look_ahead_truncation_invariance(self, params):
        # truncating the series at t must not change the signal computed at t
        full = self._series(
            [
                (date(2024, 1, 1), 0.01, 0.20),
                (date(2024, 1, 8), 0.02, 0.20),
                (date(2024, 1, 15), 0.03, 0.20),
                (date(2024, 1, 22), -0.50, 0.20),  # a big future move
            ]
        )
        at_t = compute_tsmom_rows(full[:3], params)[-1]
        with_future = compute_tsmom_rows(full, params)[2]
        assert at_t["signal"] == with_future["signal"]
        assert at_t["position"] == pytest.approx(with_future["position"])
