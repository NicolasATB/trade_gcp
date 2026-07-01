"""
Unit tests for dataflow/stages/tsmom_signal_stage.py — pure helpers only.

All BQ/Beam IO functions are marked ``# pragma: no cover`` in the stage.
These tests cover: signal-label mapping, UTC-midnight timestamp conversion,
trigger_params construction (keys + types), and the inter-stage contract.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dataflow.stages.tsmom_signal_stage import (
    _TRIGGER_PARAMS_KEYS_WRITTEN,
    _build_trigger_params,
    _signal_label,
    _week_start_to_ts,
)
from dataflow.stages.portfolio_weights_stage import (
    _TRIGGER_PARAMS_KEYS_READ_BY_STAGE_B,
)
from dataflow.strategy.tsmom_signal import TsmomParams


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def params() -> TsmomParams:
    return TsmomParams(
        formation_horizon=52,
        vol_target=0.10,
        vol_lookback=26,
        periods_per_year=52,
        max_leverage=None,
    )


@pytest.fixture()
def sample_signal_row() -> dict:
    """A compute_tsmom_rows output row with a valid directional signal."""
    return {
        "time_period_start": date(2023, 1, 2),
        "formation_return":  0.12,
        "signal":            1,
        "vol_scale":         2.0,
        "position":          2.0,
        "realized_vol_26w":  0.05,
    }


# ---------------------------------------------------------------------------
# _signal_label
# ---------------------------------------------------------------------------

class TestSignalLabel:
    def test_positive_sign_is_buy(self):
        assert _signal_label(1) == "BUY"

    def test_negative_sign_is_sell(self):
        assert _signal_label(-1) == "SELL"

    def test_zero_sign_is_neutral(self):
        assert _signal_label(0) == "NEUTRAL"

    def test_large_positive_is_buy(self):
        assert _signal_label(5) == "BUY"

    def test_large_negative_is_sell(self):
        assert _signal_label(-3) == "SELL"


# ---------------------------------------------------------------------------
# _week_start_to_ts
# ---------------------------------------------------------------------------

class TestWeekStartToTs:
    def test_date_input_produces_midnight_utc(self):
        ts = _week_start_to_ts(date(2023, 1, 2))
        dt = datetime.fromisoformat(ts)
        assert dt.hour == 0 and dt.minute == 0 and dt.second == 0

    def test_date_input_utc_timezone(self):
        ts = _week_start_to_ts(date(2023, 1, 2))
        dt = datetime.fromisoformat(ts)
        assert dt.tzinfo is not None
        assert dt.utcoffset().total_seconds() == 0

    def test_date_input_round_trips_to_same_date(self):
        d = date(2021, 1, 4)  # a Monday
        ts = _week_start_to_ts(d)
        dt = datetime.fromisoformat(ts)
        assert dt.astimezone(timezone.utc).date() == d

    def test_datetime_input_extracts_date(self):
        # Stage A may receive a datetime from BigQuery instead of a bare date.
        dt_input = datetime(2023, 1, 2, 15, 30, 0, tzinfo=timezone.utc)
        ts = _week_start_to_ts(dt_input)
        dt = datetime.fromisoformat(ts)
        assert dt.astimezone(timezone.utc).date() == date(2023, 1, 2)
        assert dt.hour == 0

    def test_string_input_parsed(self):
        ts = _week_start_to_ts("2023-01-02")
        dt = datetime.fromisoformat(ts)
        assert dt.astimezone(timezone.utc).date() == date(2023, 1, 2)

    def test_non_midnight_input_still_midnight_utc(self):
        """Guards the tz-coherence contract: a non-midnight datetime must still
        produce midnight UTC, so DATE(signal_start) == original week_start."""
        dt_input = datetime(2023, 1, 2, 23, 59, 59)  # no tz — local-ish
        ts = _week_start_to_ts(dt_input)
        dt = datetime.fromisoformat(ts)
        assert dt.hour == 0 and dt.minute == 0 and dt.second == 0
        assert dt.utcoffset().total_seconds() == 0


# ---------------------------------------------------------------------------
# _build_trigger_params — keys, types, values
# ---------------------------------------------------------------------------

class TestBuildTriggerParams:
    def test_all_expected_keys_present(self, sample_signal_row, params):
        tp = _build_trigger_params(sample_signal_row, params)
        assert set(tp.keys()) == _TRIGGER_PARAMS_KEYS_WRITTEN

    def test_signal_is_int(self, sample_signal_row, params):
        tp = _build_trigger_params(sample_signal_row, params)
        assert isinstance(tp["signal"], int)

    def test_realized_vol_26w_is_float(self, sample_signal_row, params):
        tp = _build_trigger_params(sample_signal_row, params)
        assert isinstance(tp["realized_vol_26w"], float)

    def test_signal_value_matches_input_sign(self, sample_signal_row, params):
        tp = _build_trigger_params(sample_signal_row, params)
        assert tp["signal"] == int(sample_signal_row["signal"])

    def test_vol_value_matches_input(self, sample_signal_row, params):
        tp = _build_trigger_params(sample_signal_row, params)
        assert tp["realized_vol_26w"] == pytest.approx(sample_signal_row["realized_vol_26w"])

    def test_params_embedded_for_audit(self, sample_signal_row, params):
        tp = _build_trigger_params(sample_signal_row, params)
        assert tp["formation_horizon"] == params.formation_horizon
        assert tp["vol_target"] == params.vol_target
        assert tp["vol_lookback"] == params.vol_lookback
        assert tp["max_leverage"] is None

    def test_null_vol_stored_as_none(self, params):
        row = {
            "time_period_start": date(2023, 1, 2),
            "formation_return":  0.05,
            "signal":            1,
            "vol_scale":         None,
            "position":          None,
            "realized_vol_26w":  None,
        }
        tp = _build_trigger_params(row, params)
        assert tp["realized_vol_26w"] is None

    def test_sell_signal_stored_as_minus_one(self, params):
        row = {
            "time_period_start": date(2023, 1, 2),
            "formation_return":  -0.08,
            "signal":            -1,
            "vol_scale":         1.5,
            "position":          -1.5,
            "realized_vol_26w":  0.067,
        }
        tp = _build_trigger_params(row, params)
        assert tp["signal"] == -1
        assert isinstance(tp["signal"], int)


# ---------------------------------------------------------------------------
# Inter-stage contract: Stage A keys ⊇ Stage B required keys
# ---------------------------------------------------------------------------

class TestInterStageContract:
    def test_stage_b_reads_subset_of_stage_a_writes(self):
        missing = _TRIGGER_PARAMS_KEYS_READ_BY_STAGE_B - _TRIGGER_PARAMS_KEYS_WRITTEN
        assert not missing, (
            f"Stage B reads trigger_params keys that Stage A does not write: {missing}. "
            "Either Stage A must add them or Stage B must stop reading them."
        )

    def test_stage_b_keys_are_strings(self):
        for key in _TRIGGER_PARAMS_KEYS_READ_BY_STAGE_B:
            assert isinstance(key, str), f"Key {key!r} is not a string"

    def test_real_trigger_params_contains_all_stage_b_keys(self, sample_signal_row, params):
        """Uses the real _build_trigger_params helper — not a hand-written dict —
        so any change in Stage A's output is reflected here automatically."""
        tp = _build_trigger_params(sample_signal_row, params)
        for key in _TRIGGER_PARAMS_KEYS_READ_BY_STAGE_B:
            assert key in tp, (
                f"Stage B needs trigger_params['{key}'] but Stage A did not write it. "
                "Update _build_trigger_params in tsmom_signal_stage.py."
            )

    def test_signal_type_matches_stage_b_expectation(self, sample_signal_row, params):
        """Stage B does int arithmetic on tp['signal'] — must be int, not float/str."""
        tp = _build_trigger_params(sample_signal_row, params)
        assert isinstance(tp["signal"], int)

    def test_vol_type_matches_stage_b_expectation(self, sample_signal_row, params):
        """Stage B feeds tp['realized_vol_26w'] to inverse_vol_weights — must be float."""
        tp = _build_trigger_params(sample_signal_row, params)
        assert isinstance(tp["realized_vol_26w"], float)


# ---------------------------------------------------------------------------
# Active-param uniqueness pattern (table-level, simulated)
# ---------------------------------------------------------------------------

class TestActiveParamUniqueness:
    """Verify the atomic-flip promotion pattern preserves exactly one active row.

    No BQ IO: simulates the params table as a Python list of dicts.
    """

    @staticmethod
    def _seed() -> list[dict]:
        return [{"param_version": 1, "is_active": True}]

    @staticmethod
    def _atomic_flip(rows: list[dict], new_version: int) -> list[dict]:
        """Mirror of: UPDATE … SET is_active=(param_version=@v) WHERE TRUE."""
        return [
            {**r, "is_active": (r["param_version"] == new_version)}
            for r in rows
        ]

    def test_seed_has_exactly_one_active_row(self):
        rows = self._seed()
        assert sum(1 for r in rows if r["is_active"]) == 1

    def test_promotion_to_v2_leaves_exactly_one_active(self):
        rows = self._seed()
        rows.append({"param_version": 2, "is_active": False})
        rows = self._atomic_flip(rows, 2)
        active = [r for r in rows if r["is_active"]]
        assert len(active) == 1
        assert active[0]["param_version"] == 2

    def test_promotion_activates_specified_version_not_highest(self):
        """ORDER BY param_version DESC LIMIT 1 is secondary defence; the flip
        targets the chosen version, not necessarily the highest."""
        rows = [
            {"param_version": 1, "is_active": False},
            {"param_version": 2, "is_active": True},
            {"param_version": 3, "is_active": False},
        ]
        rows = self._atomic_flip(rows, 1)  # roll back to v1
        active = [r for r in rows if r["is_active"]]
        assert len(active) == 1
        assert active[0]["param_version"] == 1

    def test_limit_1_guard_returns_active_row(self):
        """Secondary defence: ORDER BY param_version DESC LIMIT 1 returns active row
        when the convention is respected (active = highest version)."""
        rows = [
            {"param_version": 1, "is_active": False},
            {"param_version": 2, "is_active": True},
        ]
        top = max(rows, key=lambda r: r["param_version"])
        assert top["is_active"] is True
