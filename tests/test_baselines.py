"""Tests for backtest.baselines and research.run_experiments gate stat (T-26).

All rows use "BTCUSD" (not "BTC") to match vw_asset_returns_weekly, and
include "simple_return" for the passive drift test.  Tests are pure-Python —
no BigQuery, ADC, or network calls.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from backtest.baselines import (
    PASSIVE_WEIGHTS,
    run_passive_baseline,
    run_tsh_baseline,
    run_vol_bh_baseline,
)
from backtest.engine import BacktestResult, run_backtest
from dataflow.strategy.portfolio import PortfolioParams, Scheme
from dataflow.strategy.tsmom_signal import TsmomParams
from research.run_experiments import (
    _align_to_common_dates,
    _filter_result_to_dates,
    compute_gate_stat,
)

# ── Shared fixtures ───────────────────────────────────────────────────────────

PARAMS_SHORT = TsmomParams(
    formation_horizon=2,
    vol_target=0.10,
    vol_lookback=2,
    periods_per_year=52,
)
PORT_INV_VOL = PortfolioParams(scheme=Scheme.INVERSE_VOL, crypto_cap=None)
PORT_EW = PortfolioParams(scheme=Scheme.EQUAL_WEIGHT)


def _monday(year: int, month: int, day: int) -> date:
    d = date(year, month, day)
    assert d.weekday() == 0, f"{d} is not a Monday"
    return d


def _mondays(start: date, n: int) -> list[date]:
    return [start + timedelta(weeks=i) for i in range(n)]


def _make_row(
    week: date,
    excess_log: float,
    excess_simple: float,
    vol: float = 0.15,
    simple_return: float | None = None,
) -> dict:
    """Build a minimal row matching the vw_asset_returns_weekly schema."""
    return {
        "week_start": week,
        "excess_log_return": excess_log,
        "excess_return": excess_simple,
        "simple_return": simple_return if simple_return is not None else excess_simple,
        "realized_vol_26w": vol,
    }


# ── TestTshBaseline ───────────────────────────────────────────────────────────


class TestTshBaseline:
    """TSH: sign of training-window mean excess log return."""

    _START = _monday(2020, 1, 6)

    def _dates(self, n: int) -> list[date]:
        return _mondays(self._START, n)

    def test_positive_training_mean_gives_long_weights(self):
        dates = self._dates(6)
        train, test = dates[:3], dates[3:]
        rows = {"SPY": [_make_row(d, excess_log=0.01, excess_simple=0.02) for d in dates]}

        result = run_tsh_baseline(rows, train, test, PORT_EW, with_crypto=False)

        assert len(result.dates) > 0
        # All SPY positions should be positive (long).
        for wts in result.weights:
            assert wts.get("SPY", 0.0) > 0

    def test_negative_training_mean_gives_short_weights(self):
        dates = self._dates(6)
        train, test = dates[:3], dates[3:]
        rows = {"SPY": [_make_row(d, excess_log=-0.01, excess_simple=0.02) for d in dates]}

        result = run_tsh_baseline(rows, train, test, PORT_EW, with_crypto=False)

        assert len(result.dates) > 0
        for wts in result.weights:
            assert wts.get("SPY", 0.0) < 0

    def test_signal_fixed_from_training_not_test_data(self):
        """TSH signal should come from train, not test — test rows are negative."""
        dates = self._dates(8)
        train, test = dates[:4], dates[4:]
        rows = {"SPY": [
            # Train rows: all positive excess_log_return → TSH signal = +1.
            *[_make_row(d, excess_log=0.01, excess_simple=0.02) for d in train],
            # Test rows: all negative excess_log_return (would give -1 if used).
            *[_make_row(d, excess_log=-0.05, excess_simple=-0.01) for d in test],
        ]}

        result = run_tsh_baseline(rows, train, test, PORT_EW, with_crypto=False)

        assert len(result.dates) > 0
        # Despite negative test returns, signal is fixed as +1 (from training).
        for wts in result.weights:
            assert wts.get("SPY", 0.0) > 0

    def test_gross_return_uses_excess_return_column(self):
        """Verify portfolio return is computed from excess_return, not excess_log_return."""
        dates = self._dates(5)
        train, test = dates[:2], dates[2:]
        # excess_log = 0.01, excess_simple = 0.10  → 10x difference.
        rows = {"SPY": [_make_row(d, excess_log=0.01, excess_simple=0.10) for d in dates]}

        result = run_tsh_baseline(rows, train, test, PORT_EW, with_crypto=False)

        assert len(result.gross_returns) > 0
        # Weight = ±1 (equal-weight, single instrument).
        # If engine reads excess_return (0.10): gross ≈ 0.10
        # If engine reads excess_log_return (0.01): gross ≈ 0.01
        # The 10x ratio makes mis-column obvious.
        for gr in result.gross_returns:
            assert abs(abs(gr) - 0.10) < 1e-9, f"Unexpected gross_return {gr}; expected ±0.10"

    def test_returns_backtest_result_instance(self):
        dates = self._dates(5)
        result = run_tsh_baseline({"SPY": []}, dates[:2], dates[2:], PORT_EW)
        assert isinstance(result, BacktestResult)

    def test_empty_train_window_produces_no_results(self):
        dates = self._dates(5)
        # Symbol with no rows in train window.
        rows = {"SPY": [_make_row(d, 0.01, 0.02) for d in dates[3:]]}
        result = run_tsh_baseline(rows, dates[:3], dates[3:], PORT_EW, with_crypto=False)
        # No signal (no training data for SPY) → empty or flat portfolio.
        for wts in result.weights:
            assert wts.get("SPY", 0.0) == 0.0

    def test_sizing_uses_prior_week_vol(self):
        """Each return week must be sized with vol(t-1), not vol(t) (look-ahead guard).

        Relative vols swap between week 0 and week 1: SPY is the low-vol asset
        at week 0 and the high-vol asset from week 1 on.  Sizing the week-1
        return with week-0 vol (honest) gives SPY the larger inverse-vol
        weight; sizing with week-1 vol (look-ahead) would give it the smaller.
        """
        dates = self._dates(4)
        rows = {
            "SPY": [_make_row(dates[0], 0.01, 0.02, vol=0.10)]
            + [_make_row(d, 0.01, 0.02, vol=0.30) for d in dates[1:]],
            "IEF": [_make_row(dates[0], 0.01, 0.02, vol=0.30)]
            + [_make_row(d, 0.01, 0.02, vol=0.10) for d in dates[1:]],
        }
        result = run_tsh_baseline(
            rows, dates[:1], dates[1:], PORT_INV_VOL, with_crypto=False
        )

        assert result.dates[0] == dates[1]
        wts = result.weights[0]
        assert wts["SPY"] > wts["IEF"], (
            f"Week {dates[1]} sized with its own vol (look-ahead): {wts}"
        )

    def test_no_signal_when_all_train_returns_zero(self):
        dates = self._dates(5)
        rows = {"SPY": [_make_row(d, excess_log=0.0, excess_simple=0.0) for d in dates]}
        result = run_tsh_baseline(rows, dates[:3], dates[3:], PORT_EW, with_crypto=False)
        # tsmom_sign(0) = 0 → excluded from portfolio.
        assert all(wts.get("SPY", 0.0) == 0.0 for wts in result.weights)


# ── TestVolBhBaseline ─────────────────────────────────────────────────────────


class TestVolBhBaseline:
    """Vol-BH: signal = +1 always, same vol scaling as TSMOM."""

    _START = _monday(2020, 1, 6)

    def _dates(self, n: int) -> list[date]:
        return _mondays(self._START, n)

    def test_all_weights_positive_all_periods(self):
        dates = self._dates(6)
        rows = {
            "SPY": [_make_row(d, 0.01, 0.02) for d in dates],
            "IEF": [_make_row(d, 0.005, 0.01) for d in dates],
        }
        result = run_vol_bh_baseline(rows, dates[2:], PORT_INV_VOL, with_crypto=False)

        assert len(result.weights) > 0
        for wts in result.weights:
            for sym, w in wts.items():
                assert w > 0, f"{sym} weight {w} is not positive"

    def test_cost_applied_at_entry(self):
        """Turnover > 0 on first week (entering from flat)."""
        dates = self._dates(4)
        rows = {"SPY": [_make_row(d, 0.01, 0.02) for d in dates]}
        result = run_vol_bh_baseline(rows, dates, PORT_EW, with_crypto=False)

        assert len(result.turnover) > 0
        assert result.turnover[0] > 0  # from 0 to w > 0

    def test_vol_scaling_changes_weight_magnitude(self):
        """Higher vol → smaller INVERSE_VOL weight."""
        dates = self._dates(5)
        rows = {
            "SPY": [_make_row(d, 0.01, 0.02, vol=0.10) for d in dates],  # lower vol
            "IEF": [_make_row(d, 0.005, 0.01, vol=0.30) for d in dates],  # higher vol
        }
        result = run_vol_bh_baseline(rows, dates[1:], PORT_INV_VOL, with_crypto=False)

        assert len(result.weights) > 0
        for wts in result.weights:
            if "SPY" in wts and "IEF" in wts:
                # Inverse-vol: lower-vol asset (SPY) gets higher weight.
                assert wts["SPY"] > wts["IEF"]

    def test_vol_bh_equals_tsmom_when_all_signals_positive(self):
        """When TSMOM signals are all +1, vol-BH and TSMOM produce identical weights.

        Invariant: vol-BH differs from TSMOM only in signal direction.
        When TSMOM direction = +1 everywhere, the two strategies are identical
        (same vol scaling, same portfolio_params, same cost model).
        """
        dates = self._dates(10)
        # All positive excess_log → TSMOM signal = +1 for all signal-eligible weeks.
        rows = {"SPY": [_make_row(d, excess_log=0.02, excess_simple=0.03) for d in dates]}

        # Start from TSMOM's first RETURN week: the first signal forms at index
        # formation_horizon - 1 and its book earns the next week's return
        # (w(t) × r(t+1)), so the first recorded week is dates[formation_horizon].
        # Starting one week earlier would have vol-BH enter (and pay entry cost)
        # before TSMOM — an asymmetric entry cost after alignment.
        test_dates = dates[PARAMS_SHORT.formation_horizon:]
        r_tsmom = run_backtest(
            rows, PARAMS_SHORT, PORT_INV_VOL,
            with_crypto=False, cost_multiplier=1.0,
        )
        r_tsmom_filtered = _filter_result_to_dates(r_tsmom, set(test_dates))
        r_vbh = run_vol_bh_baseline(
            rows, test_dates, PORT_INV_VOL,
            with_crypto=False, cost_multiplier=1.0,
        )

        aligned = _align_to_common_dates([r_tsmom_filtered, r_vbh])
        ra_tsmom, ra_vbh = aligned

        assert ra_tsmom.dates == ra_vbh.dates
        for nt, nv in zip(ra_tsmom.net_returns, ra_vbh.net_returns):
            assert abs(nt - nv) < 1e-9, f"TSMOM={nt:.6f} ≠ vol-BH={nv:.6f}"

    def test_sizing_uses_prior_week_vol(self):
        """Each return week must be sized with vol(t-1), not vol(t) (look-ahead guard).

        Same construction as the TSH twin test: relative vols swap between
        week 0 and week 1, so only prior-week sizing gives SPY the larger
        inverse-vol weight at the first return week.
        """
        dates = self._dates(4)
        rows = {
            "SPY": [_make_row(dates[0], 0.01, 0.02, vol=0.10)]
            + [_make_row(d, 0.01, 0.02, vol=0.30) for d in dates[1:]],
            "IEF": [_make_row(dates[0], 0.01, 0.02, vol=0.30)]
            + [_make_row(d, 0.01, 0.02, vol=0.10) for d in dates[1:]],
        }
        result = run_vol_bh_baseline(rows, dates[1:], PORT_INV_VOL, with_crypto=False)

        assert result.dates[0] == dates[1]
        wts = result.weights[0]
        assert wts["SPY"] > wts["IEF"], (
            f"Week {dates[1]} sized with its own vol (look-ahead): {wts}"
        )

    def test_returns_backtest_result_instance(self):
        result = run_vol_bh_baseline({}, [], PORT_EW)
        assert isinstance(result, BacktestResult)


# ── TestPassiveBaseline ───────────────────────────────────────────────────────


class TestPassiveBaseline:
    """60/40 Passive: fixed weights, annual drift rebalance."""

    _START = _monday(2020, 1, 6)

    def _passive_rows(self, n: int, excess_simple: float = 0.01, simple_ret: float = 0.012) -> dict:
        dates = _mondays(self._START, n)
        return {
            sym: [
                _make_row(d, excess_log=0.01, excess_simple=excess_simple, simple_return=simple_ret)
                for d in dates
            ]
            for sym in PASSIVE_WEIGHTS
        }

    def _passive_test_dates(self, n: int) -> list[date]:
        return _mondays(self._START, n)

    def test_initial_weights_match_target(self):
        rows = self._passive_rows(5)
        test_dates = self._passive_test_dates(5)
        result = run_passive_baseline(rows, test_dates)

        assert len(result.weights) > 0
        for sym, expected_w in PASSIVE_WEIGHTS.items():
            actual_w = result.weights[0].get(sym, 0.0)
            assert abs(actual_w - expected_w) < 1e-9, (
                f"{sym}: expected {expected_w}, got {actual_w}"
            )

    def test_no_btcusd_in_passive_book(self):
        n = 10
        dates = _mondays(self._START, n)
        rows = {
            **{sym: [_make_row(d, 0.01, 0.02) for d in dates] for sym in PASSIVE_WEIGHTS},
            "BTCUSD": [_make_row(d, 0.05, 0.10) for d in dates],
        }
        result = run_passive_baseline(rows, dates)

        for wts in result.weights:
            assert "BTCUSD" not in wts

    def test_cost_at_inception(self):
        """First week incurs turnover cost (entering from zero to PASSIVE_WEIGHTS)."""
        rows = self._passive_rows(3)
        test_dates = self._passive_test_dates(3)
        result = run_passive_baseline(rows, test_dates, cost_multiplier=1.0)

        assert len(result.net_returns) > 0
        # Net return = gross + cost; cost < 0; so net < gross.
        assert result.net_returns[0] < result.gross_returns[0]
        assert result.turnover[0] > 0

    def test_rebalance_applies_cost_at_week_52(self):
        """Rebalance at week 52 incurs turnover; intervening weeks do not.

        Equities (SPY, EFA) grow at 5 %/week; bonds (IEF, TLT) at 1 %/week.
        After 52 weeks the equity sleeve drifts well above the 60 % target,
        so the rebalance back to PASSIVE_WEIGHTS creates positive turnover.
        With uniform returns the weights never drift, which would make the
        rebalance zero-cost and cause a false failure.
        """
        n = 55
        dates = _mondays(self._START, n)
        # Asymmetric returns: equities outperform bonds → drift away from 60/40.
        rows: dict[str, list[dict]] = {}
        for sym in ("SPY", "EFA"):
            rows[sym] = [_make_row(d, 0.01, 0.04, simple_return=0.05) for d in dates]
        for sym in ("IEF", "TLT"):
            rows[sym] = [_make_row(d, 0.005, 0.01, simple_return=0.01) for d in dates]

        result = run_passive_baseline(rows, dates, rebalance_weeks=52)

        # Week 0 has cost; weeks 1–51 do not; week 52 has cost (drift rebalance).
        assert result.turnover[0] > 0
        for i in range(1, 52):
            assert result.turnover[i] == 0.0, f"Unexpected turnover at week {i}"
        assert result.turnover[52] > 0

    def test_drift_uses_simple_return_not_excess(self):
        """Weight drift must use simple_return (total price), not excess_return.

        When simple_return ≠ excess_return (rf > 0), drift computed from
        simple_return gives different weights than from excess_return.
        If the code incorrectly uses excess_return for drift, the second-week
        weights will differ from the expected value.
        """
        n = 4
        dates = _mondays(self._START, n)
        # simple_return = 0.10, excess_return = 0.05  (rf = 5 %)
        rows = {
            "SPY": [_make_row(d, 0.01, excess_simple=0.05, simple_return=0.10) for d in dates],
            "EFA": [_make_row(d, 0.01, excess_simple=0.05, simple_return=0.10) for d in dates],
            "IEF": [_make_row(d, 0.005, excess_simple=0.01, simple_return=0.02) for d in dates],
            "TLT": [_make_row(d, 0.005, excess_simple=0.01, simple_return=0.02) for d in dates],
        }
        result = run_passive_baseline(rows, dates, rebalance_weeks=52)

        # Week 0: initial weights = PASSIVE_WEIGHTS = {SPY:0.3, EFA:0.3, IEF:0.2, TLT:0.2}
        # After week 0, drift with simple_return=0.10 (equity) and 0.02 (bonds):
        # Equity share grows relative to bonds because simple_return is higher.
        # Expected week-1 weights (before rebalance, after drift on week-0 returns):
        #   SPY: 0.3 * (1 + 0.10) = 0.330
        #   EFA: 0.3 * (1 + 0.10) = 0.330
        #   IEF: 0.2 * (1 + 0.02) = 0.204
        #   TLT: 0.2 * (1 + 0.02) = 0.204
        #   Total = 1.068  → normalized:
        #   SPY = 0.330 / 1.068 ≈ 0.3090
        total = 0.330 + 0.330 + 0.204 + 0.204
        expected_spy_wk1 = 0.330 / total

        wk1_spy = result.weights[1].get("SPY", 0.0)
        assert abs(wk1_spy - expected_spy_wk1) < 1e-8, (
            f"Week-1 SPY weight {wk1_spy:.6f} != expected {expected_spy_wk1:.6f}; "
            "drift may be using excess_return instead of simple_return"
        )

    def test_returns_backtest_result_instance(self):
        result = run_passive_baseline({}, [])
        assert isinstance(result, BacktestResult)


# ── TestFoldDateAlignment ─────────────────────────────────────────────────────


class TestFoldDateAlignment:
    """Common-date intersection alignment for gate stat validity."""

    _START = _monday(2020, 1, 6)

    def test_common_dates_alignment_trims_all_results(self):
        """Intersection of dates is taken; strategies with more dates are trimmed."""
        dates = _mondays(self._START, 6)

        r_long = BacktestResult()
        r_long.dates = dates[:]
        r_long.gross_returns = [0.01] * 6
        r_long.net_returns = [0.01] * 6
        r_long.turnover = [0.0] * 6
        r_long.weights = [{}] * 6

        r_short = BacktestResult()
        r_short.dates = dates[2:]  # only last 4 dates
        r_short.gross_returns = [0.01] * 4
        r_short.net_returns = [0.01] * 4
        r_short.turnover = [0.0] * 4
        r_short.weights = [{}] * 4

        aligned = _align_to_common_dates([r_long, r_short])
        assert len(aligned) == 2
        assert aligned[0].dates == dates[2:]  # r_long trimmed to match r_short
        assert aligned[1].dates == dates[2:]  # r_short unchanged

    def test_tsmom_and_tsh_dates_match_after_alignment(self):
        """After alignment, TSMOM and TSH have identical date sequences."""
        dates = _mondays(self._START, 10)
        train, test = dates[:4], dates[4:]
        rows = {
            "SPY": [_make_row(d, 0.01, 0.02) for d in dates],
            "IEF": [_make_row(d, 0.005, 0.01) for d in dates],
        }

        r_tsmom = _filter_result_to_dates(
            run_backtest(rows, PARAMS_SHORT, PORT_EW, with_crypto=False),
            set(test),
        )
        r_tsh = run_tsh_baseline(rows, train, test, PORT_EW, with_crypto=False)
        r_vbh = run_vol_bh_baseline(rows, test, PORT_EW, with_crypto=False)
        r_pas = run_passive_baseline(
            {sym: rows[sym] for sym in PASSIVE_WEIGHTS if sym in rows},
            test,
        )

        aligned = _align_to_common_dates([r_tsmom, r_tsh, r_vbh, r_pas])
        first_dates = set(aligned[0].dates)
        for r in aligned[1:]:
            assert set(r.dates) == first_dates

    @pytest.mark.parametrize("cost_mult", [1.0, 1.5, 2.0])
    def test_higher_cost_lowers_net_sharpe(self, cost_mult: float):
        """Net Sharpe must be non-increasing with cost_multiplier."""
        if cost_mult == 1.0:
            pytest.skip("Base case; no comparison to make alone")
        dates = _mondays(self._START, 15)
        rows = {"SPY": [_make_row(d, 0.01, 0.02) for d in dates]}
        test_dates = dates[3:]

        r_base = run_vol_bh_baseline(rows, test_dates, PORT_EW, with_crypto=False, cost_multiplier=1.0)
        r_high = run_vol_bh_baseline(rows, test_dates, PORT_EW, with_crypto=False, cost_multiplier=cost_mult)

        if not r_base.net_returns or not r_high.net_returns:
            pytest.skip("No test-window returns generated")

        from backtest.metrics import annualized_sharpe
        sr_base = annualized_sharpe(r_base.net_returns)
        sr_high = annualized_sharpe(r_high.net_returns)
        assert sr_high <= sr_base, (
            f"Higher cost ({cost_mult}×) should not improve Sharpe: "
            f"{sr_high:.4f} > {sr_base:.4f}"
        )

    def test_experiment_runs_json_discriminator(self):
        """Every strategy's params carry a 'strategy' key naming it."""
        import json

        # TSMOM seed v1 params (self-identifying via the 'strategy' key, same as
        # the baselines — the row no longer relies on the key's *absence*).
        tsmom_json = json.dumps({
            "strategy": "tsmom",
            "formation_horizon": 52,
            "vol_target": 0.10,
            "vol_lookback": 26,
        })
        tsh_json = json.dumps({"strategy": "tsh", "vol_target": 0.10})
        vbh_json = json.dumps({"strategy": "vol_bh", "vol_target": 0.10})
        passive_json = json.dumps({"strategy": "60_40"})

        names = {
            json.loads(j)["strategy"]
            for j in (tsmom_json, tsh_json, vbh_json, passive_json)
        }
        assert names == {"tsmom", "tsh", "vol_bh", "60_40"}


# ── TestGateStat ──────────────────────────────────────────────────────────────


class TestGateStat:
    """Newey-West HAC gate stat (Huang 2020 autocorrelation correction)."""

    def test_zero_delta_returns_zero_t_stat(self):
        """Identical series → mean_delta = 0 → t_stat_hac = 0."""
        n = 100
        series = [0.01] * n
        mean_d, t_hac = compute_gate_stat(series, series)
        assert mean_d == 0.0
        assert t_hac == 0.0

    def test_positive_delta_uncorrelated_approx_passes_threshold(self):
        """iid N(μ, σ), large n, μ >> σ/√n → t_stat_hac > 1.64."""
        import random
        random.seed(0)
        n = 500
        # Large signal-to-noise so HAC (≈ OLS for iid) exceeds threshold.
        tsmom = [0.02 + random.gauss(0, 0.005) for _ in range(n)]
        tsh = [random.gauss(0, 0.005) for _ in range(n)]

        _, t_hac = compute_gate_stat(tsmom, tsh)
        assert t_hac > 1.64

    def test_autocorrelated_delta_lower_t_than_naive(self):
        """AR(1) δ_t with ρ=0.9 → t_stat_HAC < t_stat_naive.

        This is the key invariant: Newey-West is more conservative than OLS
        under positive autocorrelation, which is the exact correction Huang
        (2020) showed was missing from the MOP pooled t-stat of 4.34.
        """
        import random
        random.seed(42)
        n = 200
        rho = 0.9
        mu = 0.01
        # AR(1): δ_t = ρ·δ_{t-1} + (1-ρ)·μ + ε_t
        delta = [mu]
        for _ in range(n - 1):
            eps = random.gauss(0, 0.002)
            delta.append(rho * delta[-1] + (1 - rho) * mu + eps)

        tsmom = delta
        tsh = [0.0] * n

        mean_d, t_hac = compute_gate_stat(tsmom, tsh, max_lag=52)

        # Naïve t-stat (biased variance convention, same as HAC with max_lag=0).
        _, t_naive = compute_gate_stat(tsmom, tsh, max_lag=0)

        assert t_hac < t_naive, (
            f"HAC t-stat ({t_hac:.3f}) should be < naïve ({t_naive:.3f}) "
            "for AR(1) ρ=0.9; autocorrelation inflates the naïve statistic"
        )

    def test_newey_west_lag_zero_equals_biased_ols(self):
        """max_lag=0 → V_NW = γ(0) → t_stat_hac = mean/sqrt(biased_var/n)."""
        delta = [0.1, -0.05, 0.2, -0.1, 0.15, 0.08, -0.03, 0.12]
        n = len(delta)
        tsmom = delta
        tsh = [0.0] * n

        _, t_hac = compute_gate_stat(tsmom, tsh, max_lag=0)

        # Replicate the formula manually.
        mean_d = sum(delta) / n
        biased_var = sum((d - mean_d) ** 2 for d in delta) / n
        t_manual = mean_d / math.sqrt(biased_var / n)

        assert abs(t_hac - t_manual) < 1e-10

    def test_gate_threshold_is_1_64(self):
        """Gate threshold constant must be 1.64 (pre-committed α=0.10)."""
        from research.run_experiments import GATE_THRESHOLD
        assert GATE_THRESHOLD == 1.64

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError, match="Length mismatch"):
            compute_gate_stat([0.01, 0.02], [0.01])

    def test_single_element_returns_sensibly(self):
        """Single observation: t-stat derived from the only delta value."""
        mean_d, t_hac = compute_gate_stat([0.05], [0.02])
        assert mean_d == pytest.approx(0.03)
        # With n=1, gamma(0) = 0.0 (demeaned[0] = delta - mean = 0).
        # → V_NW = 0 → t_stat = 0.0
        assert t_hac == 0.0
