"""Tests for backtest.splitter — walk-forward splits, purge/embargo, holdout gate (T-25)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backtest.splitter import (
    HOLDOUT_MIN_OBS,
    HOLDOUT_START,
    HoldoutViolationError,
    WalkForwardConfig,
    open_holdout,
    walk_forward_splits,
)


def _mondays(start: date, n: int) -> list[date]:
    """Generate ``n`` weekly Mondays starting from ``start``."""
    assert start.weekday() == 0, "start must be a Monday"
    return [start + timedelta(weeks=i) for i in range(n)]


# Pre-holdout Mondays (2010 onwards, safely before 2021-01-04).
TRAIN_START = date(2010, 1, 4)
# Holdout Mondays starting from HOLDOUT_START (2021-01-04).
HOLDOUT_START_MONDAY = HOLDOUT_START  # already a Monday


class TestHoldoutStart:
    def test_holdout_start_is_a_monday(self):
        assert HOLDOUT_START.weekday() == 0, "HOLDOUT_START must be a Monday"

    def test_holdout_start_value(self):
        assert HOLDOUT_START == date(2021, 1, 4)


class TestWalkForwardSplits:
    def _pre_holdout_dates(self, n: int) -> list[date]:
        return _mondays(TRAIN_START, n)

    def test_produces_exactly_n_splits(self):
        dates = self._pre_holdout_dates(400)
        cfg = WalkForwardConfig(n_splits=5, min_train_weeks=104)
        folds = list(walk_forward_splits(dates, cfg))
        assert len(folds) == 5

    def test_empty_input_produces_no_folds(self):
        cfg = WalkForwardConfig(n_splits=3)
        assert list(walk_forward_splits([], cfg)) == []

    def test_holdout_violation_raises(self):
        dates = _mondays(HOLDOUT_START, 10)  # all in holdout
        cfg = WalkForwardConfig(n_splits=2, min_train_weeks=3)
        with pytest.raises(HoldoutViolationError):
            list(walk_forward_splits(dates, cfg))

    def test_mixed_dates_raises_on_holdout(self):
        pre = _mondays(TRAIN_START, 200)
        post = _mondays(HOLDOUT_START, 5)
        cfg = WalkForwardConfig(n_splits=2, min_train_weeks=100)
        with pytest.raises(HoldoutViolationError):
            list(walk_forward_splits(pre + post, cfg))

    def test_no_date_exceeds_holdout_in_any_split(self):
        dates = self._pre_holdout_dates(300)
        cfg = WalkForwardConfig(n_splits=4, min_train_weeks=104)
        for train, test in walk_forward_splits(dates, cfg):
            assert all(d < HOLDOUT_START for d in train)
            assert all(d < HOLDOUT_START for d in test)

    def test_purge_zone_absent_from_train(self):
        """Last ``purge_weeks`` dates before each test start must not appear in train."""
        dates = self._pre_holdout_dates(300)
        cfg = WalkForwardConfig(n_splits=4, min_train_weeks=104, purge_weeks=3)
        folds = list(walk_forward_splits(dates, cfg))
        for train, test in folds:
            if not test:
                continue
            test_start = min(test)
            # The 3 weeks immediately before test_start must not be in train.
            purge_zone = {test_start - timedelta(weeks=k) for k in range(1, 4)}
            overlap = purge_zone & set(train)
            assert overlap == set(), f"purge zone leaked into train: {overlap}"

    def test_embargo_zone_absent_from_test(self):
        """First ``embargo_weeks`` weeks after each train end must not appear in test."""
        dates = self._pre_holdout_dates(300)
        cfg = WalkForwardConfig(n_splits=4, min_train_weeks=104,
                                purge_weeks=3, embargo_weeks=2)
        folds = list(walk_forward_splits(dates, cfg))
        for train, test in folds:
            if not train or not test:
                continue
            train_end = max(train)
            # Two weeks after the last train date must not appear in test.
            embargo_zone = {train_end + timedelta(weeks=k) for k in range(1, 3)}
            overlap = embargo_zone & set(test)
            assert overlap == set(), (
                f"embargo zone leaked into test: {overlap}; "
                f"train_end={train_end}, test_start={min(test)}"
            )

    def test_train_before_test_in_every_fold(self):
        dates = self._pre_holdout_dates(300)
        cfg = WalkForwardConfig(n_splits=4, min_train_weeks=104)
        for train, test in walk_forward_splits(dates, cfg):
            assert max(train) < min(test), "train must precede test"

    def test_expanding_mode_train_grows(self):
        dates = self._pre_holdout_dates(350)
        cfg = WalkForwardConfig(n_splits=4, min_train_weeks=104, mode="expanding")
        sizes = [len(train) for train, _ in walk_forward_splits(dates, cfg)]
        assert sizes == sorted(sizes), "expanding mode: each fold's train must be larger"

    def test_rolling_mode_fixed_train_size(self):
        dates = self._pre_holdout_dates(350)
        cfg = WalkForwardConfig(n_splits=4, min_train_weeks=104, mode="rolling")
        # Rolling mode: train size is ≤ min_train_weeks (minus purge trimming).
        for train, _ in walk_forward_splits(dates, cfg):
            assert len(train) <= cfg.min_train_weeks

    def test_insufficient_dates_raises(self):
        # Only 50 dates but min_train_weeks=104.
        dates = self._pre_holdout_dates(50)
        cfg = WalkForwardConfig(n_splits=2, min_train_weeks=104)
        with pytest.raises(ValueError):
            list(walk_forward_splits(dates, cfg))

    def test_first_fold_contains_live_signal(self):
        """First expanding fold with BTC-style ragged start (2011-08) must have ≥1 date.

        Regression: a misconfigured min_train_weeks can make the first fold empty
        because the formation warm-up (52w) consumes all of the training window.
        104w min_train = 52w warm-up + 52w effective training → first fold is non-empty.
        """
        # Synthetic BTC-ish start: 2011-08-15 (a Monday).
        btc_start = date(2011, 8, 15)
        dates = _mondays(btc_start, 600)
        # Filter to pre-holdout only.
        pre_holdout = [d for d in dates if d < HOLDOUT_START]
        cfg = WalkForwardConfig(n_splits=3, min_train_weeks=104, purge_weeks=3)
        folds = list(walk_forward_splits(pre_holdout, cfg))
        assert folds, "Expected at least 1 fold with BTC-style data starting 2011-08"
        first_train, first_test = folds[0]
        assert len(first_test) >= 1, "First fold test is empty — min_train_weeks too large"


class TestOpenHoldout:
    def test_returns_dates_on_or_after_holdout_start(self):
        pre = _mondays(TRAIN_START, 100)
        post = _mondays(HOLDOUT_START, HOLDOUT_MIN_OBS + 10)
        holdout = open_holdout(pre + post)
        assert all(d >= HOLDOUT_START for d in holdout)

    def test_returns_sorted_dates(self):
        post = _mondays(HOLDOUT_START, HOLDOUT_MIN_OBS + 5)
        holdout = open_holdout(post)
        assert holdout == sorted(holdout)

    def test_excludes_pre_holdout_dates(self):
        pre = _mondays(TRAIN_START, 100)
        post = _mondays(HOLDOUT_START, HOLDOUT_MIN_OBS)
        holdout = open_holdout(pre + post)
        assert all(d < HOLDOUT_START for d in pre)
        assert not any(d in holdout for d in pre)

    def test_raises_when_insufficient_holdout_coverage(self):
        # Fewer than HOLDOUT_MIN_OBS holdout dates → data coverage error.
        post = _mondays(HOLDOUT_START, HOLDOUT_MIN_OBS - 1)
        with pytest.raises(ValueError, match="(?i)holdout"):
            open_holdout(post)

    def test_exact_minimum_does_not_raise(self):
        post = _mondays(HOLDOUT_START, HOLDOUT_MIN_OBS)
        holdout = open_holdout(post)
        assert len(holdout) == HOLDOUT_MIN_OBS

    def test_empty_input_raises(self):
        with pytest.raises(ValueError):
            open_holdout([])
