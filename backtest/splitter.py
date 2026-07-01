"""Walk-forward splits with purge + embargo, and holdout gate (T-25).

Holdout boundary — provenance (honest, NOT arbitrary)
-----------------------------------------------------
``HOLDOUT_START = date(2021, 1, 4)`` is the first Monday of 2021.  It is *not*
a "blind" date chosen to maximise the backtest Sharpe.  It is set on a
theory-driven regime criterion known ex ante and independent of Strategy 3's
returns: the stock–bond correlation flip to positive over 2021–2023 documented
by Molenaar et al. (2024, FAJ), a macro structural break that stresses the
XTSMOM cross-asset mechanism.  The boundary was **not** chosen by trying
candidate dates and comparing strategy Sharpe across them — that would
data-snoop the split.  It is sealed as a module constant (never a parameter)
so no ticket can tune it.

Read-the-verdict asymmetry (T-27): because the post-2020 regime is adversarial
to the bond→equity channel, a *negative* holdout result is partly expected and
weakly informative; a *positive* result is the surprising, strongly informative
outcome.  State this asymmetry alongside the holdout number in T-27 so it is
not over-read.

Coverage coupling (HOLDOUT_MIN_OBS)
------------------------------------
With ``data_end ≈ 2026-07``, the holdout window contains ≈ 286 weekly
observations, comfortably above the 250-obs gate.  The three values
(``HOLDOUT_START``, ``HOLDOUT_MIN_OBS``, ``data_end``) are coupled: if a
backfill ends early or the engine runs on a data subset, ``open_holdout``
raises ``ValueError`` — that means "insufficient holdout coverage", not a bug.
Equivalent to the T-19 coverage gate; ``HOLDOUT_MIN_OBS`` is the detector.

Single-use convention (open_holdout)
-------------------------------------
``open_holdout`` is structurally prevented from leaking holdout data into CV
splits (any date >= ``HOLDOUT_START`` in a ``walk_forward_splits`` call raises
``HoldoutViolationError`` on the train side).  It is documented as "call once
in T-27" as a convention, not enforced by a counter — the residual risk (a
research script calling it to peek) is accepted as a documented convention, not
a code guard.  This is a conscious design choice.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Generator, Literal

# ── Holdout constants ────────────────────────────────────────────────────────

# First Monday of 2021 — theory-driven regime boundary (see module docstring).
# NEVER change this to a parameter; that would data-snoop the holdout split.
HOLDOUT_START: date = date(2021, 1, 4)

# Minimum number of weekly observations required in the holdout window.
# With data_end ≈ 2026-07, the actual count is ≈ 286 > 250.
HOLDOUT_MIN_OBS: int = 250


# ── Exceptions ───────────────────────────────────────────────────────────────

class HoldoutViolationError(ValueError):
    """Raised when a caller tries to include holdout dates in a CV split."""


# ── Config ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WalkForwardConfig:
    """Walk-forward split configuration.

    Attributes:
        n_splits: Number of train/test folds to generate.
        min_train_weeks: Minimum training observations before the first fold.
            Default 104 = formation_horizon(52) + vol_lookback(26) warm-up +
            ~26-week buffer; 104 − 52 = 52 effective training weeks in the first
            expanding fold.  BTC's ragged start (~2011-08) plus 52-week formation
            gives the first BTC signal ~2012-08; the first fold grows from there
            in expanding mode.
        purge_weeks: Weeks removed from the *end* of each training split to
            prevent label-horizon overlap with the test period (López de Prado).
        embargo_weeks: Weeks removed from the *start* of each test split to
            account for serial correlation not eliminated by purging alone.
            Default purge=3 + embargo=2 = 5 weeks total, as recommended by
            López de Prado for weekly data.
        mode: ``"expanding"`` (default) grows the train window from the earliest
            date; ``"rolling"`` keeps it at a fixed size.
    """

    n_splits: int
    min_train_weeks: int = 104
    purge_weeks: int = 3
    embargo_weeks: int = 2
    mode: Literal["expanding", "rolling"] = "expanding"


# ── Split generator ──────────────────────────────────────────────────────────

def walk_forward_splits(
    dates: list[date],
    config: WalkForwardConfig,
) -> Generator[tuple[list[date], list[date]], None, None]:
    """Generate walk-forward (train, test) splits over pre-holdout dates.

    Applies purge + embargo at each split boundary per López de Prado (2018).
    Only dates strictly before ``HOLDOUT_START`` are accepted; passing any
    holdout date raises ``HoldoutViolationError`` immediately.

    Args:
        dates: Sorted weekly dates (Monday), oldest first.  Must all be <
            ``HOLDOUT_START`` (the holdout is not a CV fold).
        config: Walk-forward configuration.

    Yields:
        ``(train_dates, test_dates)`` pairs.  Both lists are sorted; the purge
        zone (last ``purge_weeks`` of train) and embargo zone (first
        ``embargo_weeks`` of test) are excluded from their respective sides.

    Raises:
        HoldoutViolationError: If any date in ``dates`` is ≥ ``HOLDOUT_START``.
        ValueError: If there are not enough dates to produce at least one fold
            with ``min_train_weeks`` training observations.
    """
    if not dates:
        return

    # Guard: no holdout dates allowed in CV splits.
    holdout_dates = [d for d in dates if d >= HOLDOUT_START]
    if holdout_dates:
        raise HoldoutViolationError(
            f"walk_forward_splits received {len(holdout_dates)} date(s) >= "
            f"HOLDOUT_START ({HOLDOUT_START}).  The holdout is sealed; pass only "
            f"pre-holdout dates.  First violating date: {holdout_dates[0]}"
        )

    sorted_dates = sorted(dates)
    n = len(sorted_dates)
    n_splits = config.n_splits

    if n < config.min_train_weeks + 1:
        raise ValueError(
            f"Not enough dates ({n}) to produce a fold with "
            f"min_train_weeks={config.min_train_weeks}."
        )

    # Determine test fold positions: divide the dates after min_train_weeks
    # into n_splits roughly equal chunks.
    available_for_test = n - config.min_train_weeks
    if available_for_test < n_splits:
        raise ValueError(
            f"Only {available_for_test} dates available for testing after "
            f"min_train_weeks={config.min_train_weeks}, but n_splits={n_splits}."
        )

    test_size = available_for_test // n_splits

    for fold_idx in range(n_splits):
        test_start_idx = config.min_train_weeks + fold_idx * test_size
        test_end_idx = (
            test_start_idx + test_size
            if fold_idx < n_splits - 1
            else n  # last fold absorbs remainder
        )

        # Train: from the start to test_start, minus the purge zone.
        if config.mode == "expanding":
            train_start_idx = 0
        else:  # rolling: fixed-size window ending at test_start
            train_start_idx = max(0, test_start_idx - config.min_train_weeks)

        train_raw_end = test_start_idx  # exclusive
        purge_cutoff = max(train_start_idx, train_raw_end - config.purge_weeks)
        train_dates = sorted_dates[train_start_idx:purge_cutoff]

        # Test: from test_start, skipping the embargo zone.
        embargo_skip = min(config.embargo_weeks, test_end_idx - test_start_idx)
        test_dates = sorted_dates[test_start_idx + embargo_skip : test_end_idx]

        if not train_dates or not test_dates:
            continue

        yield train_dates, test_dates


# ── Holdout gate ─────────────────────────────────────────────────────────────

def open_holdout(all_dates: list[date]) -> list[date]:
    """Return the holdout dates (≥ ``HOLDOUT_START``) from the full date index.

    Validates that the holdout contains at least ``HOLDOUT_MIN_OBS``
    observations.  Intended to be called exactly once in T-27 (verdict
    evaluation) — not during parameter search (T-26).

    Args:
        all_dates: Full sorted weekly date index (both CV and holdout dates).

    Returns:
        Sorted list of dates that belong to the holdout window.

    Raises:
        ValueError: If the holdout window has fewer than ``HOLDOUT_MIN_OBS``
            observations — indicates insufficient data coverage (coupling note:
            with ``data_end ≈ 2026-07`` the count is ≈ 286 > 250; a smaller
            count means a backfill ended early or a subset was passed).
    """
    holdout = sorted(d for d in all_dates if d >= HOLDOUT_START)
    if len(holdout) < HOLDOUT_MIN_OBS:
        raise ValueError(
            f"Holdout window has only {len(holdout)} weekly observations "
            f"(need >= {HOLDOUT_MIN_OBS}).  Check that the full date index "
            f"reaches past {HOLDOUT_START} with enough coverage.  This is a "
            f"data-coverage error, not a code bug."
        )
    return holdout
