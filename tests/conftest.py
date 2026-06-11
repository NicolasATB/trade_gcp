"""Shared fixtures for the pipeline unit tests (T-15).

The fixtures here build the small, hand-checkable inputs the stage logic
operates on: OHLCV/RSI rows and the active strategy parameters. They are kept
deliberately tiny so the expected RSI / signal values can be derived by hand in
the tests (see ``test_rsi.py`` for the worked Wilder example).
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest


def _utc(d: date) -> datetime:
    """A timezone-aware midnight-UTC datetime for ``d`` (how the silver tables store it)."""
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


@pytest.fixture
def strategy_params() -> dict:
    """Active strategy params matching the seeded v1 row (14 / 40 / 70 / 30 / 70)."""
    return {
        "rsi_period": 14,
        "weekly_rsi_trend_start": 40.0,
        "weekly_rsi_trend_end": 70.0,
        "daily_rsi_oversold": 30.0,
        "daily_rsi_overbought": 70.0,
    }


@pytest.fixture
def make_ohlcv():
    """Factory: build OHLCV rows (only the keys ``compute_rsi_rows`` needs).

    Usage: ``make_ohlcv([(date(2024, 1, 1), 10.0), ...])`` →
    ``[{"time_period_start": <utc dt>, "price_close": 10.0}, ...]``.
    """

    def _build(pairs):
        return [
            {"time_period_start": _utc(d), "price_close": float(close)}
            for d, close in pairs
        ]

    return _build


@pytest.fixture
def make_weekly_rsi():
    """Factory: build weekly RSI history rows for the trend walk-forward.

    Usage: ``make_weekly_rsi([(date(2024, 1, 7), 35.0), ...])`` →
    ``[{"time_period_start": <utc dt>, "rsi": 35.0}, ...]``.
    """

    def _build(pairs):
        return [
            {"time_period_start": _utc(d), "rsi": float(rsi)} for d, rsi in pairs
        ]

    return _build


# Column meanings for the coverage table; coverage.py prints the table but no
# legend, so we append one after the run (see the terminal-summary hook below).
_COVERAGE_LEGEND = [
    ("Stmts",   "executable statements (blank lines/comments/docstrings excluded)"),
    ("Miss",    "statements not executed by any test"),
    ("Branch",  "decision branches (each if/for/while/except adds true+false paths)"),
    ("BrPart",  "branches only partially taken (one outcome exercised, not both)"),
    ("Cover",   "% covered, combining statements and branches"),
    ("Missing", "line numbers of the uncovered statements / partial branches"),
]


# Why the still-uncovered lines are not unit-tested, keyed by file basename. The
# coverage report shows *which* lines; this explains *why* each is left out.
_MISSING_REASONS = {
    "cleanup.py": "default BigQuery client + _parse_args CLI (need a live client / argv)",
    "rsi.py": "_ts_str str/non-isoformat branch (BQ rows always arrive as datetime)",
    "signals.py": "_as_date & _ts_str str/date branches (BQ rows always arrive as datetime)",
}


def _coverage_totals(cov):
    """Aggregate per-file coverage Numbers; return the combined object or None.

    Uses coverage's own analysis so the percentages match the table exactly
    (statements + branches). Best-effort: any API change degrades to no breakdown.
    """
    try:
        total = None
        for path in cov.get_data().measured_files():
            numbers = cov._analyze(path).numbers
            total = numbers if total is None else total + numbers
        return total
    except Exception:
        return None


@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Append a legend + a unit-test coverage breakdown after the run.

    Runs last (``trylast``) so it prints right after pytest-cov's table, and only
    when coverage is actually being reported (skipped under ``--no-cov``).
    """
    if getattr(config.option, "no_cov", False):
        return
    if not (getattr(config.option, "cov_source", None) or getattr(config.option, "cov", None)):
        return

    tr = terminalreporter
    tr.write_sep("=", "coverage column legend")
    for name, description in _COVERAGE_LEGEND:
        tr.write_line(f"  {name:<8} {description}")

    cov = getattr(getattr(config.pluginmanager.getplugin("_cov"), "cov_controller", None), "cov", None)
    if cov is None:
        return
    totals = _coverage_totals(cov)
    if totals is None:
        return

    covered_pct = totals.pc_covered
    gap_pct = 100.0 - covered_pct
    # n_statements already excludes pragma'd lines; add them back for the share of
    # code that is deliberately out of the unit-test scope (needs live GCP).
    measurable = totals.n_statements + totals.n_excluded
    excluded_pct = (totals.n_excluded / measurable * 100.0) if measurable else 0.0

    tr.write_sep("=", "unit-test coverage breakdown")
    tr.write_line(f"  covered by unit tests   (of testable code): {covered_pct:5.2f}%")
    tr.write_line(f"  not yet covered         (of testable code): {gap_pct:5.2f}%   -> reasons below")
    tr.write_line(
        f"  excluded, needs live GCP (of all code)    : {excluded_pct:5.2f}%   "
        f"({totals.n_excluded} stmts # pragma: no cover; pipeline.py also omitted)"
    )
    tr.write_line("  why the testable gap is not yet unit-tested:")
    for path in sorted(cov.get_data().measured_files()):
        try:
            _, _statements, _excluded, missing, missing_fmt = cov.analysis2(path)
        except Exception:
            continue
        if not missing:
            continue
        rel = os.path.relpath(path).replace(os.sep, "/")
        reason = _MISSING_REASONS.get(os.path.basename(path), "")
        tr.write_line(f"    {rel:<26} {missing_fmt:<20} {reason}")
