"""Regression guards on the SQL templates in the Dataflow stages.

These tests do NOT validate that BigQuery executes the SQL correctly (that is
the deferred integration test, see README technical debt). They pin the
business rules *encoded in the SQL text* so a refactor cannot silently revert
them:

  * Weeks run Monday→Sunday: ``WEEK(MONDAY)`` everywhere, never a bare ``WEEK``
    (which is Sunday-based in BigQuery).
  * The weekly candle opens with the first day's open and closes with the last
    day's close.
  * Every MERGE upserts on its table's documented natural key (the project's
    idempotency contract).
"""

from __future__ import annotations

import re
from datetime import date

import pytest

from dataflow.stages import conform, rsi, signals, tsmom_signal_stage, portfolio_weights_stage


def _norm(sql: str) -> str:
    """Collapse whitespace so assertions don't depend on SQL formatting."""
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# Weekly aggregation — Monday→Sunday business rule
# ---------------------------------------------------------------------------

class TestWeeklyAggregationRules:
    def test_weekly_merge_uses_monday_weeks(self):
        # Both the GROUP key and the range lower bound must truncate to Monday.
        assert conform._WEEKLY_MERGE_SQL.count("WEEK(MONDAY)") == 2

    def test_no_bare_week_truncation_anywhere(self):
        # A bare DATE_TRUNC(..., WEEK) is Sunday-based in BigQuery and would
        # silently revert the Monday→Sunday rule.
        for name, sql in _ALL_SQL_TEMPLATES:
            bare_week = re.search(r"DATE_TRUNC\([^)]*,\s*WEEK\s*\)", sql)
            assert bare_week is None, f"bare WEEK (Sunday-based) found in {name}"

    def test_weekly_close_is_last_days_close(self):
        assert (
            "ARRAY_AGG(price_close ORDER BY time_period_start DESC LIMIT 1)"
            in _norm(conform._WEEKLY_MERGE_SQL)
        )

    def test_weekly_open_is_first_days_open(self):
        assert (
            "ARRAY_AGG(price_open ORDER BY time_period_start ASC LIMIT 1)"
            in _norm(conform._WEEKLY_MERGE_SQL)
        )

    def test_weekly_high_low_span_the_week(self):
        normalised = _norm(conform._WEEKLY_MERGE_SQL)
        assert "MAX(price_high) AS price_high" in normalised
        assert "MIN(price_low) AS price_low" in normalised


# ---------------------------------------------------------------------------
# MERGE natural keys — the idempotency contract per table
# ---------------------------------------------------------------------------

# (template name, sql, documented natural key) — must match the
# bigquery-beam-patterns skill table and sql/DDL.sql.
_MERGE_CONTRACTS = [
    ("conform daily MERGE", conform._MERGE_SQL,
     ["symbol", "temporality", "time_period_start"]),
    ("conform weekly MERGE", conform._WEEKLY_MERGE_SQL,
     ["symbol", "temporality", "time_period_start"]),
    ("rsi MERGE", rsi._MERGE_SQL,
     ["symbol", "temporality", "rsi_period", "time_period_start"]),
    ("signals MERGE", signals._MERGE_SQL,
     ["symbol", "temporality", "signal_start", "strategy_id"]),
    # T-24: strategy_id=3 gold stages (upsert-only; strategy_id in ON clause
    # is the structural guard that prevents cross-strategy MERGE collisions).
    ("tsmom signal MERGE", tsmom_signal_stage._MERGE_SQL,
     ["symbol", "temporality", "signal_start", "strategy_id"]),
    ("portfolio weights MERGE", portfolio_weights_stage._MERGE_SQL,
     ["week_start", "strategy_id", "symbol", "include_crypto"]),
]

_ALL_SQL_TEMPLATES = [(name, sql) for name, sql, _ in _MERGE_CONTRACTS]


class TestConformMultiSourceRead:
    """The bronze read consolidates every source; highest priority wins."""

    @pytest.fixture
    def rendered(self):
        return conform._build_read_query(
            "trade-390514", date(2017, 8, 10), date(2017, 8, 20)
        )

    def test_reads_every_bronze_table(self, rendered):
        for table in conform._BRONZE_TABLES:
            assert table in rendered
        assert rendered.count("UNION ALL") == len(conform._BRONZE_TABLES) - 1

    def test_each_branch_prunes_its_partition(self, rendered):
        # One candle_date range filter per bronze table, so every branch prunes
        # its own partitions instead of scanning the full table.
        assert rendered.count(
            "WHERE candle_date BETWEEN '2017-08-10' AND '2017-08-20'"
        ) == len(conform._BRONZE_TABLES)

    def test_highest_priority_source_wins(self, rendered):
        # Business rule: on date overlap, the source with the highest
        # source_priority.priority is kept (DESC + rn = 1).
        normalised = _norm(rendered)
        assert "ORDER BY p.priority DESC" in normalised
        assert "WHERE rn = 1" in normalised
        assert conform._SOURCE_PRIORITY_TABLE in rendered

    def test_deduplicates_per_symbol_and_date(self, rendered):
        # Multi-asset: nine symbols coexist (BTC + eight ETFs), so the dedup key is
        # (symbol, candle_date), not the date alone. BTC's two raw exchange symbols
        # both canonicalise to BTCUSD in SQL, so they still collapse correctly.
        assert "PARTITION BY b.symbol, b.candle_date" in _norm(rendered)

    def test_btc_branches_canonicalise_symbol(self, rendered):
        # BTC tables are single-symbol per exchange → a literal 'BTCUSD' so the
        # dedup key lines up with the canonical silver symbol.
        assert "'BTCUSD' AS symbol" in rendered

    def test_tiingo_branch_split_adjusts_to_yahoo_basis(self, rendered):
        # Tiingo's raw close is divided by the product of split factors whose
        # ex-date is AFTER the bar, reconciling it to Yahoo's split-adjusted basis
        # (T-21). The split lookup is unbounded (s.candle_date > tt.candle_date),
        # so a chunk ending before a split is still adjusted.
        normalised = _norm(rendered)
        assert "price_close / f AS price_close" in normalised
        assert "SUM(LN(s.split_factor))" in normalised
        assert "s.candle_date > tt.candle_date" in normalised


class TestSignalsReadFilters:
    def test_signals_reads_exclude_warm_up_rows(self):
        # Warm-up rows carry rsi = NULL; reading them would crash the trend
        # walk-forward (float(None)) or emit bogus signals.
        assert "rsi IS NOT NULL" in signals._READ_WEEKLY_RSI
        assert "rsi IS NOT NULL" in signals._READ_DAILY_RSI


class TestNoDeleteBySource:
    """Guard that no MERGE has a WHEN NOT MATCHED BY SOURCE branch.

    Such a branch would delete rows in the target that are absent from the
    current staging run — for a shared table like fact_signals (strategy_id=1
    RSI + strategy_id=3 TSMOM), that would silently wipe the other strategy's
    rows whenever one strategy is re-run alone.
    """

    @pytest.mark.parametrize("name, sql, _", _MERGE_CONTRACTS,
                             ids=[c[0] for c in _MERGE_CONTRACTS])
    def test_no_when_not_matched_by_source(self, name, sql, _):
        assert "NOT MATCHED BY SOURCE" not in sql, (
            f"{name}: WHEN NOT MATCHED BY SOURCE found — would delete rows "
            "absent from the staging run (unsafe for cross-strategy tables)"
        )


class TestMergeNaturalKeys:
    @pytest.mark.parametrize("name, sql, key_columns", _MERGE_CONTRACTS,
                             ids=[c[0] for c in _MERGE_CONTRACTS])
    def test_merge_joins_on_full_natural_key(self, name, sql, key_columns):
        normalised = _norm(sql)
        for column in key_columns:
            assert f"T.{column} = S.{column}" in normalised, (
                f"{name} is missing natural-key condition on {column!r}"
            )

    @pytest.mark.parametrize("name, sql, key_columns", _MERGE_CONTRACTS,
                             ids=[c[0] for c in _MERGE_CONTRACTS])
    def test_merge_handles_both_match_branches(self, name, sql, key_columns):
        # An upsert needs UPDATE (replay) and INSERT (new rows); losing either
        # breaks idempotency or back-filling.
        assert "WHEN MATCHED THEN UPDATE" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    @pytest.mark.parametrize("name, sql, key_columns", _MERGE_CONTRACTS,
                             ids=[c[0] for c in _MERGE_CONTRACTS])
    def test_natural_key_is_never_updated(self, name, sql, key_columns):
        # Updating a key column inside UPDATE SET would let a replay rewrite
        # identity instead of content.
        update_set = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        for column in key_columns:
            assert column not in assignments, (
                f"{name} must not update natural-key column {column!r}"
            )
