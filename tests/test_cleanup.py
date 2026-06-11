"""Unit tests for the cleanup helpers in ``dataflow/cleanup.py``.

Covers the ``all`` → ``None`` resolution, the WHERE-clause/param builder, the
per-layer target selection, and ``clean_tables`` orchestration against a fake
BigQuery client (dry-run counting and delete). No live BigQuery.
"""

from __future__ import annotations

from datetime import date

import pytest

from dataflow import cleanup
from dataflow.cleanup import (
    _filter_clause,
    _resolve,
    _targets_for_layer,
    clean_tables,
)


# ---------------------------------------------------------------------------
# _resolve — the explicit `all` contract
# ---------------------------------------------------------------------------

class TestResolve:
    def test_literal_all_becomes_none(self):
        assert _resolve("all", "symbol") is None

    def test_all_is_case_insensitive(self):
        assert _resolve("ALL", "symbol") is None

    def test_missing_value_is_rejected(self):
        with pytest.raises(ValueError, match="symbol is required"):
            _resolve(None, "symbol")

    def test_concrete_value_passes_through(self):
        assert _resolve("BTCUSD", "symbol") == "BTCUSD"
        d = date(2024, 1, 1)
        assert _resolve(d, "start_date") == d


# ---------------------------------------------------------------------------
# _filter_clause — WHERE string + parameters
# ---------------------------------------------------------------------------

class TestFilterClause:
    def test_all_none_yields_true_and_no_params(self):
        where, params = _filter_clause("time_period_start", None, None, None, None)
        assert where == "TRUE"
        assert params == []

    def test_builds_all_conditions_and_params(self):
        where, params = _filter_clause(
            "signal_start", "BTCUSD", "1d", date(2024, 1, 1), date(2024, 1, 31)
        )
        assert where == (
            "symbol = @symbol AND temporality = @temporality "
            "AND DATE(signal_start) >= @start_date AND DATE(signal_start) <= @end_date"
        )
        by_name = {p.name: p.value for p in params}
        assert by_name == {
            "symbol": "BTCUSD",
            "temporality": "1d",
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 1, 31),
        }

    def test_partial_filter_only_symbol(self):
        where, params = _filter_clause("time_period_start", "BTCUSD", None, None, None)
        assert where == "symbol = @symbol"
        assert [p.name for p in params] == ["symbol"]

    def test_date_column_is_interpolated(self):
        where, _ = _filter_clause("time_period_start", None, None, date(2024, 1, 1), None)
        assert "DATE(time_period_start) >= @start_date" in where


# ---------------------------------------------------------------------------
# _targets_for_layer
# ---------------------------------------------------------------------------

class TestTargetsForLayer:
    def test_silver_has_two_tables(self):
        targets = _targets_for_layer("silver")
        assert [t for t, _ in targets] == [
            "prod_trade_silver.ohlcv_validated",
            "prod_trade_silver.rsi_features",
        ]

    def test_gold_has_one_table_with_signal_start(self):
        assert _targets_for_layer("gold") == [("prod_trade_gold.fact_signals", "signal_start")]

    def test_all_concatenates_silver_and_gold(self):
        assert len(_targets_for_layer("all")) == 3

    def test_unknown_layer_raises(self):
        with pytest.raises(ValueError, match="Unknown layer"):
            _targets_for_layer("platinum")


# ---------------------------------------------------------------------------
# clean_tables — orchestration against a fake BigQuery client
# ---------------------------------------------------------------------------

class _FakeJob:
    def __init__(self, rows=None, affected=0):
        self._rows = rows if rows is not None else []
        self.num_dml_affected_rows = affected

    def result(self):
        return self._rows


class _FakeClient:
    """Minimal stand-in for ``bigquery.Client`` recording the queries issued."""

    def __init__(self, count_rows=0, affected=0):
        self._count_rows = count_rows
        self._affected = affected
        self.queries: list[str] = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        return _FakeJob(rows=[{"n": self._count_rows}], affected=self._affected)


class TestCleanTables:
    def test_dry_run_counts_without_deleting(self):
        client = _FakeClient(count_rows=5)
        result = clean_tables(
            symbol="BTCUSD", temporality="1d",
            start_date="all", end_date="all",
            layer="gold", client=client, dry_run=True,
        )
        assert result == {"trade-390514.prod_trade_gold.fact_signals": 5}
        assert client.queries and client.queries[0].startswith("SELECT COUNT(*)")

    def test_delete_returns_affected_rows_per_table(self):
        client = _FakeClient(affected=3)
        result = clean_tables(
            symbol="all", temporality="all",
            start_date="all", end_date="all",
            layer="silver", client=client, dry_run=False,
        )
        assert result == {
            "trade-390514.prod_trade_silver.ohlcv_validated": 3,
            "trade-390514.prod_trade_silver.rsi_features": 3,
        }
        assert all(q.startswith("DELETE FROM") for q in client.queries)

    def test_end_before_start_raises(self):
        client = _FakeClient()
        with pytest.raises(ValueError, match="before start_date"):
            clean_tables(
                symbol="BTCUSD", temporality="1d",
                start_date=date(2024, 2, 1), end_date=date(2024, 1, 1),
                client=client,
            )

    def test_bad_layer_fails_before_any_io(self):
        client = _FakeClient()
        with pytest.raises(ValueError, match="Unknown layer"):
            clean_tables(
                symbol="BTCUSD", temporality="1d",
                start_date="all", end_date="all",
                layer="bogus", client=client,
            )
        assert client.queries == []  # failed fast, no queries issued

    def test_all_filters_warns(self, caplog):
        client = _FakeClient(count_rows=0)
        with caplog.at_level("WARNING"):
            clean_tables(
                symbol="all", temporality="all",
                start_date="all", end_date="all",
                layer="gold", client=client, dry_run=True,
            )
        assert any("EVERY row" in r.message for r in caplog.records)


def test_module_exposes_default_project():
    assert cleanup.PROJECT_ID  # resolved from env or the trade-390514 default
