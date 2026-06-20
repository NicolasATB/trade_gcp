"""Unit tests for the Google Trends BTC investor-attention ingest
(``orchestration/ingest/google_trends_btc_ingest.py``).

Bronze stores Google's value RAW, per request window (no stitching here). These
tests cover the pure logic — interest parsing, dropping the still-forming
(``is_partial``) week, per-window row mapping (no fabrication), alert-and-skip,
window splitting, MERGE chunking against a fake BigQuery client — plus contract
guards on the MERGE SQL (composite key ``(search_term, window_start, trend_date)``),
the bronze DDL (raw per-window schema, source 15), the SILVER stitching view
``vw_google_trends_btc_weekly`` and the daily view's ``investor_attention`` (which now
reads the silver view). The stitching itself is a SQL transform in silver, so it is
validated as a DDL contract, not as Python. No live network or BigQuery.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from orchestration.ingest import google_trends_btc_ingest as gt
from orchestration.ingest.google_trends_btc_ingest import (
    _MERGE_SQL,
    _alert_missing,
    _build_row,
    _parse_value,
    _prepare_rows,
    _upsert_rows,
    _window_bounds,
    parse_interest_over_time,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# _parse_value — Google delivers 0-100 integers
# ---------------------------------------------------------------------------

class TestParseValue:
    def test_integer_parsed(self):
        assert _parse_value("57") == 57 and isinstance(_parse_value("57"), int)

    def test_float_string_rounds_to_int(self):
        assert _parse_value("94.0") == 94

    def test_empty_nan_none(self):
        assert _parse_value("") is None
        assert _parse_value("NaN") is None
        assert _parse_value(None) is None

    def test_zero_is_kept(self):
        assert _parse_value("0") == 0


# ---------------------------------------------------------------------------
# parse_interest_over_time — weekly rows, partial week dropped
# ---------------------------------------------------------------------------

class TestParseInterestOverTime:
    ROWS = [
        {"date": "2026-05-31", "interest": 40, "is_partial": False},
        {"date": "2026-06-07", "interest": 55, "is_partial": False},
        {"date": "2026-06-14", "interest": 61, "is_partial": True},   # still-forming week
    ]

    def test_drops_partial_week(self):
        assert [r["d"] for r in parse_interest_over_time(self.ROWS)] == ["2026-05-31", "2026-06-07"]

    def test_maps_interest_value(self):
        assert parse_interest_over_time(self.ROWS)[1]["interest"] == 55

    def test_missing_interest_becomes_none(self):
        out = parse_interest_over_time([{"date": "2026-05-31", "interest": "", "is_partial": False}])
        assert out[0]["interest"] is None

    def test_empty_input(self):
        assert parse_interest_over_time([]) == []


# ---------------------------------------------------------------------------
# _window_bounds — overlapping <5-year request windows
# ---------------------------------------------------------------------------

class TestWindowBounds:
    def test_single_window_when_span_fits(self):
        assert _window_bounds(date(2022, 1, 1), date(2024, 1, 1), 1460, 365) == [
            (date(2022, 1, 1), date(2024, 1, 1))
        ]

    def test_windows_overlap_and_cover_full_span(self):
        start, end = date(2011, 1, 1), date(2026, 6, 17)
        bounds = _window_bounds(start, end, 1460, 365)
        assert bounds[0][0] == start and bounds[-1][1] == end
        for (s0, e0), (s1, _e1) in zip(bounds, bounds[1:]):
            assert s1 < e0           # consecutive windows overlap
            assert e0 > s0

    def test_degenerate_range(self):
        assert _window_bounds(date(2026, 1, 1), date(2026, 1, 1)) == [(date(2026, 1, 1), date(2026, 1, 1))]


# ---------------------------------------------------------------------------
# _build_row — per-window raw mapping (term, source, window bounds)
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    WS, WE = date(2014, 1, 1), date(2018, 1, 1)

    def test_maps_fields_to_columns(self):
        row = _build_row({"d": "2017-12-17", "interest": 100}, self.WS, self.WE, fetched_at=self.FETCHED)
        assert row["trend_date"] == date(2017, 12, 17)
        assert row["search_term"] == gt.SEARCH_TERM
        assert row["window_start"] == self.WS
        assert row["window_end"] == self.WE
        assert row["interest_raw"] == 100
        assert row["source_id"] == 15
        assert row["datetime_update"] == self.FETCHED

    def test_missing_value_stays_none_no_fabrication(self):
        row = _build_row({"d": "2017-12-17", "interest": None}, self.WS, self.WE)
        assert row["interest_raw"] is None
        assert row["trend_date"] == date(2017, 12, 17)


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_rows — alert-and-skip missing weeks
# ---------------------------------------------------------------------------

def _row(d: date, value, ws=date(2014, 1, 1), we=date(2018, 1, 1)):
    return {
        "trend_date": d, "search_term": "Bitcoin", "window_start": ws, "window_end": we,
        "interest_raw": value, "source_id": 15, "datetime_update": datetime(2026, 6, 17, tzinfo=timezone.utc),
    }


class TestAlertMissing:
    def test_returns_missing_and_warns(self, caplog):
        rows = [_row(date(2017, 12, 3), None), _row(date(2017, 12, 10), 81)]
        with caplog.at_level("WARNING"):
            missing = _alert_missing(rows, "back-fill")
        assert missing == [date(2017, 12, 3)]
        assert any("missing interest for 2017-12-03" in r.message for r in caplog.records)

    def test_no_missing_no_warning(self, caplog):
        with caplog.at_level("WARNING"):
            assert _alert_missing([_row(date(2017, 12, 10), 81)], "back-fill") == []
        assert caplog.records == []


class TestPrepareRows:
    FETCHED = datetime(2026, 6, 17, tzinfo=timezone.utc)
    WS, WE = date(2014, 1, 1), date(2018, 1, 1)

    def test_drops_missing_keeps_real_and_tags_window(self, caplog):
        records = [{"d": "2017-12-03", "interest": None}, {"d": "2017-12-10", "interest": 81}]
        with caplog.at_level("WARNING"):
            rows = _prepare_rows(records, self.WS, self.WE, self.FETCHED, "back-fill")
        assert [r["trend_date"] for r in rows] == [date(2017, 12, 10)]
        assert rows[0]["window_start"] == self.WS and rows[0]["interest_raw"] == 81
        assert any("missing interest for 2017-12-03" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency on the composite key (search_term, window_start, trend_date)
# ---------------------------------------------------------------------------

class TestMergeContract:
    def test_joins_on_all_three_key_columns(self):
        assert ("T.search_term = S.search_term AND T.window_start = S.window_start "
                "AND T.trend_date = S.trend_date") in _norm(_MERGE_SQL)

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_does_not_update_any_key_column(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        for key in ("search_term", "window_start", "trend_date"):
            assert key not in assignments

    def test_updates_raw_value_and_window_end(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "interest_raw" in update_set and "window_end" in update_set

    def test_dedupes_source_by_composite_key(self):
        assert "PARTITION BY s.search_term, s.window_start, s.trend_date" in _norm(_MERGE_SQL)
        assert "WHERE rn = 1" in _norm(_MERGE_SQL)


# ---------------------------------------------------------------------------
# _upsert_rows — chunking under the 4000-partition-per-DML limit
# ---------------------------------------------------------------------------

class _FakeJob:
    def result(self):
        return []


class _FakeClient:
    def __init__(self):
        self.queries: list[str] = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        return _FakeJob()


def _rows(n: int) -> list[dict]:
    fetched = datetime(2026, 6, 17, tzinfo=timezone.utc)
    base = date(2015, 1, 4).toordinal()
    return [
        {"trend_date": date.fromordinal(base + 7 * i), "search_term": "Bitcoin",
         "window_start": date(2014, 1, 1), "window_end": date(2018, 1, 1),
         "interest_raw": i % 100, "source_id": 15, "datetime_update": fetched}
        for i in range(n)
    ]


class TestUpsertChunking:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        _upsert_rows(client, [])
        assert client.queries == []

    def test_single_chunk_when_under_limit(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(600), chunk_size=3500)
        assert len(client.queries) == 1

    def test_splits_into_ceil_chunks(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(8), chunk_size=3)  # 3 + 3 + 2
        assert len(client.queries) == 3

    def test_every_query_is_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(5), chunk_size=2)
        assert all(
            "MERGE" in q and "google_trends_btc_weekly_raw" in q and "window_start" in q
            for q in client.queries
        )


# ---------------------------------------------------------------------------
# Bronze DDL contract — raw per-window schema + source registration
# ---------------------------------------------------------------------------

class TestBronzeDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    @pytest.fixture(scope="class")
    def block(self, ddl):
        return ddl.split("google_trends_btc_weekly_raw", 1)[1].split("Register Google Trends")[0]

    def test_table_is_declared(self, ddl):
        assert "prod_trade_bronze.google_trends_btc_weekly_raw" in ddl

    def test_partitioned_by_trend_date(self, block):
        assert "PARTITION BY trend_date" in block

    def test_raw_per_window_columns(self, block):
        assert "window_start" in block
        assert "window_end" in block
        assert "interest_raw" in block

    def test_composite_key_includes_window(self, block):
        assert "PRIMARY KEY (search_term, window_start, trend_date) NOT ENFORCED" in block

    def test_source_15_registered(self, ddl):
        assert "SELECT 15 AS source_id" in ddl
        assert "Google Trends" in ddl


# ---------------------------------------------------------------------------
# Silver DDL contract — the stitching view (transform lives downstream of bronze)
# ---------------------------------------------------------------------------

class TestSilverStitchView:
    @pytest.fixture(scope="class")
    def view(self):
        ddl = _norm(_DDL_PATH.read_text(encoding="utf-8"))
        return ddl.split("vw_google_trends_btc_weekly", 1)[1].split("prod_trade_gold (star schema)")[0]

    def test_view_declared_in_silver(self, view):
        assert "CREATE OR REPLACE VIEW" in view or "AS WITH" in view

    def test_reads_raw_bronze(self, view):
        assert "prod_trade_bronze.google_trends_btc_weekly_raw" in view

    def test_rescales_windows_on_overlap(self, view):
        # Pairwise factor = sum(prev.raw)/sum(cur.raw) over the shared weeks.
        assert "SAFE_DIVIDE(SUM(p.interest_raw), SUM(c.interest_raw))" in view

    def test_cumulative_scale_is_running_product_in_log_space(self, view):
        assert "EXP(SUM(LN(COALESCE(pairs.factor, 1.0)))" in view

    def test_keeps_earliest_window_on_overlap(self, view):
        assert "ROW_NUMBER() OVER (PARTITION BY search_term, trend_date ORDER BY window_start)" in view

    def test_renormalises_to_0_100(self, view):
        assert "100 * SAFE_DIVIDE(v, MAX(v) OVER (PARTITION BY search_term))" in view


# ---------------------------------------------------------------------------
# Daily training view — investor_attention now reads the SILVER stitched view
# ---------------------------------------------------------------------------

class TestDailyViewInvestorAttention:
    @pytest.fixture(scope="class")
    def daily(self):
        ddl = _norm(_DDL_PATH.read_text(encoding="utf-8"))
        return ddl.split("vw_btc_training_daily", 1)[1].split("vw_btc_training_weekly")[0]

    def test_exposes_investor_attention(self, daily):
        assert "AS investor_attention" in daily

    def test_reads_from_silver_stitched_view(self, daily):
        assert "prod_trade_silver.vw_google_trends_btc_weekly" in daily

    def test_uses_prior_completed_week_no_lookahead(self, daily):
        assert "DATE_ADD(g.trend_date, INTERVAL 6 DAY) < b.d" in daily

    def test_filters_to_the_search_term(self, daily):
        assert "g.search_term = 'Bitcoin'" in daily
