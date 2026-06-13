"""Unit tests for the MVRV Z-Score ingest (``airflow/ingest/bitcoin_data_mvrv_ingest.py``).

Covers the pure logic — CSV parsing, NaN handling, the one-off value override
(2026-06-03 -> 0.45), row building, MERGE chunking against a fake BigQuery
client — plus contract guards on the MERGE SQL (natural key ``mvrvz_date``) and
the bronze DDL (partition + source registration). No live BigQuery or network.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from airflow.ingest import bitcoin_data_mvrv_ingest
from airflow.ingest.bitcoin_data_mvrv_ingest import (
    _MERGE_SQL,
    _alert_missing,
    _apply_corrections,
    _build_row,
    _prepare_daily_rows,
    _upsert_rows,
    parse_mvrv_csv,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# parse_mvrv_csv — the CSV export shape
# ---------------------------------------------------------------------------

class TestParseCsv:
    CSV = (
        "d,unixTs,mvrvZscore\n"
        "2009-01-03,1230940800,0.0\n"
        "2010-07-19,1279497600,32.9641\n"
        "2026-06-03,1780444800,NaN\n"
        "2026-06-10,1781049600,0.2905\n"
    )

    def test_skips_header_and_parses_all_rows(self):
        records = parse_mvrv_csv(self.CSV)
        assert [r["d"] for r in records] == [
            "2009-01-03", "2010-07-19", "2026-06-03", "2026-06-10",
        ]

    def test_unix_ts_is_int(self):
        records = parse_mvrv_csv(self.CSV)
        assert records[0]["unixTs"] == 1230940800
        assert all(isinstance(r["unixTs"], int) for r in records)

    def test_zero_is_kept_not_nulled(self):
        # Early pre-market dates ship a real 0.0; bronze keeps it as delivered.
        records = parse_mvrv_csv(self.CSV)
        assert records[0]["mvrvZscore"] == 0.0

    def test_nan_becomes_none(self):
        records = parse_mvrv_csv(self.CSV)
        by_date = {r["d"]: r["mvrvZscore"] for r in records}
        assert by_date["2026-06-03"] is None

    def test_real_value_parsed_as_float(self):
        records = parse_mvrv_csv(self.CSV)
        by_date = {r["d"]: r["mvrvZscore"] for r in records}
        assert by_date["2026-06-10"] == pytest.approx(0.2905)

    def test_handles_missing_header(self):
        records = parse_mvrv_csv("2026-06-10,1781049600,0.2905\n")
        assert len(records) == 1 and records[0]["d"] == "2026-06-10"

    def test_blank_lines_are_ignored(self):
        records = parse_mvrv_csv("d,unixTs,mvrvZscore\n\n2026-06-10,1781049600,0.5\n\n")
        assert len(records) == 1


# ---------------------------------------------------------------------------
# _build_row — mapping + the known-value override
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)

    def test_maps_source_fields_to_columns(self):
        row = _build_row(
            {"d": "2026-06-10", "unixTs": 1781049600, "mvrvZscore": 0.2905},
            fetched_at=self.FETCHED,
        )
        assert row["mvrvz_date"] == date(2026, 6, 10)
        assert row["unix_ts"] == 1781049600
        assert row["mvrv_zscore"] == pytest.approx(0.2905)
        assert row["source_id"] == bitcoin_data_mvrv_ingest.SOURCE_ID
        assert row["datetime_update"] == self.FETCHED

    def test_missing_value_stays_none_no_fabrication(self):
        # _build_row never fills: a NaN/None stays None, even for a corrected date.
        # Filling is a back-fill-only step (_apply_corrections), not part of build.
        row = _build_row(
            {"d": "2026-06-03", "unixTs": 1780444800, "mvrvZscore": None},
            fetched_at=self.FETCHED,
        )
        assert row["mvrv_zscore"] is None

    def test_accepts_date_object_for_d(self):
        row = _build_row(
            {"d": date(2026, 6, 10), "unixTs": 1, "mvrvZscore": 1.0}, fetched_at=self.FETCHED
        )
        assert row["mvrvz_date"] == date(2026, 6, 10)


# ---------------------------------------------------------------------------
# _apply_corrections — one-off historical fills (back-fill only)
# ---------------------------------------------------------------------------

def _row(d: date, value):
    return {
        "mvrvz_date": d, "unix_ts": 1, "mvrv_zscore": value,
        "source_id": 5, "datetime_update": datetime(2026, 6, 11, tzinfo=timezone.utc),
    }


class TestApplyCorrections:
    def test_known_correction_is_registered(self):
        assert bitcoin_data_mvrv_ingest.HISTORICAL_CORRECTIONS["2026-06-03"] == 0.45

    def test_fills_only_the_missing_corrected_date(self):
        rows = [_row(date(2026, 6, 3), None), _row(date(2026, 6, 4), 0.36)]
        applied = _apply_corrections(rows, {"2026-06-03": 0.45})
        assert applied == ["2026-06-03"]
        assert rows[0]["mvrv_zscore"] == pytest.approx(0.45)
        assert rows[1]["mvrv_zscore"] == pytest.approx(0.36)  # untouched

    def test_does_not_clobber_a_present_value(self):
        # If the source already has a real value for a "corrected" date, keep it.
        rows = [_row(date(2026, 6, 3), 0.61)]
        applied = _apply_corrections(rows, {"2026-06-03": 0.45})
        assert applied == []
        assert rows[0]["mvrv_zscore"] == pytest.approx(0.61)

    def test_missing_date_without_correction_stays_none(self):
        rows = [_row(date(2025, 1, 1), None)]
        applied = _apply_corrections(rows, {"2026-06-03": 0.45})
        assert applied == []
        assert rows[0]["mvrv_zscore"] is None


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_daily_rows — the daily "alert, never fabricate" path
# ---------------------------------------------------------------------------

class TestAlertMissing:
    def test_returns_missing_dates_and_warns(self, caplog):
        rows = [_row(date(2026, 6, 3), None), _row(date(2026, 6, 4), 0.36)]
        with caplog.at_level("WARNING"):
            missing = _alert_missing(rows, "daily")
        assert missing == [date(2026, 6, 3)]
        assert any("missing value for 2026-06-03" in r.message for r in caplog.records)

    def test_no_missing_no_warning(self, caplog):
        rows = [_row(date(2026, 6, 4), 0.36)]
        with caplog.at_level("WARNING"):
            assert _alert_missing(rows, "daily") == []
        assert caplog.records == []


class TestPrepareDailyRows:
    FETCHED = datetime(2026, 6, 11, tzinfo=timezone.utc)

    def test_drops_missing_and_keeps_real_values(self, caplog):
        records = [
            {"d": "2026-06-03", "unixTs": 1, "mvrvZscore": None},
            {"d": "2026-06-04", "unixTs": 2, "mvrvZscore": 0.36},
        ]
        with caplog.at_level("WARNING"):
            rows = _prepare_daily_rows(records, self.FETCHED)
        # The missing row is alerted and skipped; only the real value is written.
        assert [r["mvrvz_date"] for r in rows] == [date(2026, 6, 4)]
        assert any("missing value for 2026-06-03" in r.message for r in caplog.records)

    def test_does_not_apply_historical_corrections(self):
        # Even for the corrected date, the daily path must NOT fabricate 0.45.
        records = [{"d": "2026-06-03", "unixTs": 1, "mvrvZscore": None}]
        rows = _prepare_daily_rows(records, self.FETCHED)
        assert rows == []


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the natural key mvrvz_date
# ---------------------------------------------------------------------------

class TestMergeContract:
    KEY = "mvrvz_date"

    def test_joins_on_natural_key(self):
        assert f"T.{self.KEY} = S.{self.KEY}" in _norm(_MERGE_SQL)

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_does_not_update_the_natural_key(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert self.KEY not in assignments

    def test_dedupes_source_by_mvrvz_date(self):
        # MERGE must never see the same key twice in the staged batch.
        assert "PARTITION BY s.mvrvz_date" in _norm(_MERGE_SQL)
        assert "WHERE rn = 1" in _norm(_MERGE_SQL)


# ---------------------------------------------------------------------------
# _upsert_rows — chunking to respect the 4000-partition-per-DML limit
# ---------------------------------------------------------------------------

class _FakeJob:
    def result(self):
        return []


class _FakeClient:
    """Records the MERGE statements issued; no real BigQuery."""

    def __init__(self):
        self.queries: list[str] = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        return _FakeJob()


def _rows(n: int) -> list[dict]:
    """n bronze rows with distinct consecutive mvrvz_dates (one partition each)."""
    fetched = datetime(2026, 6, 11, tzinfo=timezone.utc)
    base = date(2010, 1, 1).toordinal()
    return [
        {
            "mvrvz_date": date.fromordinal(base + i),
            "unix_ts": i,
            "mvrv_zscore": 1.0,
            "source_id": 5,
            "datetime_update": fetched,
        }
        for i in range(n)
    ]


class TestUpsertChunking:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        _upsert_rows(client, [])
        assert client.queries == []

    def test_single_chunk_when_under_limit(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(10), chunk_size=3500)
        assert len(client.queries) == 1

    def test_splits_into_ceil_chunks(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(8), chunk_size=3)  # 3 + 3 + 2
        assert len(client.queries) == 3

    def test_full_history_size_is_chunked(self):
        # ~6400 daily dates must not go in a single >4000-partition MERGE.
        client = _FakeClient()
        _upsert_rows(client, _rows(6400), chunk_size=3500)
        assert len(client.queries) == 2

    def test_every_query_is_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(5), chunk_size=2)
        assert all("MERGE" in q and "mvrvz_date" in q for q in client.queries)

    def test_handles_null_value_rows(self):
        # A chunk containing a NULL mvrv (unfilled NaN) must still upsert.
        client = _FakeClient()
        rows = _rows(2)
        rows[0]["mvrv_zscore"] = None
        rows[0]["unix_ts"] = None
        _upsert_rows(client, rows, chunk_size=3500)
        assert len(client.queries) == 1


# ---------------------------------------------------------------------------
# DDL contract — the bronze table and source registration exist as expected
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    def test_table_is_declared(self, ddl):
        assert "prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw" in ddl

    def test_partitioned_by_mvrvz_date(self, ddl):
        block = ddl.split("bitcoin_data_mvrv_zscore_daily_raw", 1)[1]
        assert "PARTITION BY mvrvz_date" in block

    def test_natural_key_is_mvrvz_date(self, ddl):
        block = ddl.split("bitcoin_data_mvrv_zscore_daily_raw", 1)[1]
        assert "PRIMARY KEY (mvrvz_date) NOT ENFORCED" in block

    def test_source_5_registered(self, ddl):
        assert "SELECT 5 AS source_id" in ddl
        assert "BGeometrics" in ddl


# ---------------------------------------------------------------------------
# Training views — daily (same-date) and weekly (Sunday MVRV vs Monday RSI)
# ---------------------------------------------------------------------------

class TestTrainingViews:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _norm(_DDL_PATH.read_text(encoding="utf-8"))

    @pytest.fixture(scope="class")
    def daily(self, ddl):
        return ddl.split("vw_btc_training_daily", 1)[1].split("vw_btc_training_weekly")[0]

    @pytest.fixture(scope="class")
    def weekly(self, ddl):
        return ddl.split("vw_btc_training_weekly", 1)[1]

    def test_both_views_declared(self, ddl):
        assert "CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_daily`" in ddl
        assert "CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_weekly`" in ddl

    def test_daily_joins_with_one_day_lag_shift(self, daily):
        # MVRV publishes day D-1 under mvrvz_date D, so the real value of trading
        # day D lives under mvrvz_date = D + 1 -> the join shifts +1 day.
        assert "m.mvrvz_date = DATE_ADD(DATE(r.time_period_start), INTERVAL 1 DAY)" in daily

    def test_daily_filters_and_columns(self, daily):
        assert "r.temporality = '1d'" in daily
        assert "r.rsi IS NOT NULL" in daily
        for col in ("r.price_close", "r.rsi", "m.mvrv_zscore"):
            assert col in daily

    def test_weekly_crosses_sunday_mvrv_vs_monday_rsi_with_lag(self, weekly):
        # The week's Sunday is Monday + 6; with the 1-day MVRV lag the Sunday
        # value lives under mvrvz_date = Monday + 7, so the join offset is +7.
        assert "m.mvrvz_date = DATE_ADD(DATE(r.time_period_start), INTERVAL 7 DAY)" in weekly

    def test_weekly_exposes_monday_and_sunday(self, weekly):
        assert "AS week_start_monday" in weekly
        assert "DATE_ADD(DATE(r.time_period_start), INTERVAL 6 DAY) AS week_end_sunday" in weekly

    def test_weekly_filters_and_columns(self, weekly):
        assert "r.temporality = '1w'" in weekly
        assert "r.rsi IS NOT NULL" in weekly
        for col in ("r.price_close", "r.rsi", "m.mvrv_zscore"):
            assert col in weekly

    def test_views_use_monday_weeks_only_via_offset(self, weekly):
        # The weekly view must NOT re-truncate weeks itself; it relies on
        # rsi_features already being WEEK(MONDAY)-aligned and just offsets +6 days.
        assert "WEEK(" not in weekly
