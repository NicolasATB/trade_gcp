"""Unit tests for the shared FRED ingest logic (``orchestration/ingest/fred_common.py``)
and its thin plain-series entry-points (``fred_10y_ingest``, ``fred_fedfunds_ingest``,
``fred_2y_ingest``).

Covers the pure logic — observations parsing, ``.`` -> NULL handling, row
building (parameterised by source id), the alert-and-drop missing policy, MERGE
chunking against a fake BigQuery client — plus contract guards on the MERGE SQL
(natural key ``obs_date``), the bronze DDL (both DGS10/DFF tables + source
registration), and that each entry-point pins the right ``FredSeries`` config.
No live BigQuery or network.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from orchestration.ingest import (
    fred_2y_ingest,
    fred_10y_ingest,
    fred_fedfunds_ingest,
    fred_vix_ingest,
)
from orchestration.ingest.fred_common import (
    _MERGE_SQL,
    FredSeries,
    _alert_missing,
    _build_row,
    _parse_value,
    _prepare_rows,
    _upsert_rows,
    parse_observations,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"

# A representative plain-series config for the parameterised logic tests.
CFG = FredSeries(series_id="DGS10", table="fred_dgs10_daily_raw", source_id=8)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# _parse_value — FRED's "." missing sentinel
# ---------------------------------------------------------------------------

class TestParseValue:
    def test_number_parsed(self):
        assert _parse_value("4.47") == pytest.approx(4.47)

    def test_dot_is_none(self):
        assert _parse_value(".") is None

    def test_empty_is_none(self):
        assert _parse_value("") is None
        assert _parse_value("  ") is None

    def test_nan_is_none(self):
        assert _parse_value("NaN") is None

    def test_none_is_none(self):
        assert _parse_value(None) is None

    def test_zero_is_kept(self):
        assert _parse_value("0") == 0.0


# ---------------------------------------------------------------------------
# parse_observations — the FRED observations payload shape (verified live)
# ---------------------------------------------------------------------------

class TestParseObservations:
    PAYLOAD = {
        "observations": [
            {"realtime_start": "2026-06-13", "realtime_end": "2026-06-13", "date": "2026-06-01", "value": "4.47"},
            {"realtime_start": "2026-06-13", "realtime_end": "2026-06-13", "date": "2026-06-02", "value": "."},
            {"realtime_start": "2026-06-13", "realtime_end": "2026-06-13", "date": "2026-06-03", "value": "4.49"},
        ]
    }

    def test_maps_date_and_value(self):
        records = parse_observations(self.PAYLOAD)
        assert [r["date"] for r in records] == ["2026-06-01", "2026-06-02", "2026-06-03"]

    def test_dot_value_becomes_none(self):
        records = parse_observations(self.PAYLOAD)
        assert records[1]["value"] is None

    def test_real_value_is_float(self):
        records = parse_observations(self.PAYLOAD)
        assert records[0]["value"] == pytest.approx(4.47)

    def test_empty_payload(self):
        assert parse_observations({}) == []
        assert parse_observations({"observations": []}) == []


# ---------------------------------------------------------------------------
# _build_row — pure mapping to the bronze columns (source id is a parameter)
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 13, 12, 0, tzinfo=timezone.utc)

    def test_maps_fields_to_columns(self):
        row = _build_row({"date": "2026-06-01", "value": 4.47}, source_id=8, fetched_at=self.FETCHED)
        assert row["obs_date"] == date(2026, 6, 1)
        assert row["obs_value"] == pytest.approx(4.47)
        assert row["source_id"] == 8
        assert row["datetime_update"] == self.FETCHED

    def test_missing_value_stays_none(self):
        row = _build_row({"date": "2026-06-02", "value": None}, source_id=9, fetched_at=self.FETCHED)
        assert row["obs_value"] is None
        assert row["source_id"] == 9


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_rows — alert-and-drop missing observations
# ---------------------------------------------------------------------------

def _row(d: date, value):
    return {
        "obs_date": d, "obs_value": value,
        "source_id": 8, "datetime_update": datetime(2026, 6, 13, tzinfo=timezone.utc),
    }


class TestAlertMissing:
    def test_returns_missing_and_warns(self, caplog):
        rows = [_row(date(2026, 6, 2), None), _row(date(2026, 6, 1), 4.47)]
        with caplog.at_level("WARNING"):
            missing = _alert_missing(rows, "DGS10", "daily")
        assert missing == [date(2026, 6, 2)]
        assert any("missing value for 2026-06-02" in r.message for r in caplog.records)

    def test_no_missing_no_warning(self, caplog):
        with caplog.at_level("WARNING"):
            assert _alert_missing([_row(date(2026, 6, 1), 4.47)], "DGS10", "daily") == []
        assert caplog.records == []


class TestPrepareRows:
    FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)

    def test_drops_missing_keeps_real(self, caplog):
        records = [{"date": "2026-06-02", "value": None}, {"date": "2026-06-03", "value": 4.49}]
        with caplog.at_level("WARNING"):
            rows = _prepare_rows(records, CFG, self.FETCHED, "back-fill")
        assert [r["obs_date"] for r in rows] == [date(2026, 6, 3)]
        assert all(r["source_id"] == 8 for r in rows)
        assert any("missing value for 2026-06-02" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the natural key obs_date
# ---------------------------------------------------------------------------

class TestMergeContract:
    KEY = "obs_date"

    def test_joins_on_natural_key(self):
        assert f"T.{self.KEY} = S.{self.KEY}" in _norm(_MERGE_SQL)

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_does_not_update_the_natural_key(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert self.KEY not in assignments

    def test_dedupes_source_by_key(self):
        assert "PARTITION BY s.obs_date" in _norm(_MERGE_SQL)
        assert "WHERE rn = 1" in _norm(_MERGE_SQL)


# ---------------------------------------------------------------------------
# _upsert_rows — chunking to respect the 4000-partition-per-DML limit
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
    fetched = datetime(2026, 6, 13, tzinfo=timezone.utc)
    base = date(1962, 1, 2).toordinal()
    return [
        {"obs_date": date.fromordinal(base + i), "obs_value": 1.0,
         "source_id": 8, "datetime_update": fetched}
        for i in range(n)
    ]


class TestUpsertChunking:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        _upsert_rows(client, "fred_dgs10_daily_raw", [])
        assert client.queries == []

    def test_single_chunk_when_under_limit(self):
        client = _FakeClient()
        _upsert_rows(client, "fred_dgs10_daily_raw", _rows(10), chunk_size=3500)
        assert len(client.queries) == 1

    def test_splits_into_ceil_chunks(self):
        client = _FakeClient()
        _upsert_rows(client, "fred_dgs10_daily_raw", _rows(8), chunk_size=3)  # 3 + 3 + 2
        assert len(client.queries) == 3

    def test_long_daily_history_is_chunked(self):
        # DGS10 since 1962 (~16k business days) must not go in one >4000 MERGE.
        client = _FakeClient()
        _upsert_rows(client, "fred_dgs10_daily_raw", _rows(16000), chunk_size=3500)
        assert len(client.queries) == 5

    def test_table_is_in_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, "fred_dff_daily_raw", _rows(5), chunk_size=2)
        assert all("MERGE" in q and "fred_dff_daily_raw" in q and "obs_date" in q
                   for q in client.queries)


# ---------------------------------------------------------------------------
# Entry-points — each pins the right FredSeries config
# ---------------------------------------------------------------------------

class TestEntryPointConfig:
    def test_10y_is_dgs10_source_8(self):
        s = fred_10y_ingest.SERIES
        assert (s.series_id, s.table, s.source_id) == ("DGS10", "fred_dgs10_daily_raw", 8)

    def test_fedfunds_is_dff_source_9(self):
        s = fred_fedfunds_ingest.SERIES
        assert (s.series_id, s.table, s.source_id) == ("DFF", "fred_dff_daily_raw", 9)

    def test_2y_is_dgs2_source_10(self):
        s = fred_2y_ingest.SERIES
        assert (s.series_id, s.table, s.source_id) == ("DGS2", "fred_dgs2_daily_raw", 10)

    def test_vix_is_vixcls_source_11(self):
        s = fred_vix_ingest.SERIES
        assert (s.series_id, s.table, s.source_id) == ("VIXCLS", "fred_vixcls_daily_raw", 11)

    def test_entry_points_expose_bound_functions(self):
        for mod in (fred_10y_ingest, fred_fedfunds_ingest, fred_2y_ingest, fred_vix_ingest):
            assert callable(mod.backfill_history)
            assert callable(mod.ingest_range)
            assert callable(mod.ingest_latest)
            assert callable(mod.main)


# ---------------------------------------------------------------------------
# DDL contract — both plain-FRED tables + source registration exist
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    @pytest.mark.parametrize(
        "table",
        ["fred_dgs10_daily_raw", "fred_dff_daily_raw", "fred_dgs2_daily_raw",
         "fred_vixcls_daily_raw"],
    )
    def test_table_declared_and_partitioned(self, ddl, table):
        assert f"prod_trade_bronze.{table}" in ddl
        block = ddl.split(table, 1)[1]
        # Monthly granularity: DGS10 (1962) / DFF (1954) / DGS2 (1976) / VIXCLS
        # (1990) — long daily series partitioned by month for consistency.
        assert "PARTITION BY DATE_TRUNC(obs_date, MONTH)" in block
        assert "PRIMARY KEY (obs_date) NOT ENFORCED" in block

    def test_sources_8_through_11_registered(self, ddl):
        assert "SELECT 8 AS source_id" in ddl
        assert "SELECT 9 AS source_id" in ddl
        assert "SELECT 10 AS source_id" in ddl
        assert "SELECT 11 AS source_id" in ddl
        assert "DGS10" in ddl and "DFF" in ddl and "DGS2" in ddl and "VIXCLS" in ddl
