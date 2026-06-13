"""Unit tests for the M2 point-in-time ingest (``airflow/ingest/fred_m2_ingest.py``).

Covers the vintage-aware logic — ALFRED observations parsing that keeps the
realtime window, ``.`` -> NULL handling, row building (incl. the open
``9999-12-31`` realtime_end), the alert-and-drop missing policy, MERGE chunking
against a fake BigQuery client — plus contract guards on the composite-key MERGE
SQL ``(wm2ns_date, realtime_start)`` and the bronze DDL. No live BigQuery or
network. The sample payloads mirror the live FRED/ALFRED response.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from airflow.ingest import fred_m2_ingest
from airflow.ingest.fred_m2_ingest import (
    _MERGE_SQL,
    _alert_missing,
    _build_row,
    _prepare_rows,
    _upsert_rows,
    parse_vintages,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# parse_vintages — keeps the realtime window (the point-in-time dimension)
# ---------------------------------------------------------------------------

class TestParseVintages:
    # Two vintages for 2026-04-06 (revised), one for 2026-04-13 — as live FRED.
    PAYLOAD = {
        "observations": [
            {"realtime_start": "2026-04-28", "realtime_end": "2026-05-25", "date": "2026-04-06", "value": "23115.2"},
            {"realtime_start": "2026-05-26", "realtime_end": "9999-12-31", "date": "2026-04-06", "value": "23113.9"},
            {"realtime_start": "2026-05-26", "realtime_end": "9999-12-31", "date": "2026-04-13", "value": "."},
        ]
    }

    def test_keeps_all_vintage_rows(self):
        records = parse_vintages(self.PAYLOAD)
        assert len(records) == 3

    def test_preserves_realtime_window(self):
        records = parse_vintages(self.PAYLOAD)
        assert records[0]["realtime_start"] == "2026-04-28"
        assert records[0]["realtime_end"] == "2026-05-25"
        assert records[1]["realtime_end"] == "9999-12-31"

    def test_same_date_two_vintages(self):
        records = parse_vintages(self.PAYLOAD)
        apr06 = [r for r in records if r["date"] == "2026-04-06"]
        assert len(apr06) == 2
        assert apr06[0]["value"] == pytest.approx(23115.2)
        assert apr06[1]["value"] == pytest.approx(23113.9)

    def test_dot_value_becomes_none(self):
        records = parse_vintages(self.PAYLOAD)
        assert records[2]["value"] is None

    def test_empty_payload(self):
        assert parse_vintages({}) == []


# ---------------------------------------------------------------------------
# _build_row — maps the vintage record, incl. the open realtime_end sentinel
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)

    def test_maps_all_fields(self):
        row = _build_row(
            {"date": "2026-04-06", "realtime_start": "2026-04-28",
             "realtime_end": "2026-05-25", "value": 23115.2},
            fetched_at=self.FETCHED,
        )
        assert row["wm2ns_date"] == date(2026, 4, 6)
        assert row["realtime_start"] == date(2026, 4, 28)
        assert row["realtime_end"] == date(2026, 5, 25)
        assert row["m2_value"] == pytest.approx(23115.2)
        assert row["source_id"] == fred_m2_ingest.SOURCE_ID
        assert row["datetime_update"] == self.FETCHED

    def test_open_realtime_end_maps_to_max_date(self):
        row = _build_row(
            {"date": "2026-04-06", "realtime_start": "2026-05-26",
             "realtime_end": "9999-12-31", "value": 23113.9},
            fetched_at=self.FETCHED,
        )
        assert row["realtime_end"] == date(9999, 12, 31)

    def test_missing_value_stays_none(self):
        row = _build_row(
            {"date": "2026-04-13", "realtime_start": "2026-05-26",
             "realtime_end": "9999-12-31", "value": None},
            fetched_at=self.FETCHED,
        )
        assert row["m2_value"] is None


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_rows — alert-and-drop missing vintages
# ---------------------------------------------------------------------------

def _row(d: date, rt: date, value):
    return {
        "wm2ns_date": d, "realtime_start": rt, "realtime_end": date(9999, 12, 31),
        "m2_value": value, "source_id": 7,
        "datetime_update": datetime(2026, 6, 13, tzinfo=timezone.utc),
    }


class TestAlertMissing:
    def test_returns_missing_keys_and_warns(self, caplog):
        rows = [_row(date(2026, 4, 13), date(2026, 5, 26), None),
                _row(date(2026, 4, 6), date(2026, 5, 26), 23113.9)]
        with caplog.at_level("WARNING"):
            missing = _alert_missing(rows, "daily")
        assert missing == [(date(2026, 4, 13), date(2026, 5, 26))]
        assert any("missing value for 2026-04-13" in r.message for r in caplog.records)


class TestPrepareRows:
    FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)

    def test_drops_missing_keeps_real(self, caplog):
        records = [
            {"date": "2026-04-13", "realtime_start": "2026-05-26", "realtime_end": "9999-12-31", "value": None},
            {"date": "2026-04-06", "realtime_start": "2026-05-26", "realtime_end": "9999-12-31", "value": 23113.9},
        ]
        with caplog.at_level("WARNING"):
            rows = _prepare_rows(records, self.FETCHED, "back-fill")
        assert [r["wm2ns_date"] for r in rows] == [date(2026, 4, 6)]


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the composite key (wm2ns_date, realtime_start)
# ---------------------------------------------------------------------------

class TestMergeContract:
    def test_joins_on_composite_key(self):
        norm = _norm(_MERGE_SQL)
        assert "T.wm2ns_date = S.wm2ns_date AND T.realtime_start = S.realtime_start" in norm

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_updates_realtime_end_but_not_the_key(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert "realtime_end" in assignments      # mutable: vintage gets closed off
        assert "m2_value" in assignments
        assert "wm2ns_date" not in assignments     # key never updated
        assert "realtime_start" not in assignments

    def test_dedupes_source_by_composite_key(self):
        assert "PARTITION BY s.wm2ns_date, s.realtime_start" in _norm(_MERGE_SQL)
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
    base = date(2002, 10, 31).toordinal()
    return [
        {"wm2ns_date": date.fromordinal(base + i * 7), "realtime_start": date.fromordinal(base + i * 7),
         "realtime_end": date(9999, 12, 31), "m2_value": 1.0,
         "source_id": 7, "datetime_update": fetched}
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

    def test_every_query_is_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(5), chunk_size=2)
        assert all("MERGE" in q and "wm2ns_date" in q for q in client.queries)

    def test_handles_null_value_rows(self):
        client = _FakeClient()
        rows = _rows(2)
        rows[0]["m2_value"] = None
        _upsert_rows(client, rows, chunk_size=3500)
        assert len(client.queries) == 1


# ---------------------------------------------------------------------------
# DDL contract — the vintage bronze table + source registration exist
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    def test_table_declared(self, ddl):
        assert "prod_trade_bronze.fred_wm2ns_weekly_raw" in ddl

    def test_partitioned_by_wm2ns_date(self, ddl):
        block = ddl.split("fred_wm2ns_weekly_raw", 1)[1]
        assert "PARTITION BY wm2ns_date" in block

    def test_composite_natural_key(self, ddl):
        block = ddl.split("fred_wm2ns_weekly_raw", 1)[1]
        assert "PRIMARY KEY (wm2ns_date, realtime_start) NOT ENFORCED" in block

    def test_source_7_registered(self, ddl):
        assert "SELECT 7 AS source_id" in ddl
        assert "WM2NS" in ddl
