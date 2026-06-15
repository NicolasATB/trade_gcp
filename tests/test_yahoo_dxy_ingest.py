"""Unit tests for the DXY ingest (``airflow/ingest/yahoo_dxy_ingest.py``).

Covers the pure logic — Yahoo chart payload parsing (timestamp + OHLC arrays,
null-close skip, sort, error/empty handling), row building, the closed-bar-only
filter, MERGE chunking against a fake BigQuery client — plus contract guards on
the MERGE SQL (natural key ``dxy_date``) and the bronze DDL. No live network or
BigQuery. The sample payload mirrors the live Yahoo chart response.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from airflow.ingest import yahoo_dxy_ingest
from airflow.ingest.yahoo_dxy_ingest import (
    _MERGE_SQL,
    _build_row,
    _closed_only,
    _prepare_rows,
    _upsert_rows,
    parse_chart,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


def _ts(y, m, d, hh=13, mm=30) -> int:
    """Epoch seconds at a NY-afternoon UTC time (so the UTC date is unambiguous)."""
    return int(datetime(y, m, d, hh, mm, tzinfo=timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# parse_chart — the Yahoo chart payload shape (verified live)
# ---------------------------------------------------------------------------

class TestParseChart:
    def _payload(self):
        return {
            "chart": {
                "error": None,
                "result": [
                    {
                        "timestamp": [_ts(2026, 6, 1), _ts(2026, 6, 2), _ts(2026, 6, 3)],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [99.0, 99.5, None],
                                    "high": [99.9, 100.1, None],
                                    "low": [98.7, 99.2, None],
                                    "close": [99.8, 100.0, None],  # 3rd bar: holiday/incomplete
                                    "volume": [0, 0, None],
                                }
                            ]
                        },
                    }
                ],
            }
        }

    def test_builds_one_record_per_closed_bar(self):
        records = parse_chart(self._payload())
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_skips_null_close(self):
        records = parse_chart(self._payload())
        assert all(r["close"] is not None for r in records)

    def test_maps_ohlc(self):
        records = parse_chart(self._payload())
        first = records[0]
        assert first["open"] == 99.0 and first["high"] == 99.9
        assert first["low"] == 98.7 and first["close"] == 99.8

    def test_date_is_utc_of_timestamp(self):
        records = parse_chart(self._payload())
        assert records[0]["date"] == date(2026, 6, 1)

    def test_sorts_ascending(self):
        payload = self._payload()
        payload["chart"]["result"][0]["timestamp"] = [_ts(2026, 6, 2), _ts(2026, 6, 1), _ts(2026, 6, 3)]
        records = parse_chart(payload)
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_raises_on_error(self):
        with pytest.raises(ValueError):
            parse_chart({"chart": {"error": {"code": "Not Found"}, "result": None}})

    def test_empty_result_returns_empty(self):
        assert parse_chart({"chart": {"error": None, "result": []}}) == []
        assert parse_chart({}) == []


# ---------------------------------------------------------------------------
# _build_row — pure mapping to the bronze columns
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)

    def test_maps_fields(self):
        row = _build_row(
            {"date": date(2026, 6, 1), "open": 99.0, "high": 99.9,
             "low": 98.7, "close": 99.8, "volume": 0},
            fetched_at=self.FETCHED,
        )
        assert row["dxy_date"] == date(2026, 6, 1)
        assert row["price_open"] == pytest.approx(99.0)
        assert row["price_close"] == pytest.approx(99.8)
        assert row["volume_traded"] == pytest.approx(0.0)
        assert row["source_id"] == yahoo_dxy_ingest.SOURCE_ID
        assert row["datetime_update"] == self.FETCHED

    def test_none_volume_stays_none(self):
        row = _build_row(
            {"date": date(2026, 6, 1), "open": 99.0, "high": 99.9,
             "low": 98.7, "close": 99.8, "volume": None},
            fetched_at=self.FETCHED,
        )
        assert row["volume_traded"] is None


# ---------------------------------------------------------------------------
# _closed_only / _prepare_rows — drop the in-progress current-UTC-day bar
# ---------------------------------------------------------------------------

class TestClosedOnly:
    FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)

    def _rows(self):
        return [
            _build_row({"date": date(2026, 6, 11), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0}, self.FETCHED),
            _build_row({"date": date(2026, 6, 12), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0}, self.FETCHED),
            _build_row({"date": date(2026, 6, 13), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0}, self.FETCHED),
        ]

    def test_drops_today(self):
        kept = _closed_only(self._rows(), today=date(2026, 6, 13))
        assert [r["dxy_date"] for r in kept] == [date(2026, 6, 11), date(2026, 6, 12)]

    def test_prepare_rows_keeps_only_closed(self):
        records = [
            {"date": date(2026, 6, 12), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0},
            {"date": date(2026, 6, 13), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0},
        ]
        rows = _prepare_rows(records, self.FETCHED, today=date(2026, 6, 13))
        assert [r["dxy_date"] for r in rows] == [date(2026, 6, 12)]


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the natural key dxy_date
# ---------------------------------------------------------------------------

class TestMergeContract:
    KEY = "dxy_date"

    def test_joins_on_natural_key(self):
        assert f"T.{self.KEY} = S.{self.KEY}" in _norm(_MERGE_SQL)

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_does_not_update_the_natural_key(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert self.KEY not in assignments
        assert "price_close" in assignments

    def test_dedupes_source_by_key(self):
        assert "PARTITION BY s.dxy_date" in _norm(_MERGE_SQL)
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
    base = date(1971, 1, 4).toordinal()
    return [
        {"dxy_date": date.fromordinal(base + i), "price_open": 1.0, "price_high": 1.0,
         "price_low": 1.0, "price_close": 1.0, "volume_traded": 0.0,
         "source_id": 6, "datetime_update": fetched}
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

    def test_full_history_is_chunked(self):
        # DXY since 1971 (~14k business days) must not go in one >4000 MERGE.
        client = _FakeClient()
        _upsert_rows(client, _rows(14000), chunk_size=3500)
        assert len(client.queries) == 4

    def test_every_query_is_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(5), chunk_size=2)
        assert all("MERGE" in q and "dxy_date" in q for q in client.queries)


# ---------------------------------------------------------------------------
# DDL contract — the DXY bronze table + source registration exist
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    def test_table_declared(self, ddl):
        assert "prod_trade_bronze.yahoo_dxy_daily_raw" in ddl

    def test_partitioned_by_month(self, ddl):
        # Monthly granularity: ~55y of daily bars would blow BigQuery's
        # 10000-partitions-per-table limit at daily granularity.
        block = ddl.split("yahoo_dxy_daily_raw", 1)[1]
        assert "PARTITION BY DATE_TRUNC(dxy_date, MONTH)" in block

    def test_natural_key_is_dxy_date(self, ddl):
        block = ddl.split("yahoo_dxy_daily_raw", 1)[1]
        assert "PRIMARY KEY (dxy_date) NOT ENFORCED" in block

    def test_source_6_registered(self, ddl):
        assert "SELECT 6 AS source_id" in ddl
        assert "DX-Y.NYB" in ddl
