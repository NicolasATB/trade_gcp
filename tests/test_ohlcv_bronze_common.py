"""Unit tests for the shared multi-asset OHLCV bronze loader.

Covers ``orchestration/ingest/ohlcv_bronze_common.py``: the pure row mapping,
the closed-bar-only filter, MERGE chunking against a fake BigQuery client, the
idempotency contract on the natural key ``(symbol, candle_date)``, and the DDL
contract for both per-source ETF tables. No live network or BigQuery.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from orchestration.ingest import ohlcv_bronze_common as ohlcv
from orchestration.ingest.ohlcv_bronze_common import (
    _MERGE_SQL,
    _closed_only,
    build_row,
    prepare_rows,
    upsert_rows,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"
FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


def _record(d: date, close=100.0):
    return {"date": d, "open": 99.0, "high": 101.0, "low": 98.0, "close": close, "volume": 12345}


# ---------------------------------------------------------------------------
# build_row — pure mapping to the bronze columns
# ---------------------------------------------------------------------------

class TestBuildRow:
    def test_maps_fields(self):
        row = build_row("SPY", _record(date(2026, 6, 1)), source_id=16, fetched_at=FETCHED)
        assert row["symbol"] == "SPY"
        assert row["candle_date"] == date(2026, 6, 1)
        assert row["price_open"] == pytest.approx(99.0)
        assert row["price_close"] == pytest.approx(100.0)
        assert row["volume_traded"] == pytest.approx(12345.0)
        assert row["source_id"] == 16
        assert row["datetime_update"] == FETCHED

    def test_none_volume_stays_none(self):
        rec = _record(date(2026, 6, 1))
        rec["volume"] = None
        row = build_row("SPY", rec, source_id=16, fetched_at=FETCHED)
        assert row["volume_traded"] is None

    def test_missing_optional_keys_default_to_none(self):
        row = build_row("SPY", {"date": date(2026, 6, 1), "close": 100.0}, source_id=16, fetched_at=FETCHED)
        assert row["price_open"] is None
        assert row["price_close"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# _closed_only / prepare_rows — drop the in-progress current-UTC-day bar
# ---------------------------------------------------------------------------

class TestClosedOnly:
    def test_drops_today(self):
        rows = [
            build_row("SPY", _record(date(2026, 6, 11)), 16, FETCHED),
            build_row("SPY", _record(date(2026, 6, 12)), 16, FETCHED),
            build_row("SPY", _record(date(2026, 6, 13)), 16, FETCHED),
        ]
        kept = _closed_only(rows, today=date(2026, 6, 13))
        assert [r["candle_date"] for r in kept] == [date(2026, 6, 11), date(2026, 6, 12)]

    def test_prepare_rows_keeps_only_closed(self):
        records = [_record(date(2026, 6, 12)), _record(date(2026, 6, 13))]
        rows = prepare_rows("SPY", records, 16, FETCHED, today=date(2026, 6, 13))
        assert [r["candle_date"] for r in rows] == [date(2026, 6, 12)]
        assert all(r["symbol"] == "SPY" for r in rows)


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the natural key (symbol, candle_date)
# ---------------------------------------------------------------------------

class TestMergeContract:
    def test_joins_on_composite_natural_key(self):
        merged = _norm(_MERGE_SQL)
        assert "T.symbol = S.symbol AND T.candle_date = S.candle_date" in merged

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_does_not_update_the_natural_key(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert "symbol" not in assignments
        assert "candle_date" not in assignments
        assert "price_close" in assignments

    def test_dedupes_source_by_key(self):
        merged = _norm(_MERGE_SQL)
        assert "PARTITION BY s.symbol, s.candle_date" in merged
        assert "WHERE rn = 1" in merged


# ---------------------------------------------------------------------------
# upsert_rows — chunking to respect the 4000-partition-per-DML limit
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
    base = date(2006, 1, 3).toordinal()
    return [
        build_row("SPY", _record(date.fromordinal(base + i)), 16, FETCHED)
        for i in range(n)
    ]


class TestUpsertChunking:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        upsert_rows(client, "yahoo_etf_daily_raw", [])
        assert client.queries == []

    def test_single_chunk_when_under_limit(self):
        client = _FakeClient()
        upsert_rows(client, "yahoo_etf_daily_raw", _rows(10), chunk_size=3500)
        assert len(client.queries) == 1

    def test_splits_into_ceil_chunks(self):
        client = _FakeClient()
        upsert_rows(client, "yahoo_etf_daily_raw", _rows(8), chunk_size=3)  # 3 + 3 + 2
        assert len(client.queries) == 3

    def test_target_table_is_in_every_query(self):
        client = _FakeClient()
        upsert_rows(client, "stooq_etf_daily_raw", _rows(5), chunk_size=2)
        assert all("MERGE" in q and "stooq_etf_daily_raw" in q for q in client.queries)

    def test_default_chunk_size_is_under_dml_cap(self):
        assert ohlcv.MAX_MERGE_PARTITIONS <= 4000


# ---------------------------------------------------------------------------
# DDL contract — both per-source ETF tables + source registration exist
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    @pytest.mark.parametrize("table", ["yahoo_etf_daily_raw", "stooq_etf_daily_raw"])
    def test_table_declared(self, ddl, table):
        assert f"prod_trade_bronze.{table}" in ddl

    @pytest.mark.parametrize("table", ["yahoo_etf_daily_raw", "stooq_etf_daily_raw"])
    def test_partitioned_by_month(self, ddl, table):
        block = ddl.split(table, 1)[1]
        assert "PARTITION BY DATE_TRUNC(candle_date, MONTH)" in block

    @pytest.mark.parametrize("table", ["yahoo_etf_daily_raw", "stooq_etf_daily_raw"])
    def test_clustered_by_symbol(self, ddl, table):
        block = ddl.split(table, 1)[1]
        assert "CLUSTER BY symbol" in block

    @pytest.mark.parametrize("table", ["yahoo_etf_daily_raw", "stooq_etf_daily_raw"])
    def test_natural_key_is_symbol_candle_date(self, ddl, table):
        block = ddl.split(table, 1)[1]
        assert "PRIMARY KEY (symbol, candle_date) NOT ENFORCED" in block

    def test_yahoo_source_16_outranks_stooq_17(self, ddl):
        # Yahoo (16) is primary with the higher priority; stooq (17) is fallback.
        assert "SELECT 16 AS source_id" in ddl
        assert "SELECT 17 AS source_id" in ddl
        # Yahoo seeds priority 2, stooq priority 1 (relative order is what matters).
        assert re.search(r"VALUES \(16,.*?,\s*2,\s*TRUE", ddl)
        assert re.search(r"VALUES \(17,.*?,\s*1,\s*TRUE", ddl)
