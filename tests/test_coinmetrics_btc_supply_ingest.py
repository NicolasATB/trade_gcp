"""Unit tests for the Coin Metrics BTC circulating-supply ingest
(``airflow/ingest/coinmetrics_btc_supply_ingest.py``).

Covers the pure logic — metrics parsing, ``NaN``/empty -> NULL, pure row mapping
(no fabrication), the alert-and-skip missing policy, the query-URL builder, MERGE
chunking against a fake BigQuery client — plus contract guards on the MERGE SQL
(natural key ``supply_date``) and the bronze DDL (table, partition, source 12).
No live network or BigQuery.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from airflow.ingest.coinmetrics_btc_supply_ingest import (
    _MERGE_SQL,
    _alert_missing,
    _build_row,
    _parse_value,
    _prepare_rows,
    _query_url,
    _upsert_rows,
    parse_metrics,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# _parse_value
# ---------------------------------------------------------------------------

class TestParseValue:
    def test_number_parsed(self):
        assert _parse_value("19687292.99271483") == pytest.approx(19687292.99271483)

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
# parse_metrics — the Coin Metrics asset-metrics page shape (verified live)
# ---------------------------------------------------------------------------

class TestParseMetrics:
    PAYLOAD = {
        "data": [
            {"asset": "btc", "time": "2024-04-19T00:00:00.000000000Z", "SplyCur": "19687292.99271483"},
            {"asset": "btc", "time": "2024-04-20T00:00:00.000000000Z", "SplyCur": "19687699.24271483"},
            {"asset": "btc", "time": "2024-04-21T00:00:00.000000000Z", "SplyCur": ""},
        ]
    }

    def test_maps_date_string(self):
        records = parse_metrics(self.PAYLOAD)
        assert [r["d"] for r in records] == [
            "2024-04-19T00:00:00.000000000Z",
            "2024-04-20T00:00:00.000000000Z",
            "2024-04-21T00:00:00.000000000Z",
        ]

    def test_real_value_is_float(self):
        records = parse_metrics(self.PAYLOAD)
        assert records[0]["circ_supply"] == pytest.approx(19687292.99271483)

    def test_missing_value_becomes_none(self):
        records = parse_metrics(self.PAYLOAD)
        assert records[2]["circ_supply"] is None

    def test_empty_payload(self):
        assert parse_metrics({}) == []
        assert parse_metrics({"data": []}) == []


# ---------------------------------------------------------------------------
# _build_row — pure mapping (date sliced from the ISO timestamp)
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 14, 12, 0, tzinfo=timezone.utc)

    def test_maps_fields_to_columns(self):
        row = _build_row(
            {"d": "2024-04-19T00:00:00.000000000Z", "circ_supply": 19687292.99},
            fetched_at=self.FETCHED,
        )
        assert row["supply_date"] == date(2024, 4, 19)
        assert row["circ_supply"] == pytest.approx(19687292.99)
        assert row["source_id"] == 12
        assert row["datetime_update"] == self.FETCHED

    def test_missing_value_stays_none_no_fabrication(self):
        row = _build_row({"d": "2024-04-21T00:00:00.000000000Z", "circ_supply": None})
        assert row["circ_supply"] is None
        assert row["supply_date"] == date(2024, 4, 21)


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_rows — alert-and-skip missing observations
# ---------------------------------------------------------------------------

def _row(d: date, value):
    return {
        "supply_date": d, "circ_supply": value,
        "source_id": 12, "datetime_update": datetime(2026, 6, 14, tzinfo=timezone.utc),
    }


class TestAlertMissing:
    def test_returns_missing_and_warns(self, caplog):
        rows = [_row(date(2024, 4, 21), None), _row(date(2024, 4, 19), 19687292.99)]
        with caplog.at_level("WARNING"):
            missing = _alert_missing(rows, "daily")
        assert missing == [date(2024, 4, 21)]
        assert any("missing value for 2024-04-21" in r.message for r in caplog.records)

    def test_no_missing_no_warning(self, caplog):
        with caplog.at_level("WARNING"):
            assert _alert_missing([_row(date(2024, 4, 19), 19687292.99)], "daily") == []
        assert caplog.records == []


class TestPrepareRows:
    FETCHED = datetime(2026, 6, 14, tzinfo=timezone.utc)

    def test_drops_missing_keeps_real(self, caplog):
        records = [
            {"d": "2024-04-21T00:00:00Z", "circ_supply": None},
            {"d": "2024-04-22T00:00:00Z", "circ_supply": 19688136.74},
        ]
        with caplog.at_level("WARNING"):
            rows = _prepare_rows(records, self.FETCHED, "back-fill")
        assert [r["supply_date"] for r in rows] == [date(2024, 4, 22)]
        assert any("missing value for 2024-04-21" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _query_url — request builder (pure)
# ---------------------------------------------------------------------------

class TestQueryUrl:
    def test_contains_asset_metric_and_range(self):
        url = _query_url(date(2010, 7, 18), date(2026, 6, 14))
        assert "assets=btc" in url
        assert "metrics=SplyCur" in url
        assert "frequency=1d" in url
        assert "start_time=2010-07-18" in url
        assert "end_time=2026-06-14" in url


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the natural key supply_date
# ---------------------------------------------------------------------------

class TestMergeContract:
    KEY = "supply_date"

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
        assert "PARTITION BY s.supply_date" in _norm(_MERGE_SQL)
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
    fetched = datetime(2026, 6, 14, tzinfo=timezone.utc)
    base = date(2010, 7, 18).toordinal()
    return [
        {"supply_date": date.fromordinal(base + i), "circ_supply": 1.0 * i,
         "source_id": 12, "datetime_update": fetched}
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
        # ~5800 daily dates must not go in a single >4000-partition MERGE.
        client = _FakeClient()
        _upsert_rows(client, _rows(5800), chunk_size=3500)
        assert len(client.queries) == 2

    def test_every_query_is_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(5), chunk_size=2)
        assert all("MERGE" in q and "coinmetrics_btc_supply_daily_raw" in q
                   and "supply_date" in q for q in client.queries)


# ---------------------------------------------------------------------------
# DDL contract — bronze table + source registration exist
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    def test_table_is_declared(self, ddl):
        assert "prod_trade_bronze.coinmetrics_btc_supply_daily_raw" in ddl

    def test_partitioned_by_supply_date(self, ddl):
        block = ddl.split("coinmetrics_btc_supply_daily_raw", 1)[1]
        assert "PARTITION BY supply_date" in block

    def test_natural_key_is_supply_date(self, ddl):
        block = ddl.split("coinmetrics_btc_supply_daily_raw", 1)[1]
        assert "PRIMARY KEY (supply_date) NOT ENFORCED" in block

    def test_source_12_registered(self, ddl):
        assert "SELECT 12 AS source_id" in ddl
        assert "Coin Metrics" in ddl
