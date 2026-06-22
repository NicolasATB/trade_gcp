"""Unit tests for the shared Coin Metrics ingest logic
(``orchestration/ingest/coinmetrics_common.py``) and the three thin entry-points
(circulating supply ``SplyCur``, active addresses ``AdrActCnt``, transaction count
``TxCnt``).

Covers the pure logic — metric parsing, ``NaN``/empty -> NULL, INT64 vs FLOAT64
coercion, pure row mapping (no fabrication), the alert-and-skip missing policy, the
query-URL builder, MERGE chunking against a fake BigQuery client — plus contract
guards on the per-series MERGE SQL (natural date key), the bronze DDL (tables,
partitions, sources 12/13/14), the entry-point configs, and the daily training
view's on-chain features. No live network or BigQuery.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from orchestration.ingest import coinmetrics_common as cm
from orchestration.ingest.coinmetrics_btc_active_addresses_ingest import SERIES as ADDR
from orchestration.ingest.coinmetrics_btc_supply_ingest import SERIES as SUPPLY
from orchestration.ingest.coinmetrics_btc_tx_count_ingest import SERIES as TX

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# _parse_value / _coerce_value
# ---------------------------------------------------------------------------

class TestParseValue:
    def test_number_parsed(self):
        assert cm._parse_value("19687292.99271483") == pytest.approx(19687292.99271483)

    def test_empty_is_none(self):
        assert cm._parse_value("") is None
        assert cm._parse_value("  ") is None

    def test_nan_and_none(self):
        assert cm._parse_value("NaN") is None
        assert cm._parse_value(None) is None

    def test_zero_is_kept(self):
        assert cm._parse_value("0") == 0.0


class TestCoerceValue:
    def test_int64_rounds_to_int(self):
        v = cm._coerce_value(1023456.0, "INT64")
        assert v == 1023456 and isinstance(v, int)

    def test_float64_stays_float(self):
        v = cm._coerce_value(19687292.99, "FLOAT64")
        assert v == pytest.approx(19687292.99) and isinstance(v, float)

    def test_none_stays_none(self):
        assert cm._coerce_value(None, "INT64") is None
        assert cm._coerce_value(None, "FLOAT64") is None


# ---------------------------------------------------------------------------
# parse_metrics — the asset-metrics page shape, read by cfg.metric
# ---------------------------------------------------------------------------

class TestParseMetrics:
    PAYLOAD = {
        "data": [
            {"asset": "btc", "time": "2024-04-19T00:00:00.000000000Z", "AdrActCnt": "950000"},
            {"asset": "btc", "time": "2024-04-20T00:00:00.000000000Z", "AdrActCnt": "975123"},
            {"asset": "btc", "time": "2024-04-21T00:00:00.000000000Z", "AdrActCnt": ""},
        ]
    }

    def test_reads_the_configured_metric_cell(self):
        records = cm.parse_metrics(self.PAYLOAD, ADDR)
        assert [r["d"] for r in records] == [
            "2024-04-19T00:00:00.000000000Z",
            "2024-04-20T00:00:00.000000000Z",
            "2024-04-21T00:00:00.000000000Z",
        ]
        assert records[0]["value"] == pytest.approx(950000.0)

    def test_missing_value_becomes_none(self):
        records = cm.parse_metrics(self.PAYLOAD, ADDR)
        assert records[2]["value"] is None

    def test_empty_payload(self):
        assert cm.parse_metrics({}, ADDR) == []
        assert cm.parse_metrics({"data": []}, ADDR) == []


# ---------------------------------------------------------------------------
# _build_row — pure mapping, date sliced from the ISO timestamp, typed value
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)

    def test_supply_maps_float_columns(self):
        row = cm._build_row(
            {"d": "2024-04-19T00:00:00Z", "value": 19687292.99}, SUPPLY, fetched_at=self.FETCHED
        )
        assert row["supply_date"] == date(2024, 4, 19)
        assert row["circ_supply"] == pytest.approx(19687292.99)
        assert isinstance(row["circ_supply"], float)
        assert row["source_id"] == 12
        assert row["datetime_update"] == self.FETCHED

    def test_active_addresses_maps_int_columns(self):
        row = cm._build_row(
            {"d": "2024-04-19T00:00:00Z", "value": 975123.0}, ADDR, fetched_at=self.FETCHED
        )
        assert row["metric_date"] == date(2024, 4, 19)
        assert row["active_addresses"] == 975123
        assert isinstance(row["active_addresses"], int)
        assert row["source_id"] == 13

    def test_missing_value_stays_none_no_fabrication(self):
        row = cm._build_row({"d": "2024-04-21T00:00:00Z", "value": None}, TX)
        assert row["tx_count"] is None
        assert row["metric_date"] == date(2024, 4, 21)


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_rows — alert-and-skip missing observations
# ---------------------------------------------------------------------------

def _addr_row(d: date, value):
    return {
        "metric_date": d, "active_addresses": value,
        "source_id": 13, "datetime_update": datetime(2026, 6, 17, tzinfo=timezone.utc),
    }


class TestAlertMissing:
    def test_returns_missing_and_warns(self, caplog):
        rows = [_addr_row(date(2024, 4, 21), None), _addr_row(date(2024, 4, 19), 975123)]
        with caplog.at_level("WARNING"):
            missing = cm._alert_missing(rows, ADDR, "daily")
        assert missing == [date(2024, 4, 21)]
        assert any("missing value for 2024-04-21" in r.message for r in caplog.records)
        assert any("AdrActCnt" in r.message for r in caplog.records)

    def test_no_missing_no_warning(self, caplog):
        with caplog.at_level("WARNING"):
            assert cm._alert_missing([_addr_row(date(2024, 4, 19), 975123)], ADDR, "daily") == []
        assert caplog.records == []


class TestPrepareRows:
    FETCHED = datetime(2026, 6, 17, tzinfo=timezone.utc)

    def test_drops_missing_keeps_real(self, caplog):
        records = [
            {"d": "2024-04-21T00:00:00Z", "value": None},
            {"d": "2024-04-22T00:00:00Z", "value": 981000.0},
        ]
        with caplog.at_level("WARNING"):
            rows = cm._prepare_rows(records, ADDR, self.FETCHED, "back-fill")
        assert [r["metric_date"] for r in rows] == [date(2024, 4, 22)]
        assert rows[0]["active_addresses"] == 981000
        assert any("missing value for 2024-04-21" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _query_url — request builder (pure), per metric
# ---------------------------------------------------------------------------

class TestQueryUrl:
    def test_contains_asset_metric_and_range(self):
        url = cm._query_url(TX, date(2010, 7, 18), date(2026, 6, 17))
        assert "assets=btc" in url
        assert "metrics=TxCnt" in url
        assert "frequency=1d" in url
        assert "start_time=2010-07-18" in url
        assert "end_time=2026-06-17" in url


# ---------------------------------------------------------------------------
# build_merge_sql — idempotency contract on each series' natural date key
# ---------------------------------------------------------------------------

class TestMergeContract:
    @pytest.mark.parametrize("cfg,key", [(SUPPLY, "supply_date"), (ADDR, "metric_date"), (TX, "metric_date")])
    def test_joins_on_natural_key(self, cfg, key):
        assert f"T.{key} = S.{key}" in _norm(cm.build_merge_sql(cfg))

    @pytest.mark.parametrize("cfg", [SUPPLY, ADDR, TX])
    def test_has_both_match_branches(self, cfg):
        sql = cm.build_merge_sql(cfg)
        assert "WHEN MATCHED THEN UPDATE" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    @pytest.mark.parametrize("cfg,key", [(SUPPLY, "supply_date"), (ADDR, "metric_date")])
    def test_does_not_update_the_natural_key(self, cfg, key):
        sql = cm.build_merge_sql(cfg)
        update_set = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert key not in assignments

    @pytest.mark.parametrize("cfg,key", [(SUPPLY, "supply_date"), (TX, "metric_date")])
    def test_dedupes_source_by_key(self, cfg, key):
        norm = _norm(cm.build_merge_sql(cfg))
        assert f"PARTITION BY s.{key}" in norm
        assert "WHERE rn = 1" in norm

    def test_value_column_is_updated_and_inserted(self):
        sql = cm.build_merge_sql(ADDR)
        assert "active_addresses     = S.active_addresses" in sql


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


def _rows(n: int, cfg) -> list[dict]:
    fetched = datetime(2026, 6, 17, tzinfo=timezone.utc)
    base = date(2010, 7, 18).toordinal()
    return [
        {cfg.date_col: date.fromordinal(base + i), cfg.value_col: i,
         "source_id": cfg.source_id, "datetime_update": fetched}
        for i in range(n)
    ]


class TestUpsertChunking:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        cm._upsert_rows(client, SUPPLY, [])
        assert client.queries == []

    def test_single_chunk_when_under_limit(self):
        client = _FakeClient()
        cm._upsert_rows(client, ADDR, _rows(10, ADDR), chunk_size=3500)
        assert len(client.queries) == 1

    def test_splits_into_ceil_chunks(self):
        client = _FakeClient()
        cm._upsert_rows(client, TX, _rows(8, TX), chunk_size=3)  # 3 + 3 + 2
        assert len(client.queries) == 3

    def test_full_history_size_is_chunked(self):
        # ~5800 daily dates must not go in a single >4000-partition MERGE.
        client = _FakeClient()
        cm._upsert_rows(client, SUPPLY, _rows(5800, SUPPLY), chunk_size=3500)
        assert len(client.queries) == 2

    def test_every_query_is_the_series_merge(self):
        client = _FakeClient()
        cm._upsert_rows(client, ADDR, _rows(5, ADDR), chunk_size=2)
        assert all(
            "MERGE" in q
            and "coinmetrics_btc_active_addresses_daily_raw" in q
            and "metric_date" in q
            for q in client.queries
        )


# ---------------------------------------------------------------------------
# Entry-point configs — each thin module pins the right series
# ---------------------------------------------------------------------------

class TestEntryPointConfigs:
    def test_supply_config(self):
        assert (SUPPLY.metric, SUPPLY.date_col, SUPPLY.value_col, SUPPLY.source_id, SUPPLY.value_type) == \
            ("SplyCur", "supply_date", "circ_supply", 12, "FLOAT64")

    def test_active_addresses_config(self):
        assert (ADDR.metric, ADDR.date_col, ADDR.value_col, ADDR.source_id, ADDR.value_type) == \
            ("AdrActCnt", "metric_date", "active_addresses", 13, "INT64")

    def test_tx_count_config(self):
        assert (TX.metric, TX.date_col, TX.value_col, TX.source_id, TX.value_type) == \
            ("TxCnt", "metric_date", "tx_count", 14, "INT64")


# ---------------------------------------------------------------------------
# DDL contract — bronze tables + source registrations exist
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    @pytest.mark.parametrize(
        "table,date_col,source_id",
        [
            ("coinmetrics_btc_supply_daily_raw", "supply_date", 12),
            ("coinmetrics_btc_active_addresses_daily_raw", "metric_date", 13),
            ("coinmetrics_btc_tx_count_daily_raw", "metric_date", 14),
        ],
    )
    def test_table_partition_key_and_source(self, ddl, table, date_col, source_id):
        assert f"prod_trade_bronze.{table}" in ddl
        block = ddl.split(table, 1)[1]
        assert f"PARTITION BY {date_col}" in block
        assert f"PRIMARY KEY ({date_col}) NOT ENFORCED" in block
        assert f"SELECT {source_id} AS source_id" in ddl


# ---------------------------------------------------------------------------
# Daily training view — the on-chain stationary features and their as-of joins
# ---------------------------------------------------------------------------

class TestDailyViewOnChainFeatures:
    @pytest.fixture(scope="class")
    def daily(self):
        ddl = _norm(_DDL_PATH.read_text(encoding="utf-8"))
        return ddl.split("vw_btc_training_daily", 1)[1].split("vw_btc_training_weekly")[0]

    def test_exposes_yoy_log_columns(self, daily):
        assert "AS active_addresses_yoy_log" in daily
        assert "AS tx_count_yoy_log" in daily

    def test_yoy_uses_365_day_lag_over_native_series(self, daily):
        assert "LAG(active_addresses, 365) OVER (ORDER BY metric_date)" in daily
        assert "LAG(tx_count, 365) OVER (ORDER BY metric_date)" in daily

    def test_onchain_asof_joins_latest_on_or_before_d(self, daily):
        assert "ON a.metric_date <= b.d" in daily
        assert "ON t.metric_date <= b.d" in daily


# ---------------------------------------------------------------------------
# Monitoring view — Looker Studio dashboard source (raw levels + on-chain)
# ---------------------------------------------------------------------------

class TestMonitorView:
    @pytest.fixture(scope="class")
    def monitor(self):
        ddl = _norm(_DDL_PATH.read_text(encoding="utf-8"))
        return ddl.split("vw_btc_monitor_daily", 1)[1]

    def test_view_declared_in_gold(self):
        ddl = _DDL_PATH.read_text(encoding="utf-8")
        assert "CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_monitor_daily`" in ddl

    def test_exposes_raw_price_level(self, monitor):
        # The monitor view DOES expose price_close (the training view hides it).
        assert "s.price_close" in monitor

    def test_exposes_coinmetrics_raw_counts_and_yoy(self, monitor):
        assert "a.active_addresses" in monitor
        assert "t.tx_count" in monitor
        assert "active_addresses_yoy_log" in monitor
        assert "tx_count_yoy_log" in monitor

    def test_attention_aliased_as_google_trends(self, monitor):
        assert "d.investor_attention  AS google_trends" in monitor or "investor_attention AS google_trends" in _norm(monitor)

    def test_built_on_training_view_and_bronze_onchain(self, monitor):
        assert "prod_trade_gold.vw_btc_training_daily" in monitor
        assert "coinmetrics_btc_active_addresses_daily_raw" in monitor
        assert "coinmetrics_btc_tx_count_daily_raw" in monitor
