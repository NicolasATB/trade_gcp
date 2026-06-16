"""Unit tests for the shared CCXT candle ingest
(``orchestration/ingest/ccxt_candle_common.py``) and its thin exchange entry-points
(``binance_btc_ingest``, ``bitstamp_btc_ingest``).

Covers the pure logic — row building (parameterised by the exchange config),
the closed-candle filter and ascending sort in ``fetch_daily_candles_range``
against a fake exchange, the idempotent MERGE on ``(symbol, candle_date)`` issued
against a fake BigQuery client — plus that each entry-point pins the right
``CcxtCandleSource`` config and the package re-exports the Binance functions.
No live network or BigQuery.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone

import pytest

import orchestration.ingest as ingest_pkg
from orchestration.ingest import binance_btc_ingest, bitstamp_btc_ingest
from orchestration.ingest.ccxt_candle_common import (
    CcxtCandleSource,
    _build_row,
    _day_start_ms,
    _upsert_rows,
    fetch_daily_candles_range,
)

CFG = CcxtCandleSource(exchange_id="binance", symbol="BTC/USDT",
                       table="binance_btcusd_daily_raw", source_id=3)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# _build_row — maps a CCXT candle tuple to a bronze row, using the config
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 13, tzinfo=timezone.utc)

    def test_maps_tuple_and_config(self):
        open_ms = _day_start_ms(date(2024, 1, 15))
        candle = [open_ms, 42000.0, 42500.0, 41800.0, 42300.0, 1234.5]
        row = _build_row(candle, CFG, fetched_at=self.FETCHED)
        assert row["symbol"] == "BTC/USDT"
        assert row["source_id"] == 3
        assert row["candle_date"] == date(2024, 1, 15)
        assert row["open_time"] == open_ms
        assert row["price_open"] == pytest.approx(42000.0)
        assert row["price_close"] == pytest.approx(42300.0)
        assert row["volume_traded"] == pytest.approx(1234.5)
        assert row["datetime_update"] == self.FETCHED

    def test_uses_config_symbol_and_source(self):
        cfg = CcxtCandleSource("bitstamp", "BTC/USD", "bitstamp_btcusd_daily_raw", 4)
        row = _build_row([_day_start_ms(date(2015, 5, 1)), 1, 1, 1, 1, 1], cfg, fetched_at=self.FETCHED)
        assert row["symbol"] == "BTC/USD"
        assert row["source_id"] == 4


# ---------------------------------------------------------------------------
# fetch_daily_candles_range — closed-candle filter, sort, validation
# ---------------------------------------------------------------------------

class _FakeExchange:
    """Returns a fixed candle list regardless of the query args."""

    def __init__(self, candles):
        self._candles = candles
        self.calls = []

    def fetch_ohlcv(self, symbol, timeframe=None, since=None, limit=None, params=None):
        self.calls.append({"symbol": symbol, "since": since, "limit": limit, "params": params})
        return list(self._candles)


class TestFetchRange:
    def test_drops_unclosed_and_sorts(self):
        # Two closed days (well in the past) given out of order + today's
        # in-progress candle (open now -> not closed) which must be dropped.
        d1 = _day_start_ms(date(2024, 1, 2))
        d2 = _day_start_ms(date(2024, 1, 1))
        today_open = _day_start_ms(datetime.now(timezone.utc).date())
        exchange = _FakeExchange([
            [d1, 1, 1, 1, 1, 1],
            [d2, 1, 1, 1, 1, 1],
            [today_open, 9, 9, 9, 9, 9],
        ])
        candles = fetch_daily_candles_range(CFG, date(2024, 1, 1), date(2024, 1, 3), exchange=exchange)
        opens = [c[0] for c in candles]
        assert opens == [d2, d1]          # sorted ascending, today's bar dropped

    def test_passes_total_days_as_limit(self):
        # limit must be the TOTAL days in the range (not a constant 1000) so
        # transparent pagination doesn't truncate long back-fills.
        exchange = _FakeExchange([])
        fetch_daily_candles_range(CFG, date(2024, 1, 1), date(2024, 1, 31), exchange=exchange)
        assert exchange.calls[0]["limit"] == 31

    def test_rejects_inverted_range(self):
        with pytest.raises(ValueError):
            fetch_daily_candles_range(CFG, date(2024, 1, 2), date(2024, 1, 1), exchange=_FakeExchange([]))


# ---------------------------------------------------------------------------
# _upsert_rows — idempotent MERGE on (symbol, candle_date) against a fake client
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
    base = date(2017, 8, 17).toordinal()
    return [
        {"source_id": 3, "datetime_update": fetched, "symbol": "BTC/USDT",
         "candle_date": date.fromordinal(base + i), "open_time": i,
         "price_open": 1.0, "price_high": 1.0, "price_low": 1.0,
         "price_close": 1.0, "volume_traded": 1.0}
        for i in range(n)
    ]


class TestUpsert:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        _upsert_rows(client, "binance_btcusd_daily_raw", [])
        assert client.queries == []

    def test_single_merge_with_table_and_key(self):
        client = _FakeClient()
        _upsert_rows(client, "binance_btcusd_daily_raw", _rows(3))
        assert len(client.queries) == 1
        q = _norm(client.queries[0])
        assert "binance_btcusd_daily_raw" in q
        assert "T.symbol = S.symbol AND T.candle_date = S.candle_date" in q
        assert "WHEN MATCHED THEN UPDATE" in q and "WHEN NOT MATCHED THEN INSERT" in q

    def test_dedupes_source_by_composite_key(self):
        client = _FakeClient()
        _upsert_rows(client, "binance_btcusd_daily_raw", _rows(2))
        q = _norm(client.queries[0])
        assert "PARTITION BY s.symbol, s.candle_date" in q
        assert "WHERE rn = 1" in q

    def test_does_not_update_the_key(self):
        client = _FakeClient()
        _upsert_rows(client, "binance_btcusd_daily_raw", _rows(1))
        update_set = client.queries[0].split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert "symbol" not in assignments
        assert "candle_date" not in assignments
        assert "price_close" in assignments


# ---------------------------------------------------------------------------
# Entry-points — each pins the right CcxtCandleSource; package re-exports
# ---------------------------------------------------------------------------

class TestEntryPointConfig:
    def test_binance_config(self):
        s = binance_btc_ingest.SOURCE
        assert (s.exchange_id, s.symbol, s.table, s.source_id) == (
            "binance", "BTC/USDT", "binance_btcusd_daily_raw", 3)

    def test_bitstamp_config(self):
        s = bitstamp_btc_ingest.SOURCE
        assert (s.exchange_id, s.symbol, s.table, s.source_id) == (
            "bitstamp", "BTC/USD", "bitstamp_btcusd_daily_raw", 4)

    def test_entry_points_expose_bound_functions(self):
        for mod in (binance_btc_ingest, bitstamp_btc_ingest):
            assert callable(mod.ingest_daily_candles)
            assert callable(mod.fetch_daily_candles_range)
            assert callable(mod.main)

    def test_package_reexports_binance(self):
        # Public API preserved for the (pending) DAG: orchestration.ingest.<fn>.
        assert ingest_pkg.ingest_daily_candles is binance_btc_ingest.ingest_daily_candles
        assert ingest_pkg.fetch_daily_candles_range is binance_btc_ingest.fetch_daily_candles_range
