"""Unit tests for the shared Tiingo EOD fetch/parse (``orchestration/ingest/tiingo_common.py``).

Covers ``parse_prices`` (OHLC mapping, missing-close skip, sort, error object) and
``_api_key`` (the required ``TIINGO_API_KEY``). No live network. The sample bars
mirror the live Tiingo daily EOD JSON response.
"""

from __future__ import annotations

from datetime import date

import pytest

from orchestration.ingest.tiingo_common import (
    API_KEY_ENV,
    _api_key,
    _parse_float,
    parse_prices,
)

_BARS = [
    {"date": "2026-06-01T00:00:00.000Z", "open": 99.0, "high": 99.9, "low": 98.7, "close": 99.8, "volume": 1000},
    {"date": "2026-06-02T00:00:00.000Z", "open": 99.5, "high": 100.1, "low": 99.2, "close": 100.0, "volume": 2000},
]


class TestParsePrices:
    def test_one_record_per_bar(self):
        records = parse_prices(_BARS)
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_maps_ohlc(self):
        first = parse_prices(_BARS)[0]
        assert first["open"] == 99.0 and first["high"] == 99.9
        assert first["low"] == 98.7 and first["close"] == 99.8
        assert first["volume"] == 1000.0

    def test_sorts_ascending(self):
        records = parse_prices(list(reversed(_BARS)))
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_skips_bars_with_missing_close(self):
        bars = [
            {"date": "2026-06-01T00:00:00.000Z", "open": 99.0, "close": None, "volume": 0},
            {"date": "2026-06-02T00:00:00.000Z", "open": 99.5, "close": 100.0, "volume": 2000},
        ]
        records = parse_prices(bars)
        assert [r["date"] for r in records] == [date(2026, 6, 2)]

    def test_missing_volume_is_none(self):
        bars = [{"date": "2026-06-01T00:00:00.000Z", "open": 99.0, "close": 99.8}]
        assert parse_prices(bars)[0]["volume"] is None

    def test_keeps_split_factor_and_div_cash(self):
        # The EFA 3:1 ex-date bar: Tiingo carries splitFactor=3.0 — the raw field
        # the silver step uses to rebuild a split-only close.
        bars = [{
            "date": "2005-06-09T00:00:00.000Z", "open": 52.0, "high": 53.0,
            "low": 51.0, "close": 52.54, "volume": 100, "splitFactor": 3.0, "divCash": 0.21,
        }]
        rec = parse_prices(bars)[0]
        assert rec["split_factor"] == 3.0
        assert rec["div_cash"] == 0.21

    def test_missing_corporate_actions_are_none(self):
        # _BARS carry no splitFactor/divCash (the common case) → kept as None.
        rec = parse_prices(_BARS)[0]
        assert rec["split_factor"] is None
        assert rec["div_cash"] is None

    def test_empty_array_yields_no_records(self):
        # A valid ticker with no bars in range yields an empty array.
        assert parse_prices([]) == []

    def test_error_object_raises(self):
        # Tiingo returns {"detail": ...} (a dict, not an array) for an unknown ticker.
        with pytest.raises(ValueError, match="Tiingo error"):
            parse_prices({"detail": "Not found: ZZZZ"})


class TestApiKey:
    def test_missing_key_raises(self, monkeypatch):
        monkeypatch.delenv(API_KEY_ENV, raising=False)
        with pytest.raises(RuntimeError, match=API_KEY_ENV):
            _api_key()

    def test_returns_stripped_key(self, monkeypatch):
        monkeypatch.setenv(API_KEY_ENV, "  tok123  ")
        assert _api_key() == "tok123"


class TestParseFloat:
    def test_parses_number(self):
        assert _parse_float(99.8) == 99.8

    def test_none_stays_none(self):
        assert _parse_float(None) is None
