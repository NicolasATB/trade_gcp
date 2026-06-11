"""Unit tests for the bronze→silver normalisation in ``dataflow/stages/conform.py``.

Covers ``_NormaliseBinanceRow.process``: symbol mapping, the fixed ``1d``
temporality, the 00:00:00 / 23:59:59 day bounds, and accepting ``candle_date``
as either a ``date`` or an ISO string. No BigQuery/Beam IO.
"""

from __future__ import annotations

from datetime import date

import pytest

from dataflow.stages.conform import _NormaliseBinanceRow


def _normalise(element: dict) -> dict:
    (row,) = list(_NormaliseBinanceRow().process(element))
    return row


class TestNormaliseBinanceRow:
    def test_maps_binance_symbol_to_canonical(self):
        row = _normalise({"candle_date": date(2024, 1, 7), "symbol": "BTC/USDT"})
        assert row["symbol"] == "BTCUSD"

    @pytest.mark.parametrize("raw", ["BTC/USDT", "BTC/USD"])
    def test_known_symbols_map_to_btcusd(self, raw):
        assert _normalise({"candle_date": date(2024, 1, 7), "symbol": raw})["symbol"] == "BTCUSD"

    def test_unknown_symbol_falls_back_to_btcusd(self):
        # _SYMBOL_MAP.get(..., "BTCUSD") default for anything unmapped.
        row = _normalise({"candle_date": date(2024, 1, 7), "symbol": "DOGE/USDT"})
        assert row["symbol"] == "BTCUSD"

    def test_temporality_is_daily(self):
        assert _normalise({"candle_date": date(2024, 1, 7)})["temporality"] == "1d"

    def test_day_bounds(self):
        row = _normalise({"candle_date": date(2024, 1, 7)})
        assert row["time_period_start"] == "2024-01-07T00:00:00+00:00"
        assert row["time_period_end"] == "2024-01-07T23:59:59+00:00"
        assert row["time_open"] == row["time_period_start"]
        assert row["time_close"] == row["time_period_end"]

    def test_accepts_candle_date_as_string(self):
        row = _normalise({"candle_date": "2024-01-07"})
        assert row["time_period_start"] == "2024-01-07T00:00:00+00:00"

    def test_passes_through_prices_and_nulls_trades_count(self):
        element = {
            "candle_date": date(2024, 1, 7),
            "price_open": 1.0,
            "price_high": 2.0,
            "price_low": 0.5,
            "price_close": 1.5,
            "volume_traded": 100.0,
            "source_id": 7,
        }
        row = _normalise(element)
        assert row["price_open"] == 1.0
        assert row["price_high"] == 2.0
        assert row["price_low"] == 0.5
        assert row["price_close"] == 1.5
        assert row["volume_traded"] == 100.0
        assert row["source_id"] == 7
        assert row["trades_count"] is None

    def test_missing_optional_fields_become_none(self):
        row = _normalise({"candle_date": date(2024, 1, 7)})
        assert row["price_close"] is None
        assert row["source_id"] is None
