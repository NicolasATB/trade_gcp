"""Unit tests for the bronze→silver normalisation in ``dataflow/stages/conform.py``.

Covers ``_NormaliseOhlcvRow.process``: the canonical symbol passthrough (the read
query already chose it — ``BTCUSD`` for BTC, the ETF symbol otherwise), the fixed
``1d`` temporality, the 00:00:00 / 23:59:59 day bounds, and accepting
``candle_date`` as either a ``date`` or an ISO string. No BigQuery/Beam IO.
"""

from __future__ import annotations

from datetime import date

import pytest

from dataflow.stages.conform import _NormaliseOhlcvRow


def _normalise(element: dict) -> dict:
    element = {"symbol": "BTCUSD", **element}
    (row,) = list(_NormaliseOhlcvRow().process(element))
    return row


class TestNormaliseOhlcvRow:
    @pytest.mark.parametrize("symbol", ["BTCUSD", "SPY", "EFA"])
    def test_symbol_passes_through(self, symbol):
        # The read query already canonicalised the symbol; the DoFn keeps it.
        row = _normalise({"candle_date": date(2024, 1, 7), "symbol": symbol})
        assert row["symbol"] == symbol

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
            "symbol": "SPY",
            "price_open": 1.0,
            "price_high": 2.0,
            "price_low": 0.5,
            "price_close": 1.5,
            "volume_traded": 100.0,
            "source_id": 16,
        }
        row = _normalise(element)
        assert row["price_open"] == 1.0
        assert row["price_high"] == 2.0
        assert row["price_low"] == 0.5
        assert row["price_close"] == 1.5
        assert row["volume_traded"] == 100.0
        assert row["source_id"] == 16
        assert row["trades_count"] is None

    def test_missing_optional_fields_become_none(self):
        row = _normalise({"candle_date": date(2024, 1, 7)})
        assert row["price_close"] is None
        assert row["source_id"] is None
