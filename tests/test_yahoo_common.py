"""Unit tests for the shared Yahoo chart fetch/parse (``orchestration/ingest/yahoo_common.py``).

Covers ``parse_chart`` (timestamp + OHLC arrays, null-close skip, sort,
error/empty handling) and ``epoch``, plus that ``yahoo_dxy_ingest`` re-exports
``parse_chart`` from here after the T-20 extraction (so the DXY ingest and the
ETF ingest share one parser). No live network.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from orchestration.ingest import yahoo_common
from orchestration.ingest.yahoo_common import epoch, parse_chart


def _ts(y, m, d, hh=13, mm=30) -> int:
    """Epoch seconds at a NY-afternoon UTC time (so the UTC date is unambiguous)."""
    return int(datetime(y, m, d, hh, mm, tzinfo=timezone.utc).timestamp())


def _payload():
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
                                "volume": [1000, 2000, None],
                            }
                        ]
                    },
                }
            ],
        }
    }


class TestParseChart:
    def test_builds_one_record_per_closed_bar(self):
        records = parse_chart(_payload())
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_skips_null_close(self):
        assert all(r["close"] is not None for r in parse_chart(_payload()))

    def test_maps_ohlc(self):
        first = parse_chart(_payload())[0]
        assert first["open"] == 99.0 and first["high"] == 99.9
        assert first["low"] == 98.7 and first["close"] == 99.8
        assert first["volume"] == 1000

    def test_sorts_ascending(self):
        payload = _payload()
        payload["chart"]["result"][0]["timestamp"] = [_ts(2026, 6, 2), _ts(2026, 6, 1), _ts(2026, 6, 3)]
        records = parse_chart(payload)
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_raises_on_error(self):
        with pytest.raises(ValueError):
            parse_chart({"chart": {"error": {"code": "Not Found"}, "result": None}})

    def test_empty_result_returns_empty(self):
        assert parse_chart({"chart": {"error": None, "result": []}}) == []
        assert parse_chart({}) == []


class TestEpoch:
    def test_epoch_is_utc_midnight(self):
        assert epoch(date(2026, 6, 1)) == int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp())


def test_dxy_ingest_reuses_the_shared_parser():
    from orchestration.ingest import yahoo_dxy_ingest

    assert yahoo_dxy_ingest.parse_chart is yahoo_common.parse_chart
