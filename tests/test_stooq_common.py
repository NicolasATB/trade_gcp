"""Unit tests for the shared stooq CSV fetch/parse (``orchestration/ingest/stooq_common.py``).

Covers ``parse_csv`` (header handling, OHLC mapping, missing-close skip, ``N/D``
markers, sort) and ``_parse_float``. No live network. The sample bodies mirror
the live stooq daily CSV response.
"""

from __future__ import annotations

from datetime import date

import pytest

from orchestration.ingest.stooq_common import _parse_float, parse_csv

_CSV = (
    "Date,Open,High,Low,Close,Volume\n"
    "2026-06-01,99.0,99.9,98.7,99.8,1000\n"
    "2026-06-02,99.5,100.1,99.2,100.0,2000\n"
)


class TestParseCsv:
    def test_one_record_per_row(self):
        records = parse_csv(_CSV)
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_maps_ohlc(self):
        first = parse_csv(_CSV)[0]
        assert first["open"] == 99.0 and first["high"] == 99.9
        assert first["low"] == 98.7 and first["close"] == 99.8
        assert first["volume"] == 1000.0

    def test_sorts_ascending(self):
        scrambled = (
            "Date,Open,High,Low,Close,Volume\n"
            "2026-06-02,99.5,100.1,99.2,100.0,2000\n"
            "2026-06-01,99.0,99.9,98.7,99.8,1000\n"
        )
        records = parse_csv(scrambled)
        assert [r["date"] for r in records] == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_skips_rows_with_missing_close(self):
        body = (
            "Date,Open,High,Low,Close,Volume\n"
            "2026-06-01,99.0,99.9,98.7,N/D,0\n"
            "2026-06-02,99.5,100.1,99.2,100.0,2000\n"
        )
        records = parse_csv(body)
        assert [r["date"] for r in records] == [date(2026, 6, 2)]

    def test_missing_volume_is_none(self):
        body = "Date,Open,High,Low,Close,Volume\n2026-06-01,99.0,99.9,98.7,99.8,\n"
        assert parse_csv(body)[0]["volume"] is None

    def test_unknown_ticker_body_yields_no_records(self):
        # stooq returns a body like "No data" (no header) for an unknown ticker.
        assert parse_csv("No data\n") == []

    def test_empty_body_yields_no_records(self):
        assert parse_csv("") == []

    def test_html_challenge_body_raises(self):
        # A JS/PoW bot-challenge page must surface as an error, not "no data".
        html = '<!DOCTYPE html><html><head></head><body><noscript>enable JS</noscript></body></html>'
        with pytest.raises(ValueError, match="HTML body"):
            parse_csv(html)


class TestParseFloat:
    def test_parses_number(self):
        assert _parse_float("99.8") == 99.8

    def test_missing_markers_become_none(self):
        for raw in ("", "N/D", "n/d", "NaN", "null", None):
            assert _parse_float(raw) is None
