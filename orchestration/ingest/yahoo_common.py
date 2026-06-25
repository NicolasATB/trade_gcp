"""Shared Yahoo Finance chart fetch + parse (bronze ingest primitive).

Not an entry-point. Holds the provider-specific half of every Yahoo ingest: the
public, no-key chart endpoint call and the JSON parse into provider-agnostic OHLC
records. Both the single-symbol DXY index ingest (``yahoo_dxy_ingest``) and the
multi-asset ETF ingest (``yahoo_etf_ingest``) build on it; the symbol, target
table and natural key differ downstream, the chart shape does not.

The public chart endpoint
(``https://query1.finance.yahoo.com/v8/finance/chart/<ticker>``) returns daily
OHLC bars: ``period1``/``period2`` (epoch seconds) bound the range, and the
response carries a ``timestamp`` array plus ``indicators.quote[0]`` OHLC arrays.
Bars Yahoo returns with a null close (e.g. holidays inside the range) are skipped.

Authentication: none for Yahoo (a browser-like User-Agent is required; the
default urllib UA is rejected).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

API_BASE = os.environ.get("YAHOO_API_BASE", "https://query1.finance.yahoo.com/v8/finance/chart")
# Yahoo rejects default urllib UAs; a browser-like UA is required.
USER_AGENT = os.environ.get(
    "YAHOO_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
)
HTTP_TIMEOUT = int(os.environ.get("YAHOO_HTTP_TIMEOUT", "30"))


def parse_chart(payload: dict) -> list[dict]:
    """Parse a Yahoo chart JSON payload into raw OHLC records.

    Returns one ``{"date", "open", "high", "low", "close", "volume"}`` per daily
    bar, ascending by date. The bar date is the UTC date of the Yahoo timestamp
    (a NY index/ETF opens in the afternoon UTC, so the calendar date is
    unambiguous). Bars with a null close (holidays inside the range) are skipped.
    Raises ``ValueError`` if Yahoo reports an error.
    """
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise ValueError(f"Yahoo chart error: {chart['error']}")
    results = chart.get("result") or []
    if not results:
        return []
    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_blocks = (result.get("indicators") or {}).get("quote") or [{}]
    quote = quote_blocks[0] or {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    records: list[dict] = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        if close is None:  # incomplete/holiday bar — skip
            continue
        bar_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        records.append(
            {
                "date": bar_date,
                "open": opens[i] if i < len(opens) else None,
                "high": highs[i] if i < len(highs) else None,
                "low": lows[i] if i < len(lows) else None,
                "close": close,
                "volume": volumes[i] if i < len(volumes) else None,
            }
        )
    records.sort(key=lambda r: r["date"])
    return records


def epoch(d: date) -> int:
    """Epoch seconds at 00:00 UTC of ``d`` (chart ``period1``/``period2`` bound)."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


@retry(
    retry=retry_if_exception_type(urllib.error.URLError),
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _http_get(url: str) -> str:  # pragma: no cover - live network
    """GET ``url`` with a browser UA; retried on transient URLErrors."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8")


def fetch_chart(symbol: str, period1: int, period2: int) -> dict:  # pragma: no cover - live network
    """Fetch the daily chart for ``symbol`` over ``[period1, period2]`` (epoch s)."""
    params = {"period1": period1, "period2": period2, "interval": "1d", "events": "history"}
    url = f"{API_BASE}/{urllib.parse.quote(symbol)}?{urllib.parse.urlencode(params)}"
    return json.loads(_http_get(url))
