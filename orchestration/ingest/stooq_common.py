"""Shared stooq daily CSV fetch + parse (bronze ingest primitive).

Not an entry-point. Holds the provider-specific half of the stooq ingest: the
public, no-key daily CSV endpoint call and the parse into provider-agnostic OHLC
records (the same shape ``yahoo_common.parse_chart`` returns), so the multi-asset
ETF ingest (``stooq_etf_ingest``) shares ``ohlcv_bronze_common`` with the Yahoo
path. stooq is the **fallback** source for the non-crypto classes; it competes
with Yahoo by ``priority`` in the silver consolidation (T-21).

The daily CSV endpoint
(``https://stooq.com/q/d/l/?s=<ticker>&i=d``) returns a header line
``Date,Open,High,Low,Close,Volume`` followed by one ascending row per trading
day; ``d1``/``d2`` (``YYYYMMDD``) bound the range. A row whose close is missing
(``N/D`` / empty) is skipped, so bronze holds only real bars. When the ticker is
unknown stooq returns a body like ``No data`` (no header) — parsed as no records.

Authentication: none for stooq, but a browser-like User-Agent is required (the
default urllib UA gets a 404). stooq may also gate the keyless CSV behind a
JavaScript proof-of-work challenge for some clients/IPs; that returns an HTML
body instead of CSV, which :func:`parse_csv` surfaces as a clear error rather
than silently treating it as "no data" (we do not try to solve the challenge).
"""

from __future__ import annotations

import csv
import io
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

API_BASE = os.environ.get("STOOQ_API_BASE", "https://stooq.com/q/d/l/")
HTTP_TIMEOUT = int(os.environ.get("STOOQ_HTTP_TIMEOUT", "30"))
# stooq 404s the default urllib UA; a browser-like UA is required (as for Yahoo).
USER_AGENT = os.environ.get(
    "STOOQ_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
)

_MISSING = {"", "n/d", "nan", "null"}


def _parse_float(raw) -> float | None:
    """Parse a stooq CSV cell to float; missing markers (``N/D``/empty) -> ``None``."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text.lower() in _MISSING:
        return None
    return float(text)


def parse_csv(text: str) -> list[dict]:
    """Parse a stooq daily CSV body into raw OHLC records.

    Returns one ``{"date", "open", "high", "low", "close", "volume"}`` per row,
    ascending by date (stooq already delivers them ascending; we sort to be
    safe). Rows with a missing close are skipped. A body without the expected
    header (e.g. ``No data`` for an unknown ticker) yields no records.
    """
    stripped = text.lstrip()
    if stripped.startswith("<"):
        # stooq served an HTML page instead of CSV — a JavaScript proof-of-work
        # bot challenge for this client/IP. Surface it clearly instead of
        # silently returning no rows (we do not attempt to solve the challenge).
        raise ValueError(
            "stooq returned an HTML body, not CSV — likely a JavaScript/PoW bot "
            "challenge for this IP. Retry from another network (e.g. the VM) or "
            "use the Yahoo primary source."
        )
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "Date" not in reader.fieldnames:
        return []
    records: list[dict] = []
    for row in reader:
        close = _parse_float(row.get("Close"))
        if close is None:  # no real bar — skip
            continue
        records.append(
            {
                "date": datetime.strptime(row["Date"].strip(), "%Y-%m-%d").date(),
                "open": _parse_float(row.get("Open")),
                "high": _parse_float(row.get("High")),
                "low": _parse_float(row.get("Low")),
                "close": close,
                "volume": _parse_float(row.get("Volume")),
            }
        )
    records.sort(key=lambda r: r["date"])
    return records


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


def fetch_csv(
    ticker: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:  # pragma: no cover - live network
    """Fetch the daily CSV for ``ticker``; ``[start_date, end_date]`` bound it if given."""
    params = {"s": ticker, "i": "d"}
    if start_date is not None:
        params["d1"] = start_date.strftime("%Y%m%d")
    if end_date is not None:
        params["d2"] = end_date.strftime("%Y%m%d")
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    return _http_get(url)
