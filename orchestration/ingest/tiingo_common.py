"""Shared Tiingo daily EOD fetch + parse (bronze ingest primitive).

Not an entry-point. Holds the provider-specific half of the Tiingo ingest: the
token-authenticated daily EOD endpoint call and the JSON parse into
provider-agnostic OHLC records (the same shape ``yahoo_common.parse_chart``
returns), so the multi-asset ETF ingest (``tiingo_etf_ingest``) shares
``ohlcv_bronze_common`` with the Yahoo path. Tiingo is the **fallback** source
for the non-crypto classes; it competes with Yahoo by ``priority`` in the silver
consolidation (T-21), so a source that starts gapping fails over to the other.

The EOD endpoint
(``https://api.tiingo.com/tiingo/daily/<ticker>/prices``) returns a JSON array of
daily bars, one object per trading day with an ISO ``date`` and raw
``open``/``high``/``low``/``close``/``volume`` (plus adjusted fields we ignore —
bronze stores the **raw** bar, matching the Yahoo path, so the two sources are
comparable when they compete by priority). ``startDate``/``endDate``
(``YYYY-MM-DD``) bound the range. An unknown ticker yields a JSON object with a
``detail`` message (not an array), surfaced as a clear error; a valid ticker with
no bars in range yields an empty array.

Authentication: a free Tiingo API token, read from ``TIINGO_API_KEY`` (never
committed) and sent as an ``Authorization: Token <key>`` header. Unlike stooq's
keyless CSV — which a JavaScript/proof-of-work bot challenge made unreachable from
cloud IPs (the dev host and the VM), the reason it was dropped on 2026-06-25 —
Tiingo is a real token API, so it behaves identically from the dev host, the VM
and CI. That reliability is why it replaced stooq as the competing fallback.
"""

from __future__ import annotations

import json
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

API_BASE = os.environ.get("TIINGO_API_BASE", "https://api.tiingo.com/tiingo/daily")
HTTP_TIMEOUT = int(os.environ.get("TIINGO_HTTP_TIMEOUT", "30"))
API_KEY_ENV = "TIINGO_API_KEY"


def _api_key() -> str:
    """Return the Tiingo API token from ``TIINGO_API_KEY``, or raise a clear error."""
    key = os.environ.get(API_KEY_ENV, "").strip()
    if not key:
        raise RuntimeError(
            f"{API_KEY_ENV} is not set. Tiingo needs a free API token "
            "(https://www.tiingo.com); export it as an environment variable — "
            "never commit it."
        )
    return key


def _parse_float(value) -> float | None:
    return None if value is None else float(value)


def parse_prices(payload: list) -> list[dict]:
    """Parse a Tiingo EOD JSON array into raw OHLC records.

    Returns one ``{"date", "open", "high", "low", "close", "volume"}`` per bar,
    ascending by date; ``date`` is the calendar date of the ISO timestamp Tiingo
    returns. Bars with a missing close are skipped (bronze holds only real bars).
    Raises ``ValueError`` if Tiingo returned an error object instead of an array
    (e.g. ``{"detail": "Not found"}`` for an unknown ticker).
    """
    if isinstance(payload, dict):
        raise ValueError(f"Tiingo error: {payload.get('detail') or payload}")
    records: list[dict] = []
    for bar in payload:
        close = _parse_float(bar.get("close"))
        if close is None:  # no real bar — skip
            continue
        records.append(
            {
                "date": datetime.strptime(str(bar["date"])[:10], "%Y-%m-%d").date(),
                "open": _parse_float(bar.get("open")),
                "high": _parse_float(bar.get("high")),
                "low": _parse_float(bar.get("low")),
                "close": close,
                "volume": _parse_float(bar.get("volume")),
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
def _http_get(url: str, headers: dict) -> str:  # pragma: no cover - live network
    """GET ``url`` with the given headers; retried on transient URLErrors."""
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8")


def fetch_prices(
    ticker: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list:  # pragma: no cover - live network
    """Fetch the daily EOD bars for ``ticker``; ``[start_date, end_date]`` bound them."""
    params = {"format": "json"}
    if start_date is not None:
        params["startDate"] = start_date.strftime("%Y-%m-%d")
    if end_date is not None:
        params["endDate"] = end_date.strftime("%Y-%m-%d")
    url = f"{API_BASE}/{urllib.parse.quote(ticker)}/prices?{urllib.parse.urlencode(params)}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {_api_key()}",
    }
    return json.loads(_http_get(url, headers))
