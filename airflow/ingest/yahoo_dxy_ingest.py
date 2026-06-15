"""DXY ingestion: Yahoo Finance chart API -> BigQuery bronze.

Downloads the daily ICE U.S. Dollar Index (DXY, Yahoo symbol ``DX-Y.NYB``) and
upserts the OHLC bars into ``prod_trade_bronze.yahoo_dxy_daily_raw``. The upsert
(MERGE on the business key ``dxy_date``) makes re-runs idempotent: replaying any
day never duplicates a bar.

The public, no-key chart endpoint
(``https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB``) returns the
real ICE DXY (6-currency) with history since 1971 — unlike the Fed's broad
``DTWEXBGS`` proxy. ``period1``/``period2`` (epoch seconds) bound the range; the
response carries a ``timestamp`` array plus ``indicators.quote[0]`` OHLC arrays.

Two entry points share one idempotent upsert:

  * **Back-fill** the full history once (``--backfill``): ``period1=0`` to now.
  * **Daily refresh** (no args, or ``--start/--end``): a recent window, MERGEd in.

"Closed bar only": the in-progress bar for the current UTC day is dropped (its
close is not final yet), mirroring the Binance ingest. The next day's run fills
that date with the settled value (idempotent MERGE). Bars Yahoo returns with a
null close (e.g. holidays inside the range) are skipped.

The table partitions by ``DATE_TRUNC(dxy_date, MONTH)`` (not by day): ~55 years
of daily bars since 1971 would exceed BigQuery's hard 10000-partitions-per-table
limit at daily granularity. The back-fill is still chunked to
``MAX_MERGE_PARTITIONS`` rows as a safety margin under the 4000-partitions-per-DML
cap (with monthly partitions a full back-fill touches well under that). See the
bigquery-beam-patterns skill.

Authentication: none for Yahoo; Google Application Default Credentials for
BigQuery, so the same code runs locally and on the VM/CI.

Run standalone:
  python -m airflow.ingest.yahoo_dxy_ingest --backfill                 # full history
  python -m airflow.ingest.yahoo_dxy_ingest --start 2026-06-01 --end 2026-06-10
  python -m airflow.ingest.yahoo_dxy_ingest                            # recent window (daily)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment variables) -------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")
BQ_DATASET = os.environ.get("BQ_BRONZE_DATASET", "prod_trade_bronze")
BQ_TABLE = os.environ.get("BQ_DXY_TABLE", "yahoo_dxy_daily_raw")
SYMBOL = os.environ.get("YAHOO_SYMBOL", "DX-Y.NYB")
SOURCE_ID = int(os.environ.get("YAHOO_SOURCE_ID", "6"))

API_BASE = os.environ.get("YAHOO_API_BASE", "https://query1.finance.yahoo.com/v8/finance/chart")
# Yahoo rejects default urllib UAs; a browser-like UA is required.
USER_AGENT = os.environ.get(
    "YAHOO_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
)
HTTP_TIMEOUT = int(os.environ.get("YAHOO_HTTP_TIMEOUT", "30"))
DAILY_LOOKBACK_DAYS = int(os.environ.get("YAHOO_DAILY_LOOKBACK_DAYS", "10"))

# A single MERGE (DML) may modify at most 4000 partitions; the table partitions
# by dxy_date, so a full back-fill (>4000 days since 1971) must be chunked.
MAX_MERGE_PARTITIONS = int(os.environ.get("YAHOO_MAX_MERGE_PARTITIONS", "3500"))


def _table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


# --- Parsing / row building (pure, unit-tested) ------------------------------

def parse_chart(payload: dict) -> list[dict]:
    """Parse a Yahoo chart JSON payload into raw OHLC records.

    Returns one ``{"date", "open", "high", "low", "close", "volume"}`` per daily
    bar, ascending by date. The bar date is the UTC date of the Yahoo timestamp
    (a NY index opens in the afternoon UTC, so the calendar date is unambiguous).
    Bars with a null close (holidays inside the range) are skipped. Raises
    ``ValueError`` if Yahoo reports an error.
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


def _build_row(record: dict, fetched_at: datetime | None = None) -> dict:
    """Map one OHLC record to a bronze table row (pure mapping)."""
    return {
        "dxy_date": record["date"],
        "price_open": _as_float(record.get("open")),
        "price_high": _as_float(record.get("high")),
        "price_low": _as_float(record.get("low")),
        "price_close": _as_float(record.get("close")),
        "volume_traded": _as_float(record.get("volume")),
        "source_id": SOURCE_ID,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _as_float(value) -> float | None:
    return None if value is None else float(value)


def _closed_only(rows: list[dict], today: date | None = None) -> list[dict]:
    """Drop the in-progress bar for the current UTC day (close not final yet)."""
    today = today or datetime.now(timezone.utc).date()
    return [r for r in rows if r["dxy_date"] < today]


def _prepare_rows(records: list[dict], fetched_at: datetime, today: date | None = None) -> list[dict]:
    """Build rows and keep only fully closed bars."""
    rows = [_build_row(r, fetched_at=fetched_at) for r in records]
    return _closed_only(rows, today=today)


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural key ``dxy_date``. Source rows are deduped
# within the batch (latest datetime_update wins) so MERGE never sees a key twice.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.dxy_date ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.dxy_date = S.dxy_date
WHEN MATCHED THEN UPDATE SET
  price_open      = S.price_open,
  price_high      = S.price_high,
  price_low       = S.price_low,
  price_close     = S.price_close,
  volume_traded   = S.volume_traded,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  dxy_date, price_open, price_high, price_low, price_close,
  volume_traded, source_id, datetime_update
) VALUES (
  S.dxy_date, S.price_open, S.price_high, S.price_low, S.price_close,
  S.volume_traded, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("dxy_date", "DATE", row["dxy_date"]),
        bigquery.ScalarQueryParameter("price_open", "FLOAT64", row["price_open"]),
        bigquery.ScalarQueryParameter("price_high", "FLOAT64", row["price_high"]),
        bigquery.ScalarQueryParameter("price_low", "FLOAT64", row["price_low"]),
        bigquery.ScalarQueryParameter("price_close", "FLOAT64", row["price_close"]),
        bigquery.ScalarQueryParameter("volume_traded", "FLOAT64", row["volume_traded"]),
        bigquery.ScalarQueryParameter("source_id", "INT64", row["source_id"]),
        bigquery.ScalarQueryParameter("datetime_update", "TIMESTAMP", row["datetime_update"]),
    )


def _upsert_chunk(client: bigquery.Client, rows: list[dict]) -> None:
    struct_params = [_struct_param(r) for r in rows]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)]
    )
    client.query(_MERGE_SQL.format(table=_table_fqn()), job_config=job_config).result()


def _upsert_rows(client: bigquery.Client, rows: list[dict], chunk_size: int | None = None) -> None:
    """Idempotent MERGE of DXY rows, chunked to stay under the partition limit.

    Each row is a distinct ``dxy_date`` (= one partition), so a chunk of
    ``chunk_size`` rows touches at most that many partitions, keeping every MERGE
    below BigQuery's 4000-partitions-per-DML cap. Replaying any range MERGEs the
    same keys (idempotent).
    """
    if not rows:
        return
    size = chunk_size or MAX_MERGE_PARTITIONS
    for chunk in _chunked(rows, size):
        _upsert_chunk(client, chunk)


# --- HTTP fetch (live I/O) ----------------------------------------------------

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


def fetch_chart(period1: int, period2: int) -> dict:  # pragma: no cover - live network
    """Fetch the daily chart for ``[period1, period2]`` (epoch seconds) as JSON."""
    params = {"period1": period1, "period2": period2, "interval": "1d", "events": "history"}
    url = f"{API_BASE}/{urllib.parse.quote(SYMBOL)}?{urllib.parse.urlencode(params)}"
    return json.loads(_http_get(url))


def _epoch(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


# --- Public entry points ------------------------------------------------------

def backfill_history(client: bigquery.Client | None = None) -> int:  # pragma: no cover - live network
    """Back-fill the full DXY history from Yahoo into bronze (period1=0 to now)."""
    fetched_at = datetime.now(timezone.utc)
    now = int(fetched_at.timestamp())
    payload = fetch_chart(0, now)
    rows = _prepare_rows(parse_chart(payload), fetched_at)
    if not rows:
        logger.warning("DXY back-fill: no closed bars parsed from Yahoo")
        return 0
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    logger.info(
        "DXY back-fill: upserting %d bar(s) %s..%s into %s",
        len(rows), rows[0]["dxy_date"].isoformat(),
        rows[-1]["dxy_date"].isoformat(), _table_fqn(),
    )
    _upsert_rows(client, rows)
    logger.info("DXY back-fill complete: %d bar(s) written", len(rows))
    return len(rows)


def ingest_range(
    start_date: date,
    end_date: date,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch ``[start_date, end_date]`` from Yahoo and upsert the closed bars."""
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    fetched_at = datetime.now(timezone.utc)
    # +1 day on period2 so the end date is included (period2 is exclusive-ish).
    payload = fetch_chart(_epoch(start_date), _epoch(end_date + timedelta(days=1)))
    rows = _prepare_rows(parse_chart(payload), fetched_at)
    if not rows:
        logger.warning("DXY range: no closed bars for %s..%s", start_date, end_date)
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    _upsert_rows(client, rows)
    return rows


def ingest_latest(client: bigquery.Client | None = None) -> list[dict]:  # pragma: no cover - live network
    """Refresh a recent window (last ``DAILY_LOOKBACK_DAYS`` days) for the daily job."""
    today = datetime.now(timezone.utc).date()
    return ingest_range(today - timedelta(days=DAILY_LOOKBACK_DAYS), today, client=client)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest daily DXY (ICE U.S. Dollar Index) from Yahoo Finance into BigQuery bronze."
    )
    def iso_date(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full history from Yahoo (ignores --start/--end).",
    )
    parser.add_argument(
        "--start", type=iso_date, default=None,
        help="First date to ingest (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end", type=iso_date, default=None,
        help="Last date (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover - CLI wiring
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    if args.backfill:
        n = backfill_history()
        logger.info("Back-filled %d bar(s)", n)
    elif args.start is not None:
        end = args.end if args.end is not None else args.start
        rows = ingest_range(args.start, end)
        logger.info("Wrote %d row(s)", len(rows))
    else:
        rows = ingest_latest()
        logger.info("Wrote %d row(s)", len(rows))


if __name__ == "__main__":  # pragma: no cover
    main()
