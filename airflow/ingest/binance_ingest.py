"""Daily BTC candle ingestion: Binance (via CCXT) -> BigQuery bronze.

Downloads daily BTC/USDT candles from Binance and upserts them into
``prod_trade_bronze.binance_btcusd_daily_raw``. The upsert (MERGE on the
business key ``symbol + candle_date``) makes re-runs idempotent: replaying any
day never duplicates a candle.

``ingest_daily_candles`` handles both the daily job (no arguments -> yesterday's
candle) and back-fills (pass an earlier ``start_date``); a range is fetched with
CCXT's ``since``/``until``/``paginate``. Designed to run as an Airflow
``PythonOperator`` and to be runnable standalone for local testing:
``python -m airflow.ingest.binance_ingest --start 2024-01-01 --end 2024-01-31``.

Authentication is handled by Google Application Default Credentials, so the same
code works in every environment without changes:
  * Local: ``gcloud auth application-default login`` (ADC), or set
    ``GOOGLE_APPLICATION_CREDENTIALS`` to the service-account JSON.
  * VM/CI: ``GOOGLE_APPLICATION_CREDENTIALS`` pointing at the service-account
    key, or the VM's attached service account (metadata server).
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone

import ccxt
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
BQ_TABLE = os.environ.get("BQ_BRONZE_TABLE", "binance_btcusd_daily_raw")
EXCHANGE_ID = os.environ.get("CCXT_EXCHANGE", "binance")
SYMBOL = os.environ.get("BINANCE_SYMBOL", "BTC/USDT")
TIMEFRAME = "1d"
SOURCE_ID = int(os.environ.get("BINANCE_SOURCE_ID", "3"))

_MS_PER_DAY = 24 * 60 * 60 * 1000


def _table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


def _day_start_ms(target_date: date) -> int:
    """Epoch milliseconds at 00:00 UTC of ``target_date``."""
    dt = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ccxt.NetworkError is the base class for transient errors (RequestTimeout,
# ExchangeNotAvailable, DDoSProtection…). BadSymbol and AuthenticationError are
# permanent failures — not retried.
@retry(
    retry=retry_if_exception_type(ccxt.NetworkError),
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_daily_candles_range(
    start_date: date,
    end_date: date,
    exchange=None,
    symbol: str = SYMBOL,
) -> list:
    """Return the closed daily candles for ``[start_date, end_date]`` (UTC, inclusive).

    Each item is the CCXT tuple ``[open_time_ms, open, high, low, close,
    volume]``, sorted by ascending open time. The range is bounded with CCXT's
    unified ``since`` (start) and ``params['until']`` (end), and
    ``params['paginate']`` lets CCXT walk Binance's 1000-candle pages for us.
    With transparent pagination, ``limit`` is the TOTAL number of candles
    requested (not the per-page size), so it must cover the whole range —
    passing a constant 1000 would silently truncate back-fills longer than
    1000 days at the first page.
    Candles whose day has not fully closed yet (open time within the last 24 h)
    are dropped, mirroring the "closed candle only" rule of ``fetch_daily_candle``.

    Transient network errors (``ccxt.NetworkError`` subclasses) retry the whole
    range up to 5 times with exponential back-off + jitter (max 30 s per wait).
    """
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date "
            f"({start_date.isoformat()})."
        )
    if exchange is None:
        exchange = getattr(ccxt, EXCHANGE_ID)({"enableRateLimit": True})

    since = _day_start_ms(start_date)
    until = _day_start_ms(end_date)
    # A daily candle for day D is closed once D+1 has started.
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    total_days = (end_date - start_date).days + 1

    candles = exchange.fetch_ohlcv(
        symbol,
        timeframe=TIMEFRAME,
        since=since,
        limit=total_days,
        params={"until": until, "paginate": True},
    )

    closed = [c for c in candles if c[0] + _MS_PER_DAY <= now_ms]
    closed.sort(key=lambda c: c[0])
    return closed


def _build_row(candle, symbol: str = SYMBOL, fetched_at: datetime | None = None) -> dict:
    open_time_ms, price_open, price_high, price_low, price_close, volume = candle
    candle_date = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).date()
    return {
        "source_id": SOURCE_ID,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
        "symbol": symbol,
        "candle_date": candle_date,
        "open_time": int(open_time_ms),
        "price_open": float(price_open),
        "price_high": float(price_high),
        "price_low": float(price_low),
        "price_close": float(price_close),
        "volume_traded": float(volume),
    }


def _upsert_rows(client: bigquery.Client, rows: list[dict]) -> None:
    """Idempotent MERGE of one or more candle rows into the bronze table.

    Rows are passed as a single ``ARRAY<STRUCT>`` query parameter and unnested
    server-side, so any batch (a single day or a full back-fill) is one MERGE
    with no per-row round trips. The MERGE key stays ``symbol + candle_date``,
    so re-running overlapping ranges never duplicates a candle.
    """
    if not rows:
        return

    struct_params = [
        bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter("source_id", "INT64", r["source_id"]),
            bigquery.ScalarQueryParameter("datetime_update", "TIMESTAMP", r["datetime_update"]),
            bigquery.ScalarQueryParameter("symbol", "STRING", r["symbol"]),
            bigquery.ScalarQueryParameter("candle_date", "DATE", r["candle_date"]),
            bigquery.ScalarQueryParameter("open_time", "INT64", r["open_time"]),
            bigquery.ScalarQueryParameter("price_open", "FLOAT64", r["price_open"]),
            bigquery.ScalarQueryParameter("price_high", "FLOAT64", r["price_high"]),
            bigquery.ScalarQueryParameter("price_low", "FLOAT64", r["price_low"]),
            bigquery.ScalarQueryParameter("price_close", "FLOAT64", r["price_close"]),
            bigquery.ScalarQueryParameter("volume_traded", "FLOAT64", r["volume_traded"]),
        )
        for r in rows
    ]
    query = f"""
    MERGE `{_table_fqn()}` AS T
    USING (
      -- Dedupe within the batch so MERGE never sees the same key twice.
      SELECT * EXCEPT(rn) FROM (
        SELECT s.*, ROW_NUMBER() OVER (
          PARTITION BY s.symbol, s.candle_date ORDER BY s.datetime_update DESC
        ) AS rn
        FROM UNNEST(@rows) AS s
      ) WHERE rn = 1
    ) AS S
    ON T.symbol = S.symbol AND T.candle_date = S.candle_date
    WHEN MATCHED THEN UPDATE SET
      source_id       = S.source_id,
      datetime_update = S.datetime_update,
      open_time       = S.open_time,
      price_open      = S.price_open,
      price_high      = S.price_high,
      price_low       = S.price_low,
      price_close     = S.price_close,
      volume_traded   = S.volume_traded
    WHEN NOT MATCHED THEN INSERT (
      source_id, datetime_update, symbol, candle_date, open_time,
      price_open, price_high, price_low, price_close, volume_traded
    ) VALUES (
      S.source_id, S.datetime_update, S.symbol, S.candle_date, S.open_time,
      S.price_open, S.price_high, S.price_low, S.price_close, S.volume_traded
    )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)]
    )
    client.query(query, job_config=job_config).result()


def ingest_daily_candles(
    start_date: date | None = None,
    end_date: date | None = None,
    client: bigquery.Client | None = None,
) -> list[dict]:
    """Fetch every closed daily BTC candle in ``[start_date, end_date]`` and upsert them.

    Both bounds default to yesterday (UTC), the most recent fully closed daily
    candle, so calling with no arguments ingests just that one day (the daily
    job); passing an earlier ``start_date`` back-fills the range. Returns the
    rows that were written (empty if the range contains no closed candle). The
    MERGE on ``symbol + candle_date`` keeps overlapping back-fills idempotent.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    if start_date is None:
        start_date = yesterday
    if end_date is None:
        end_date = yesterday

    try:
        logger.info(
            "Fetching %s %s candles for %s..%s",
            SYMBOL, TIMEFRAME, start_date.isoformat(), end_date.isoformat(),
        )
        fetched_at = datetime.now(timezone.utc)
        candles = fetch_daily_candles_range(start_date, end_date)
        if not candles:
            logger.warning(
                "No closed candles for %s in %s..%s",
                SYMBOL, start_date.isoformat(), end_date.isoformat(),
            )
            return []
        rows = [_build_row(c, fetched_at=fetched_at) for c in candles]

        if client is None:
            client = bigquery.Client(project=PROJECT_ID)

        logger.info(
            "Upserting %d candle(s) %s..%s into %s",
            len(rows), rows[0]["candle_date"].isoformat(),
            rows[-1]["candle_date"].isoformat(), _table_fqn(),
        )
        _upsert_rows(client, rows)
        logger.info("Ingestion complete: %d candle(s) written", len(rows))
        return rows
    except Exception:
        logger.exception(
            "Ingestion failed for %s..%s",
            start_date.isoformat(), end_date.isoformat(),
        )
        raise


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest daily BTC candles from Binance into BigQuery bronze."
    )
    iso_date = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
    parser.add_argument(
        "--start",
        type=iso_date,
        default=None,
        help="First UTC date to ingest (YYYY-MM-DD). Defaults to yesterday (UTC).",
    )
    parser.add_argument(
        "--end",
        type=iso_date,
        default=None,
        help="Last UTC date to ingest (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    # End defaults to start so "--start D" ingests a single day; with neither,
    # ingest_daily_candles falls back to yesterday for both bounds.
    end = args.end if args.end is not None else args.start
    rows = ingest_daily_candles(start_date=args.start, end_date=end)
    logger.info("Wrote %d row(s)", len(rows))


if __name__ == "__main__":
    main()
