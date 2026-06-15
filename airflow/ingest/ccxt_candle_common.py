"""Shared CCXT daily-candle ingestion logic (bronze loader).

This module holds the logic common to every CCXT exchange we ingest BTC candles
from; it is **not an entry-point**. Each exchange has a thin
``<source>_<symbol>_ingest.py`` wrapper (e.g. ``binance_btc_ingest.py``,
``bitstamp_btc_ingest.py``) that pins a :class:`CcxtCandleSource` config and
re-exports the bound functions. Keeping one implementation here means a fix
(e.g. the CCXT pagination-truncation fix) lands once for all exchanges; the
bronze schema and the idempotent MERGE on ``(symbol, candle_date)`` are identical
across them, so only the config (exchange id, symbol, target table, source id)
differs.

Authentication for BigQuery is Google Application Default Credentials, so the
same code runs locally and on the VM/CI.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
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
TIMEFRAME = "1d"

_MS_PER_DAY = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class CcxtCandleSource:
    """Per-exchange config for the shared CCXT candle ingest.

    The thin entry-points build one of these and pass it to the functions below;
    everything that differs between exchanges lives here.
    """

    exchange_id: str   # CCXT id, e.g. "binance" / "bitstamp"
    symbol: str        # CCXT unified symbol, e.g. "BTC/USDT" / "BTC/USD"
    table: str         # bronze table name (in BQ_DATASET)
    source_id: int     # FK into prod_trade_control.source_priority


def _table_fqn(table: str) -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{table}"


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
    cfg: CcxtCandleSource,
    start_date: date,
    end_date: date,
    exchange=None,
) -> list:
    """Return the closed daily candles for ``[start_date, end_date]`` (UTC, inclusive).

    Each item is the CCXT tuple ``[open_time_ms, open, high, low, close,
    volume]``, sorted by ascending open time. The range is bounded with CCXT's
    unified ``since`` (start) and ``params['until']`` (end), and
    ``params['paginate']`` lets CCXT walk the exchange's 1000-candle pages for
    us. With transparent pagination, ``limit`` is the TOTAL number of candles
    requested (not the per-page size), so it must cover the whole range —
    passing a constant 1000 would silently truncate back-fills longer than
    1000 days at the first page.
    Candles whose day has not fully closed yet (open time within the last 24 h)
    are dropped, keeping the "closed candle only" rule.

    Transient network errors (``ccxt.NetworkError`` subclasses) retry the whole
    range up to 5 times with exponential back-off + jitter (max 30 s per wait).
    """
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date "
            f"({start_date.isoformat()})."
        )
    if exchange is None:
        exchange = getattr(ccxt, cfg.exchange_id)({"enableRateLimit": True})

    since = _day_start_ms(start_date)
    until = _day_start_ms(end_date)
    # A daily candle for day D is closed once D+1 has started.
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    total_days = (end_date - start_date).days + 1

    candles = exchange.fetch_ohlcv(
        cfg.symbol,
        timeframe=TIMEFRAME,
        since=since,
        limit=total_days,
        params={"until": until, "paginate": True},
    )

    closed = [c for c in candles if c[0] + _MS_PER_DAY <= now_ms]
    closed.sort(key=lambda c: c[0])
    return closed


def _build_row(candle, cfg: CcxtCandleSource, fetched_at: datetime | None = None) -> dict:
    open_time_ms, price_open, price_high, price_low, price_close, volume = candle
    candle_date = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).date()
    return {
        "source_id": cfg.source_id,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
        "symbol": cfg.symbol,
        "candle_date": candle_date,
        "open_time": int(open_time_ms),
        "price_open": float(price_open),
        "price_high": float(price_high),
        "price_low": float(price_low),
        "price_close": float(price_close),
        "volume_traded": float(volume),
    }


def _upsert_rows(client: bigquery.Client, table: str, rows: list[dict]) -> None:
    """Idempotent MERGE of one or more candle rows into ``table``.

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
    MERGE `{_table_fqn(table)}` AS T
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
    cfg: CcxtCandleSource,
    start_date: date | None = None,
    end_date: date | None = None,
    client: bigquery.Client | None = None,
) -> list[dict]:
    """Fetch every closed daily candle in ``[start_date, end_date]`` and upsert them.

    Both bounds default to yesterday (UTC), the most recent fully closed daily
    candle, so calling with no dates ingests just that one day (the daily job);
    passing an earlier ``start_date`` back-fills the range. Returns the rows that
    were written (empty if the range contains no closed candle). The MERGE on
    ``symbol + candle_date`` keeps overlapping back-fills idempotent.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    if start_date is None:
        start_date = yesterday
    if end_date is None:
        end_date = yesterday

    try:
        logger.info(
            "Fetching %s %s candles for %s..%s",
            cfg.symbol, TIMEFRAME, start_date.isoformat(), end_date.isoformat(),
        )
        fetched_at = datetime.now(timezone.utc)
        candles = fetch_daily_candles_range(cfg, start_date, end_date)
        if not candles:
            logger.warning(
                "No closed candles for %s in %s..%s",
                cfg.symbol, start_date.isoformat(), end_date.isoformat(),
            )
            return []
        rows = [_build_row(c, cfg, fetched_at=fetched_at) for c in candles]

        if client is None:  # pragma: no cover - live client
            client = bigquery.Client(project=PROJECT_ID)

        logger.info(
            "Upserting %d candle(s) %s..%s into %s",
            len(rows), rows[0]["candle_date"].isoformat(),
            rows[-1]["candle_date"].isoformat(), _table_fqn(cfg.table),
        )
        _upsert_rows(client, cfg.table, rows)
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
        description="Ingest daily BTC candles from a CCXT exchange into BigQuery bronze."
    )
    def iso_date(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument(
        "--start", type=iso_date, default=None,
        help="First UTC date to ingest (YYYY-MM-DD). Defaults to yesterday (UTC).",
    )
    parser.add_argument(
        "--end", type=iso_date, default=None,
        help="Last UTC date to ingest (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    return parser.parse_args(argv)


def run_cli(cfg: CcxtCandleSource, argv=None) -> None:  # pragma: no cover - CLI wiring
    """Shared CLI for the thin exchange entry-points."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    # End defaults to start so "--start D" ingests a single day; with neither,
    # ingest_daily_candles falls back to yesterday for both bounds.
    end = args.end if args.end is not None else args.start
    rows = ingest_daily_candles(cfg, start_date=args.start, end_date=end)
    logger.info("Wrote %d row(s)", len(rows))
