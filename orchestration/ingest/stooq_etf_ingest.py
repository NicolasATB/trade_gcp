"""Multi-asset ETF ingestion: stooq daily CSV -> BigQuery bronze (fallback).

Downloads the daily OHLC bars for the eight non-crypto ETFs of the frozen T-19
universe (``orchestration.ingest.strategy_3_universe.ETF_UNIVERSE``) from stooq and upserts
them into ``prod_trade_bronze.stooq_etf_daily_raw``. stooq is the **fallback**
source for these classes (``yahoo_etf_ingest`` is primary); the two compete by
``priority`` in the silver consolidation (T-21), so a source that starts gapping
fails over to the other. Strategy 3 (cross-asset TSMOM).

Thin entry-point: the provider-specific fetch/parse lives in ``stooq_common`` and
the provider-agnostic row mapping + idempotent MERGE on ``(symbol, candle_date)``
in ``ohlcv_bronze_common``. This module only pins the config (target table,
``source_id``) and drives the universe. Bronze is raw — the Yahoo↔stooq
consolidation is a downstream silver step, not done here.

Same shape as ``yahoo_etf_ingest``: ``--backfill`` (full history) /
``--start/--end`` / no args (recent window). "Closed bar only": the in-progress
current-UTC-day bar is dropped; the next run settles it (idempotent MERGE). The
table partitions by ``DATE_TRUNC(candle_date, MONTH)`` (10000-partitions cap).

Authentication: none for stooq; Google Application Default Credentials for
BigQuery.

Run standalone:
  python -m orchestration.ingest.stooq_etf_ingest --backfill                 # full history, all ETFs
  python -m orchestration.ingest.stooq_etf_ingest --start 2026-06-01 --end 2026-06-10
  python -m orchestration.ingest.stooq_etf_ingest --symbol SPY --backfill    # one ETF
  python -m orchestration.ingest.stooq_etf_ingest                            # recent window (daily)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery

from orchestration.ingest import ohlcv_bronze_common as ohlcv
from orchestration.ingest import stooq_common
from orchestration.ingest.strategy_3_universe import ETF_UNIVERSE, Instrument

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment variables) -------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")
BQ_TABLE = os.environ.get("BQ_STOOQ_ETF_TABLE", "stooq_etf_daily_raw")
SOURCE_ID = int(os.environ.get("STOOQ_ETF_SOURCE_ID", "17"))
DAILY_LOOKBACK_DAYS = int(os.environ.get("STOOQ_ETF_DAILY_LOOKBACK_DAYS", "10"))


def _instruments(symbol: str | None = None) -> tuple[Instrument, ...]:
    """The ETF universe, optionally narrowed to a single ``symbol``."""
    if symbol is None:
        return ETF_UNIVERSE
    wanted = symbol.upper()
    picked = tuple(i for i in ETF_UNIVERSE if i.symbol == wanted)
    if not picked:
        known = ", ".join(i.symbol for i in ETF_UNIVERSE)
        raise ValueError(f"Unknown ETF symbol {symbol!r}. Known: {known}.")
    return picked


def _ingest_instruments(
    instruments: tuple[Instrument, ...],
    start_date: date | None,
    end_date: date | None,
    fetched_at: datetime,
    client: bigquery.Client,
    today: date | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch each instrument's CSV for the range and upsert the closed bars.

    Each symbol is fetched and MERGEd independently so one provider hiccup never
    loses the rest; the per-symbol MERGE keeps the whole run idempotent.
    """
    all_rows: list[dict] = []
    for inst in instruments:
        text = stooq_common.fetch_csv(inst.stooq_ticker, start_date, end_date)
        records = stooq_common.parse_csv(text)
        rows = ohlcv.prepare_rows(inst.symbol, records, SOURCE_ID, fetched_at, today=today)
        if not rows:
            logger.warning("stooq ETF %s: no closed bars for the requested range", inst.symbol)
            continue
        logger.info(
            "stooq ETF %s: upserting %d bar(s) %s..%s",
            inst.symbol, len(rows), rows[0]["candle_date"].isoformat(),
            rows[-1]["candle_date"].isoformat(),
        )
        ohlcv.upsert_rows(client, BQ_TABLE, rows)
        all_rows.extend(rows)
    return all_rows


# --- Public entry points ------------------------------------------------------

def backfill_history(
    symbol: str | None = None,
    client: bigquery.Client | None = None,
) -> int:  # pragma: no cover - live network
    """Back-fill the full ETF history from stooq into bronze (no date bounds)."""
    fetched_at = datetime.now(timezone.utc)
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    rows = _ingest_instruments(_instruments(symbol), None, None, fetched_at, client)
    logger.info("stooq ETF back-fill complete: %d bar(s) written", len(rows))
    return len(rows)


def ingest_range(
    start_date: date,
    end_date: date,
    symbol: str | None = None,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch ``[start_date, end_date]`` from stooq and upsert the closed bars."""
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    fetched_at = datetime.now(timezone.utc)
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    return _ingest_instruments(_instruments(symbol), start_date, end_date, fetched_at, client)


def ingest_latest(
    symbol: str | None = None,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Refresh a recent window (last ``DAILY_LOOKBACK_DAYS`` days) for the daily job."""
    today = datetime.now(timezone.utc).date()
    return ingest_range(today - timedelta(days=DAILY_LOOKBACK_DAYS), today, symbol=symbol, client=client)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest daily ETF bars (Strategy 3 universe) from stooq into BigQuery bronze."
    )
    def iso_date(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full history from stooq (ignores --start/--end).",
    )
    parser.add_argument(
        "--start", type=iso_date, default=None,
        help="First date to ingest (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end", type=iso_date, default=None,
        help="Last date (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    parser.add_argument(
        "--symbol", default=None,
        help="Restrict to a single universe symbol (e.g. SPY). Defaults to all ETFs.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover - CLI wiring
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    if args.backfill:
        n = backfill_history(symbol=args.symbol)
        logger.info("Back-filled %d bar(s)", n)
    elif args.start is not None:
        end = args.end if args.end is not None else args.start
        rows = ingest_range(args.start, end, symbol=args.symbol)
        logger.info("Wrote %d row(s)", len(rows))
    else:
        rows = ingest_latest(symbol=args.symbol)
        logger.info("Wrote %d row(s)", len(rows))


if __name__ == "__main__":  # pragma: no cover
    main()
