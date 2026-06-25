"""Multi-asset ETF ingestion: Yahoo Finance chart API -> BigQuery bronze (primary).

Downloads the daily OHLC bars for the eight non-crypto ETFs of the frozen T-19
universe (``orchestration.ingest.strategy_3_universe.ETF_UNIVERSE``) from Yahoo Finance and
upserts them into ``prod_trade_bronze.yahoo_etf_daily_raw``. Yahoo is the
**primary** source for these classes; ``tiingo_etf_ingest`` is the competing
fallback (the two sources compete by ``priority`` in the silver consolidation,
T-21). Strategy 3 (cross-asset TSMOM); BTC reuses the existing spot ingest.

Thin entry-point: the provider-specific fetch/parse lives in ``yahoo_common`` and
the provider-agnostic row mapping + idempotent MERGE on ``(symbol, candle_date)``
in ``ohlcv_bronze_common``. This module only pins the config (target table,
``source_id``) and drives the universe. Bronze is raw — no stitching/dedup here;
the Yahoo↔Tiingo consolidation is a downstream silver step.

Two entry points share one idempotent upsert:

  * **Back-fill** the full history once (``--backfill``): ``period1=0`` to now.
  * **Daily refresh** (no args, or ``--start/--end``): a recent window, MERGEd in.

"Closed bar only": the in-progress bar for the current UTC day is dropped (its
close is not final yet). The next day's run fills that date with the settled
value (idempotent MERGE).

The table partitions by ``DATE_TRUNC(candle_date, MONTH)`` (not by day): SPY's
history since 1993 across several symbols would approach BigQuery's hard
10000-partitions-per-table limit at daily granularity. See the
bigquery-beam-patterns skill.

Authentication: none for Yahoo; Google Application Default Credentials for
BigQuery, so the same code runs locally and on the VM/CI.

Run standalone:
  python -m orchestration.ingest.yahoo_etf_ingest --backfill                 # full history, all ETFs
  python -m orchestration.ingest.yahoo_etf_ingest --start 2026-06-01 --end 2026-06-10
  python -m orchestration.ingest.yahoo_etf_ingest --symbol SPY --backfill    # one ETF
  python -m orchestration.ingest.yahoo_etf_ingest                            # recent window (daily)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery

from orchestration.ingest import ohlcv_bronze_common as ohlcv
from orchestration.ingest import yahoo_common
from orchestration.ingest.strategy_3_universe import ETF_UNIVERSE, Instrument

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment variables) -------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")
BQ_TABLE = os.environ.get("BQ_YAHOO_ETF_TABLE", "yahoo_etf_daily_raw")
SOURCE_ID = int(os.environ.get("YAHOO_ETF_SOURCE_ID", "16"))
DAILY_LOOKBACK_DAYS = int(os.environ.get("YAHOO_ETF_DAILY_LOOKBACK_DAYS", "10"))


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
    period1: int,
    period2: int,
    fetched_at: datetime,
    client: bigquery.Client,
    today: date | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch ``[period1, period2]`` for each instrument and upsert the closed bars.

    Each symbol is fetched and MERGEd independently so one provider hiccup never
    loses the rest; the per-symbol MERGE keeps the whole run idempotent.
    """
    all_rows: list[dict] = []
    for inst in instruments:
        payload = yahoo_common.fetch_chart(inst.yahoo_ticker, period1, period2)
        records = yahoo_common.parse_chart(payload)
        rows = ohlcv.prepare_rows(inst.symbol, records, SOURCE_ID, fetched_at, today=today)
        if not rows:
            logger.warning("Yahoo ETF %s: no closed bars for the requested range", inst.symbol)
            continue
        logger.info(
            "Yahoo ETF %s: upserting %d bar(s) %s..%s",
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
    """Back-fill the full ETF history from Yahoo into bronze (period1=0 to now)."""
    fetched_at = datetime.now(timezone.utc)
    now = int(fetched_at.timestamp())
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    rows = _ingest_instruments(_instruments(symbol), 0, now, fetched_at, client)
    logger.info("Yahoo ETF back-fill complete: %d bar(s) written", len(rows))
    return len(rows)


def ingest_range(
    start_date: date,
    end_date: date,
    symbol: str | None = None,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch ``[start_date, end_date]`` from Yahoo and upsert the closed bars."""
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    fetched_at = datetime.now(timezone.utc)
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    # +1 day on period2 so the end date is included (period2 is exclusive-ish).
    return _ingest_instruments(
        _instruments(symbol),
        yahoo_common.epoch(start_date),
        yahoo_common.epoch(end_date + timedelta(days=1)),
        fetched_at,
        client,
    )


def ingest_latest(
    symbol: str | None = None,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Refresh a recent window (last ``DAILY_LOOKBACK_DAYS`` days) for the daily job."""
    today = datetime.now(timezone.utc).date()
    return ingest_range(today - timedelta(days=DAILY_LOOKBACK_DAYS), today, symbol=symbol, client=client)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest daily ETF bars (Strategy 3 universe) from Yahoo Finance into BigQuery bronze."
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
