"""BTC circulating-supply ingestion: Coin Metrics (community) -> BigQuery bronze.

Loads BTC's daily circulating supply (Coin Metrics metric ``SplyCur``) from the
free community API into ``prod_trade_bronze.coinmetrics_btc_supply_daily_raw``.
Same idempotent-upsert pattern as the MVRV ingest (MERGE on the business key
``supply_date``):

  * **Back-fill** the full history once (``--backfill``), following the API's
    ``next_page_url`` pagination.
  * **Daily refresh** of a recent window (last ``DAILY_LOOKBACK_DAYS`` days) or an
    explicit ``--start/--end`` range.

Supply is the on-chain input the daily training view turns into halving-cycle
features: with the halving epoch fixed by date and a constant block subsidy within
an epoch, circulating supply recovers the block-count fraction of the cycle.

The community API needs no key. Like every recurring ingest here it **never
fabricates a value**: a missing datum is *alerted* (``_alert_missing``, the hook
the Telegram alert will use) and the row is skipped. BigQuery auth is Application
Default Credentials.

Run standalone:
  python -m orchestration.ingest.coinmetrics_btc_supply_ingest --backfill         # full history
  python -m orchestration.ingest.coinmetrics_btc_supply_ingest --start 2026-06-01 --end 2026-06-10
  python -m orchestration.ingest.coinmetrics_btc_supply_ingest                    # recent window (daily)
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
BQ_TABLE = os.environ.get("BQ_SUPPLY_TABLE", "coinmetrics_btc_supply_daily_raw")
SOURCE_ID = int(os.environ.get("SUPPLY_SOURCE_ID", "12"))

API_BASE = os.environ.get(
    "COINMETRICS_API_BASE",
    "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics",
)
ASSET = os.environ.get("COINMETRICS_ASSET", "btc")
METRIC = os.environ.get("COINMETRICS_METRIC", "SplyCur")
HISTORY_START = os.environ.get("SUPPLY_HISTORY_START", "2010-01-01")
HTTP_TIMEOUT = int(os.environ.get("COINMETRICS_HTTP_TIMEOUT", "30"))
PAGE_SIZE = int(os.environ.get("COINMETRICS_PAGE_SIZE", "10000"))
DAILY_LOOKBACK_DAYS = int(os.environ.get("SUPPLY_DAILY_LOOKBACK_DAYS", "7"))

# A single MERGE (DML) may modify at most 4000 partitions; the table partitions
# by supply_date, so a full-history back-fill (>5000 dates) must be chunked.
MAX_MERGE_PARTITIONS = int(os.environ.get("SUPPLY_MAX_MERGE_PARTITIONS", "3500"))


def _table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


# --- Parsing / row building (pure, unit-tested) ------------------------------

def _to_date(value) -> date:
    """Coerce a Coin Metrics ``time`` (ISO string ``YYYY-MM-DD...``) to a date."""
    if isinstance(value, date):
        return value
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _parse_value(raw) -> float | None:
    """Parse a raw metric cell; empty/``NaN``/None -> ``None`` (SQL NULL)."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "" or text.lower() == "nan":
        return None
    return float(text)


def parse_metrics(payload: dict, metric: str = METRIC) -> list[dict]:
    """Parse a Coin Metrics ``asset-metrics`` JSON page into raw records.

    Each record is ``{"d": <str date>, "circ_supply": <float|None>}``; a missing
    metric cell becomes ``None``.
    """
    records: list[dict] = []
    for row in payload.get("data", []):
        records.append({"d": row["time"], "circ_supply": _parse_value(row.get(metric))})
    return records


def _build_row(record: dict, fetched_at: datetime | None = None) -> dict:
    """Map one source record to a bronze table row (pure mapping; no fabrication)."""
    value = record.get("circ_supply")
    return {
        "supply_date": _to_date(record["d"]),
        "circ_supply": float(value) if value is not None else None,
        "source_id": SOURCE_ID,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _alert_missing(rows: list[dict], context: str) -> list[date]:
    """Alert (log WARNING) for every row with a missing value; return the dates.

    The seam the Telegram alert (T-10) plugs into. The ingest relies on it instead
    of silently writing NULLs or fabricating values.
    """
    missing = [r["supply_date"] for r in rows if r["circ_supply"] is None]
    for d in missing:
        logger.warning(
            "Coin Metrics supply %s: missing value for %s - source delivered no datum",
            context, d.isoformat(),
        )
    return missing


def _prepare_rows(records: list[dict], fetched_at: datetime, context: str) -> list[dict]:
    """Build rows, alert on any missing value, and drop them (alert-and-skip)."""
    rows = [_build_row(r, fetched_at=fetched_at) for r in records]
    _alert_missing(rows, context)
    return [r for r in rows if r["circ_supply"] is not None]


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural key ``supply_date``. Source rows are deduped
# within the batch (latest datetime_update wins) so MERGE never sees a key twice.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.supply_date ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.supply_date = S.supply_date
WHEN MATCHED THEN UPDATE SET
  circ_supply     = S.circ_supply,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  supply_date, circ_supply, source_id, datetime_update
) VALUES (
  S.supply_date, S.circ_supply, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("supply_date", "DATE", row["supply_date"]),
        bigquery.ScalarQueryParameter("circ_supply", "FLOAT64", row["circ_supply"]),
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
    """Idempotent MERGE of supply rows, chunked to stay under the partition limit.

    Each row is a distinct ``supply_date`` (= one partition), so a chunk of
    ``chunk_size`` rows touches at most that many partitions, keeping every MERGE
    below BigQuery's 4000-partitions-per-DML cap. Chunking is transparent to
    idempotency: replaying any range MERGEs the same keys.
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
def _http_get_json(url: str) -> dict:  # pragma: no cover - live network
    """GET ``url`` and return the parsed JSON; retried on transient URLErrors."""
    req = urllib.request.Request(url, headers={"User-Agent": "trade-gcp/1.0"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _query_url(start_date: date, end_date: date) -> str:
    """Build the first-page URL for a date range (pure, unit-tested)."""
    params = {
        "assets": ASSET,
        "metrics": METRIC,
        "frequency": "1d",
        "page_size": str(PAGE_SIZE),
        "start_time": start_date.isoformat(),
        "end_time": end_date.isoformat(),
    }
    return f"{API_BASE}?{urllib.parse.urlencode(params)}"


def fetch_range(start_date: date, end_date: date) -> list[dict]:  # pragma: no cover - live network
    """Return all SplyCur records for ``[start_date, end_date]``, following pages."""
    records: list[dict] = []
    url = _query_url(start_date, end_date)
    while url:
        payload = _http_get_json(url)
        records.extend(parse_metrics(payload))
        url = payload.get("next_page_url")
    return records


# --- Public entry points ------------------------------------------------------

def backfill_history(
    client: bigquery.Client | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
    """Back-fill the full supply history into bronze. Returns the row count."""
    start = start_date or datetime.strptime(HISTORY_START, "%Y-%m-%d").date()
    end = end_date or datetime.now(timezone.utc).date()
    fetched_at = datetime.now(timezone.utc)
    rows = _prepare_rows(fetch_range(start, end), fetched_at, "back-fill")
    if not rows:
        logger.warning("Coin Metrics supply back-fill: no writable records for %s..%s", start, end)
        return 0
    if client is None:  # pragma: no cover - live client
        client = bigquery.Client(project=PROJECT_ID)
    logger.info(
        "Coin Metrics supply back-fill: upserting %d row(s) %s..%s into %s",
        len(rows), rows[0]["supply_date"].isoformat(),
        rows[-1]["supply_date"].isoformat(), _table_fqn(),
    )
    _upsert_rows(client, rows)
    logger.info("Coin Metrics supply back-fill complete: %d row(s) written", len(rows))
    return len(rows)


def ingest_range(
    start_date: date,
    end_date: date,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch a date range from the API and upsert it (daily/catch-up)."""
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    fetched_at = datetime.now(timezone.utc)
    rows = _prepare_rows(fetch_range(start_date, end_date), fetched_at, "range")
    if not rows:
        logger.warning("Coin Metrics supply range: no writable records for %s..%s", start_date, end_date)
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
        description="Ingest BTC circulating supply (Coin Metrics SplyCur) into BigQuery bronze."
    )
    def iso_date(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full history (ignores --start/--end).",
    )
    parser.add_argument(
        "--start", type=iso_date, default=None,
        help="First UTC date to ingest from the API (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end", type=iso_date, default=None,
        help="Last UTC date (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover - CLI wiring
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    if args.backfill:
        n = backfill_history()
        logger.info("Back-filled %d row(s)", n)
    elif args.start is not None:
        end = args.end if args.end is not None else args.start
        rows = ingest_range(args.start, end)
        logger.info("Wrote %d row(s)", len(rows))
    else:
        rows = ingest_latest()
        logger.info("Wrote %d row(s)", len(rows))


if __name__ == "__main__":  # pragma: no cover
    main()
