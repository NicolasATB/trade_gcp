"""Shared Coin Metrics ingestion logic (bronze loader for asset-metrics series).

Not an entry-point. Holds the logic for a **single daily metric** from the Coin
Metrics community ``asset-metrics`` endpoint (one value per UTC date), parameterised
by :class:`CoinMetricsSeries`. The thin ``coinmetrics_btc_<series>_ingest.py``
entry-points (circulating supply ``SplyCur``, active addresses ``AdrActCnt``,
transaction count ``TxCnt``) pin a config and re-export the bound functions. They
all share one implementation: same endpoint and pagination, same ``""``/``NaN`` ->
NULL handling, same one-value-per-date schema, same MERGE on the metric's date
column, same chunking — only the metric id, target table, column names, value type
and source id differ.

Like MVRV these series are **not** candle data and do **not** feed ``conform``: each
gets its own bronze table partitioned by its date, with an idempotent MERGE on the
natural date key. The community API needs no key. Every recurring ingest here
**never fabricates a value**: a missing datum is *alerted* (``_alert_missing``, the
seam the Telegram alert will use) and the row is skipped, so the table holds only
real values and any gap stays visible. BigQuery auth is Application Default
Credentials, so the same code runs locally and on the VM/CI.

A single MERGE (DML) may modify at most 4000 partitions; these tables partition by
their date column, so a full-history back-fill (>5000 dates) is chunked to
``MAX_MERGE_PARTITIONS`` rows to stay under the cap.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
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
API_BASE = os.environ.get(
    "COINMETRICS_API_BASE",
    "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics",
)
ASSET = os.environ.get("COINMETRICS_ASSET", "btc")
HTTP_TIMEOUT = int(os.environ.get("COINMETRICS_HTTP_TIMEOUT", "30"))
PAGE_SIZE = int(os.environ.get("COINMETRICS_PAGE_SIZE", "10000"))
DAILY_LOOKBACK_DAYS = int(os.environ.get("COINMETRICS_DAILY_LOOKBACK_DAYS", "7"))

# A single MERGE (DML) may modify at most 4000 partitions; the tables partition by
# their date column, so a long daily back-fill (>4000 dates) must be chunked.
MAX_MERGE_PARTITIONS = int(os.environ.get("COINMETRICS_MAX_MERGE_PARTITIONS", "3500"))


@dataclass(frozen=True)
class CoinMetricsSeries:
    """Per-series config for one daily Coin Metrics asset-metrics value."""

    metric: str        # Coin Metrics metric id, e.g. "SplyCur" / "AdrActCnt" / "TxCnt"
    table: str         # bronze table name (in BQ_DATASET)
    date_col: str      # natural-key date column, e.g. "supply_date" / "metric_date"
    value_col: str     # value column, e.g. "circ_supply" / "active_addresses"
    source_id: int     # FK into prod_trade_control.source_priority
    history_start: str = "2010-01-01"   # first date the back-fill requests
    value_type: str = "FLOAT64"         # BigQuery type of value_col ("INT64" for counts)


def _table_fqn(table: str) -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{table}"


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


def _coerce_value(value: float | None, value_type: str) -> float | int | None:
    """Coerce a parsed value to the column's BigQuery type (INT64 counts -> int)."""
    if value is None:
        return None
    if value_type == "INT64":
        return int(round(value))
    return float(value)


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def parse_metrics(payload: dict, cfg: CoinMetricsSeries) -> list[dict]:
    """Parse a Coin Metrics ``asset-metrics`` JSON page into raw records.

    Each record is ``{"d": <str date>, "value": <float|None>}`` reading the cell
    named ``cfg.metric``; a missing/empty/``NaN`` cell becomes ``None``.
    """
    records: list[dict] = []
    for row in payload.get("data", []):
        records.append({"d": row["time"], "value": _parse_value(row.get(cfg.metric))})
    return records


def _build_row(record: dict, cfg: CoinMetricsSeries, fetched_at: datetime | None = None) -> dict:
    """Map one source record to a bronze row (pure mapping; no fabrication)."""
    return {
        cfg.date_col: _to_date(record["d"]),
        cfg.value_col: _coerce_value(record.get("value"), cfg.value_type),
        "source_id": cfg.source_id,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _alert_missing(rows: list[dict], cfg: CoinMetricsSeries, context: str) -> list[date]:
    """Alert (log WARNING) for every row with a missing value; return the dates.

    The seam the Telegram alert (T-10) plugs into. The ingest relies on it instead
    of silently writing NULLs or fabricating values.
    """
    missing = [r[cfg.date_col] for r in rows if r[cfg.value_col] is None]
    for d in missing:
        logger.warning(
            "Coin Metrics %s %s: missing value for %s - source delivered no datum",
            cfg.metric, context, d.isoformat(),
        )
    return missing


def _prepare_rows(records: list[dict], cfg: CoinMetricsSeries, fetched_at: datetime, context: str) -> list[dict]:
    """Build rows, alert on any missing value, and drop them (alert-and-skip)."""
    rows = [_build_row(r, cfg, fetched_at=fetched_at) for r in records]
    _alert_missing(rows, cfg, context)
    return [r for r in rows if r[cfg.value_col] is not None]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural date key. Source rows are deduped within the batch
# (latest datetime_update wins) so MERGE never sees a key twice.
_MERGE_TEMPLATE = """
MERGE `{{table}}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.{date_col} ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.{date_col} = S.{date_col}
WHEN MATCHED THEN UPDATE SET
  {value_col}     = S.{value_col},
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  {date_col}, {value_col}, source_id, datetime_update
) VALUES (
  S.{date_col}, S.{value_col}, S.source_id, S.datetime_update
)
"""


def build_merge_sql(cfg: CoinMetricsSeries) -> str:
    """Return the idempotent MERGE for ``cfg`` (still with a ``{table}`` placeholder)."""
    return _MERGE_TEMPLATE.format(date_col=cfg.date_col, value_col=cfg.value_col)


def _struct_param(row: dict, cfg: CoinMetricsSeries) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter(cfg.date_col, "DATE", row[cfg.date_col]),
        bigquery.ScalarQueryParameter(cfg.value_col, cfg.value_type, row[cfg.value_col]),
        bigquery.ScalarQueryParameter("source_id", "INT64", row["source_id"]),
        bigquery.ScalarQueryParameter("datetime_update", "TIMESTAMP", row["datetime_update"]),
    )


def _upsert_chunk(client: bigquery.Client, cfg: CoinMetricsSeries, rows: list[dict]) -> None:
    struct_params = [_struct_param(r, cfg) for r in rows]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)]
    )
    merge_sql = build_merge_sql(cfg).format(table=_table_fqn(cfg.table))
    client.query(merge_sql, job_config=job_config).result()


def _upsert_rows(
    client: bigquery.Client, cfg: CoinMetricsSeries, rows: list[dict], chunk_size: int | None = None
) -> None:
    """Idempotent MERGE of rows, chunked to stay under the partition limit.

    Each row is a distinct date (= one partition), so a chunk of ``chunk_size`` rows
    touches at most that many partitions, keeping every MERGE below BigQuery's
    4000-partitions-per-DML cap. Chunking is transparent to idempotency: replaying
    any range MERGEs the same keys.
    """
    if not rows:
        return
    size = chunk_size or MAX_MERGE_PARTITIONS
    for chunk in _chunked(rows, size):
        _upsert_chunk(client, cfg, chunk)


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


def _query_url(cfg: CoinMetricsSeries, start_date: date, end_date: date) -> str:
    """Build the first-page URL for a date range (pure, unit-tested)."""
    params = {
        "assets": ASSET,
        "metrics": cfg.metric,
        "frequency": "1d",
        "page_size": str(PAGE_SIZE),
        "start_time": start_date.isoformat(),
        "end_time": end_date.isoformat(),
    }
    return f"{API_BASE}?{urllib.parse.urlencode(params)}"


def fetch_range(cfg: CoinMetricsSeries, start_date: date, end_date: date) -> list[dict]:  # pragma: no cover - live network
    """Return all records for ``[start_date, end_date]``, following ``next_page_url``."""
    records: list[dict] = []
    url = _query_url(cfg, start_date, end_date)
    while url:
        payload = _http_get_json(url)
        records.extend(parse_metrics(payload, cfg))
        url = payload.get("next_page_url")
    return records


# --- Entry logic (parameterised by CoinMetricsSeries) ------------------------

def backfill_history(
    cfg: CoinMetricsSeries,
    client: bigquery.Client | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
    """Back-fill the full history into bronze. Returns the row count written."""
    start = start_date or datetime.strptime(cfg.history_start, "%Y-%m-%d").date()
    end = end_date or datetime.now(timezone.utc).date()
    fetched_at = datetime.now(timezone.utc)
    rows = _prepare_rows(fetch_range(cfg, start, end), cfg, fetched_at, "back-fill")
    if not rows:
        logger.warning(
            "Coin Metrics %s back-fill: no writable records for %s..%s", cfg.metric, start, end
        )
        return 0
    if client is None:  # pragma: no cover - live client
        client = bigquery.Client(project=PROJECT_ID)
    logger.info(
        "Coin Metrics %s back-fill: upserting %d row(s) %s..%s into %s",
        cfg.metric, len(rows), rows[0][cfg.date_col].isoformat(),
        rows[-1][cfg.date_col].isoformat(), _table_fqn(cfg.table),
    )
    _upsert_rows(client, cfg, rows)
    logger.info("Coin Metrics %s back-fill complete: %d row(s) written", cfg.metric, len(rows))
    return len(rows)


def ingest_range(
    cfg: CoinMetricsSeries,
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
    rows = _prepare_rows(fetch_range(cfg, start_date, end_date), cfg, fetched_at, "range")
    if not rows:
        logger.warning(
            "Coin Metrics %s range: no writable records for %s..%s",
            cfg.metric, start_date, end_date,
        )
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    _upsert_rows(client, cfg, rows)
    return rows


def ingest_latest(cfg: CoinMetricsSeries, client: bigquery.Client | None = None) -> list[dict]:  # pragma: no cover - live network
    """Refresh a recent window (last ``DAILY_LOOKBACK_DAYS`` days) for the daily job."""
    today = datetime.now(timezone.utc).date()
    return ingest_range(cfg, today - timedelta(days=DAILY_LOOKBACK_DAYS), today, client=client)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest a daily Coin Metrics asset-metric into BigQuery bronze."
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


def run_cli(cfg: CoinMetricsSeries, argv=None) -> None:  # pragma: no cover - CLI wiring
    """Shared CLI for the thin Coin Metrics entry-points."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    if args.backfill:
        n = backfill_history(cfg)
        logger.info("Back-filled %d row(s)", n)
    elif args.start is not None:
        end = args.end if args.end is not None else args.start
        rows = ingest_range(cfg, args.start, end)
        logger.info("Wrote %d row(s)", len(rows))
    else:
        rows = ingest_latest(cfg)
        logger.info("Wrote %d row(s)", len(rows))
