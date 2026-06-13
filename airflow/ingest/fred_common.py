"""Shared FRED ingestion logic + low-level primitives (bronze loader).

Not an entry-point. Holds:

  * the FRED request/parse primitives (``request_observations_json``,
    ``_parse_value``, ``_to_date``, ``_chunked``) reused by every FRED ingest,
    including the point-in-time M2 module (``fred_m2_ingest``);
  * the logic for a **plain, non-revised** daily series (one ``(obs_date,
    obs_value)`` row per date), parameterised by :class:`FredSeries`. The thin
    ``fred_<symbol>_ingest.py`` entry-points (10Y Treasury ``DGS10``, Fed funds
    ``DFF``) pin a config and re-export the bound functions. Both share one
    implementation: same observations endpoint, same ``.`` -> NULL handling,
    same ``(obs_date, obs_value)`` schema, same MERGE on ``obs_date``, same
    chunking — only ``series_id``, target table and source id differ.

FRED delivers a missing value as ``"."`` (non-publication days, e.g. holidays
for DGS10). Those rows are **alerted and dropped**, never stored as NULL, so the
table holds only real observations and any gap stays visible. These tables
partition by ``DATE_TRUNC(obs_date, MONTH)`` (not by day): a long daily history
(DGS10 since 1962, DFF since 1954) would exceed BigQuery's hard
10000-partitions-per-table limit at daily granularity. The back-fill is still
chunked to ``MAX_MERGE_PARTITIONS`` rows as a safety margin under the
4000-partitions-per-DML cap.

Authentication: the FRED API key is read from the ``FRED_API_KEY`` environment
variable (never hard-coded). Google Application Default Credentials authenticate
BigQuery.
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
API_BASE = os.environ.get("FRED_API_BASE", "https://api.stlouisfed.org/fred/series/observations")
HTTP_TIMEOUT = int(os.environ.get("FRED_HTTP_TIMEOUT", "30"))
# Daily refresh window: how many days back to re-request and MERGE (covers
# late-published days / small revisions without a full back-fill).
DAILY_LOOKBACK_DAYS = int(os.environ.get("FRED_DAILY_LOOKBACK_DAYS", "14"))

# A single MERGE (DML) may modify at most 4000 partitions; the table partitions
# by obs_date, so a long daily back-fill (>4000 dates) must be chunked.
MAX_MERGE_PARTITIONS = int(os.environ.get("FRED_MAX_MERGE_PARTITIONS", "3500"))


@dataclass(frozen=True)
class FredSeries:
    """Per-series config for a plain (non-revised) FRED daily series."""

    series_id: str   # FRED series id, e.g. "DGS10" / "DFF"
    table: str       # bronze table name (in BQ_DATASET)
    source_id: int   # FK into prod_trade_control.source_priority


def _table_fqn(table: str) -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{table}"


def _api_key() -> str:
    """Return the FRED API key from the environment or raise a clear error."""
    key = os.environ.get("FRED_API_KEY")
    if not key:
        raise RuntimeError(
            "FRED_API_KEY is not set. Export it (it lives in .env, never committed) "
            "before running the FRED ingest."
        )
    return key


# --- Parsing primitives (pure, unit-tested; shared with fred_m2_ingest) ------

def _to_date(value) -> date:
    """Coerce a source value (``date`` or ``YYYY-MM-DD`` string) to a ``date``."""
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_value(raw) -> float | None:
    """Parse a FRED observation value; ``"."``/empty/``NaN`` -> ``None`` (SQL NULL)."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text in ("", ".") or text.lower() == "nan":
        return None
    return float(text)


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def parse_observations(payload: dict) -> list[dict]:
    """Parse a FRED ``series/observations`` JSON payload into raw records.

    Returns one ``{"date": <str>, "value": <float|None>}`` per observation; a
    FRED ``"."`` becomes ``None``. The realtime (vintage) fields are dropped on
    purpose — this is the non-revised path; see ``fred_m2_ingest`` for vintages.
    """
    records: list[dict] = []
    for obs in payload.get("observations", []):
        records.append({"date": obs["date"], "value": _parse_value(obs.get("value"))})
    return records


# --- Row building (pure, parameterised by FredSeries) ------------------------

def _build_row(record: dict, source_id: int, fetched_at: datetime | None = None) -> dict:
    """Map one observation record to a bronze table row (pure mapping)."""
    return {
        "obs_date": _to_date(record["date"]),
        "obs_value": record["value"],
        "source_id": source_id,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _alert_missing(rows: list[dict], series_id: str, context: str) -> list[date]:
    """Alert (log WARNING) for every row with a missing value; return the dates.

    Seam the Telegram alert (T-10) will plug into. The ingest relies on it
    instead of silently writing NULLs or fabricating values.
    """
    missing = [r["obs_date"] for r in rows if r["obs_value"] is None]
    for d in missing:
        logger.warning(
            "FRED %s %s: missing value for %s - source delivered `.`",
            series_id, context, d.isoformat(),
        )
    return missing


def _prepare_rows(records: list[dict], cfg: FredSeries, fetched_at: datetime, context: str) -> list[dict]:
    """Build rows, alert on any missing value, and drop them.

    Non-publication days (FRED ``"."``) are surfaced and skipped so the table
    holds only real observations and every gap is visible.
    """
    rows = [_build_row(r, cfg.source_id, fetched_at=fetched_at) for r in records]
    _alert_missing(rows, cfg.series_id, context)
    return [r for r in rows if r["obs_value"] is not None]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural key ``obs_date``. Source rows are deduped
# within the batch (latest datetime_update wins) so MERGE never sees a key twice.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.obs_date ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.obs_date = S.obs_date
WHEN MATCHED THEN UPDATE SET
  obs_value       = S.obs_value,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  obs_date, obs_value, source_id, datetime_update
) VALUES (
  S.obs_date, S.obs_value, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("obs_date", "DATE", row["obs_date"]),
        bigquery.ScalarQueryParameter("obs_value", "FLOAT64", row["obs_value"]),
        bigquery.ScalarQueryParameter("source_id", "INT64", row["source_id"]),
        bigquery.ScalarQueryParameter("datetime_update", "TIMESTAMP", row["datetime_update"]),
    )


def _upsert_chunk(client: bigquery.Client, table: str, rows: list[dict]) -> None:
    struct_params = [_struct_param(r) for r in rows]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)]
    )
    client.query(_MERGE_SQL.format(table=_table_fqn(table)), job_config=job_config).result()


def _upsert_rows(client: bigquery.Client, table: str, rows: list[dict], chunk_size: int | None = None) -> None:
    """Idempotent MERGE of FRED rows, chunked to stay under the partition limit.

    Each row is a distinct ``obs_date`` (= one partition), so a chunk of
    ``chunk_size`` rows touches at most that many partitions, keeping every MERGE
    below BigQuery's 4000-partitions-per-DML cap. Chunking is transparent to
    idempotency: replaying any range MERGEs the same keys.
    """
    if not rows:
        return
    size = chunk_size or MAX_MERGE_PARTITIONS
    for chunk in _chunked(rows, size):
        _upsert_chunk(client, table, chunk)


# --- HTTP fetch (live I/O; shared with fred_m2_ingest) -----------------------

@retry(
    retry=retry_if_exception_type(urllib.error.URLError),
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _http_get(url: str) -> str:  # pragma: no cover - live network
    """GET ``url`` and return the body text; retried on transient URLErrors."""
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8")


def request_observations_json(
    series_id: str,
    observation_start: date | None = None,
    observation_end: date | None = None,
    realtime_start: str | None = None,
    realtime_end: str | None = None,
) -> dict:  # pragma: no cover - live network
    """Call the FRED ``series/observations`` endpoint and return the raw JSON.

    ``realtime_start``/``realtime_end`` request ALFRED vintages (used by the M2
    module); left unset, FRED returns the latest revision.
    """
    params = {"series_id": series_id, "api_key": _api_key(), "file_type": "json"}
    if observation_start is not None:
        params["observation_start"] = observation_start.isoformat()
    if observation_end is not None:
        params["observation_end"] = observation_end.isoformat()
    if realtime_start is not None:
        params["realtime_start"] = realtime_start
    if realtime_end is not None:
        params["realtime_end"] = realtime_end
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    return json.loads(_http_get(url))


# --- Plain-series entry logic (parameterised by FredSeries) ------------------

def backfill_history(
    cfg: FredSeries,
    client: bigquery.Client | None = None,
    observation_start: date | None = None,
    observation_end: date | None = None,
) -> int:
    """Back-fill the full series history from the FRED API into bronze.

    Defaults to the entire series. Drops non-publication days, then upserts every
    real observation (chunked). Returns the row count written.
    """
    fetched_at = datetime.now(timezone.utc)
    payload = request_observations_json(
        cfg.series_id, observation_start=observation_start, observation_end=observation_end
    )
    records = parse_observations(payload)
    rows = _prepare_rows(records, cfg, fetched_at, "back-fill")
    if not rows:
        logger.warning("FRED %s back-fill: no writable observations parsed", cfg.series_id)
        return 0
    if client is None:  # pragma: no cover - live client
        client = bigquery.Client(project=PROJECT_ID)
    logger.info(
        "FRED %s back-fill: upserting %d row(s) %s..%s into %s",
        cfg.series_id, len(rows), rows[0]["obs_date"].isoformat(),
        rows[-1]["obs_date"].isoformat(), _table_fqn(cfg.table),
    )
    _upsert_rows(client, cfg.table, rows)
    logger.info("FRED %s back-fill complete: %d row(s) written", cfg.series_id, len(rows))
    return len(rows)


def ingest_range(
    cfg: FredSeries,
    start_date: date,
    end_date: date,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch ``[start_date, end_date]`` from the FRED API and upsert it (daily/catch-up)."""
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    fetched_at = datetime.now(timezone.utc)
    payload = request_observations_json(
        cfg.series_id, observation_start=start_date, observation_end=end_date
    )
    rows = _prepare_rows(parse_observations(payload), cfg, fetched_at, "range")
    if not rows:
        logger.warning("FRED %s range: no writable observations for %s..%s",
                       cfg.series_id, start_date, end_date)
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    _upsert_rows(client, cfg.table, rows)
    return rows


def ingest_latest(cfg: FredSeries, client: bigquery.Client | None = None) -> list[dict]:  # pragma: no cover - live network
    """Refresh a recent window (last ``DAILY_LOOKBACK_DAYS`` days) for the daily job."""
    today = datetime.now(timezone.utc).date()
    return ingest_range(cfg, today - timedelta(days=DAILY_LOOKBACK_DAYS), today, client=client)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest a plain (non-revised) daily FRED series into BigQuery bronze."
    )
    iso_date = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full series history (ignores --start/--end).",
    )
    parser.add_argument(
        "--start", type=iso_date, default=None,
        help="First date to ingest from the API (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end", type=iso_date, default=None,
        help="Last date (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    return parser.parse_args(argv)


def run_cli(cfg: FredSeries, argv=None) -> None:  # pragma: no cover - CLI wiring
    """Shared CLI for the thin plain-series entry-points (DGS10, DFF)."""
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
