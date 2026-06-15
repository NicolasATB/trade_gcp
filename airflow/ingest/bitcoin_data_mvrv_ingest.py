"""MVRV Z-Score ingestion: bitcoin-data.com (BGeometrics) -> BigQuery bronze.

Loads BTC's daily MVRV Z-Score from bitcoin-data.com into
``prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw``. Two entry points share
the same idempotent upsert (MERGE on the business key ``mvrvz_date``):

  * **Back-fill** the full history once from the CSV export
    (``https://bitcoin-data.com/v1/mvrv-zscore/csv``, from 2009-01-03). The JSON
    API only serves a rolling 4-year window, so the CSV is the only free source
    of the complete series.
  * **Daily refresh** from the JSON API (``/last`` for the latest day, or a date
    range via ``?startday=&endday=`` to catch up gaps).

Because the bronze table is partitioned by ``mvrvz_date`` and a single MERGE
(DML) statement may touch at most 4000 partitions, the full-history back-fill is
split into chunks of ``MAX_MERGE_PARTITIONS`` dates so each MERGE stays under the
limit (see the bigquery-beam-patterns skill). Re-running any range never
duplicates a row.

The source occasionally delivers ``NaN`` for a date with no computed value yet.
Two different policies apply, on purpose:
  * **Daily process** — never fabricates a value. A missing datum is *alerted*
    (``_alert_missing``, the hook the Telegram alert will use) and the row is
    skipped, so the table only ever holds real values and the gap stays visible.
  * **Back-fill** — applies one-off, documented historical corrections
    (``HISTORICAL_CORRECTIONS``, e.g. 2026-06-03 -> 0.45) to patch specific past
    dates, and still alerts on any value left missing afterwards. These
    corrections are NOT part of the recurring process.

Authentication uses Google Application Default Credentials, so the same code runs
everywhere (local ``gcloud auth application-default login`` or
``GOOGLE_APPLICATION_CREDENTIALS``; on the VM/CI the attached service account).

Run standalone:
  python -m airflow.ingest.bitcoin_data_mvrv_ingest --backfill          # full history (CSV)
  python -m airflow.ingest.bitcoin_data_mvrv_ingest --start 2026-06-01 --end 2026-06-10  # API range
  python -m airflow.ingest.bitcoin_data_mvrv_ingest                     # latest day (daily job)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import urllib.request
from datetime import date, datetime, timezone

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
BQ_TABLE = os.environ.get("BQ_MVRV_TABLE", "bitcoin_data_mvrv_zscore_daily_raw")
SOURCE_ID = int(os.environ.get("MVRV_SOURCE_ID", "5"))

API_BASE = os.environ.get("MVRV_API_BASE", "https://bitcoin-data.com/v1/mvrv-zscore")
CSV_HISTORY_URL = os.environ.get("MVRV_CSV_URL", "https://bitcoin-data.com/v1/mvrv-zscore/csv")
HTTP_TIMEOUT = int(os.environ.get("MVRV_HTTP_TIMEOUT", "30"))

# A single MERGE (DML) may modify at most 4000 partitions; the table partitions
# by mvrvz_date, so a full-history back-fill (>6000 dates) must be chunked.
MAX_MERGE_PARTITIONS = int(os.environ.get("MVRV_MAX_MERGE_PARTITIONS", "3500"))

# One-off HISTORICAL data corrections — NOT part of the recurring process.
# Dates the source shipped without a value (NaN) that we patch once with a known
# published figure, applied ONLY during the historical back-fill and only when
# the value is missing (a real value is never clobbered). The daily ingest never
# fills: it ALERTS on a missing datum (_alert_missing) and skips it.
HISTORICAL_CORRECTIONS: dict[str, float] = {
    "2026-06-03": 0.45,
}


def _table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


# --- Parsing / row building (pure, unit-tested) ------------------------------

def _to_date(value) -> date:
    """Coerce a source ``d`` (``date`` or ``YYYY-MM-DD`` string) to a ``date``."""
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_value(raw) -> float | None:
    """Parse a raw mvrvZscore cell; ``NaN``/empty -> ``None`` (SQL NULL)."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "" or text.lower() == "nan":
        return None
    return float(text)


def parse_mvrv_csv(text: str) -> list[dict]:
    """Parse the bitcoin-data.com CSV export into raw records.

    Accepts the ``d,unixTs,mvrvZscore`` export (header optional). Each record is
    ``{"d": <str>, "unixTs": <int>, "mvrvZscore": <float|None>}``; ``NaN`` cells
    become ``None``. Override fills (``KNOWN_VALUE_OVERRIDES``) are applied later
    in ``_build_row`` so both the CSV and API paths share one fill policy.
    """
    records: list[dict] = []
    lines = text.splitlines()
    if lines and lines[0].lower().lstrip().startswith("d,"):
        lines = lines[1:]
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 3:
            continue
        records.append(
            {"d": parts[0].strip(), "unixTs": int(parts[1]), "mvrvZscore": _parse_value(parts[2])}
        )
    return records


def _build_row(record: dict, fetched_at: datetime | None = None) -> dict:
    """Map one source record (CSV or API) to a bronze table row.

    Pure mapping: a missing/``NaN`` value stays ``None`` (SQL NULL). Filling
    (back-fill corrections) and missing-value alerting are the callers' job,
    never here — the recurring process must not silently fabricate a value.
    """
    mvrvz_date = _to_date(record["d"])
    value = record.get("mvrvZscore")
    unix_ts = record.get("unixTs")
    return {
        "mvrvz_date": mvrvz_date,
        "unix_ts": int(unix_ts) if unix_ts is not None else None,
        "mvrv_zscore": float(value) if value is not None else None,
        "source_id": SOURCE_ID,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _apply_corrections(rows: list[dict], corrections: dict[str, float]) -> list[str]:
    """Patch missing values with one-off historical corrections (back-fill only).

    Fills only rows whose value is missing (never clobbers a real value).
    Returns the ISO dates actually corrected, for logging.
    """
    applied: list[str] = []
    for row in rows:
        iso = row["mvrvz_date"].isoformat()
        if row["mvrv_zscore"] is None and iso in corrections:
            row["mvrv_zscore"] = float(corrections[iso])
            applied.append(iso)
    return applied


def _alert_missing(rows: list[dict], context: str) -> list[date]:
    """Alert (log WARNING) for every row with a missing value; return the dates.

    This is the seam the Telegram alert (T-10) will plug into. The recurring
    ingest relies on it instead of silently writing NULLs or fabricating values.
    """
    missing = [r["mvrvz_date"] for r in rows if r["mvrv_zscore"] is None]
    for d in missing:
        logger.warning(
            "MVRV %s: missing value for %s - source delivered no datum", context, d.isoformat()
        )
    return missing


def _prepare_daily_rows(records: list[dict], fetched_at: datetime) -> list[dict]:
    """Build rows for the daily path: alert on missing values and drop them.

    The recurring process never fabricates or stores a missing value — it alerts
    and skips, so the table holds only real data and any gap is surfaced.
    """
    rows = [_build_row(r, fetched_at=fetched_at) for r in records]
    _alert_missing(rows, "daily")
    return [r for r in rows if r["mvrv_zscore"] is not None]


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural key ``mvrvz_date``. Source rows are deduped
# within the batch (latest datetime_update wins) so MERGE never sees a key twice.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.mvrvz_date ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.mvrvz_date = S.mvrvz_date
WHEN MATCHED THEN UPDATE SET
  unix_ts         = S.unix_ts,
  mvrv_zscore     = S.mvrv_zscore,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  mvrvz_date, unix_ts, mvrv_zscore, source_id, datetime_update
) VALUES (
  S.mvrvz_date, S.unix_ts, S.mvrv_zscore, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("mvrvz_date", "DATE", row["mvrvz_date"]),
        bigquery.ScalarQueryParameter("unix_ts", "INT64", row["unix_ts"]),
        bigquery.ScalarQueryParameter("mvrv_zscore", "FLOAT64", row["mvrv_zscore"]),
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
    """Idempotent MERGE of MVRV rows, chunked to stay under the partition limit.

    Each row is a distinct ``mvrvz_date`` (= one partition), so a chunk of
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
def _http_get(url: str) -> str:  # pragma: no cover - live network
    """GET ``url`` and return the body text; retried on transient URLErrors."""
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8")


def fetch_latest() -> dict:  # pragma: no cover - live network
    """Return the latest available MVRV record from the API (``/last``)."""
    return json.loads(_http_get(f"{API_BASE}/last"))


def fetch_range(start_date: date, end_date: date) -> list[dict]:  # pragma: no cover - live network
    """Return MVRV records for ``[start_date, end_date]`` from the JSON API.

    Note the JSON API only serves a rolling ~4-year window; older dates need the
    CSV back-fill. Used for daily catch-up, not history.
    """
    url = f"{API_BASE}?startday={start_date.isoformat()}&endday={end_date.isoformat()}"
    return json.loads(_http_get(url))


def _read_csv_source(csv_source: str) -> str:  # pragma: no cover - live network / fs
    """Read the CSV export from a URL (http/https) or a local file path."""
    if csv_source.startswith(("http://", "https://")):
        return _http_get(csv_source)
    with open(csv_source, encoding="utf-8") as fh:
        return fh.read()


# --- Public entry points ------------------------------------------------------

def backfill_history(
    client: bigquery.Client | None = None,
    csv_source: str | None = None,
    corrections: dict[str, float] | None = None,
) -> int:
    """Back-fill the full MVRV history from the CSV export into bronze.

    Reads ``csv_source`` (defaults to ``CSV_HISTORY_URL``), parses it, applies
    the one-off historical ``corrections`` (defaults to ``HISTORICAL_CORRECTIONS``)
    to patch specific past dates, alerts on any value still missing afterwards,
    and upserts every row (chunked). Returns the row count.
    """
    csv_source = csv_source or CSV_HISTORY_URL
    corrections = HISTORICAL_CORRECTIONS if corrections is None else corrections
    fetched_at = datetime.now(timezone.utc)
    text = _read_csv_source(csv_source)
    records = parse_mvrv_csv(text)
    rows = [_build_row(r, fetched_at=fetched_at) for r in records]
    if not rows:
        logger.warning("MVRV back-fill: no records parsed from %s", csv_source)
        return 0
    applied = _apply_corrections(rows, corrections)
    if applied:
        logger.info(
            "MVRV back-fill: applied %d historical correction(s): %s",
            len(applied), ", ".join(applied),
        )
    _alert_missing(rows, "back-fill")  # surface anything still missing post-correction
    if client is None:  # pragma: no cover - live client
        client = bigquery.Client(project=PROJECT_ID)
    logger.info(
        "MVRV back-fill: upserting %d row(s) %s..%s into %s",
        len(rows), rows[0]["mvrvz_date"].isoformat(),
        rows[-1]["mvrvz_date"].isoformat(), _table_fqn(),
    )
    _upsert_rows(client, rows)
    logger.info("MVRV back-fill complete: %d row(s) written", len(rows))
    return len(rows)


def ingest_latest(client: bigquery.Client | None = None) -> list[dict]:  # pragma: no cover - live network
    """Fetch the latest MVRV value from the API and upsert it (daily job).

    Alerts and skips if the latest datum is missing (never writes a NULL or a
    fabricated value); returns the rows actually written (``[]`` if skipped).
    """
    fetched_at = datetime.now(timezone.utc)
    rows = _prepare_daily_rows([fetch_latest()], fetched_at)
    if not rows:
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    logger.info("MVRV daily: upserting %s into %s", rows[0]["mvrvz_date"].isoformat(), _table_fqn())
    _upsert_rows(client, rows)
    return rows


def ingest_range(
    start_date: date,
    end_date: date,
    client: bigquery.Client | None = None,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch a date range from the API and upsert it (catch-up within 4y window)."""
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    fetched_at = datetime.now(timezone.utc)
    rows = _prepare_daily_rows(fetch_range(start_date, end_date), fetched_at)
    if not rows:
        logger.warning("MVRV range: no writable records for %s..%s", start_date, end_date)
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    _upsert_rows(client, rows)
    return rows


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest BTC MVRV Z-Score from bitcoin-data.com into BigQuery bronze."
    )
    def iso_date(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full history from the CSV export (ignores --start/--end).",
    )
    parser.add_argument(
        "--csv", default=None,
        help="CSV source for --backfill: URL or local path (defaults to the public export).",
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
        n = backfill_history(csv_source=args.csv)
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
