"""M2 money stock (WM2NS) point-in-time ingestion: FRED/ALFRED -> BigQuery bronze.

M2 is the one macro series here that is **revised and published with a lag**, so
to build features without look-ahead we store every ALFRED *vintage* rather than
just the latest revision. Each observation date can carry several rows, one per
revision, each tagged with the realtime window during which it was the published
value. The bronze table
``prod_trade_bronze.fred_wm2ns_weekly_raw`` therefore keys on
``(wm2ns_date, realtime_start)``.

To reconstruct what M2 looked like on any day ``X`` (no look-ahead), pick, for
each ``wm2ns_date``, the row with ``realtime_start <= X <= realtime_end``.

Two entry points share one idempotent upsert (MERGE on the composite key):

  * **Back-fill** the full vintage history once (``--backfill``): every revision
    of every observation, pulled with ALFRED's
    ``realtime_start=1776-07-04 & realtime_end=9999-12-31``.
  * **Daily refresh** (no args): re-request recent observations with a recent
    ``realtime_start`` (and open ``realtime_end``) to capture any new vintage,
    then MERGE. ``realtime_end`` is mutable — a prior vintage's open
    ``9999-12-31`` is closed off when a newer revision supersedes it.

FRED delivers a missing value as ``"."``; such rows are alerted and dropped (the
table holds only real values). The back-fill is chunked to ``MAX_MERGE_PARTITIONS``
so each MERGE stays under BigQuery's 4000-partitions-per-DML cap (partition is
``wm2ns_date``). The low-level FRED request/parse helpers are reused from
``fred_common``.

Authentication: ``FRED_API_KEY`` (env, never committed) for FRED; Application
Default Credentials for BigQuery.

Run standalone:
  python -m orchestration.ingest.fred_m2_ingest --backfill          # full vintage history
  python -m orchestration.ingest.fred_m2_ingest                     # recent vintages (daily)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery

from orchestration.ingest.fred_common import (
    _chunked,
    _parse_value,
    _to_date,
    request_observations_json,
)

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment variables) -------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")
BQ_DATASET = os.environ.get("BQ_BRONZE_DATASET", "prod_trade_bronze")
BQ_TABLE = os.environ.get("BQ_M2_TABLE", "fred_wm2ns_weekly_raw")
SERIES_ID = os.environ.get("FRED_M2_SERIES_ID", "WM2NS")
SOURCE_ID = int(os.environ.get("FRED_M2_SOURCE_ID", "7"))

# ALFRED sentinels: the widest realtime window returns *all* vintages; the open
# end (9999-12-31) marks the currently-latest revision.
REALTIME_MIN = "1776-07-04"
REALTIME_MAX = "9999-12-31"
# Daily refresh window: how far back to look for newly published vintages.
DAILY_LOOKBACK_DAYS = int(os.environ.get("FRED_M2_DAILY_LOOKBACK_DAYS", "60"))

# A single MERGE (DML) may modify at most 4000 partitions; the table partitions
# by wm2ns_date, so the full vintage back-fill is chunked by row.
MAX_MERGE_PARTITIONS = int(os.environ.get("FRED_M2_MAX_MERGE_PARTITIONS", "3500"))


def _table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


# --- Parsing / row building (pure, unit-tested) ------------------------------

def parse_vintages(payload: dict) -> list[dict]:
    """Parse a FRED/ALFRED observations payload, keeping the vintage fields.

    Returns one record per ``(date, realtime_start)`` with its ``realtime_end``
    and value (``"."`` -> ``None``). Unlike ``fred_common.parse_observations``,
    the realtime window is preserved — that is what makes the series
    point-in-time.
    """
    records: list[dict] = []
    for obs in payload.get("observations", []):
        records.append(
            {
                "date": obs["date"],
                "realtime_start": obs["realtime_start"],
                "realtime_end": obs["realtime_end"],
                "value": _parse_value(obs.get("value")),
            }
        )
    return records


def _build_row(record: dict, fetched_at: datetime | None = None) -> dict:
    """Map one ALFRED vintage record to a bronze table row (pure mapping)."""
    return {
        "wm2ns_date": _to_date(record["date"]),
        "realtime_start": _to_date(record["realtime_start"]),
        "realtime_end": _to_date(record["realtime_end"]),
        "m2_value": record["value"],
        "source_id": SOURCE_ID,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _alert_missing(rows: list[dict], context: str) -> list[tuple[date, date]]:
    """Alert (log WARNING) for every vintage row with a missing value.

    Returns the ``(wm2ns_date, realtime_start)`` keys alerted. Seam for the
    Telegram alert (T-10); the ingest never silently stores a NULL value.
    """
    missing = [
        (r["wm2ns_date"], r["realtime_start"]) for r in rows if r["m2_value"] is None
    ]
    for obs_date, rt_start in missing:
        logger.warning(
            "FRED M2 %s: missing value for %s (vintage %s) - source delivered `.`",
            context, obs_date.isoformat(), rt_start.isoformat(),
        )
    return missing


def _prepare_rows(records: list[dict], fetched_at: datetime, context: str) -> list[dict]:
    """Build vintage rows, alert on any missing value, and drop them."""
    rows = [_build_row(r, fetched_at=fetched_at) for r in records]
    _alert_missing(rows, context)
    return [r for r in rows if r["m2_value"] is not None]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the composite natural key (wm2ns_date, realtime_start).
# Source rows are deduped within the batch (latest datetime_update wins) so MERGE
# never sees the same vintage twice. realtime_end is mutable (UPDATE SET): an
# open vintage gets closed off when a newer revision supersedes it.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.wm2ns_date, s.realtime_start ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.wm2ns_date = S.wm2ns_date AND T.realtime_start = S.realtime_start
WHEN MATCHED THEN UPDATE SET
  realtime_end    = S.realtime_end,
  m2_value        = S.m2_value,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  wm2ns_date, realtime_start, realtime_end, m2_value, source_id, datetime_update
) VALUES (
  S.wm2ns_date, S.realtime_start, S.realtime_end, S.m2_value, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("wm2ns_date", "DATE", row["wm2ns_date"]),
        bigquery.ScalarQueryParameter("realtime_start", "DATE", row["realtime_start"]),
        bigquery.ScalarQueryParameter("realtime_end", "DATE", row["realtime_end"]),
        bigquery.ScalarQueryParameter("m2_value", "FLOAT64", row["m2_value"]),
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
    """Idempotent MERGE of vintage rows, chunked to stay under the partition cap.

    Each row's partition is its ``wm2ns_date``; a chunk of ``chunk_size`` rows
    spans at most that many partitions. Replaying any range MERGEs the same
    composite keys (idempotent).
    """
    if not rows:
        return
    size = chunk_size or MAX_MERGE_PARTITIONS
    for chunk in _chunked(rows, size):
        _upsert_chunk(client, chunk)


# --- Public entry points ------------------------------------------------------

def backfill_history(client: bigquery.Client | None = None) -> int:  # pragma: no cover - live network
    """Back-fill the full vintage history of WM2NS from ALFRED into bronze.

    Pulls every revision (widest realtime window), drops missing values, and
    upserts all vintage rows (chunked). Returns the row count written.
    """
    fetched_at = datetime.now(timezone.utc)
    payload = request_observations_json(
        SERIES_ID, realtime_start=REALTIME_MIN, realtime_end=REALTIME_MAX
    )
    rows = _prepare_rows(parse_vintages(payload), fetched_at, "back-fill")
    if not rows:
        logger.warning("FRED M2 back-fill: no writable vintages parsed")
        return 0
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    logger.info(
        "FRED M2 back-fill: upserting %d vintage row(s) into %s", len(rows), _table_fqn()
    )
    _upsert_rows(client, rows)
    logger.info("FRED M2 back-fill complete: %d row(s) written", len(rows))
    return len(rows)


def ingest_latest(client: bigquery.Client | None = None) -> list[dict]:  # pragma: no cover - live network
    """Refresh recent vintages for the daily job.

    Re-requests the last ``DAILY_LOOKBACK_DAYS`` of observations with a recent
    ``realtime_start`` (open ``realtime_end``) so any newly published or revised
    vintage is captured and MERGEd in.
    """
    today = datetime.now(timezone.utc).date()
    lookback = (today - timedelta(days=DAILY_LOOKBACK_DAYS))
    fetched_at = datetime.now(timezone.utc)
    payload = request_observations_json(
        SERIES_ID,
        observation_start=lookback,
        realtime_start=lookback.isoformat(),
        realtime_end=REALTIME_MAX,
    )
    rows = _prepare_rows(parse_vintages(payload), fetched_at, "daily")
    if not rows:
        logger.warning("FRED M2 daily: no writable vintages in the recent window")
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    _upsert_rows(client, rows)
    return rows


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest M2 (WM2NS) with point-in-time vintages from FRED/ALFRED into bronze."
    )
    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full vintage history (ALFRED widest realtime window).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover - CLI wiring
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    if args.backfill:
        n = backfill_history()
        logger.info("Back-filled %d vintage row(s)", n)
    else:
        rows = ingest_latest()
        logger.info("Wrote %d vintage row(s)", len(rows))


if __name__ == "__main__":  # pragma: no cover
    main()
