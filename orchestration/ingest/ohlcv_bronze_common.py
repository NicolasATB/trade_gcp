"""Shared multi-asset OHLCV bronze loader (provider-agnostic).

Holds the part of the multi-asset ETF ingest that does **not** depend on the
provider: mapping a parsed OHLC record to a bronze row, dropping the in-progress
current-UTC-day bar, and the idempotent ``MERGE`` on the natural key
``(symbol, candle_date)`` (chunked under BigQuery's 4000-partitions-per-DML cap).
It is **not an entry-point**.

Both ETF ingests (``yahoo_etf_ingest`` over Yahoo Finance, ``stooq_etf_ingest``
over stooq) share one bronze schema and one upsert, so a fix lands once for both;
only the fetch/parse (the provider-specific part, in ``yahoo_common`` /
``stooq_common``) and the config (target table, ``source_id``) differ. The two
sources compete by ``priority`` in the silver consolidation (T-21), so they write
to separate per-source tables with the identical shape.

Bronze is raw: this loader only maps and MERGEs; it never stitches, re-scales or
dedups by priority — those transforms live downstream (silver). See the
bigquery-beam-patterns skill.

Authentication for BigQuery is Google Application Default Credentials, so the
same code runs locally and on the VM/CI.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment variables) -------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")
BQ_DATASET = os.environ.get("BQ_BRONZE_DATASET", "prod_trade_bronze")

# A single MERGE (DML) may modify at most 4000 partitions; the ETF tables
# partition by DATE_TRUNC(candle_date, MONTH), so even a full multi-symbol
# back-fill touches well under that — the chunk size is a safety margin.
MAX_MERGE_PARTITIONS = int(os.environ.get("ETF_MAX_MERGE_PARTITIONS", "3500"))


def _table_fqn(table: str) -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{table}"


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _as_float(value) -> float | None:
    return None if value is None else float(value)


# --- Row building (pure, unit-tested) ----------------------------------------

def build_row(symbol: str, record: dict, source_id: int, fetched_at: datetime | None = None) -> dict:
    """Map one parsed OHLC ``record`` for ``symbol`` to a bronze table row.

    ``record`` is the provider-agnostic shape the fetch/parse layer returns:
    ``{"date", "open", "high", "low", "close", "volume"}`` (``date`` is a
    :class:`datetime.date`). Pure mapping — no I/O, no transforms.
    """
    return {
        "symbol": symbol,
        "candle_date": record["date"],
        "price_open": _as_float(record.get("open")),
        "price_high": _as_float(record.get("high")),
        "price_low": _as_float(record.get("low")),
        "price_close": _as_float(record.get("close")),
        "volume_traded": _as_float(record.get("volume")),
        "source_id": source_id,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _closed_only(rows: list[dict], today: date | None = None) -> list[dict]:
    """Drop the in-progress bar for the current UTC day (close not final yet)."""
    today = today or datetime.now(timezone.utc).date()
    return [r for r in rows if r["candle_date"] < today]


def prepare_rows(
    symbol: str,
    records: list[dict],
    source_id: int,
    fetched_at: datetime,
    today: date | None = None,
) -> list[dict]:
    """Build rows for ``symbol`` and keep only fully closed bars."""
    rows = [build_row(symbol, r, source_id, fetched_at=fetched_at) for r in records]
    return _closed_only(rows, today=today)


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural key (symbol, candle_date). Source rows are
# deduped within the batch (latest datetime_update wins) so MERGE never sees a
# key twice.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.symbol, s.candle_date ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.symbol = S.symbol AND T.candle_date = S.candle_date
WHEN MATCHED THEN UPDATE SET
  price_open      = S.price_open,
  price_high      = S.price_high,
  price_low       = S.price_low,
  price_close     = S.price_close,
  volume_traded   = S.volume_traded,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  symbol, candle_date, price_open, price_high, price_low, price_close,
  volume_traded, source_id, datetime_update
) VALUES (
  S.symbol, S.candle_date, S.price_open, S.price_high, S.price_low, S.price_close,
  S.volume_traded, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("symbol", "STRING", row["symbol"]),
        bigquery.ScalarQueryParameter("candle_date", "DATE", row["candle_date"]),
        bigquery.ScalarQueryParameter("price_open", "FLOAT64", row["price_open"]),
        bigquery.ScalarQueryParameter("price_high", "FLOAT64", row["price_high"]),
        bigquery.ScalarQueryParameter("price_low", "FLOAT64", row["price_low"]),
        bigquery.ScalarQueryParameter("price_close", "FLOAT64", row["price_close"]),
        bigquery.ScalarQueryParameter("volume_traded", "FLOAT64", row["volume_traded"]),
        bigquery.ScalarQueryParameter("source_id", "INT64", row["source_id"]),
        bigquery.ScalarQueryParameter("datetime_update", "TIMESTAMP", row["datetime_update"]),
    )


def _upsert_chunk(client: bigquery.Client, table: str, rows: list[dict]) -> None:
    struct_params = [_struct_param(r) for r in rows]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)]
    )
    client.query(_MERGE_SQL.format(table=_table_fqn(table)), job_config=job_config).result()


def upsert_rows(client: bigquery.Client, table: str, rows: list[dict], chunk_size: int | None = None) -> None:
    """Idempotent MERGE of multi-asset OHLCV rows, chunked under the partition limit.

    Rows may span several symbols; the MERGE key is ``(symbol, candle_date)``, so
    replaying any overlapping range MERGEs the same keys (idempotent). Chunking by
    ``chunk_size`` rows keeps every MERGE below the 4000-partitions-per-DML cap.
    """
    if not rows:
        return
    size = chunk_size or MAX_MERGE_PARTITIONS
    for chunk in _chunked(rows, size):
        _upsert_chunk(client, table, chunk)
