"""
Stage 2 — silver (ohlcv_validated) → silver (rsi_features).

Computes Wilder-smoothed RSI incrementally and stores the recursive state
(avg_gain / avg_loss) so that future runs require only the latest candle.

Wilder smoothing is written in its exponential form with ``alfa = 1/N``, which
is algebraically identical:
    avg = gain·alfa + avg_prev·(1−alfa)  ≡  (avg_prev·(N−1) + gain) / N

Two execution modes, selected automatically:

  Bootstrap — rsi_features is empty for (symbol, temporality, rsi_period):
    All historical OHLCV rows are read, sorted by date, and the recursion
    *starts at zero* on the first candle (avg_gain = avg_loss = 0).  A row is
    emitted from the very first candle; the zero seed decays geometrically, so
    recent values converge to the standard RSI.

  Incremental — prior state exists in rsi_features:
    Only rows after the last computed date are read.  Each new candle continues
    the recursion from the stored (avg_gain, avg_loss) state — the same Wilder
    step as the bootstrap, so both modes share one code path.

Beam's role: reads OHLCV rows from BigQuery, collects them into a single
Python list (sequential algorithm requirement), applies the RSI computation
in a FlatMap, and writes results to a staging table.  Prior state is fetched
outside Beam via a direct BigQuery client call (avoids a complex side input
for a singleton query).

Write pattern: staging (WRITE_TRUNCATE) → MERGE into rsi_features.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_PROJECT = "trade-390514"
_SILVER_TABLE = "prod_trade_silver.ohlcv_validated"
_STAGING = "prod_trade_silver.rsi_features_staging"
_TARGET = "prod_trade_silver.rsi_features"

_STAGING_SCHEMA = {
    "fields": [
        {"name": "symbol",            "type": "STRING",    "mode": "REQUIRED"},
        {"name": "temporality",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "rsi_period",        "type": "INT64",     "mode": "REQUIRED"},
        {"name": "datetime_update",   "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "time_period_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "price_close",       "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "var_p_recursive",   "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "var_n_recursive",   "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "rsi",               "type": "FLOAT64",   "mode": "NULLABLE"},
    ]
}

_READ_ALL_OHLCV = """
SELECT time_period_start, price_close
FROM `{project}.{table}`
WHERE symbol = '{symbol}' AND temporality = '{temporality}'
ORDER BY time_period_start
"""

_READ_NEW_OHLCV = """
SELECT time_period_start, price_close
FROM `{project}.{table}`
WHERE symbol = '{symbol}' AND temporality = '{temporality}'
  AND DATE(time_period_start) > '{last_date}'
ORDER BY time_period_start
"""

_READ_LAST_STATE = """
SELECT price_close, var_p_recursive, var_n_recursive, time_period_start
FROM `{project}.{target}`
WHERE symbol = '{symbol}' AND temporality = '{temporality}' AND rsi_period = {rsi_period}
ORDER BY time_period_start DESC
LIMIT 1
"""

_MERGE_SQL = """
MERGE `{project}.{target}` AS T
USING `{project}.{staging}` AS S
ON  T.symbol            = S.symbol
AND T.temporality       = S.temporality
AND T.rsi_period        = S.rsi_period
AND T.time_period_start = S.time_period_start
WHEN MATCHED THEN UPDATE SET
  datetime_update = S.datetime_update,
  price_close     = S.price_close,
  var_p_recursive = S.var_p_recursive,
  var_n_recursive = S.var_n_recursive,
  rsi             = S.rsi
WHEN NOT MATCHED THEN INSERT (
  symbol, temporality, rsi_period, datetime_update,
  time_period_start, price_close, var_p_recursive, var_n_recursive, rsi
) VALUES (
  S.symbol, S.temporality, S.rsi_period, S.datetime_update,
  S.time_period_start, S.price_close, S.var_p_recursive, S.var_n_recursive, S.rsi
)
"""


# ---------------------------------------------------------------------------
# Pure RSI computation (no Beam dependency — easy to unit-test separately)
# ---------------------------------------------------------------------------

def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    # rs = 0 when avg_loss = 0, so the zero-seed first candle yields RSI 0
    # (rather than the conventional 100). Scale is 0–100.
    rs = 0.0 if avg_loss == 0.0 else avg_gain / avg_loss
    return 100.0 * (1.0 - 1.0 / (1.0 + rs))


def _ts_str(ts: Any) -> str:
    if isinstance(ts, str):
        return ts
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


def compute_rsi_rows(
    ohlcv_rows: list[dict],
    prior_state: dict | None,
    symbol: str,
    temporality: str,
    rsi_period: int,
) -> list[dict]:
    """Compute RSI rows from a list of OHLCV dicts.

    ``ohlcv_rows`` must contain at least ``time_period_start`` and
    ``price_close`` keys.  Rows are sorted internally by
    ``time_period_start``.

    When ``prior_state`` is ``None`` (bootstrap), the recursion starts at zero
    on the first candle and a row is emitted from that first candle onward.
    When ``prior_state`` is provided (incremental), the recursion continues from
    the stored (avg_gain, avg_loss) state for each new row. Both modes apply the
    identical Wilder step, so they share one loop.

    Returns a list of dicts matching ``rsi_features`` schema.
    """
    if not ohlcv_rows:
        return []

    rows = sorted(ohlcv_rows, key=lambda r: r["time_period_start"])
    now_str = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    def _make_row(ts, close: float, avg_gain: float, avg_loss: float) -> dict:
        return {
            "symbol":            symbol,
            "temporality":       temporality,
            "rsi_period":        rsi_period,
            "datetime_update":   now_str,
            "time_period_start": _ts_str(ts),
            "price_close":       close,
            "var_p_recursive":   avg_gain,
            "var_n_recursive":   avg_loss,
            "rsi":               _rsi_value(avg_gain, avg_loss),
        }

    if prior_state is None:
        # Bootstrap: seed the recursion at zero and emit the first candle as-is.
        avg_gain, avg_loss = 0.0, 0.0
        prev_close = rows[0]["price_close"]
        results.append(_make_row(
            rows[0]["time_period_start"], rows[0]["price_close"], avg_gain, avg_loss
        ))
        start_idx = 1
    else:
        # Incremental: continue from the stored state; the first new candle
        # steps off the prior candle's close.
        avg_gain   = prior_state["var_p_recursive"]
        avg_loss   = prior_state["var_n_recursive"]
        prev_close = prior_state["price_close"]
        start_idx = 0

    # One Wilder step per candle. Stepping by list index (not calendar date)
    # keeps the recursion correct even if a daily/weekly candle is missing.
    for i in range(start_idx, len(rows)):
        close    = rows[i]["price_close"]
        delta    = close - prev_close
        avg_gain = (avg_gain * (rsi_period - 1) + max(delta, 0.0)) / rsi_period
        avg_loss = (avg_loss * (rsi_period - 1) + max(-delta, 0.0)) / rsi_period
        results.append(_make_row(rows[i]["time_period_start"], close, avg_gain, avg_loss))
        prev_close = close

    return results


# ---------------------------------------------------------------------------
# Beam stage
# ---------------------------------------------------------------------------

def _fetch_last_rsi_state(
    bq: bigquery.Client,
    project: str,
    symbol: str,
    temporality: str,
    rsi_period: int,
) -> dict | None:
    """Return the last rsi_features row or None if the table is empty."""
    query = _READ_LAST_STATE.format(
        project=project, target=_TARGET,
        symbol=symbol, temporality=temporality, rsi_period=rsi_period,
    )
    rows = list(bq.query(query).result())
    if not rows:
        return None
    row = dict(rows[0])
    # Ensure numeric types (BigQuery client may return Decimal).
    row["var_p_recursive"] = float(row["var_p_recursive"])
    row["var_n_recursive"] = float(row["var_n_recursive"])
    row["price_close"]     = float(row["price_close"])
    return row


def run_rsi(config: dict) -> None:
    """Run Stage 2: ohlcv_validated → rsi_features."""
    project      = config.get("project", _PROJECT)
    symbol       = config.get("symbol", "BTCUSD")
    temporality  = config.get("temporality", "1d")
    rsi_period   = config.get("rsi_period", 14)
    pipeline_args = config.get("pipeline_args", [])

    bq = bigquery.Client(project=project)
    prior_state = _fetch_last_rsi_state(bq, project, symbol, temporality, rsi_period)

    if prior_state is None:
        logger.info("No prior RSI state found — bootstrapping full history.")
        read_query = _READ_ALL_OHLCV.format(
            project=project, table=_SILVER_TABLE,
            symbol=symbol, temporality=temporality,
        )
    else:
        last_ts = prior_state["time_period_start"]
        last_date = last_ts.date() if hasattr(last_ts, "date") else last_ts
        logger.info("Prior RSI state found at %s — incremental update.", last_date)
        read_query = _READ_NEW_OHLCV.format(
            project=project, table=_SILVER_TABLE,
            symbol=symbol, temporality=temporality,
            last_date=str(last_date),
        )

    options = PipelineOptions(
        pipeline_args,
        project=project,
        region=config.get("region", "us-central1"),
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadOHLCV" >> ReadFromBigQuery(
                query=read_query,
                use_standard_sql=True,
                project=project,
                gcs_location=config.get("temp_location"),
            )
            # Collect all rows into one list — required for the sequential
            # Wilder RSI algorithm.
            | "CollectAll" >> beam.combiners.ToList()
            | "ComputeRSI" >> beam.FlatMap(
                compute_rsi_rows,
                prior_state=prior_state,
                symbol=symbol,
                temporality=temporality,
                rsi_period=rsi_period,
            )
            | "WriteStaging" >> WriteToBigQuery(
                table=f"{project}:{_STAGING}",
                schema=_STAGING_SCHEMA,
                write_disposition=beam_bq.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam_bq.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    bq.query(
        _MERGE_SQL.format(project=project, target=_TARGET, staging=_STAGING)
    ).result()
    logger.info("Stage 2 complete: rsi_features updated for %s/%s rsi_period=%d",
                symbol, temporality, rsi_period)
