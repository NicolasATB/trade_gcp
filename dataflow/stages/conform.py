"""
Stage 1 — bronze → silver (ohlcv_validated), multi-asset.

Reads daily candles for the ``[start_date, end_date]`` range from every bronze
candle table — BTC spot (Bitstamp for the pre-Binance history up to 2017-08-16,
Binance from 2017-08-17 onward) plus the eight Strategy-3 ETFs from two competing
sources (Yahoo primary, Tiingo fallback) — canonicalises the symbol, reconciles
Tiingo's raw close to Yahoo's split-adjusted basis (split-only back-adjustment,
T-21), consolidates by source priority (business rule: the source with the
**highest** ``priority`` in ``prod_trade_control.source_priority`` wins when more
than one source covers the same ``(symbol, date)``), normalises fields to the
canonical OHLCV schema, writes to a staging table, and merges into
``prod_trade_silver.ohlcv_validated``.

Idempotency: the staging table is truncated before the Beam write; the
final MERGE upserts on the natural key (symbol, temporality,
time_period_start), so replaying any day is safe.

Write pattern (staging + MERGE):
  1. Beam writes normalised rows to ``ohlcv_validated_staging`` (WRITE_TRUNCATE
     via FILE_LOADS — requires ``--temp_location gs://...``).
  2. A BigQuery MERGE propagates changes to the target table.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_PROJECT = "trade-390514"
# BTC candle tables: single canonical symbol BTCUSD; the raw close is the
# conformed value (crypto spot has no splits/dividends to adjust).
_BTC_BRONZE_TABLES = [
    "prod_trade_bronze.bitstamp_btcusd_daily_raw",  # pre-Binance history (≤ 2017-08-16)
    "prod_trade_bronze.binance_btcusd_daily_raw",   # 2017-08-17 onward
]
# ETF tables (Strategy 3 universe): symbol is already canonical (SPY, EFA, …).
# Yahoo's close is split-adjusted as delivered; Tiingo's is raw and gets the
# split-only back-adjustment below, so both sources share one basis (T-21).
_YAHOO_ETF_TABLE = "prod_trade_bronze.yahoo_etf_daily_raw"
_TIINGO_ETF_TABLE = "prod_trade_bronze.tiingo_etf_daily_raw"
# Every bronze table the consolidation reads (BTC spot + both ETF sources).
_BRONZE_TABLES = [*_BTC_BRONZE_TABLES, _YAHOO_ETF_TABLE, _TIINGO_ETF_TABLE]
_SOURCE_PRIORITY_TABLE = "prod_trade_control.source_priority"
_STAGING = "prod_trade_silver.ohlcv_validated_staging"
_TARGET = "prod_trade_silver.ohlcv_validated"

_STAGING_SCHEMA = {
    "fields": [
        {"name": "symbol",            "type": "STRING",    "mode": "REQUIRED"},
        {"name": "temporality",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "source_id",         "type": "INT64",     "mode": "NULLABLE"},
        {"name": "datetime_update",   "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "time_period_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "time_period_end",   "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "time_open",         "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "time_close",        "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "price_open",        "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "price_high",        "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "price_low",         "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "price_close",       "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "volume_traded",     "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "trades_count",      "type": "INT64",     "mode": "NULLABLE"},
    ]
}

# UNION ALL branches, one family at a time. Every branch projects the SAME
# columns (canonical symbol, candle_date, OHLCV, source_id) and prunes its own
# candle_date partition. The canonical symbol is set in SQL: a literal 'BTCUSD'
# for the BTC tables (single-symbol per exchange), a passthrough for the ETF
# tables (already canonical), so the consolidation dedups per (symbol, date).
_READ_BTC_BRANCH = """  SELECT 'BTCUSD' AS symbol, candle_date, price_open, price_high, price_low,
         price_close, volume_traded, source_id
  FROM `{project}.{table}`
  WHERE candle_date BETWEEN '{start_date}' AND '{end_date}'"""

# Yahoo ETF: close already split-adjusted as delivered → passthrough.
_READ_YAHOO_ETF_BRANCH = """  SELECT symbol, candle_date, price_open, price_high, price_low,
         price_close, volume_traded, source_id
  FROM `{project}.{table}`
  WHERE candle_date BETWEEN '{start_date}' AND '{end_date}'"""

# Tiingo ETF: raw close → split-ONLY back-adjusted to Yahoo's basis (T-21). Divide
# every price by f = product of split_factors whose ex-date is AFTER the bar
# (volume scales the inverse way). The split lookup scans the WHOLE table (splits
# are a handful, clustered by symbol), not the read window, so a chunk that ends
# before a split is still adjusted correctly. Yahoo never needs this (pre-adjusted
# close); BTC never needs this (no corporate actions).
_READ_TIINGO_ETF_BRANCH = """  SELECT symbol, candle_date,
         price_open / f AS price_open, price_high / f AS price_high,
         price_low / f AS price_low, price_close / f AS price_close,
         volume_traded * f AS volume_traded, source_id
  FROM (
    SELECT tt.symbol, tt.candle_date, tt.price_open, tt.price_high, tt.price_low,
           tt.price_close, tt.volume_traded, tt.source_id,
           IFNULL(EXP((
             SELECT SUM(LN(s.split_factor))
             FROM `{project}.{table}` AS s
             WHERE s.symbol = tt.symbol AND s.candle_date > tt.candle_date
               AND s.split_factor IS NOT NULL AND s.split_factor != 1.0
           )), 1.0) AS f
    FROM `{project}.{table}` AS tt
    WHERE candle_date BETWEEN '{start_date}' AND '{end_date}'
  )"""

# Multi-source consolidation: business rule — the source with the HIGHEST
# `priority` in source_priority wins when several sources cover the same
# (symbol, date). BTC: Bitstamp (4) > Binance (3), date-disjoint today so the
# tie-break is a safety net. ETFs: Yahoo (2) > Tiingo (1), so a symbol/date Yahoo
# gaps fails over to Tiingo. Dedup is per (symbol, candle_date): BTC's raw
# exchange symbols both canonicalise to BTCUSD in SQL, and the eight ETFs are
# distinct symbols. Unregistered sources (priority NULL) sort last (DESC → NULLs last).
_READ_QUERY = """
WITH bronze AS (
{union_sql}
)
SELECT symbol, candle_date, price_open, price_high, price_low,
       price_close, volume_traded, source_id
FROM (
  SELECT b.*, ROW_NUMBER() OVER (
           PARTITION BY b.symbol, b.candle_date
           ORDER BY p.priority DESC
         ) AS rn
  FROM bronze AS b
  LEFT JOIN `{project}.{priority_table}` AS p USING (source_id)
)
WHERE rn = 1
"""


def _build_read_query(project: str, start_date, end_date) -> str:
    """Render the multi-asset, multi-source bronze read for ``[start_date, end_date]``.

    BTC branches canonicalise to ``BTCUSD``; the Yahoo ETF branch passes its
    already-split-adjusted close through; the Tiingo ETF branch back-adjusts its
    raw close to the same split-only basis. All branches feed one priority dedup
    per ``(symbol, candle_date)``.
    """
    s, e = start_date.isoformat(), end_date.isoformat()
    branches = [
        _READ_BTC_BRANCH.format(project=project, table=table, start_date=s, end_date=e)
        for table in _BTC_BRONZE_TABLES
    ]
    branches.append(
        _READ_YAHOO_ETF_BRANCH.format(project=project, table=_YAHOO_ETF_TABLE, start_date=s, end_date=e)
    )
    branches.append(
        _READ_TIINGO_ETF_BRANCH.format(project=project, table=_TIINGO_ETF_TABLE, start_date=s, end_date=e)
    )
    union_sql = "\n  UNION ALL\n".join(branches)
    return _READ_QUERY.format(
        union_sql=union_sql, project=project, priority_table=_SOURCE_PRIORITY_TABLE,
    )

# MERGE from staging → target on the natural key.
_MERGE_SQL = """
MERGE `{project}.{target}` AS T
USING `{project}.{staging}` AS S
ON  T.symbol            = S.symbol
AND T.temporality       = S.temporality
AND T.time_period_start = S.time_period_start
WHEN MATCHED THEN UPDATE SET
  source_id       = S.source_id,
  datetime_update = S.datetime_update,
  time_period_end = S.time_period_end,
  time_open       = S.time_open,
  time_close      = S.time_close,
  price_open      = S.price_open,
  price_high      = S.price_high,
  price_low       = S.price_low,
  price_close     = S.price_close,
  volume_traded   = S.volume_traded,
  trades_count    = S.trades_count
WHEN NOT MATCHED THEN INSERT (
  symbol, temporality, source_id, datetime_update,
  time_period_start, time_period_end, time_open, time_close,
  price_open, price_high, price_low, price_close, volume_traded, trades_count
) VALUES (
  S.symbol, S.temporality, S.source_id, S.datetime_update,
  S.time_period_start, S.time_period_end, S.time_open, S.time_close,
  S.price_open, S.price_high, S.price_low, S.price_close, S.volume_traded, S.trades_count
)
"""

# Aggregates daily rows in ohlcv_validated to a weekly candle and merges it
# back into the same table with temporality='1w'.  Only the current week is
# recomputed, so historical weekly candles are never touched.
_WEEKLY_MERGE_SQL = """
MERGE `{project}.{target}` AS T
USING (
  SELECT
    symbol,
    '1w'               AS temporality,
    MAX(source_id)     AS source_id,
    CURRENT_TIMESTAMP() AS datetime_update,
    TIMESTAMP(week_start)                                                           AS time_period_start,
    TIMESTAMP(DATE_ADD(week_start, INTERVAL 6 DAY))                                 AS time_period_end,
    ARRAY_AGG(price_open  ORDER BY time_period_start ASC  LIMIT 1)[SAFE_OFFSET(0)]   AS price_open,
    MAX(price_high)    AS price_high,
    MIN(price_low)     AS price_low,
    ARRAY_AGG(price_close ORDER BY time_period_start DESC LIMIT 1)[SAFE_OFFSET(0)]   AS price_close,
    SUM(volume_traded) AS volume_traded,
    CAST(NULL AS INT64) AS trades_count
  FROM (
    SELECT *, DATE_TRUNC(DATE(time_period_start), WEEK(MONDAY)) AS week_start
    FROM `{project}.{target}`
    WHERE temporality = '1d'
      AND DATE(time_period_start) >= DATE_TRUNC(DATE '{start_date}', WEEK(MONDAY))
  )
  GROUP BY symbol, week_start
) AS S
ON  T.symbol            = S.symbol
AND T.temporality       = S.temporality
AND T.time_period_start = S.time_period_start
WHEN MATCHED THEN UPDATE SET
  source_id       = S.source_id,
  datetime_update = S.datetime_update,
  time_period_end = S.time_period_end,
  price_open      = S.price_open,
  price_high      = S.price_high,
  price_low       = S.price_low,
  price_close     = S.price_close,
  volume_traded   = S.volume_traded
WHEN NOT MATCHED THEN INSERT (
  symbol, temporality, source_id, datetime_update,
  time_period_start, time_period_end,
  price_open, price_high, price_low, price_close,
  volume_traded, trades_count
) VALUES (
  S.symbol, S.temporality, S.source_id, S.datetime_update,
  S.time_period_start, S.time_period_end,
  S.price_open, S.price_high, S.price_low, S.price_close,
  S.volume_traded, S.trades_count
)
"""


def _truncate_staging(bq: bigquery.Client, project: str) -> None:  # pragma: no cover - requires live BigQuery
    """Guarantee an empty staging table before the Beam write.

    With FILE_LOADS, WRITE_TRUNCATE only takes effect when at least one record
    triggers a load job. A zero-row run (e.g. a date range with no bronze rows)
    leaves the previous staging contents in place, and the unconditional MERGE
    would replay them. Truncating up front closes that gap.
    """
    try:
        bq.query(f"TRUNCATE TABLE `{project}.{_STAGING}`").result()
    except NotFound:
        pass  # first run: staging not created yet (CREATE_IF_NEEDED will make it)


def _aggregate_weekly(bq: bigquery.Client, project: str, start_date) -> None:  # pragma: no cover - requires live BigQuery
    # Recompute every week from the range's first week onward, keeping weekly
    # candles consistent with the daily candles just merged.
    bq.query(
        _WEEKLY_MERGE_SQL.format(
            project=project, target=_TARGET, start_date=start_date.isoformat()
        )
    ).result()


def _ts_str(ts) -> str:  # pragma: no cover - unused helper kept for symmetry with other stages
    """Convert a datetime or date-like value to an ISO timestamp string."""
    if isinstance(ts, str):
        return ts
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


class _NormaliseOhlcvRow(beam.DoFn):
    """Map a consolidated bronze row → ``ohlcv_validated`` schema.

    The canonical ``symbol`` and the split-adjusted prices are already chosen in
    the read query (BTC → ``BTCUSD``; ETFs passthrough / back-adjusted), so this
    DoFn only reshapes the row: it derives the UTC day bounds and nulls
    ``trades_count``.
    """

    def process(self, element):
        candle_date = element["candle_date"]
        if isinstance(candle_date, str):
            from datetime import date
            candle_date = date.fromisoformat(candle_date)

        tps = datetime(candle_date.year, candle_date.month, candle_date.day,
                       tzinfo=timezone.utc)
        tpe = datetime(candle_date.year, candle_date.month, candle_date.day,
                       23, 59, 59, tzinfo=timezone.utc)

        yield {
            "symbol":            element["symbol"],
            "temporality":       "1d",
            "source_id":         element.get("source_id"),
            "datetime_update":   datetime.now(timezone.utc).isoformat(),
            "time_period_start": tps.isoformat(),
            "time_period_end":   tpe.isoformat(),
            "time_open":         tps.isoformat(),
            "time_close":        tpe.isoformat(),
            "price_open":        element.get("price_open"),
            "price_high":        element.get("price_high"),
            "price_low":         element.get("price_low"),
            "price_close":       element.get("price_close"),
            "volume_traded":     element.get("volume_traded"),
            "trades_count":      None,
        }


def run_conform(config: dict) -> None:  # pragma: no cover - Beam/Dataflow + BigQuery
    """Run Stage 1: bronze → ohlcv_validated for ``[start_date, end_date]``."""
    project      = config.get("project", _PROJECT)
    start_date   = config["start_date"]
    end_date     = config.get("end_date", start_date)
    pipeline_args = config.get("pipeline_args", [])

    options = PipelineOptions(
        pipeline_args,
        project=project,
        region=config.get("region", "us-central1"),
    )

    read_query = _build_read_query(project, start_date, end_date)

    bq = bigquery.Client(project=project)
    _truncate_staging(bq, project)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadBronze" >> ReadFromBigQuery(
                query=read_query,
                use_standard_sql=True,
                project=project,
                gcs_location=config.get("temp_location"),
            )
            | "Normalise" >> beam.ParDo(_NormaliseOhlcvRow())
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
    logger.info("Daily candles merged into ohlcv_validated for %s..%s", start_date, end_date)

    # Aggregate daily candles to weekly and upsert every week touched by the
    # range (from the start week onward). Business rule: weeks run Monday→Sunday
    # (BigQuery WEEK(MONDAY)), so time_period_start is the week's Monday and
    # partially-filled weeks stay up to date as new daily candles arrive.
    _aggregate_weekly(bq, project, start_date)
    logger.info("Stage 1 complete: weekly ohlcv_validated updated from week of %s", start_date)
