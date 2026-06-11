"""
Stage 3 — silver (rsi_features) → gold (fact_signals).

Replicates the strategy from ``ag-determina-parametros-de-estrategia-rsi.ipynb``:

  Trend filter (weekly RSI)
  ─────────────────────────
  The strategy is only active inside a bullish window defined by the weekly
  RSI.  The window opens when weekly RSI enters the range
  [weekly_rsi_trend_start, weekly_rsi_trend_end] and closes when weekly RSI
  leaves it.  Because trend state is inherently sequential, this pipeline
  reads all weekly RSI history, computes trend state with a stateful walk-
  forward, then joins to the target daily candle.

  Signal rules (daily RSI, within trend window)
  ──────────────────────────────────────────────
  BUY    — daily RSI < daily_rsi_oversold
  SELL   — daily RSI > daily_rsi_overbought  OR  trend just ended
  NEUTRAL — otherwise

  Parameters are read from ``prod_trade_strategy.strategy_rsi_daily_week``
  (latest active version).  They are calibrated offline by the AG notebook
  and stored as a versioned row; the pipeline never hard-codes thresholds.

Write pattern: results are written to a staging table (WRITE_TRUNCATE) and
then merged into ``prod_trade_gold.fact_signals``.

RSI scale: values in rsi_features are stored on a 0–100 scale, matching the
``trigger_params`` examples in the schema ("rsi": 29.8, "oversold": 30).
"""

from __future__ import annotations

import bisect
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_PROJECT = "trade-390514"
_RSI_TABLE = "prod_trade_silver.rsi_features"
_PARAMS_TABLE = "prod_trade_strategy.strategy_rsi_daily_week"
_STAGING = "prod_trade_gold.fact_signals_staging"
_TARGET = "prod_trade_gold.fact_signals"

_STAGING_SCHEMA = {
    "fields": [
        {"name": "symbol",           "type": "STRING",    "mode": "REQUIRED"},
        {"name": "temporality",      "type": "STRING",    "mode": "REQUIRED"},
        {"name": "strategy_id",      "type": "INT64",     "mode": "REQUIRED"},
        {"name": "signal",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "trigger_params",   "type": "JSON",      "mode": "NULLABLE"},
        {"name": "signal_start",     "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "signal_created_at","type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

# Read all weekly RSI history for a symbol (needed for stateful trend walk-forward).
# Warm-up rows (rsi IS NULL, the first rsi_period candles of a bootstrap) carry
# no valid RSI and are excluded.
_READ_WEEKLY_RSI = """
SELECT time_period_start, rsi
FROM `{project}.{table}`
WHERE symbol = '{symbol}' AND temporality = '1w' AND rsi_period = {rsi_period}
  AND rsi IS NOT NULL
ORDER BY time_period_start
"""

# Read the daily RSI rows for the target date range (inclusive). Warm-up rows
# (rsi IS NULL) produce no signal.
_READ_DAILY_RSI = """
SELECT time_period_start, rsi
FROM `{project}.{table}`
WHERE symbol = '{symbol}' AND temporality = '1d' AND rsi_period = {rsi_period}
  AND DATE(time_period_start) BETWEEN '{start_date}' AND '{end_date}'
  AND rsi IS NOT NULL
ORDER BY time_period_start
"""

_READ_ACTIVE_PARAMS = """
SELECT rsi_period, weekly_rsi_trend_start, weekly_rsi_trend_end,
       daily_rsi_oversold, daily_rsi_overbought
FROM `{project}.{table}`
WHERE is_active = TRUE
ORDER BY param_version DESC
LIMIT 1
"""

_MERGE_SQL = """
MERGE `{project}.{target}` AS T
USING `{project}.{staging}` AS S
ON  T.symbol        = S.symbol
AND T.temporality   = S.temporality
AND T.signal_start  = S.signal_start
AND T.strategy_id   = S.strategy_id
WHEN MATCHED THEN UPDATE SET
  signal           = S.signal,
  trigger_params   = S.trigger_params,
  signal_created_at = S.signal_created_at
WHEN NOT MATCHED THEN INSERT (
  symbol, temporality, strategy_id, signal,
  trigger_params, signal_start, signal_created_at
) VALUES (
  S.symbol, S.temporality, S.strategy_id, S.signal,
  S.trigger_params, S.signal_start, S.signal_created_at
)
"""


# ---------------------------------------------------------------------------
# Strategy logic (no Beam dependency — easy to unit-test)
# ---------------------------------------------------------------------------

def _week_start(d: date) -> date:
    """Monday-based week start, matching BigQuery ``DATE_TRUNC(d, WEEK(MONDAY))``.

    Business rule: weeks run Monday→Sunday, so the weekly candle's
    ``time_period_start`` is the Monday that opens the week."""
    return d - timedelta(days=d.weekday())


def _as_date(ts: Any) -> date:
    if isinstance(ts, date) and not isinstance(ts, datetime):
        return ts
    if hasattr(ts, "date"):
        return ts.date()
    return date.fromisoformat(str(ts)[:10])


def _compute_trend_states(
    weekly_rsi_history: list[dict],
    trend_start: float,
    trend_end: float,
) -> dict[date, bool]:
    """Walk-forward over weekly RSI history → trend state effective at each week.

    Mirrors the AG notebook's stateful loop:
      - Trend opens  when weekly RSI enters  [trend_start, trend_end].
      - Trend closes when weekly RSI exits the range (RSI >= trend_end).
    Transitions are driven by the *previous* week's RSI, so a week's own RSI
    never flips its own state. Returns ``{week_start_date: in_trend}`` so each
    daily candle can look up the trend state of its own week — essential when
    back-filling a range of historical days in one run.
    """
    if not weekly_rsi_history:
        return {}

    rows = sorted(weekly_rsi_history, key=lambda r: r["time_period_start"])
    states: dict[date, bool] = {}
    in_trend = False
    states[_week_start(_as_date(rows[0]["time_period_start"]))] = in_trend

    for i in range(1, len(rows)):
        prev_rsi = float(rows[i - 1]["rsi"])
        if not in_trend and prev_rsi <= trend_start:
            in_trend = True
        elif in_trend and prev_rsi >= trend_end:
            in_trend = False
        states[_week_start(_as_date(rows[i]["time_period_start"]))] = in_trend

    return states


def _compute_trend_state(
    weekly_rsi_history: list[dict],
    trend_start: float,
    trend_end: float,
) -> bool:
    """Trend state of the most recent week (thin wrapper over ``_compute_trend_states``)."""
    states = _compute_trend_states(weekly_rsi_history, trend_start, trend_end)
    if not states:
        return False
    return states[max(states)]


def _apply_signal(
    daily_rsi: float,
    in_trend: bool,
    params: dict,
) -> str:
    """Return BUY / SELL / NEUTRAL for a single daily candle."""
    if in_trend:
        if daily_rsi < params["daily_rsi_oversold"]:
            return "BUY"
        if daily_rsi > params["daily_rsi_overbought"]:
            return "SELL"
    else:
        # Outside trend window: flat — no open position encouraged.
        if daily_rsi > params["daily_rsi_overbought"]:
            return "SELL"
    return "NEUTRAL"


def _ts_str(ts: Any) -> str:
    if isinstance(ts, str):
        return ts
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


# ---------------------------------------------------------------------------
# Beam transform
# ---------------------------------------------------------------------------

class _ComputeSignalFn(beam.DoFn):
    """Join a daily RSI row with its week's trend state and emit a fact_signals row."""

    def __init__(
        self,
        trend_states: dict[date, bool],
        params: dict,
        strategy_id: int,
    ):
        self._trend_states = trend_states
        self._sorted_weeks = sorted(trend_states)
        self._params = params
        self._strategy_id = strategy_id

    def _trend_for(self, daily_ts: Any) -> bool:
        """Trend state of the week containing ``daily_ts``.

        Falls back to the most recent week on/before that week when the exact
        week has no weekly RSI yet (e.g. the current, still-forming week)."""
        wk = _week_start(_as_date(daily_ts))
        state = self._trend_states.get(wk)
        if state is not None:
            return state
        idx = bisect.bisect_right(self._sorted_weeks, wk) - 1
        return self._trend_states[self._sorted_weeks[idx]] if idx >= 0 else False

    def process(self, element):
        daily_rsi = float(element["rsi"])
        symbol = element.get("symbol", "BTCUSD")
        temporality = element.get("temporality", "1d")

        in_trend = self._trend_for(element["time_period_start"])
        signal = _apply_signal(daily_rsi, in_trend, self._params)

        trigger = {
            "daily_rsi":              round(daily_rsi, 4),
            "in_trend":               in_trend,
            "daily_rsi_oversold":     self._params["daily_rsi_oversold"],
            "daily_rsi_overbought":   self._params["daily_rsi_overbought"],
            "weekly_rsi_trend_start": self._params["weekly_rsi_trend_start"],
            "weekly_rsi_trend_end":   self._params["weekly_rsi_trend_end"],
        }

        yield {
            "symbol":            symbol,
            "temporality":       temporality,
            "strategy_id":       self._strategy_id,
            "signal":            signal,
            # Emit the dict (parsed JSON), not a json.dumps string: the column
            # is BigQuery JSON type, so FILE_LOADS must receive a JSON object to
            # store it as queryable JSON. A string would be stored as an escaped
            # JSON string and JSON_VALUE() would return NULL.
            "trigger_params":    trigger,
            "signal_start":      _ts_str(element["time_period_start"]),
            "signal_created_at": datetime.now(timezone.utc).isoformat(),
        }


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def _truncate_staging(bq: bigquery.Client, project: str) -> None:  # pragma: no cover - requires live BigQuery
    """Guarantee an empty staging table before the Beam write.

    With FILE_LOADS, WRITE_TRUNCATE only takes effect when at least one record
    triggers a load job. A zero-row run (e.g. a date range with no daily RSI
    rows) leaves the previous staging contents in place, and the unconditional
    MERGE would replay them. Truncating up front closes that gap.
    """
    try:
        bq.query(f"TRUNCATE TABLE `{project}.{_STAGING}`").result()
    except NotFound:
        pass  # first run: staging not created yet (CREATE_IF_NEEDED will make it)


def _fetch_active_params(bq: bigquery.Client, project: str) -> dict:  # pragma: no cover - requires live BigQuery
    query = _READ_ACTIVE_PARAMS.format(project=project, table=_PARAMS_TABLE)
    rows = list(bq.query(query).result())
    if not rows:
        raise RuntimeError(
            f"No active strategy params found in {project}.{_PARAMS_TABLE}. "
            "Run the DDL seed first."
        )
    row = dict(rows[0])
    for key in ("weekly_rsi_trend_start", "weekly_rsi_trend_end",
                "daily_rsi_oversold", "daily_rsi_overbought"):
        row[key] = float(row[key])
    row["rsi_period"] = int(row["rsi_period"])
    return row


def _fetch_weekly_rsi_history(  # pragma: no cover - requires live BigQuery
    bq: bigquery.Client, project: str, symbol: str, rsi_period: int
) -> list[dict]:
    query = _READ_WEEKLY_RSI.format(
        project=project, table=_RSI_TABLE,
        symbol=symbol, rsi_period=rsi_period,
    )
    rows = list(bq.query(query).result())
    return [{"time_period_start": r["time_period_start"], "rsi": float(r["rsi"])}
            for r in rows]


def run_signals(config: dict) -> None:  # pragma: no cover - Beam/Dataflow + BigQuery
    """Run Stage 3: rsi_features → fact_signals for ``[start_date, end_date]``."""
    project      = config.get("project", _PROJECT)
    symbol       = config.get("symbol", "BTCUSD")
    strategy_id  = config.get("strategy_id", 1)
    start_date   = config["start_date"]
    end_date     = config.get("end_date", start_date)
    pipeline_args = config.get("pipeline_args", [])

    bq = bigquery.Client(project=project)

    params = _fetch_active_params(bq, project)
    rsi_period = params["rsi_period"]
    logger.info("Active strategy params (v%s): %s", strategy_id, params)

    weekly_history = _fetch_weekly_rsi_history(bq, project, symbol, rsi_period)
    trend_states = _compute_trend_states(
        weekly_history,
        params["weekly_rsi_trend_start"],
        params["weekly_rsi_trend_end"],
    )
    logger.info(
        "Loaded %d weekly RSI rows → trend state for %d week(s).",
        len(weekly_history), len(trend_states),
    )

    read_query = _READ_DAILY_RSI.format(
        project=project, table=_RSI_TABLE,
        symbol=symbol, rsi_period=rsi_period,
        start_date=start_date.isoformat(), end_date=end_date.isoformat(),
    )

    options = PipelineOptions(
        pipeline_args,
        project=project,
        region=config.get("region", "us-central1"),
    )

    _truncate_staging(bq, project)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadDailyRSI" >> ReadFromBigQuery(
                query=read_query,
                use_standard_sql=True,
                project=project,
                gcs_location=config.get("temp_location"),
            )
            | "AddSymbolTemporality" >> beam.Map(
                lambda row, sym, tmp: {**row, "symbol": sym, "temporality": tmp},
                sym=symbol, tmp="1d",
            )
            | "ComputeSignal" >> beam.ParDo(
                _ComputeSignalFn(
                    trend_states=trend_states,
                    params=params,
                    strategy_id=strategy_id,
                )
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
    logger.info("Stage 3 complete: fact_signals updated for %s..%s", start_date, end_date)
