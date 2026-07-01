"""
Stage A — silver (vw_asset_returns_weekly) → gold (fact_signals, strategy_id=3).

Reads the full weekly history for the nine frozen Strategy-3 instruments from the
T-21 silver view, runs T-22's per-instrument TSMOM signal logic, and merges the
results into ``prod_trade_gold.fact_signals`` under ``strategy_id=3``.

Key design decisions
--------------------
* **Separate staging table** (``fact_signals_tsmom_staging``, not RSI's
  ``fact_signals_staging``) — eliminates any concurrent-run collision risk between
  the daily RSI job and a manual/backfill TSMOM run.
* **Upsert-only MERGE** — the ON clause includes ``strategy_id`` so rows from this
  stage (strategy_id=3) can never match, update, or delete RSI rows (strategy_id=1).
  There is no ``WHEN NOT MATCHED BY SOURCE THEN DELETE`` branch.
* **Full-history read, no date windowing** — ``compute_tsmom_rows`` (T-22) needs
  the full trailing window; the weekly series for 9 symbols is tiny so re-processing
  the whole history each run is the right trade-off (same "read all weekly history"
  pattern RSI uses for its trend-state walk-forward in ``signals.py``).
* **Explicit sort within GroupByKey** — Beam's shuffle does not preserve order;
  each per-symbol group is sorted by ``week_start`` before ``compute_tsmom_rows``.
* **Warm-up rows are dropped** — rows where ``signal is None`` (first
  ``formation_horizon − 1`` weeks) produce no ``fact_signals`` row, mirroring the
  ``rsi IS NOT NULL`` filter convention in ``signals.py``.
* **``signal`` column is a human-readable audit label** (BUY/SELL/NEUTRAL); Stage B
  reads the numeric sign from ``trigger_params["signal"]`` to avoid the round-trip.
* **``signal_start`` = ``week_start`` at UTC midnight** — Stage B groups by
  ``DATE(signal_start)`` to recover ``week_start``; UTC-midnight timestamps make that
  round-trip exact regardless of locale.
* **``trigger_params`` carries everything Stage B needs** so Stage B never re-queries
  silver: numeric ``signal`` (int), ``realized_vol_26w`` (float), plus
  ``formation_return``, ``vol_scale``, ``position`` and the active params for audit.

Write pattern: results go to a staging table (WRITE_TRUNCATE) then are merged into
``fact_signals`` via SQL outside the Beam graph.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timezone
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from dataflow.strategy.tsmom_signal import TsmomParams, compute_tsmom_rows
from orchestration.ingest.strategy_3_universe import FULL_UNIVERSE

logger = logging.getLogger(__name__)

_PROJECT = "trade-390514"
_SILVER_VIEW = "prod_trade_silver.vw_asset_returns_weekly"
_PARAMS_TABLE = "prod_trade_strategy.strategy_tsmom_multiasset"
_STAGING = "prod_trade_gold.fact_signals_tsmom_staging"
_TARGET = "prod_trade_gold.fact_signals"
_STRATEGY_ID = 3
_TEMPORALITY = "1w"

# Keys written to trigger_params by Stage A.
# Stage B reads this subset; the contract test in test_stage_tsmom_signal.py
# asserts that _TRIGGER_PARAMS_KEYS_WRITTEN ⊇ _TRIGGER_PARAMS_KEYS_READ_BY_STAGE_B.
_TRIGGER_PARAMS_KEYS_WRITTEN: frozenset[str] = frozenset({
    "signal",            # int -1/0/+1 — Stage B reads this (not the string column)
    "realized_vol_26w",  # float — Stage B feeds to inverse_vol_weights
    "formation_return",
    "vol_scale",
    "position",
    "formation_horizon",
    "vol_target",
    "vol_lookback",
    "max_leverage",
})

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

_READ_SILVER = """
SELECT symbol, week_start, excess_log_return, realized_vol_26w
FROM `{project}.{view}`
WHERE symbol IN ({symbols})
  AND excess_log_return IS NOT NULL
ORDER BY symbol, week_start
"""

_READ_ACTIVE_PARAMS = """
SELECT param_version, formation_horizon, vol_target, vol_lookback,
       periods_per_year, max_leverage, scheme, crypto_cap
FROM `{project}.{table}`
WHERE is_active = TRUE
ORDER BY param_version DESC
LIMIT 1
"""

# Upsert-only: no WHEN NOT MATCHED BY SOURCE branch.
# ON includes strategy_id so RSI rows (strategy_id=1) are never matched.
_MERGE_SQL = """
MERGE `{project}.{target}` AS T
USING `{project}.{staging}` AS S
ON  T.symbol       = S.symbol
AND T.temporality  = S.temporality
AND T.signal_start = S.signal_start
AND T.strategy_id  = S.strategy_id
WHEN MATCHED THEN UPDATE SET
  signal            = S.signal,
  trigger_params    = S.trigger_params,
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
# Pure helpers (unit-testable, no BQ/Beam imports)
# ---------------------------------------------------------------------------

def _signal_label(sign: int) -> str:
    """Map TSMOM numeric sign to the fact_signals string convention.

    The string is a human-readable audit label; Stage B reads the numeric
    sign from ``trigger_params["signal"]``, not this column.
    """
    if sign > 0:
        return "BUY"
    if sign < 0:
        return "SELL"
    return "NEUTRAL"


def _week_start_to_ts(week_start: Any) -> str:
    """Convert a ``week_start`` DATE value to a UTC-midnight ISO timestamp.

    Stage B groups by ``DATE(signal_start)`` to recover ``week_start``.
    UTC-midnight guarantees the round-trip is exact regardless of locale.
    """
    if isinstance(week_start, datetime):
        d = week_start.date()
    elif isinstance(week_start, date):
        d = week_start
    else:
        d = date.fromisoformat(str(week_start)[:10])
    return datetime.combine(d, time.min, tzinfo=timezone.utc).isoformat()


def _build_trigger_params(signal_row: dict, params: TsmomParams) -> dict:
    """Build the trigger_params JSON dict for one fact_signals row.

    Pure function — no BQ/Beam IO — so it is callable in isolation by the
    contract test (``tests/test_stage_tsmom_signal.py``).

    Args:
        signal_row: One output dict from ``compute_tsmom_rows``, containing
            keys ``signal`` (int|-1/0/+1), ``vol_scale``, ``formation_return``,
            ``position``, and the original silver key ``realized_vol_26w``.
        params: Active ``TsmomParams`` (formation_horizon, vol_target, …).

    Returns:
        Dict whose keys match ``_TRIGGER_PARAMS_KEYS_WRITTEN``.
    """
    return {
        "signal":            int(signal_row["signal"]),
        "realized_vol_26w":  float(signal_row["realized_vol_26w"])
                             if signal_row.get("realized_vol_26w") is not None
                             else None,
        "formation_return":  signal_row.get("formation_return"),
        "vol_scale":         signal_row.get("vol_scale"),
        "position":          signal_row.get("position"),
        "formation_horizon": params.formation_horizon,
        "vol_target":        params.vol_target,
        "vol_lookback":      params.vol_lookback,
        "max_leverage":      params.max_leverage,
    }


# ---------------------------------------------------------------------------
# Beam transform
# ---------------------------------------------------------------------------

class _ComputeTsmomSignalFn(beam.DoFn):
    """Per-symbol group: sort rows, compute TSMOM signal, emit fact_signals dicts."""

    def __init__(self, params: TsmomParams, strategy_id: int) -> None:
        self._params = params
        self._strategy_id = strategy_id

    def process(self, element):
        symbol, rows = element
        # Beam's GroupByKey shuffle does not preserve order — sort explicitly.
        sorted_rows = sorted(rows, key=lambda r: r["week_start"])

        signal_rows = compute_tsmom_rows(sorted_rows, self._params)
        now = datetime.now(timezone.utc).isoformat()

        for silver_row, sig in zip(sorted_rows, signal_rows):
            sign = sig["signal"]
            if sign is None:
                # Warm-up: no full formation window yet — skip, never write.
                continue

            # Attach realized_vol_26w from the silver row so _build_trigger_params
            # can include it (compute_tsmom_rows doesn't pass it back in its output).
            sig_with_vol = {**sig, "realized_vol_26w": silver_row.get("realized_vol_26w")}

            yield {
                "symbol":            symbol,
                "temporality":       _TEMPORALITY,
                "strategy_id":       self._strategy_id,
                "signal":            _signal_label(sign),
                "trigger_params":    _build_trigger_params(sig_with_vol, self._params),
                "signal_start":      _week_start_to_ts(silver_row["week_start"]),
                "signal_created_at": now,
            }


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def _truncate_staging(bq: bigquery.Client, project: str) -> None:  # pragma: no cover
    try:
        bq.query(f"TRUNCATE TABLE `{project}.{_STAGING}`").result()
    except NotFound:
        pass


def _fetch_active_params(bq: bigquery.Client, project: str) -> tuple[TsmomParams, dict]:  # pragma: no cover
    query = _READ_ACTIVE_PARAMS.format(project=project, table=_PARAMS_TABLE)
    rows = list(bq.query(query).result())
    if not rows:
        raise RuntimeError(
            f"No active TSMOM params found in {project}.{_PARAMS_TABLE}. "
            "Run the DDL seed first."
        )
    row = dict(rows[0])
    params = TsmomParams(
        formation_horizon=int(row["formation_horizon"]),
        vol_target=float(row["vol_target"]),
        vol_lookback=int(row["vol_lookback"]),
        periods_per_year=int(row["periods_per_year"]),
        max_leverage=float(row["max_leverage"]) if row.get("max_leverage") is not None else None,
    )
    return params, row


def run_tsmom_signal(config: dict) -> None:  # pragma: no cover
    """Run Stage A: vw_asset_returns_weekly → fact_signals (strategy_id=3)."""
    project = config.get("project", _PROJECT)
    pipeline_args = config.get("pipeline_args", [])

    bq = bigquery.Client(project=project)
    params, param_row = _fetch_active_params(bq, project)
    logger.info("Active TSMOM params (v%s): %s", param_row.get("param_version"), params)

    symbols_sql = ", ".join(f"'{i.symbol}'" for i in FULL_UNIVERSE)
    read_query = _READ_SILVER.format(
        project=project, view=_SILVER_VIEW, symbols=symbols_sql
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
            | "ReadSilver" >> ReadFromBigQuery(
                query=read_query,
                use_standard_sql=True,
                project=project,
                gcs_location=config.get("temp_location"),
            )
            | "KeyBySymbol" >> beam.Map(lambda row: (row["symbol"], row))
            | "GroupBySymbol" >> beam.GroupByKey()
            | "ComputeSignal" >> beam.ParDo(
                _ComputeTsmomSignalFn(params=params, strategy_id=_STRATEGY_ID)
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
    logger.info("Stage A complete: fact_signals updated for strategy_id=%d", _STRATEGY_ID)
