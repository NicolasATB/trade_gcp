"""
Stage B — gold (fact_signals, strategy_id=3) → gold (fact_portfolio_weights).

Reads Stage A's per-instrument TSMOM signals from ``fact_signals``, groups them
by rebalance week, and calls T-23's ``build_portfolio`` to produce two weight books
per week (with and without the BTC crypto sleeve) under the active parameter set.

Key design decisions
--------------------
* **Gold→gold stage** — a new pattern in this pipeline (RSI has no portfolio step);
  Stage B depends on Stage A's *committed* MERGE output. It must be called as a
  separate sequential invocation after ``run_tsmom_signal`` returns, not wired into
  the same Beam graph (BigQuery does not guarantee visibility within a single graph
  across a write that is not a Beam data dependency).
* **Reads numeric sign from ``trigger_params["signal"]``** — Stage A's
  ``fact_signals.signal`` column (BUY/SELL/NEUTRAL) is a human-readable audit label;
  the numeric sign and ``realized_vol_26w`` travel in ``trigger_params`` so Stage B
  never re-reads silver.
* **``is_crypto`` flag from ``FULL_UNIVERSE``** — a static Python dict built at
  startup, not a BigQuery join.
* **``DATE(signal_start)`` = ``week_start``** — Stage A writes UTC-midnight timestamps
  so ``DATE(signal_start)`` in UTC always equals the original Monday ``week_start``.
  The tz-coherence unit test pins this invariant.
* **Ragged cross-sections** — early weeks where only 2–3 of the 9 instruments have
  emerged from warm-up produce partial weight books; T-23's ``build_portfolio`` handles
  them correctly (it drops ``signal=None`` rows before weighting).
* **Separate staging table** — ``fact_portfolio_weights_staging``, own natural key.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from dataflow.strategy.portfolio import PortfolioParams, Scheme, build_portfolio
from dataflow.stages.tsmom_signal_stage import (
    _PARAMS_TABLE,
    _PROJECT,
    _READ_ACTIVE_PARAMS,
    _STRATEGY_ID,
    _TRIGGER_PARAMS_KEYS_WRITTEN,
    _fetch_active_params,
)
from orchestration.ingest.strategy_3_universe import FULL_UNIVERSE

logger = logging.getLogger(__name__)

_STAGING = "prod_trade_gold.fact_portfolio_weights_staging"
_TARGET = "prod_trade_gold.fact_portfolio_weights"

# Keys Stage B reads from trigger_params (must be ⊆ _TRIGGER_PARAMS_KEYS_WRITTEN).
_TRIGGER_PARAMS_KEYS_READ_BY_STAGE_B: frozenset[str] = frozenset({
    "signal",           # int -1/0/+1
    "realized_vol_26w", # float, for inverse_vol_weights
})

# Static lookup: symbol → is_crypto (built once, no BQ join needed).
_IS_CRYPTO: dict[str, bool] = {
    i.symbol: (i.asset_class == "crypto") for i in FULL_UNIVERSE
}

_STAGING_SCHEMA = {
    "fields": [
        {"name": "week_start",     "type": "DATE",      "mode": "REQUIRED"},
        {"name": "strategy_id",    "type": "INT64",     "mode": "REQUIRED"},
        {"name": "symbol",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "include_crypto", "type": "BOOL",      "mode": "REQUIRED"},
        {"name": "scheme",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "weight",         "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "param_version",  "type": "INT64",     "mode": "REQUIRED"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

_READ_SIGNALS = """
SELECT symbol, signal_start, trigger_params
FROM `{project}.{target}`
WHERE strategy_id = {strategy_id}
ORDER BY symbol, signal_start
"""

# Upsert-only merge on the portfolio-weights natural key.
_MERGE_SQL = """
MERGE `{project}.{target}` AS T
USING `{project}.{staging}` AS S
ON  T.week_start      = S.week_start
AND T.strategy_id     = S.strategy_id
AND T.symbol          = S.symbol
AND T.include_crypto  = S.include_crypto
WHEN MATCHED THEN UPDATE SET
  scheme        = S.scheme,
  weight        = S.weight,
  param_version = S.param_version,
  created_at    = S.created_at
WHEN NOT MATCHED THEN INSERT (
  week_start, strategy_id, symbol, include_crypto,
  scheme, weight, param_version, created_at
) VALUES (
  S.week_start, S.strategy_id, S.symbol, S.include_crypto,
  S.scheme, S.weight, S.param_version, S.created_at
)
"""


# ---------------------------------------------------------------------------
# Pure helpers (unit-testable, no BQ/Beam imports)
# ---------------------------------------------------------------------------

def _signal_start_to_week_start(signal_start: Any) -> date:
    """Extract the DATE from a ``signal_start`` TIMESTAMP (UTC-midnight).

    Stage A writes signal_start at UTC midnight, so DATE(signal_start) in UTC
    always equals the original Monday week_start. Explicit UTC extraction pins the
    tz-coherence contract.
    """
    if isinstance(signal_start, datetime):
        return signal_start.astimezone(timezone.utc).date()
    if isinstance(signal_start, date):
        return signal_start
    return date.fromisoformat(str(signal_start)[:10])


def _parse_signal_row(row: dict) -> dict | None:
    """Extract sign and vol from a fact_signals row's trigger_params.

    Returns a cross-section row suitable for ``build_portfolio``, or ``None``
    if trigger_params is missing required keys (e.g. legacy rows).
    """
    tp = row.get("trigger_params") or {}
    sign = tp.get("signal")
    vol = tp.get("realized_vol_26w")
    if sign is None:
        return None
    symbol = row["symbol"]
    return {
        "symbol":         symbol,
        "signal":         int(sign),
        "realized_vol_26w": float(vol) if vol is not None else None,
        "is_crypto":      _IS_CRYPTO.get(symbol, False),
    }


# ---------------------------------------------------------------------------
# Beam transform
# ---------------------------------------------------------------------------

class _BuildPortfolioWeightsFn(beam.DoFn):
    """Per-week cross-section: call build_portfolio twice, emit weight rows."""

    def __init__(
        self,
        portfolio_params: PortfolioParams,
        scheme_str: str,
        param_version: int,
        strategy_id: int,
    ) -> None:
        self._portfolio_params = portfolio_params
        self._scheme_str = scheme_str
        self._param_version = param_version
        self._strategy_id = strategy_id

    def process(self, element):
        week_start, signal_rows = element
        rows = [_parse_signal_row(r) for r in signal_rows]
        rows = [r for r in rows if r is not None]

        now = datetime.now(timezone.utc).isoformat()

        for include_crypto in (True, False):
            weights = build_portfolio(
                rows,
                self._portfolio_params,
                include_crypto=include_crypto,
                sign_key="signal",
                vol_key="realized_vol_26w",
                symbol_key="symbol",
                crypto_key="is_crypto",
            )
            for symbol, weight in weights.items():
                yield {
                    "week_start":     week_start.isoformat(),
                    "strategy_id":    self._strategy_id,
                    "symbol":         symbol,
                    "include_crypto": include_crypto,
                    "scheme":         self._scheme_str,
                    "weight":         weight,
                    "param_version":  self._param_version,
                    "created_at":     now,
                }


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def _truncate_staging(bq: bigquery.Client, project: str) -> None:  # pragma: no cover
    try:
        bq.query(f"TRUNCATE TABLE `{project}.{_STAGING}`").result()
    except NotFound:
        pass


def run_portfolio_weights(config: dict) -> None:  # pragma: no cover
    """Run Stage B: fact_signals (strategy_id=3) → fact_portfolio_weights.

    Must be called AFTER ``run_tsmom_signal`` has returned — Stage A's Beam
    pipeline and MERGE must both be complete before Stage B reads ``fact_signals``.
    """
    project = config.get("project", _PROJECT)
    pipeline_args = config.get("pipeline_args", [])

    bq = bigquery.Client(project=project)
    params, param_row = _fetch_active_params(bq, project)
    param_version = int(param_row["param_version"])
    scheme_str = str(param_row["scheme"])
    portfolio_params = PortfolioParams(
        scheme=Scheme(scheme_str),
        crypto_cap=float(param_row["crypto_cap"]) if param_row.get("crypto_cap") is not None else None,
    )
    logger.info(
        "Stage B — active TSMOM params (v%d): scheme=%s crypto_cap=%s",
        param_version, scheme_str, portfolio_params.crypto_cap,
    )

    read_query = _READ_SIGNALS.format(
        project=project,
        target=_TARGET.replace("fact_portfolio_weights", "fact_signals"),
        strategy_id=_STRATEGY_ID,
    )
    # Re-read the fact_signals target (Stage A's committed output).
    read_query = (
        f"SELECT symbol, signal_start, trigger_params\n"
        f"FROM `{project}.prod_trade_gold.fact_signals`\n"
        f"WHERE strategy_id = {_STRATEGY_ID}\n"
        f"ORDER BY symbol, signal_start"
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
            | "ReadSignals" >> ReadFromBigQuery(
                query=read_query,
                use_standard_sql=True,
                project=project,
                gcs_location=config.get("temp_location"),
            )
            | "KeyByWeek" >> beam.Map(
                lambda row: (
                    _signal_start_to_week_start(row["signal_start"]),
                    row,
                )
            )
            | "GroupByWeek" >> beam.GroupByKey()
            | "BuildWeights" >> beam.ParDo(
                _BuildPortfolioWeightsFn(
                    portfolio_params=portfolio_params,
                    scheme_str=scheme_str,
                    param_version=param_version,
                    strategy_id=_STRATEGY_ID,
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
    logger.info("Stage B complete: fact_portfolio_weights updated for strategy_id=%d", _STRATEGY_ID)
