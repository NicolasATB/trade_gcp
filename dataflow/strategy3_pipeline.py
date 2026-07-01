"""
Strategy-3 TSMOM pipeline — entry point (T-24).

Runs two sequential Beam stages for the cross-asset TSMOM strategy
(strategy_id=3). Each stage blocks until its own Beam pipeline and BigQuery
MERGE are complete before the next stage starts. This is not a single Beam
graph — BigQuery does not guarantee visibility of a write to a subsequent read
unless they are separated by a commit barrier.

  Stage A · TSMOM Signal
    silver.vw_asset_returns_weekly → gold.fact_signals (strategy_id=3)
    (dataflow/stages/tsmom_signal_stage.py)

  Stage B · Portfolio Weights
    gold.fact_signals (strategy_id=3) → gold.fact_portfolio_weights
    (dataflow/stages/portfolio_weights_stage.py)
    Called ONLY after Stage A's MERGE .result() returns.

This pipeline does NOT touch dataflow/pipeline.py (the RSI pipeline) or
orchestration/dags/daily_btc_signal_dag.py. It is run manually / on-demand
until Epic 8 validates the strategy and wires it into production scheduling.

Usage — DirectRunner (needs GCS for BigQuery FILE_LOADS):
    python -m dataflow.strategy3_pipeline \\
        --temp_location gs://BUCKET/temp \\
        --staging_location gs://BUCKET/staging

Usage — DataflowRunner (GCP):
    python -m dataflow.strategy3_pipeline \\
        --runner DataflowRunner \\
        --project trade-390514 \\
        --region us-central1 \\
        --temp_location gs://BUCKET/temp \\
        --staging_location gs://BUCKET/staging \\
        --service_account_email SA@PROJECT.iam.gserviceaccount.com

Run a single stage:
    python -m dataflow.strategy3_pipeline --stage tsmom_signal ...
    python -m dataflow.strategy3_pipeline --stage portfolio_weights ...
"""

from __future__ import annotations

import argparse
import logging

from dataflow.pipeline import (
    _apply_worker_defaults,
    _extract_temp_location,
)
from dataflow.stages.portfolio_weights_stage import run_portfolio_weights
from dataflow.stages.tsmom_signal_stage import run_tsmom_signal

logger = logging.getLogger(__name__)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Strategy-3 TSMOM Beam pipeline: silver → fact_signals → fact_portfolio_weights."
    )
    parser.add_argument(
        "--stage", default="all",
        choices=["all", "tsmom_signal", "portfolio_weights"],
        help="Which stage(s) to execute.",
    )
    parser.add_argument("--project", default="trade-390514")
    parser.add_argument("--region",  default="us-central1")
    # Remaining args (--runner, --temp_location, etc.) pass through to Beam.
    return parser.parse_known_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    known, pipeline_args = _parse_args(argv)
    pipeline_args = _apply_worker_defaults(pipeline_args)

    config = {
        "project":       known.project,
        "region":        known.region,
        "pipeline_args": pipeline_args,
        "temp_location": _extract_temp_location(pipeline_args),
    }

    if known.stage in ("all", "tsmom_signal"):
        logger.info("=== Stage A: TSMOM Signal (silver → fact_signals strategy_id=3) ===")
        run_tsmom_signal(config)

    if known.stage in ("all", "portfolio_weights"):
        logger.info("=== Stage B: Portfolio Weights (fact_signals → fact_portfolio_weights) ===")
        # Stage B reads Stage A's committed fact_signals output — run_tsmom_signal
        # above blocks until its MERGE .result() returns, so the data is visible.
        run_portfolio_weights(config)

    logger.info("Strategy-3 pipeline complete (stage=%s).", known.stage)


if __name__ == "__main__":
    main()
