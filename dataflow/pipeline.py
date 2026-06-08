"""
BTC RSI signal pipeline — entry point.

Runs three sequential Beam stages that implement the medallion flow:

  Stage 1 · Conform  bronze → silver (ohlcv_validated)
    - Normalises Binance candles to canonical OHLCV schema.
    - Aggregates daily candles to weekly (same table, temporality='1w').

  Stage 2 · RSI     silver (ohlcv_validated) → silver (rsi_features)
    - Computes Wilder-smoothed RSI for both '1d' and '1w' temporalities.
    - Bootstraps from full history on first run; subsequent runs are
      incremental (one Wilder step per new candle).

  Stage 3 · Signals  silver (rsi_features) → gold (fact_signals)
    - Applies the weekly-trend + daily-RSI strategy from the AG notebook.
    - Parameters are read from prod_trade_strategy.strategy_rsi_daily_week.

Each stage writes to a staging table and merges into the target via SQL,
making every re-run idempotent.

Date range: ``--start_date`` selects the first processing day and ``--end_date``
(optional, inclusive) the last. When ``--end_date`` is omitted only
``--start_date`` is processed; with neither, both default to yesterday — the
most recent fully closed candle (the daily job). A range back-fills several days
in one run.

Usage — DirectRunner (needs GCS for BigQuery FILE_LOADS):
    python -m dataflow.pipeline \\
        --start_date 2026-06-04 \\
        --temp_location gs://BUCKET/temp \\
        --staging_location gs://BUCKET/staging

Usage — DataflowRunner (GCP), back-filling a range:
    python -m dataflow.pipeline \\
        --runner DataflowRunner \\
        --project trade-390514 \\
        --region us-central1 \\
        --temp_location gs://BUCKET/temp \\
        --staging_location gs://BUCKET/staging \\
        --service_account_email SA@PROJECT.iam.gserviceaccount.com \\
        --start_date 2026-05-01 --end_date 2026-05-31

    Right-sized worker defaults (single small fixed worker, no autoscaling) are
    injected automatically for DataflowRunner — see ``_apply_worker_defaults``.
    The pipeline's volume is KB/day, so the Dataflow defaults (autoscaling +
    n1-standard-1) are oversized. Pass any of those flags explicitly on the CLI
    to override a per-run value.

Run a single stage:
    python -m dataflow.pipeline --stage conform --start_date 2026-06-04 ...
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from dataflow.stages.conform import run_conform
from dataflow.stages.rsi import run_rsi
from dataflow.stages.signals import run_signals

logger = logging.getLogger(__name__)

# Right-sized worker defaults for this pipeline's tiny volume (KB/day): a single
# small fixed worker with autoscaling off. The Dataflow defaults (autoscaling +
# n1-standard-1) are oversized and the dominant cost here is worker start-up, not
# compute. Applied only for DataflowRunner and only when the caller hasn't set
# the flag, so any value stays tunable per run from the CLI.
_DATAFLOW_WORKER_DEFAULTS = {
    "--num_workers":           "1",
    "--max_num_workers":       "1",
    "--autoscaling_algorithm": "NONE",
    "--worker_machine_type":   "e2-small",
    "--disk_size_gb":          "30",
}


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Medallion Beam pipeline: bronze → silver → gold."
    )
    parser.add_argument(
        "--start_date", default=None,
        help="First processing date YYYY-MM-DD (UTC). Defaults to yesterday.",
    )
    parser.add_argument(
        "--end_date", default=None,
        help="Last processing date YYYY-MM-DD (UTC), inclusive. "
             "Defaults to --start_date (a single day).",
    )
    parser.add_argument(
        "--stage", default="all",
        choices=["all", "conform", "rsi", "signals"],
        help="Which stage(s) to execute.",
    )
    parser.add_argument("--project",     default="trade-390514")
    parser.add_argument("--region",      default="us-central1")
    parser.add_argument("--symbol",      default="BTCUSD")
    parser.add_argument("--rsi_period",  default=14, type=int)
    parser.add_argument("--strategy_id", default=1,  type=int)
    # Remaining args (--runner, --temp_location, etc.) are passed through to
    # Beam's PipelineOptions.
    return parser.parse_known_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    known, pipeline_args = _parse_args(argv)
    pipeline_args = _apply_worker_defaults(pipeline_args)

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    if known.start_date is None:
        start_date = yesterday
    else:
        start_date = datetime.strptime(known.start_date, "%Y-%m-%d").date()
    # End defaults to start, so a single day is processed when --end_date is omitted.
    if known.end_date is None:
        end_date = start_date
    else:
        end_date = datetime.strptime(known.end_date, "%Y-%m-%d").date()

    if end_date < start_date:
        raise ValueError(
            f"--end_date ({end_date.isoformat()}) is before "
            f"--start_date ({start_date.isoformat()})."
        )

    logger.info(
        "Pipeline start_date=%s end_date=%s stage=%s",
        start_date, end_date, known.stage,
    )

    config = {
        "project":       known.project,
        "region":        known.region,
        "symbol":        known.symbol,
        "rsi_period":    known.rsi_period,
        "strategy_id":   known.strategy_id,
        "start_date":    start_date,
        "end_date":      end_date,
        "pipeline_args": pipeline_args,
        # Extract --temp_location for ReadFromBigQuery's gcs_location if present.
        "temp_location": _extract_temp_location(pipeline_args),
    }

    if known.stage in ("all", "conform"):
        logger.info("=== Stage 1/3: Conform ===")
        run_conform(config)

    if known.stage in ("all", "rsi"):
        logger.info("=== Stage 2/3: RSI (daily) ===")
        run_rsi({**config, "temporality": "1d"})

        logger.info("=== Stage 2/3: RSI (weekly) ===")
        run_rsi({**config, "temporality": "1w"})

    if known.stage in ("all", "signals"):
        logger.info("=== Stage 3/3: Signals ===")
        run_signals(config)

    logger.info("Pipeline complete for %s..%s", start_date, end_date)


def _runner_is_dataflow(pipeline_args: list[str]) -> bool:
    for i, arg in enumerate(pipeline_args):
        if arg == "--runner" and i + 1 < len(pipeline_args):
            return pipeline_args[i + 1] == "DataflowRunner"
        if arg.startswith("--runner="):
            return arg.split("=", 1)[1] == "DataflowRunner"
    return False


def _flag_present(pipeline_args: list[str], flag: str) -> bool:
    return any(a == flag or a.startswith(flag + "=") for a in pipeline_args)


def _apply_worker_defaults(pipeline_args: list[str]) -> list[str]:
    """Inject right-sized worker defaults for DataflowRunner.

    Adds the flags in ``_DATAFLOW_WORKER_DEFAULTS`` only when running on
    DataflowRunner and only for flags the caller did not already pass, so any
    value can still be overridden per run. DirectRunner (local) is left
    untouched — those flags don't apply there.
    """
    if not _runner_is_dataflow(pipeline_args):
        return pipeline_args
    args = list(pipeline_args)
    for flag, value in _DATAFLOW_WORKER_DEFAULTS.items():
        if not _flag_present(args, flag):
            args.extend([flag, value])
    return args


def _extract_temp_location(pipeline_args: list[str]) -> str | None:
    for i, arg in enumerate(pipeline_args):
        if arg.startswith("--temp_location="):
            return arg.split("=", 1)[1]
        if arg == "--temp_location" and i + 1 < len(pipeline_args):
            return pipeline_args[i + 1]
    return None


if __name__ == "__main__":
    main()
