"""Airflow-free helpers for the ``daily_btc_signal`` DAG (T-12).

Kept out of ``orchestration/dags/`` on purpose:

* The Airflow DAG parser scans ``dags/`` and would try to import this as a DAG.
* CI does not install ``apache-airflow``; this module imports only the ingest
  package (whose deps live in the Airflow image *and* in CI), so the pure logic
  here — the Dataflow launch command and the list of daily ingest steps — stays
  unit-testable without Airflow.

The DAG (``orchestration/dags/daily_btc_signal_dag.py``) wires Airflow operators
around :data:`INGEST_STEPS` and :func:`build_dataflow_command`.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

# Trailing window (in days) re-processed every run for the candle ingest and the
# Dataflow launch. Re-fetching/re-computing the last few days is idempotent
# (MERGE on natural keys) and self-heals gaps: if a day's candle isn't published
# yet when the DAG fires, the next run's window backfills it — no permanent hole.
# Also dodges a ccxt edge case where a single-day fetch (since == until) with
# pagination returns nothing.
CANDLE_LOOKBACK_DAYS = 3

# NOTE: the ingest modules are imported lazily inside each wrapper below, not at
# module top level. Importing them eagerly pulls in heavy dependencies (notably
# ``ccxt``, which registers hundreds of exchange classes) — slow enough that the
# Airflow DAG parser hits its ``dagbag_import_timeout`` (30 s) on the e2-micro
# VM, so the DAG fails to load. Deferring the imports keeps DAG parsing instant;
# ``ccxt`` & friends are only imported when a task actually runs.

# Defaults mirror dataflow/pipeline.py and the GCS bucket used for the Dataflow
# runs (see comandos.md / T-08), so the launch behaves the same whether or not
# the VM's .env overrides them.
_DEFAULT_PROJECT = "trade-390514"
_DEFAULT_REGION = "us-central1"
_DEFAULT_TEMP_LOCATION = "gs://trade-dataflow-trade-390514/temp"
_DEFAULT_STAGING_LOCATION = "gs://trade-dataflow-trade-390514/staging"
_DEFAULT_SERVICE_ACCOUNT = "trade-pipeline@trade-390514.iam.gserviceaccount.com"

# The isolated venv that holds apache-beam[gcp] (see orchestration/Dockerfile);
# its pins conflict with the Airflow environment, so the pipeline is launched
# with this interpreter, not the Airflow one.
_BEAM_PYTHON = "/opt/beam-venv/bin/python"


def _to_date(ds):
    """Coerce an Airflow ``ds`` string (``YYYY-MM-DD``) to a ``date``.

    ``None`` and ``date`` pass through unchanged so a manual call with no logical
    date falls back to the ingest module's own "yesterday" default.
    """
    if ds is None or isinstance(ds, date):
        return ds
    return datetime.strptime(ds, "%Y-%m-%d").date()


# --- Daily ingest steps -----------------------------------------------------
# Each wrapper accepts an optional ``ds`` (the run's logical date). Only the
# Binance candle uses it (to make a per-day re-run deterministic); the macro /
# on-chain / attention series use their own look-back windows and ignore it.
# Google Trends is weekly: ``ingest_latest`` refreshes the latest <5-year window
# in place (idempotent MERGE), so a daily firing just picks up the newest closed
# week without re-requesting or re-stitching the whole history. Bitstamp is
# deliberately absent: it is pre-2017 history (one-off back-fill), not a daily
# source.


def run_binance_btc(ds=None):
    from orchestration.ingest import binance_btc_ingest

    end = _to_date(ds)
    # Re-fetch a trailing window (idempotent) so a late/missed daily candle is
    # backfilled on a later run instead of leaving a permanent gap.
    start = end - timedelta(days=CANDLE_LOOKBACK_DAYS) if end else None
    binance_btc_ingest.ingest_daily_candles(start_date=start, end_date=end)


def run_mvrv(ds=None):
    from orchestration.ingest import bitcoin_data_mvrv_ingest

    bitcoin_data_mvrv_ingest.ingest_latest()


def run_dxy(ds=None):
    from orchestration.ingest import yahoo_dxy_ingest

    yahoo_dxy_ingest.ingest_latest()


def run_treasury_10y(ds=None):
    from orchestration.ingest import fred_10y_ingest

    fred_10y_ingest.ingest_latest()


def run_treasury_2y(ds=None):
    from orchestration.ingest import fred_2y_ingest

    fred_2y_ingest.ingest_latest()


def run_fed_funds(ds=None):
    from orchestration.ingest import fred_fedfunds_ingest

    fred_fedfunds_ingest.ingest_latest()


def run_vix(ds=None):
    from orchestration.ingest import fred_vix_ingest

    fred_vix_ingest.ingest_latest()


def run_m2(ds=None):
    from orchestration.ingest import fred_m2_ingest

    fred_m2_ingest.ingest_latest()


def run_supply(ds=None):
    from orchestration.ingest import coinmetrics_btc_supply_ingest

    coinmetrics_btc_supply_ingest.ingest_latest()


def run_active_addresses(ds=None):
    from orchestration.ingest import coinmetrics_btc_active_addresses_ingest

    coinmetrics_btc_active_addresses_ingest.ingest_latest()


def run_tx_count(ds=None):
    from orchestration.ingest import coinmetrics_btc_tx_count_ingest

    coinmetrics_btc_tx_count_ingest.ingest_latest()


def run_trends(ds=None):
    from orchestration.ingest import google_trends_btc_ingest

    google_trends_btc_ingest.ingest_latest()


INGEST_STEPS = [
    ("ingest_binance_btc", run_binance_btc),
    ("ingest_mvrv", run_mvrv),
    ("ingest_dxy", run_dxy),
    ("ingest_10y", run_treasury_10y),
    ("ingest_2y", run_treasury_2y),
    ("ingest_fedfunds", run_fed_funds),
    ("ingest_vix", run_vix),
    ("ingest_m2", run_m2),
    ("ingest_supply", run_supply),
    ("ingest_active_addresses", run_active_addresses),
    ("ingest_tx_count", run_tx_count),
    ("ingest_trends", run_trends),
]


def build_dataflow_command(
    start_date,
    end_date=None,
    project=None,
    region=None,
    temp_location=None,
    staging_location=None,
    service_account=None,
):
    """Build the ``python -m dataflow.pipeline`` argv for the daily run.

    Launches the full medallion pipeline (conform → rsi → signals) on
    DataflowRunner over ``[start_date, end_date]`` (``end_date`` defaults to
    ``start_date``). The DAG passes a trailing window (``ds - CANDLE_LOOKBACK_DAYS
    .. ds``) so a late candle propagates to silver/gold on a later run instead of
    leaving a gap; every stage MERGEs on natural keys, so re-processing recent
    days is idempotent. Right-sized worker defaults are injected by
    ``pipeline._apply_worker_defaults``; ``--setup_file ./setup.py`` packages the
    ``dataflow`` module for the GCP workers.

    Unset arguments fall back to environment variables (set on the VM via
    ``.env``) and finally to the module defaults.
    """
    end_date = end_date or start_date
    project = project or os.environ.get("GCP_PROJECT", _DEFAULT_PROJECT)
    region = region or os.environ.get("GCP_REGION", _DEFAULT_REGION)
    temp_location = temp_location or os.environ.get(
        "DATAFLOW_TEMP_LOCATION", _DEFAULT_TEMP_LOCATION
    )
    staging_location = staging_location or os.environ.get(
        "DATAFLOW_STAGING_LOCATION", _DEFAULT_STAGING_LOCATION
    )
    service_account = service_account or os.environ.get(
        "DATAFLOW_SERVICE_ACCOUNT", _DEFAULT_SERVICE_ACCOUNT
    )
    return [
        _BEAM_PYTHON, "-m", "dataflow.pipeline",
        "--runner", "DataflowRunner",
        "--setup_file", "./setup.py",
        "--project", project,
        "--region", region,
        "--temp_location", temp_location,
        "--staging_location", staging_location,
        "--service_account_email", service_account,
        "--start_date", start_date,
        "--end_date", end_date,
    ]
