"""2-Year Treasury yield ingestion: FRED DGS2 -> BigQuery bronze.

Thin entry-point over :mod:`airflow.ingest.fred_common`: it pins the DGS2 config
and re-exports the bound functions. DGS2 (2-Year Treasury Constant Maturity
yield, daily, percent, since 1976) is not revised, so it is a plain
``(obs_date, obs_value)`` series — same code path as DGS10 / Fed funds (``DFF``),
only the config differs. It is ingested raw so the gold training views can derive
the 10Y-2Y term spread (DGS10 - DGS2), its recent change/slope and the
dis-inversion (steepening-from-negative) flag.

Authentication: ``FRED_API_KEY`` (env, never committed) for FRED; Application
Default Credentials for BigQuery.

Run standalone:
  python -m airflow.ingest.fred_2y_ingest --backfill                   # full history
  python -m airflow.ingest.fred_2y_ingest --start 2026-06-01 --end 2026-06-10
  python -m airflow.ingest.fred_2y_ingest                              # recent window (daily)
"""

from __future__ import annotations

import os

from airflow.ingest.fred_common import (
    FredSeries,
    backfill_history as _backfill,
    ingest_latest as _ingest_latest,
    ingest_range as _ingest_range,
    run_cli,
)

SERIES = FredSeries(
    series_id=os.environ.get("FRED_2Y_SERIES_ID", "DGS2"),
    table=os.environ.get("BQ_FRED_2Y_TABLE", "fred_dgs2_daily_raw"),
    source_id=int(os.environ.get("FRED_2Y_SOURCE_ID", "10")),
)


def backfill_history(client=None, observation_start=None, observation_end=None):
    """DGS2-bound :func:`fred_common.backfill_history`."""
    return _backfill(SERIES, client=client, observation_start=observation_start,
                     observation_end=observation_end)


def ingest_range(start_date, end_date, client=None):
    """DGS2-bound :func:`fred_common.ingest_range`."""
    return _ingest_range(SERIES, start_date, end_date, client=client)


def ingest_latest(client=None):
    """DGS2-bound :func:`fred_common.ingest_latest`."""
    return _ingest_latest(SERIES, client=client)


def main(argv=None) -> None:
    run_cli(SERIES, argv)


if __name__ == "__main__":
    main()
