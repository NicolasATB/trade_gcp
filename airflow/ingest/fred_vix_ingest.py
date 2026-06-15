"""VIX ingestion: FRED VIXCLS -> BigQuery bronze.

Thin entry-point over :mod:`airflow.ingest.fred_common`: it pins the VIXCLS config
and re-exports the bound functions. VIXCLS (CBOE Volatility Index, daily close,
index points, since 1990) is not revised, so it is a plain ``(obs_date,
obs_value)`` series — same code path as DGS10 / DGS2 / DFF, only the config
differs. It is a macro risk-appetite feature for the training views.

Authentication: ``FRED_API_KEY`` (env, never committed) for FRED; Application
Default Credentials for BigQuery.

Run standalone:
  python -m airflow.ingest.fred_vix_ingest --backfill                  # full history
  python -m airflow.ingest.fred_vix_ingest --start 2026-06-01 --end 2026-06-10
  python -m airflow.ingest.fred_vix_ingest                             # recent window (daily)
"""

from __future__ import annotations

import os

from airflow.ingest.fred_common import (
    FredSeries,
    run_cli,
)
from airflow.ingest.fred_common import (
    backfill_history as _backfill,
)
from airflow.ingest.fred_common import (
    ingest_latest as _ingest_latest,
)
from airflow.ingest.fred_common import (
    ingest_range as _ingest_range,
)

SERIES = FredSeries(
    series_id=os.environ.get("FRED_VIX_SERIES_ID", "VIXCLS"),
    table=os.environ.get("BQ_FRED_VIX_TABLE", "fred_vixcls_daily_raw"),
    source_id=int(os.environ.get("FRED_VIX_SOURCE_ID", "11")),
)


def backfill_history(client=None, observation_start=None, observation_end=None):
    """VIXCLS-bound :func:`fred_common.backfill_history`."""
    return _backfill(SERIES, client=client, observation_start=observation_start,
                     observation_end=observation_end)


def ingest_range(start_date, end_date, client=None):
    """VIXCLS-bound :func:`fred_common.ingest_range`."""
    return _ingest_range(SERIES, start_date, end_date, client=client)


def ingest_latest(client=None):
    """VIXCLS-bound :func:`fred_common.ingest_latest`."""
    return _ingest_latest(SERIES, client=client)


def main(argv=None) -> None:
    run_cli(SERIES, argv)


if __name__ == "__main__":
    main()
