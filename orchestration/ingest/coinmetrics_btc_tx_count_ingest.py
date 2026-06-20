"""BTC transaction-count ingestion: Coin Metrics (community) -> BigQuery bronze.

Thin entry-point over :mod:`orchestration.ingest.coinmetrics_common`: it pins the
``TxCnt`` (count of confirmed on-chain transactions) config and re-exports the bound
functions. An on-chain network-activity factor; the daily training view turns the
raw count into a stationary year-over-year log-growth feature (``tx_count_yoy_log``).

The community API needs no key. Like every recurring ingest here it **never
fabricates a value**: a missing datum is *alerted* and the row is skipped. BigQuery
auth is Application Default Credentials.

Run standalone:
  python -m orchestration.ingest.coinmetrics_btc_tx_count_ingest --backfill
  python -m orchestration.ingest.coinmetrics_btc_tx_count_ingest --start 2026-06-01 --end 2026-06-10
  python -m orchestration.ingest.coinmetrics_btc_tx_count_ingest                    # recent window (daily)
"""

from __future__ import annotations

import os

from orchestration.ingest.coinmetrics_common import CoinMetricsSeries, run_cli
from orchestration.ingest.coinmetrics_common import backfill_history as _backfill
from orchestration.ingest.coinmetrics_common import ingest_latest as _ingest_latest
from orchestration.ingest.coinmetrics_common import ingest_range as _ingest_range

SERIES = CoinMetricsSeries(
    metric=os.environ.get("COINMETRICS_TX_METRIC", "TxCnt"),
    table=os.environ.get("BQ_TX_COUNT_TABLE", "coinmetrics_btc_tx_count_daily_raw"),
    date_col="metric_date",
    value_col="tx_count",
    source_id=int(os.environ.get("TX_COUNT_SOURCE_ID", "14")),
    history_start=os.environ.get("TX_COUNT_HISTORY_START", "2010-01-01"),
    value_type="INT64",
)


def backfill_history(client=None, start_date=None, end_date=None):
    """TxCnt-bound :func:`coinmetrics_common.backfill_history`."""
    return _backfill(SERIES, client=client, start_date=start_date, end_date=end_date)


def ingest_range(start_date, end_date, client=None):
    """TxCnt-bound :func:`coinmetrics_common.ingest_range`."""
    return _ingest_range(SERIES, start_date, end_date, client=client)


def ingest_latest(client=None):
    """TxCnt-bound :func:`coinmetrics_common.ingest_latest`."""
    return _ingest_latest(SERIES, client=client)


def main(argv=None) -> None:
    run_cli(SERIES, argv)


if __name__ == "__main__":
    main()
