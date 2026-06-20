"""BTC circulating-supply ingestion: Coin Metrics (community) -> BigQuery bronze.

Thin entry-point over :mod:`orchestration.ingest.coinmetrics_common`: it pins the
``SplyCur`` (circulating supply) config and re-exports the bound functions. Supply
is the on-chain input the daily training view turns into halving-cycle features:
with the halving epoch fixed by date and a constant block subsidy within an epoch,
circulating supply recovers the block-count fraction of the cycle.

The community API needs no key. Like every recurring ingest here it **never
fabricates a value**: a missing datum is *alerted* and the row is skipped. BigQuery
auth is Application Default Credentials.

Run standalone:
  python -m orchestration.ingest.coinmetrics_btc_supply_ingest --backfill         # full history
  python -m orchestration.ingest.coinmetrics_btc_supply_ingest --start 2026-06-01 --end 2026-06-10
  python -m orchestration.ingest.coinmetrics_btc_supply_ingest                    # recent window (daily)
"""

from __future__ import annotations

import os

from orchestration.ingest.coinmetrics_common import CoinMetricsSeries, run_cli
from orchestration.ingest.coinmetrics_common import backfill_history as _backfill
from orchestration.ingest.coinmetrics_common import ingest_latest as _ingest_latest
from orchestration.ingest.coinmetrics_common import ingest_range as _ingest_range

SERIES = CoinMetricsSeries(
    metric=os.environ.get("COINMETRICS_SUPPLY_METRIC", "SplyCur"),
    table=os.environ.get("BQ_SUPPLY_TABLE", "coinmetrics_btc_supply_daily_raw"),
    date_col="supply_date",
    value_col="circ_supply",
    source_id=int(os.environ.get("SUPPLY_SOURCE_ID", "12")),
    history_start=os.environ.get("SUPPLY_HISTORY_START", "2010-01-01"),
    value_type="FLOAT64",
)


def backfill_history(client=None, start_date=None, end_date=None):
    """SplyCur-bound :func:`coinmetrics_common.backfill_history`."""
    return _backfill(SERIES, client=client, start_date=start_date, end_date=end_date)


def ingest_range(start_date, end_date, client=None):
    """SplyCur-bound :func:`coinmetrics_common.ingest_range`."""
    return _ingest_range(SERIES, start_date, end_date, client=client)


def ingest_latest(client=None):
    """SplyCur-bound :func:`coinmetrics_common.ingest_latest`."""
    return _ingest_latest(SERIES, client=client)


def main(argv=None) -> None:
    run_cli(SERIES, argv)


if __name__ == "__main__":
    main()
