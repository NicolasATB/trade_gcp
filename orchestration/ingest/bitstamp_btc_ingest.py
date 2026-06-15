"""Daily BTC candle ingestion: Bitstamp (via CCXT) -> BigQuery bronze.

Thin entry-point over :mod:`orchestration.ingest.ccxt_candle_common`: it only pins the
Bitstamp config and re-exports the bound functions. Same shape and idempotent
MERGE on ``(symbol, candle_date)`` as the Binance entry-point — only the
exchange id, symbol, target table and source id differ.

Bitstamp extends BTC history before Binance's BTC/USDT listing: it trades
BTC/USD continuously since ~2011-08. Cut-over policy: load Bitstamp rows only up
to 2017-08-16; Binance covers 2017-08-17 onward, so the two sources never
overlap by date. Source priority 4 (preferred over Binance=3 on consolidation
ties; moot in practice given the disjoint date ranges).

CCXT/Bitstamp peculiarity: its endpoint returns the LAST 1000 candles of the
requested range (it does not paginate backwards), so back-fills longer than
1000 days must be run in <1000-day chunks.

Run standalone (single day, or a range/back-fill):
  python -m orchestration.ingest.bitstamp_btc_ingest --start 2014-11-21 --end 2015-08-16
"""

from __future__ import annotations

import os

from orchestration.ingest.ccxt_candle_common import (
    CcxtCandleSource,
    run_cli,
)
from orchestration.ingest.ccxt_candle_common import (
    fetch_daily_candles_range as _fetch_range,
)
from orchestration.ingest.ccxt_candle_common import (
    ingest_daily_candles as _ingest,
)

SOURCE = CcxtCandleSource(
    exchange_id=os.environ.get("CCXT_EXCHANGE", "bitstamp"),
    symbol=os.environ.get("BITSTAMP_SYMBOL", "BTC/USD"),
    table=os.environ.get("BQ_BRONZE_TABLE", "bitstamp_btcusd_daily_raw"),
    source_id=int(os.environ.get("BITSTAMP_SOURCE_ID", "4")),
)


def fetch_daily_candles_range(start_date, end_date, exchange=None):
    """Bitstamp-bound :func:`ccxt_candle_common.fetch_daily_candles_range`."""
    return _fetch_range(SOURCE, start_date, end_date, exchange=exchange)


def ingest_daily_candles(start_date=None, end_date=None, client=None):
    """Bitstamp-bound :func:`ccxt_candle_common.ingest_daily_candles`."""
    return _ingest(SOURCE, start_date=start_date, end_date=end_date, client=client)


def main(argv=None) -> None:
    run_cli(SOURCE, argv)


if __name__ == "__main__":
    main()
