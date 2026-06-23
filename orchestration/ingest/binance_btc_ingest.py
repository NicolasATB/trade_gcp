"""Daily BTC candle ingestion: Binance (via CCXT) -> BigQuery bronze.

Thin entry-point over :mod:`orchestration.ingest.ccxt_candle_common`: it only pins the
Binance config (exchange id, symbol, target table, source id) and re-exports the
bound functions. All the actual logic — CCXT pagination, the closed-candle rule,
the idempotent MERGE on ``(symbol, candle_date)``, retries — lives in the shared
module, so a fix there benefits Binance and Bitstamp alike.

Binance lists BTC/USDT from 2017-08-17 onward; earlier history comes from
Bitstamp (see ``bitstamp_btc_ingest.py``).

Run standalone (single day, or a range/back-fill):
  python -m orchestration.ingest.binance_btc_ingest
  python -m orchestration.ingest.binance_btc_ingest --start 2024-01-01 --end 2024-01-31
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

# Config overridable via env vars (kept for parity with the rest of the ingests).
# public_api_url routes Binance's public market data to data-api.binance.vision:
# api.binance.com returns HTTP 451 from cloud IPs (the orchestration VM), while
# the vision mirror serves the same klines/exchangeInfo unblocked.
SOURCE = CcxtCandleSource(
    exchange_id=os.environ.get("CCXT_EXCHANGE", "binance"),
    symbol=os.environ.get("BINANCE_SYMBOL", "BTC/USDT"),
    table=os.environ.get("BQ_BRONZE_TABLE", "binance_btcusd_daily_raw"),
    source_id=int(os.environ.get("BINANCE_SOURCE_ID", "3")),
    public_api_url=os.environ.get(
        "BINANCE_PUBLIC_API_URL", "https://data-api.binance.vision/api/v3"
    ),
    # Load only spot markets so load_markets() doesn't query the futures (fapi) /
    # delivery (dapi) hosts, which are not on the vision mirror and 451 from the VM.
    options={"fetchMarkets": ["spot"]},
)


def fetch_daily_candles_range(start_date, end_date, exchange=None):
    """Binance-bound :func:`ccxt_candle_common.fetch_daily_candles_range`."""
    return _fetch_range(SOURCE, start_date, end_date, exchange=exchange)


def ingest_daily_candles(start_date=None, end_date=None, client=None):
    """Binance-bound :func:`ccxt_candle_common.ingest_daily_candles`."""
    return _ingest(SOURCE, start_date=start_date, end_date=end_date, client=client)


def main(argv=None) -> None:
    run_cli(SOURCE, argv)


if __name__ == "__main__":
    main()
