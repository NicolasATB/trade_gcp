"""Live-BigQuery integration: the T-19/T-20 multi-asset coverage gate.

The reproducible form of the T-19 coverage probe. Instead of re-downloading from
the providers, it validates the **ingest output** in
``prod_trade_bronze.yahoo_etf_daily_raw`` (the primary source): every frozen-universe
ETF has continuous history reaching back to its tier window with no real gaps on
trading days, and the natural key is unique (idempotency). This is the gate
``strategy_3_analysis.md`` defers to T-20.

Execution model (same as ``test_integration_bq.py``):

  * **Read-only**; nothing is written.
  * **Deselected by default** (``-m "not integration"``); run with
    ``pytest -m integration --no-cov``.
  * **Skipped automatically** without GCP credentials / connectivity, and
    **per-symbol** when the back-fill has not populated that series yet, so the
    suite never fails on a fresh checkout or before T-20's manual back-fill.

Calendar note (per the contract): the 8 ETFs close Friday (NYSE); the coverage
ratio nets out ~9 holidays/yr (≈96%), and the max calendar gap between trading
days stays small (long weekends + holidays), so a larger gap flags a real hole.
"""

from __future__ import annotations

import pytest

from orchestration.ingest.strategy_3_universe import ETF_UNIVERSE

PROJECT = "trade-390514"
YAHOO_ETF = f"{PROJECT}.prod_trade_bronze.yahoo_etf_daily_raw"
TIINGO_ETF = f"{PROJECT}.prod_trade_bronze.tiingo_etf_daily_raw"

# Tier-A coverage-gate frontier (T-19): every core ETF must start on or before
# this date so ≥ ~13 yrs precede the post-2020 holdout.
TIER_A_START_BY = "2007-12-31"
# Coverage ratio of trading days present vs NYSE weekdays in the span. The probe
# measured ~96.4% (holidays netted out); 0.95 leaves margin without hiding gaps.
MIN_COVERAGE = 0.95
# Largest acceptable calendar gap between consecutive trading days (long weekend
# + holiday). A bigger gap is a real hole, not a holiday week.
MAX_GAP_DAYS = 7

pytestmark = pytest.mark.integration

_ETF_IDS = [i.symbol for i in ETF_UNIVERSE]


@pytest.fixture(scope="module")
def bq():
    """Live BigQuery client, or skip the module when unavailable."""
    from google.cloud import bigquery

    try:
        client = bigquery.Client(project=PROJECT)
        client.query("SELECT 1").result()
    except Exception as exc:  # noqa: BLE001 - any auth/connectivity issue → skip
        pytest.skip(f"No live BigQuery access: {exc}")
    return client


def _row(bq, sql, symbol):
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("sym", "STRING", symbol)]
    )
    try:
        return list(bq.query(sql, job_config=job_config).result())[0]
    except Exception as exc:  # noqa: BLE001 - table not created yet → skip
        pytest.skip(f"{YAHOO_ETF} not queryable yet: {exc}")


def _coverage(bq, symbol):
    """Per-symbol coverage stats from the primary (Yahoo) ETF table, or skip."""
    stats = _row(bq, f"""
        SELECT COUNT(DISTINCT candle_date) AS trading_days,
               MIN(candle_date) AS first_date,
               MAX(candle_date) AS last_date
        FROM `{YAHOO_ETF}` WHERE symbol = @sym
    """, symbol)
    if not stats["trading_days"]:
        pytest.skip(f"{symbol}: no rows yet (run the T-20 back-fill first)")
    return stats


class TestEtfCoverageGate:
    @pytest.mark.parametrize("symbol", _ETF_IDS)
    def test_history_reaches_tier_a_window(self, bq, symbol):
        stats = _coverage(bq, symbol)
        assert stats["first_date"].isoformat() <= TIER_A_START_BY, (
            f"{symbol} starts {stats['first_date']}, after the Tier-A frontier {TIER_A_START_BY}"
        )

    @pytest.mark.parametrize("symbol", _ETF_IDS)
    def test_coverage_ratio_over_trading_days(self, bq, symbol):
        stats = _coverage(bq, symbol)
        nyse_weekdays = list(bq.query(f"""
            SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)) AS n
            FROM UNNEST(GENERATE_DATE_ARRAY(DATE('{stats['first_date']}'),
                                            DATE('{stats['last_date']}'))) AS d
        """).result())[0]["n"]
        ratio = stats["trading_days"] / nyse_weekdays
        assert ratio >= MIN_COVERAGE, f"{symbol} coverage {ratio:.3f} < {MIN_COVERAGE}"

    @pytest.mark.parametrize("symbol", _ETF_IDS)
    def test_no_large_gaps_on_trading_days(self, bq, symbol):
        _coverage(bq, symbol)  # skip if empty
        max_gap = _row(bq, f"""
            SELECT MAX(gap) AS max_gap FROM (
              SELECT DATE_DIFF(candle_date,
                               LAG(candle_date) OVER (ORDER BY candle_date), DAY) AS gap
              FROM (SELECT DISTINCT candle_date FROM `{YAHOO_ETF}` WHERE symbol = @sym)
            )
        """, symbol)["max_gap"]
        assert max_gap is not None and max_gap <= MAX_GAP_DAYS, (
            f"{symbol} has a {max_gap}-day gap (> {MAX_GAP_DAYS}); likely a real hole"
        )


class TestEtfIdempotency:
    @pytest.mark.parametrize("table", [YAHOO_ETF, TIINGO_ETF],
                             ids=["yahoo_etf", "tiingo_etf"])
    def test_no_duplicate_natural_keys(self, bq, table):
        try:
            duplicates = list(bq.query(f"""
                SELECT COUNT(*) AS n FROM (
                  SELECT 1 FROM `{table}` GROUP BY symbol, candle_date HAVING COUNT(*) > 1
                )
            """).result())[0]["n"]
        except Exception as exc:  # noqa: BLE001 - table not created yet → skip
            pytest.skip(f"{table} not queryable yet: {exc}")
        assert duplicates == 0
