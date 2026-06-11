"""Live-BigQuery integration tests (formerly tracked as technical debt).

These validate what unit tests cannot: that the deployed tables in
``trade-390514`` actually satisfy the pipeline's contracts — Monday-labelled
weeks, weekly OHLC aggregation, Wilder warm-up NULLs, natural-key uniqueness
(idempotency), and queryable JSON trigger params. They are the automated form
of the manual ``bq`` validations used during T-08 and the Monday-week
migration.

Execution model:

  * **Read-only** against the production tables; nothing is written or
    deleted (except the opt-in replay below, which only re-runs the
    idempotent pipeline).
  * **Deselected by default** (``-m "not integration"`` in ``pyproject.toml``)
    so the unit suite stays fast and CI-safe. Run explicitly with::

        pytest -m integration --no-cov

  * **Skipped automatically** when no GCP credentials / connectivity are
    available, so the suite never fails on a machine without ADC.
  * The end-to-end replay (``test_pipeline_replay_is_idempotent``) re-runs the
    full pipeline twice over the latest bronze day and asserts stable counts —
    the T-08 manual check. It takes minutes and incurs GCP cost, so it
    additionally requires ``TRADE_GCP_TEMP_LOCATION`` (a ``gs://`` path) to be
    set.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = "trade-390514"
OHLCV = f"{PROJECT}.prod_trade_silver.ohlcv_validated"
RSI = f"{PROJECT}.prod_trade_silver.rsi_features"
SIGNALS = f"{PROJECT}.prod_trade_gold.fact_signals"
BRONZE = f"{PROJECT}.prod_trade_bronze.binance_btcusd_daily_raw"
BRONZE_BITSTAMP = f"{PROJECT}.prod_trade_bronze.bitstamp_btcusd_daily_raw"

pytestmark = pytest.mark.integration


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


def _scalar(bq, sql):
    """Run a query and return the first column of its single row."""
    row = list(bq.query(sql).result())[0]
    return list(row.values())[0]


# ---------------------------------------------------------------------------
# Weekly candle contract (Monday → Sunday business rule)
# ---------------------------------------------------------------------------

class TestWeeklyCandles:
    def test_weekly_candles_exist(self, bq):
        if _scalar(bq, f"SELECT COUNT(*) FROM `{OHLCV}` WHERE temporality = '1w'") == 0:
            pytest.skip("no weekly candles loaded yet")

    def test_weekly_candles_are_monday_labelled(self, bq):
        # BigQuery DAYOFWEEK: Sunday=1, Monday=2.
        not_monday = _scalar(bq, f"""
            SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM DATE(time_period_start)) != 2)
            FROM `{OHLCV}` WHERE temporality = '1w'
        """)
        assert not_monday == 0

    def test_weekly_candle_spans_monday_to_sunday(self, bq):
        bad_span = _scalar(bq, f"""
            SELECT COUNTIF(DATE(time_period_end)
                           != DATE_ADD(DATE(time_period_start), INTERVAL 6 DAY))
            FROM `{OHLCV}` WHERE temporality = '1w'
        """)
        assert bad_span == 0

    def test_weekly_candle_matches_daily_aggregation(self, bq):
        # Every weekly candle must equal the OHLC aggregation of its dailies:
        # open = first day's open, close = last day's close, high/low = extremes.
        mismatches = _scalar(bq, f"""
            WITH agg AS (
              SELECT symbol,
                     DATE_TRUNC(DATE(time_period_start), WEEK(MONDAY)) AS wk,
                     ARRAY_AGG(price_open  ORDER BY time_period_start ASC  LIMIT 1)[OFFSET(0)] AS o,
                     MAX(price_high) AS h,
                     MIN(price_low)  AS l,
                     ARRAY_AGG(price_close ORDER BY time_period_start DESC LIMIT 1)[OFFSET(0)] AS c
              FROM `{OHLCV}` WHERE temporality = '1d'
              GROUP BY symbol, wk
            )
            SELECT COUNT(*)
            FROM `{OHLCV}` w
            JOIN agg ON w.symbol = agg.symbol AND DATE(w.time_period_start) = agg.wk
            WHERE w.temporality = '1w'
              AND (w.price_open != agg.o OR w.price_high != agg.h
                   OR w.price_low != agg.l OR w.price_close != agg.c)
        """)
        assert mismatches == 0


# ---------------------------------------------------------------------------
# Bronze sources: the date cut-over keeps them disjoint
# ---------------------------------------------------------------------------

class TestBronzeSources:
    def test_bronze_sources_are_date_disjoint(self, bq):
        # Cut-over policy: Bitstamp ≤ 2017-08-16, Binance ≥ 2017-08-17. An
        # overlap would make conform's priority tie-break decide real data, so
        # it should be a deliberate choice — flag it here if it ever appears.
        overlapping_dates = _scalar(bq, f"""
            SELECT COUNT(*)
            FROM `{BRONZE_BITSTAMP}` s
            JOIN `{BRONZE}` b USING (candle_date)
        """)
        assert overlapping_dates == 0

    def test_bronze_history_is_continuous_across_sources(self, bq):
        # Union of both sources must have no missing days end to end.
        missing = _scalar(bq, f"""
            SELECT DATE_DIFF(MAX(candle_date), MIN(candle_date), DAY) + 1 - COUNT(DISTINCT candle_date)
            FROM (
              SELECT candle_date FROM `{BRONZE_BITSTAMP}`
              UNION ALL
              SELECT candle_date FROM `{BRONZE}`
            )
        """)
        assert missing == 0


# ---------------------------------------------------------------------------
# Idempotency: natural keys are unique in every table
# ---------------------------------------------------------------------------

_NATURAL_KEYS = [
    (BRONZE, "symbol, candle_date"),
    (OHLCV, "symbol, temporality, time_period_start"),
    (RSI, "symbol, temporality, rsi_period, time_period_start"),
    (SIGNALS, "symbol, temporality, signal_start, strategy_id"),
]


class TestNaturalKeyUniqueness:
    @pytest.mark.parametrize("table, key", _NATURAL_KEYS,
                             ids=[t.split(".")[-1] for t, _ in _NATURAL_KEYS])
    def test_no_duplicate_natural_keys(self, bq, table, key):
        duplicates = _scalar(bq, f"""
            SELECT COUNT(*) FROM (
              SELECT 1 FROM `{table}` GROUP BY {key} HAVING COUNT(*) > 1
            )
        """)
        assert duplicates == 0


# ---------------------------------------------------------------------------
# RSI data: warm-up NULLs and 0–100 scale on real exchange data
# ---------------------------------------------------------------------------

class TestRsiData:
    def test_warm_up_rows_are_exactly_the_first_period(self, bq):
        # Per series, row N is NULL iff N <= rsi_period (the Wilder warm-up).
        mismatches = _scalar(bq, f"""
            SELECT COUNTIF((rn <= rsi_period) != (rsi IS NULL))
            FROM (
              SELECT rsi, rsi_period,
                     ROW_NUMBER() OVER (
                       PARTITION BY symbol, temporality, rsi_period
                       ORDER BY time_period_start) AS rn
              FROM `{RSI}`
            )
        """)
        assert mismatches == 0

    def test_published_rsi_within_bounds_on_real_data(self, bq):
        # Validates the 0–100 scale against real Binance-ingested history —
        # the agreed substitute for Hypothesis property-based testing.
        out_of_bounds = _scalar(bq, f"""
            SELECT COUNTIF(rsi < 0 OR rsi > 100)
            FROM `{RSI}` WHERE rsi IS NOT NULL
        """)
        assert out_of_bounds == 0


# ---------------------------------------------------------------------------
# Signals data: provenance and queryable JSON (T-08 regression, data level)
# ---------------------------------------------------------------------------

class TestSignalsData:
    def test_every_signal_has_a_published_daily_rsi(self, bq):
        orphans = _scalar(bq, f"""
            SELECT COUNT(*)
            FROM `{SIGNALS}` s
            LEFT JOIN `{RSI}` r
              ON r.symbol = s.symbol AND r.temporality = '1d'
             AND r.time_period_start = s.signal_start AND r.rsi IS NOT NULL
            WHERE r.time_period_start IS NULL
        """)
        assert orphans == 0

    def test_signal_values_are_canonical(self, bq):
        bad = _scalar(bq, f"""
            SELECT COUNTIF(signal NOT IN ('BUY', 'SELL', 'NEUTRAL'))
            FROM `{SIGNALS}`
        """)
        assert bad == 0

    def test_trigger_params_is_queryable_json(self, bq):
        # If trigger_params were stored as an escaped string (the T-08 bug),
        # JSON_VALUE would return NULL for every row.
        unqueryable = _scalar(bq, f"""
            SELECT COUNTIF(JSON_VALUE(trigger_params, '$.daily_rsi') IS NULL)
            FROM `{SIGNALS}`
        """)
        assert unqueryable == 0


# ---------------------------------------------------------------------------
# End-to-end replay (opt-in): the manual T-08 idempotency check
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.environ.get("TRADE_GCP_TEMP_LOCATION"),
    reason="set TRADE_GCP_TEMP_LOCATION=gs://... to run the pipeline replay (slow, costs money)",
)
def test_pipeline_replay_is_idempotent(bq):
    """Run the full pipeline twice over the latest bronze day: counts must not move."""
    temp_location = os.environ["TRADE_GCP_TEMP_LOCATION"]
    last_day = _scalar(bq, f"SELECT MAX(candle_date) FROM `{BRONZE}`")
    if last_day is None:
        pytest.skip("bronze is empty")

    repo_root = Path(__file__).resolve().parents[1]

    def run_pipeline():
        subprocess.run(
            [sys.executable, "-m", "dataflow.pipeline",
             "--start_date", str(last_day), "--end_date", str(last_day),
             "--temp_location", temp_location,
             "--staging_location", temp_location],
            check=True, cwd=repo_root, timeout=900,
        )

    def table_counts():
        row = list(bq.query(f"""
            SELECT
              (SELECT COUNT(*) FROM `{OHLCV}`)   AS ohlcv,
              (SELECT COUNT(*) FROM `{RSI}`)     AS rsi,
              (SELECT COUNT(*) FROM `{SIGNALS}`) AS signals
        """).result())[0]
        return dict(row)

    run_pipeline()
    counts_after_first = table_counts()
    run_pipeline()
    assert table_counts() == counts_after_first
