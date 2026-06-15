"""Unit tests for the MVRV Z-Score ingest (``airflow/ingest/bitcoin_data_mvrv_ingest.py``).

Covers the pure logic — CSV parsing, NaN handling, the one-off value override
(2026-06-03 -> 0.45), row building, MERGE chunking against a fake BigQuery
client — plus contract guards on the MERGE SQL (natural key ``mvrvz_date``) and
the bronze DDL (partition + source registration). No live BigQuery or network.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from airflow.ingest import bitcoin_data_mvrv_ingest
from airflow.ingest.bitcoin_data_mvrv_ingest import (
    _MERGE_SQL,
    _alert_missing,
    _apply_corrections,
    _build_row,
    _prepare_daily_rows,
    _upsert_rows,
    parse_mvrv_csv,
)

_DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "DDL.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ---------------------------------------------------------------------------
# parse_mvrv_csv — the CSV export shape
# ---------------------------------------------------------------------------

class TestParseCsv:
    CSV = (
        "d,unixTs,mvrvZscore\n"
        "2009-01-03,1230940800,0.0\n"
        "2010-07-19,1279497600,32.9641\n"
        "2026-06-03,1780444800,NaN\n"
        "2026-06-10,1781049600,0.2905\n"
    )

    def test_skips_header_and_parses_all_rows(self):
        records = parse_mvrv_csv(self.CSV)
        assert [r["d"] for r in records] == [
            "2009-01-03", "2010-07-19", "2026-06-03", "2026-06-10",
        ]

    def test_unix_ts_is_int(self):
        records = parse_mvrv_csv(self.CSV)
        assert records[0]["unixTs"] == 1230940800
        assert all(isinstance(r["unixTs"], int) for r in records)

    def test_zero_is_kept_not_nulled(self):
        # Early pre-market dates ship a real 0.0; bronze keeps it as delivered.
        records = parse_mvrv_csv(self.CSV)
        assert records[0]["mvrvZscore"] == 0.0

    def test_nan_becomes_none(self):
        records = parse_mvrv_csv(self.CSV)
        by_date = {r["d"]: r["mvrvZscore"] for r in records}
        assert by_date["2026-06-03"] is None

    def test_real_value_parsed_as_float(self):
        records = parse_mvrv_csv(self.CSV)
        by_date = {r["d"]: r["mvrvZscore"] for r in records}
        assert by_date["2026-06-10"] == pytest.approx(0.2905)

    def test_handles_missing_header(self):
        records = parse_mvrv_csv("2026-06-10,1781049600,0.2905\n")
        assert len(records) == 1 and records[0]["d"] == "2026-06-10"

    def test_blank_lines_are_ignored(self):
        records = parse_mvrv_csv("d,unixTs,mvrvZscore\n\n2026-06-10,1781049600,0.5\n\n")
        assert len(records) == 1


# ---------------------------------------------------------------------------
# _build_row — mapping + the known-value override
# ---------------------------------------------------------------------------

class TestBuildRow:
    FETCHED = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)

    def test_maps_source_fields_to_columns(self):
        row = _build_row(
            {"d": "2026-06-10", "unixTs": 1781049600, "mvrvZscore": 0.2905},
            fetched_at=self.FETCHED,
        )
        assert row["mvrvz_date"] == date(2026, 6, 10)
        assert row["unix_ts"] == 1781049600
        assert row["mvrv_zscore"] == pytest.approx(0.2905)
        assert row["source_id"] == bitcoin_data_mvrv_ingest.SOURCE_ID
        assert row["datetime_update"] == self.FETCHED

    def test_missing_value_stays_none_no_fabrication(self):
        # _build_row never fills: a NaN/None stays None, even for a corrected date.
        # Filling is a back-fill-only step (_apply_corrections), not part of build.
        row = _build_row(
            {"d": "2026-06-03", "unixTs": 1780444800, "mvrvZscore": None},
            fetched_at=self.FETCHED,
        )
        assert row["mvrv_zscore"] is None

    def test_accepts_date_object_for_d(self):
        row = _build_row(
            {"d": date(2026, 6, 10), "unixTs": 1, "mvrvZscore": 1.0}, fetched_at=self.FETCHED
        )
        assert row["mvrvz_date"] == date(2026, 6, 10)


# ---------------------------------------------------------------------------
# _apply_corrections — one-off historical fills (back-fill only)
# ---------------------------------------------------------------------------

def _row(d: date, value):
    return {
        "mvrvz_date": d, "unix_ts": 1, "mvrv_zscore": value,
        "source_id": 5, "datetime_update": datetime(2026, 6, 11, tzinfo=timezone.utc),
    }


class TestApplyCorrections:
    def test_known_correction_is_registered(self):
        assert bitcoin_data_mvrv_ingest.HISTORICAL_CORRECTIONS["2026-06-03"] == 0.45

    def test_fills_only_the_missing_corrected_date(self):
        rows = [_row(date(2026, 6, 3), None), _row(date(2026, 6, 4), 0.36)]
        applied = _apply_corrections(rows, {"2026-06-03": 0.45})
        assert applied == ["2026-06-03"]
        assert rows[0]["mvrv_zscore"] == pytest.approx(0.45)
        assert rows[1]["mvrv_zscore"] == pytest.approx(0.36)  # untouched

    def test_does_not_clobber_a_present_value(self):
        # If the source already has a real value for a "corrected" date, keep it.
        rows = [_row(date(2026, 6, 3), 0.61)]
        applied = _apply_corrections(rows, {"2026-06-03": 0.45})
        assert applied == []
        assert rows[0]["mvrv_zscore"] == pytest.approx(0.61)

    def test_missing_date_without_correction_stays_none(self):
        rows = [_row(date(2025, 1, 1), None)]
        applied = _apply_corrections(rows, {"2026-06-03": 0.45})
        assert applied == []
        assert rows[0]["mvrv_zscore"] is None


# ---------------------------------------------------------------------------
# _alert_missing / _prepare_daily_rows — the daily "alert, never fabricate" path
# ---------------------------------------------------------------------------

class TestAlertMissing:
    def test_returns_missing_dates_and_warns(self, caplog):
        rows = [_row(date(2026, 6, 3), None), _row(date(2026, 6, 4), 0.36)]
        with caplog.at_level("WARNING"):
            missing = _alert_missing(rows, "daily")
        assert missing == [date(2026, 6, 3)]
        assert any("missing value for 2026-06-03" in r.message for r in caplog.records)

    def test_no_missing_no_warning(self, caplog):
        rows = [_row(date(2026, 6, 4), 0.36)]
        with caplog.at_level("WARNING"):
            assert _alert_missing(rows, "daily") == []
        assert caplog.records == []


class TestPrepareDailyRows:
    FETCHED = datetime(2026, 6, 11, tzinfo=timezone.utc)

    def test_drops_missing_and_keeps_real_values(self, caplog):
        records = [
            {"d": "2026-06-03", "unixTs": 1, "mvrvZscore": None},
            {"d": "2026-06-04", "unixTs": 2, "mvrvZscore": 0.36},
        ]
        with caplog.at_level("WARNING"):
            rows = _prepare_daily_rows(records, self.FETCHED)
        # The missing row is alerted and skipped; only the real value is written.
        assert [r["mvrvz_date"] for r in rows] == [date(2026, 6, 4)]
        assert any("missing value for 2026-06-03" in r.message for r in caplog.records)

    def test_does_not_apply_historical_corrections(self):
        # Even for the corrected date, the daily path must NOT fabricate 0.45.
        records = [{"d": "2026-06-03", "unixTs": 1, "mvrvZscore": None}]
        rows = _prepare_daily_rows(records, self.FETCHED)
        assert rows == []


# ---------------------------------------------------------------------------
# _MERGE_SQL — idempotency contract on the natural key mvrvz_date
# ---------------------------------------------------------------------------

class TestMergeContract:
    KEY = "mvrvz_date"

    def test_joins_on_natural_key(self):
        assert f"T.{self.KEY} = S.{self.KEY}" in _norm(_MERGE_SQL)

    def test_has_both_match_branches(self):
        assert "WHEN MATCHED THEN UPDATE" in _MERGE_SQL
        assert "WHEN NOT MATCHED THEN INSERT" in _MERGE_SQL

    def test_does_not_update_the_natural_key(self):
        update_set = _MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assignments = re.findall(r"(\w+)\s*=\s*S\.", update_set)
        assert self.KEY not in assignments

    def test_dedupes_source_by_mvrvz_date(self):
        # MERGE must never see the same key twice in the staged batch.
        assert "PARTITION BY s.mvrvz_date" in _norm(_MERGE_SQL)
        assert "WHERE rn = 1" in _norm(_MERGE_SQL)


# ---------------------------------------------------------------------------
# _upsert_rows — chunking to respect the 4000-partition-per-DML limit
# ---------------------------------------------------------------------------

class _FakeJob:
    def result(self):
        return []


class _FakeClient:
    """Records the MERGE statements issued; no real BigQuery."""

    def __init__(self):
        self.queries: list[str] = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        return _FakeJob()


def _rows(n: int) -> list[dict]:
    """n bronze rows with distinct consecutive mvrvz_dates (one partition each)."""
    fetched = datetime(2026, 6, 11, tzinfo=timezone.utc)
    base = date(2010, 1, 1).toordinal()
    return [
        {
            "mvrvz_date": date.fromordinal(base + i),
            "unix_ts": i,
            "mvrv_zscore": 1.0,
            "source_id": 5,
            "datetime_update": fetched,
        }
        for i in range(n)
    ]


class TestUpsertChunking:
    def test_empty_rows_issue_no_query(self):
        client = _FakeClient()
        _upsert_rows(client, [])
        assert client.queries == []

    def test_single_chunk_when_under_limit(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(10), chunk_size=3500)
        assert len(client.queries) == 1

    def test_splits_into_ceil_chunks(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(8), chunk_size=3)  # 3 + 3 + 2
        assert len(client.queries) == 3

    def test_full_history_size_is_chunked(self):
        # ~6400 daily dates must not go in a single >4000-partition MERGE.
        client = _FakeClient()
        _upsert_rows(client, _rows(6400), chunk_size=3500)
        assert len(client.queries) == 2

    def test_every_query_is_the_merge(self):
        client = _FakeClient()
        _upsert_rows(client, _rows(5), chunk_size=2)
        assert all("MERGE" in q and "mvrvz_date" in q for q in client.queries)

    def test_handles_null_value_rows(self):
        # A chunk containing a NULL mvrv (unfilled NaN) must still upsert.
        client = _FakeClient()
        rows = _rows(2)
        rows[0]["mvrv_zscore"] = None
        rows[0]["unix_ts"] = None
        _upsert_rows(client, rows, chunk_size=3500)
        assert len(client.queries) == 1


# ---------------------------------------------------------------------------
# DDL contract — the bronze table and source registration exist as expected
# ---------------------------------------------------------------------------

class TestDdlContract:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _DDL_PATH.read_text(encoding="utf-8")

    def test_table_is_declared(self, ddl):
        assert "prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw" in ddl

    def test_partitioned_by_mvrvz_date(self, ddl):
        block = ddl.split("bitcoin_data_mvrv_zscore_daily_raw", 1)[1]
        assert "PARTITION BY mvrvz_date" in block

    def test_natural_key_is_mvrvz_date(self, ddl):
        block = ddl.split("bitcoin_data_mvrv_zscore_daily_raw", 1)[1]
        assert "PRIMARY KEY (mvrvz_date) NOT ENFORCED" in block

    def test_source_5_registered(self, ddl):
        assert "SELECT 5 AS source_id" in ddl
        assert "BGeometrics" in ddl


# ---------------------------------------------------------------------------
# Training views — daily (same-date) and weekly (Sunday MVRV vs Monday RSI),
# plus the point-in-time as-of macro features (DXY, 10Y, Fed funds, M2)
# ---------------------------------------------------------------------------

class TestTrainingViews:
    @pytest.fixture(scope="class")
    def ddl(self):
        return _norm(_DDL_PATH.read_text(encoding="utf-8"))

    @pytest.fixture(scope="class")
    def daily(self, ddl):
        return ddl.split("vw_btc_training_daily", 1)[1].split("vw_btc_training_weekly")[0]

    @pytest.fixture(scope="class")
    def weekly(self, ddl):
        return ddl.split("vw_btc_training_weekly", 1)[1]

    def test_both_views_declared(self, ddl):
        assert "CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_daily`" in ddl
        assert "CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_weekly`" in ddl

    def test_daily_joins_with_one_day_lag_shift(self, daily):
        # MVRV publishes day D-1 under mvrvz_date D, so the real value of trading
        # day D lives under mvrvz_date = D + 1 -> the join shifts +1 day.
        assert "m.mvrvz_date = DATE_ADD(b.d, INTERVAL 1 DAY)" in daily

    def test_daily_filters_and_columns(self, daily):
        assert "r.temporality = '1d'" in daily
        assert "r.rsi IS NOT NULL" in daily
        for col in ("r.price_close", "r.rsi", "m.mvrv_zscore"):
            assert col in daily

    def test_weekly_crosses_sunday_mvrv_vs_monday_rsi_with_lag(self, weekly):
        # The week's Sunday is Monday + 6; with the 1-day MVRV lag the Sunday
        # value lives under mvrvz_date = Monday + 7, so the join offset is +7.
        assert "m.mvrvz_date = DATE_ADD(b.week_start_monday, INTERVAL 7 DAY)" in weekly

    def test_weekly_exposes_monday_and_sunday(self, weekly):
        assert "AS week_start_monday" in weekly
        assert "DATE_ADD(DATE(r.time_period_start), INTERVAL 6 DAY) AS week_end_sunday" in weekly

    def test_weekly_filters_and_columns(self, weekly):
        assert "r.temporality = '1w'" in weekly
        assert "r.rsi IS NOT NULL" in weekly
        for col in ("r.price_close", "r.rsi", "m.mvrv_zscore"):
            assert col in weekly

    def test_views_use_monday_weeks_only_via_offset(self, weekly):
        # The weekly view must NOT re-truncate weeks itself; it relies on
        # rsi_features already being WEEK(MONDAY)-aligned and just offsets +6 days.
        assert "WEEK(" not in weekly

    # --- macro features (DXY, 10Y, Fed funds, M2) ---------------------------

    @pytest.mark.parametrize(
        "col",
        ("AS dxy", "AS treasury_10y", "AS treasury_2y", "AS m2"),
    )
    def test_both_views_expose_macro_columns(self, daily, weekly, col):
        # These appear in both views (as CTE-derived values; the daily view uses
        # them internally even when they are not exposed as output columns).
        assert col in daily
        assert col in weekly

    def test_fed_funds_is_weekly_only(self, daily, weekly):
        # fed_funds was dropped from the daily model-feature view; weekly keeps it.
        assert "AS fed_funds" in weekly
        assert "AS fed_funds" not in daily

    def test_daily_macro_is_asof_known_on_or_before_D(self, daily):
        # As-of: a range-join (date <= the trading day) trimmed to the latest row
        # with QUALIFY ROW_NUMBER, forward-filling weekend/holiday gaps. DXY is
        # taken from its EMA-carrying CTE (alias c); DGS10 from its raw table.
        assert "ON c.dxy_date <= b.d" in daily
        assert "ON y.obs_date <= b.d" in daily
        assert "ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY c.dxy_date DESC) = 1" in daily

    def test_daily_m2_is_point_in_time_vintage(self, daily):
        # M2 picks the vintage whose realtime window contains D (no look-ahead),
        # latest observation week first.
        assert "ON w.realtime_start <= b.d AND w.realtime_end >= b.d" in daily
        assert "ORDER BY w.wm2ns_date DESC, w.realtime_start DESC) = 1" in daily

    def test_weekly_macro_is_asof_the_sunday(self, weekly):
        # Weekly features are as-of the week's Sunday (Monday + 6 days).
        assert "ON x.dxy_date <= b.week_end_sunday" in weekly
        assert "ON w.realtime_start <= b.week_end_sunday AND w.realtime_end >= b.week_end_sunday" in weekly

    # --- 2Y Treasury + 10Y-2Y term spread, change and dis-inversion flag -----

    def test_daily_2y_is_asof_known_on_or_before_D(self, daily):
        # DGS2 as-of join, same pattern as the other daily macro series.
        assert "ON v.obs_date <= b.d" in daily
        assert "ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY v.obs_date DESC) = 1" in daily

    def test_weekly_2y_is_asof_the_sunday(self, weekly):
        assert "ON v.obs_date <= b.week_end_sunday" in weekly

    def test_spread_is_10y_minus_2y(self, daily, weekly):
        # Curve level: negative spread = inverted.
        spread = "dgs10_asof.treasury_10y - dgs2_asof.treasury_2y AS spread_10y_2y"
        assert spread in daily
        assert spread in weekly

    def test_daily_spread_change_uses_one_month_lag(self, daily):
        # ~1 month = 30 daily rows; positive change = steepening.
        assert ("c.spread_10y_2y - LAG(c.spread_10y_2y, 30) OVER (ORDER BY c.date) "
                "AS spread_10y_2y_chg_1m") in daily

    def test_weekly_spread_change_uses_four_week_lag(self, weekly):
        # ~1 month = 4 weekly rows.
        assert ("j.spread_10y_2y - LAG(j.spread_10y_2y, 4) OVER (ORDER BY j.week_start_monday) "
                "AS spread_10y_2y_chg_1m") in weekly

    def test_daily_dis_inverting_flag(self, daily):
        # TRUE when the spread was inverted a month ago and has risen since.
        assert "AS dis_inverting_from_neg" in daily
        assert "LAG(c.spread_10y_2y, 30) OVER (ORDER BY c.date) < 0" in daily

    def test_weekly_dis_inverting_flag(self, weekly):
        assert "AS dis_inverting_from_neg" in weekly
        assert "LAG(j.spread_10y_2y, 4) OVER (ORDER BY j.week_start_monday) < 0" in weekly

    # --- daily-only features: VIX, EMA365, weekly-RSI as-of, realised vol, ---
    # --- M2 log YoY, halving-cycle (phase/sin/cos/issuance) ------------------

    @pytest.mark.parametrize(
        "col",
        ("AS vix", "AS rsi_weekly", "AS price_vs_ema365", "AS dxy_vs_ema365",
         "AS realized_vol_30d", "AS m2_yoy_log", "AS m2_roc_13w_ann", "AS teny_chg_30d",
         "AS cycle_phase", "AS cycle_phase_sin", "AS cycle_phase_cos", "AS issuance_rate_ann"),
    )
    def test_daily_exposes_new_feature_columns(self, daily, col):
        assert col in daily

    def test_price_vs_ema365_is_ratio_minus_one(self, daily):
        assert "SAFE_DIVIDE(c.price_close, pc.ema365) - 1 AS price_vs_ema365" in daily

    def test_dxy_vs_ema365_uses_dxy_own_ema(self, daily):
        assert "AS dxy_ema365" in daily
        assert "SAFE_DIVIDE(c.dxy, c.dxy_ema365) - 1 AS dxy_vs_ema365" in daily

    def test_m2_roc_13w_annualised(self, daily):
        assert "w.wm2ns_date <= DATE_SUB(b.d, INTERVAL 91 DAY)" in daily
        assert "POW(SAFE_DIVIDE(c.m2, NULLIF(c.m2_13w, 0)), 52/13) - 1 AS m2_roc_13w_ann" in daily

    def test_teny_chg_30d_is_level_change(self, daily):
        assert "c.treasury_10y - LAG(c.treasury_10y, 30) OVER (ORDER BY c.date) AS teny_chg_30d" in daily

    def test_vix_is_asof_from_vixcls(self, daily):
        assert "fred_vixcls_daily_raw` AS x" in daily
        assert "x.obs_value AS vix" in daily

    def test_weekly_rsi_asof_uses_previous_week(self, daily):
        # The weekly RSI is the PREVIOUS week's: the most recent week ending
        # strictly before the week containing d (its Monday < d's Monday). This
        # excludes the still-forming current week and the week closing on d (a
        # Sunday), so the value is stable across the whole week with no look-ahead.
        assert "wk.temporality = '1w'" in daily
        assert "DATE(wk.time_period_start) < DATE_TRUNC(b.d, WEEK(MONDAY))" in daily
        assert "wk.rsi AS rsi_weekly" in daily

    def test_realized_vol_is_std_of_30_log_returns_annualised(self, daily):
        assert "LN(price_close / LAG(price_close) OVER (ORDER BY d)) AS logret" in daily
        assert ("STDDEV_SAMP(logret) OVER (ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) "
                "* SQRT(365)") in daily
        assert "IF(i >= 30," in daily          # full-window warm-up NULL (like RSI)
        assert "AS realized_vol_30d" in daily

    def test_ema365_is_closed_form_exact(self, daily):
        # alpha = 2/366; closed-form recursive EMA in one pass.
        assert "EXP(i * LN(1 - 2/366)) *" in daily
        assert "SUM(EXP(-i * LN(1 - 2/366)) * price_close) OVER (ORDER BY d)" in daily

    def test_ema365_warmup_nulls_first_365_rows(self, daily):
        # NULL until 365 rows of EMA history, consistent with the RSI warm-up.
        assert "IF(i >= 365," in daily

    def test_m2_yoy_log_and_52w_lag(self, daily):
        assert "w.wm2ns_date <= DATE_SUB(b.d, INTERVAL 364 DAY)" in daily
        assert "SAFE.LN(c.m2 / NULLIF(c.m2_52w, 0)) AS m2_yoy_log" in daily

    def test_cycle_phase_from_supply_and_epoch(self, daily):
        # Epoch fixed by date; cycle_phase = block fraction from circulating supply.
        assert "s.supply_date <= b.d" in daily
        assert "WHEN b.d < '2024-04-20' THEN 3" in daily
        assert "50 / POW(2, j.halving_epoch) AS block_subsidy" in daily
        assert "21000000 * (1 - POW(2, -j.halving_epoch))" in daily

    def test_cycle_sin_cos_and_issuance(self, daily):
        assert "SIN(2 * ACOS(-1) * c.cycle_phase) AS cycle_phase_sin" in daily
        assert "COS(2 * ACOS(-1) * c.cycle_phase) AS cycle_phase_cos" in daily
        assert "SAFE_DIVIDE(52560 * c.block_subsidy, c.circ_supply) AS issuance_rate_ann" in daily
