-- =====================================================================
-- BigQuery DDL - Trading signals pipeline (medallion architecture)
-- Project: trade-390514 | Region: us-central1
--
-- Run with:
--   bq query --use_legacy_sql=false --project_id=trade-390514 < ddl_prod_trade.sql
-- (or paste into the BigQuery console).
--
-- Notes:
--   * Statements are idempotent (IF NOT EXISTS).
--   * All object descriptions are in English.
--   * PRIMARY KEY / FOREIGN KEY are NOT ENFORCED (documentation and optimizer
--     hints only; BigQuery does not enforce them). Remove if undesired.
--   * Create order respects FK references: control + strategy first.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Schemas (datasets)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_control`
OPTIONS (location = 'us-central1',
  description = 'Control layer: operational metadata such as the source registry and load priorities.');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_strategy`
OPTIONS (location = 'us-central1',
  description = 'Strategy layer: strategy catalog and one versioned-parameters table per strategy.');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_bronze`
OPTIONS (location = 'us-central1',
  description = 'Bronze layer: raw landing zone, one table per source/symbol/temporality. Data kept as delivered by the source.');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_silver`
OPTIONS (location = 'us-central1',
  description = 'Silver layer: conformed OHLCV and derived indicator features, generic (symbol and temporality as columns).');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_gold`
OPTIONS (location = 'us-central1',
  description = 'Gold layer: business outcomes (trading signals) modeled as a star-schema fact table.');


-- ---------------------------------------------------------------------
-- prod_trade_control
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_control.source_priority` (
  source_id       INT64     NOT NULL OPTIONS(description = "Unique source identifier (logical key)."),
  label           STRING    OPTIONS(description = "Human-readable name of the source."),
  priority        INT64     OPTIONS(description = "Preference order when consolidating multiple sources (higher value is preferred)."),
  is_active       BOOL      OPTIONS(description = "Whether the source is currently active."),
  url_source      STRING    OPTIONS(description = "Download URL/endpoint of the source."),
  name_source     STRING    OPTIONS(description = "Name of the data provider (e.g. CoinAPI, Investing.com)."),
  datetime_update TIMESTAMP OPTIONS(description = "Execution timestamp of the process that wrote the row (audit)."),
  PRIMARY KEY (source_id) NOT ENFORCED
)
OPTIONS(description = "Registry of data sources and their consolidation priority.");


-- ---------------------------------------------------------------------
-- prod_trade_strategy
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_strategy.strategy` (
  strategy_id    INT64     NOT NULL OPTIONS(description = "Unique strategy identifier."),
  strategy_name  STRING    OPTIONS(description = "Human-readable name; equals the name of this strategy's parameter table (e.g. strategy_rsi_daily_week)."),
  description    STRING    OPTIONS(description = "Short description of the strategy."),
  indicator_type STRING    OPTIONS(description = "Base indicator (e.g. RSI)."),
  is_active      BOOL      OPTIONS(description = "Whether the strategy is currently active."),
  created_at     TIMESTAMP OPTIONS(description = "Creation timestamp."),
  PRIMARY KEY (strategy_id) NOT ENFORCED
)
OPTIONS(description = "Catalog of trading strategies.");


-- ---------------------------------------------------------------------
-- prod_trade_bronze (Option A: one table per source/symbol/temporality)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.coinapi_btcusd_daily_raw` (
  source_id         INT64     OPTIONS(description = "FK to prod_trade_control.source."),
  datetime_update   TIMESTAMP OPTIONS(description = "Download execution timestamp (audit)."),
  time_period_start STRING    OPTIONS(description = "Period start as delivered by CoinAPI (raw ISO string)."),
  time_period_end   STRING    OPTIONS(description = "Period end (raw)."),
  time_open         STRING    OPTIONS(description = "Timestamp of first trade in the period (raw)."),
  time_close        STRING    OPTIONS(description = "Timestamp of last trade in the period (raw)."),
  price_open        FLOAT64   OPTIONS(description = "Open price."),
  price_high        FLOAT64   OPTIONS(description = "High price."),
  price_low         FLOAT64   OPTIONS(description = "Low price."),
  price_close       FLOAT64   OPTIONS(description = "Close price."),
  volume_traded     FLOAT64   OPTIONS(description = "Traded volume."),
  trades_count      INT64     OPTIONS(description = "Number of trades in the period."),
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE(datetime_update)
OPTIONS(description = "Raw daily BTC/USD candles from CoinAPI (native format; time fields kept as STRING).");

-- Column names sanitized to BigQuery-safe identifiers; original Spanish names noted in descriptions.
-- id_source and datetime_update added for lineage/audit (not present in the original export).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.investing_btcusd_daily_raw` (
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source (added for lineage)."),
  datetime_update TIMESTAMP OPTIONS(description = "Download execution timestamp (audit; added)."),
  fecha           INT64     OPTIONS(description = "Raw date from Investing.com export, format ddmmyyyy (dmmyyyy when day < 10). Source column: Fecha."),
  ultimo          FLOAT64   OPTIONS(description = "Close price. Source column: Ultimo."),
  apertura        FLOAT64   OPTIONS(description = "Open price. Source column: Apertura."),
  maximo          FLOAT64   OPTIONS(description = "High price. Source column: Maximo."),
  minimo          FLOAT64   OPTIONS(description = "Low price. Source column: Minimo."),
  vol             STRING    OPTIONS(description = "Volume as text with K/M suffixes; needs parsing. Source column: Vol."),
  var_pct         FLOAT64   OPTIONS(description = "Daily percentage change. Source column: % var."),
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE(datetime_update)
OPTIONS(description = "Raw daily BTC/USD candles exported from Investing.com. Column names sanitized to BigQuery-safe identifiers.");


-- Binance candles ingested via CCXT (T-04). CCXT's fetch_ohlcv returns a
-- normalized tuple [open_time_ms, open, high, low, close, volume]; this table
-- stores exactly that, plus lineage/audit columns. Unlike the CoinAPI/Investing
-- bronze tables (partitioned by datetime_update, append-style dumps), this one is
-- partitioned by the candle's own date because the daily ingest upserts (MERGE)
-- on the business key (symbol, candle_date) to stay idempotent.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.binance_btcusd_daily_raw` (
  symbol          STRING    NOT NULL OPTIONS(description = "CCXT unified symbol as requested (e.g. BTC/USDT)."),
  candle_date     DATE      NOT NULL OPTIONS(description = "Candle open date (UTC), derived from open_time. Business key and partition column."),
  open_time       INT64     NOT NULL OPTIONS(description = "Candle open time in epoch milliseconds, as delivered by CCXT/Binance (raw)."),
  price_open      FLOAT64   OPTIONS(description = "Open price."),
  price_high      FLOAT64   OPTIONS(description = "High price."),
  price_low       FLOAT64   OPTIONS(description = "Low price."),
  price_close     FLOAT64   OPTIONS(description = "Close price."),
  volume_traded   FLOAT64   OPTIONS(description = "Base-asset traded volume."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (symbol, candle_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY candle_date
CLUSTER BY symbol
OPTIONS(description = "Raw daily BTC candles from Binance via CCXT. Idempotent daily upsert on (symbol, candle_date).");

-- Register Binance as a source so the bronze FK resolves. Idempotent seed.
-- Priority is illustrative (higher value preferred); adjust to your consolidation policy.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 3 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (3, 'Binance (CCXT)', 3, TRUE, 'https://api.binance.com', 'Binance', CURRENT_TIMESTAMP());

-- Bitstamp candles ingested via CCXT (bitstamp_btc_ingest.py, a thin entry-point
-- over the same ccxt_candle_common.py shared by Binance). Extends BTC history
-- before Binance's BTC/USDT listing
-- (2017-08-17): Bitstamp trades BTC/USD continuously since ~2011-08. Same
-- shape and upsert pattern as the Binance table: partitioned by the candle's
-- own date, idempotent MERGE on (symbol, candle_date). Cut-over policy:
-- Bitstamp rows are loaded only up to 2017-08-16; Binance covers 2017-08-17
-- onward, so the two sources never overlap by date.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.bitstamp_btcusd_daily_raw` (
  symbol          STRING    NOT NULL OPTIONS(description = "CCXT unified symbol as requested (e.g. BTC/USD)."),
  candle_date     DATE      NOT NULL OPTIONS(description = "Candle open date (UTC), derived from open_time. Business key and partition column."),
  open_time       INT64     NOT NULL OPTIONS(description = "Candle open time in epoch milliseconds, as delivered by CCXT/Bitstamp (raw)."),
  price_open      FLOAT64   OPTIONS(description = "Open price."),
  price_high      FLOAT64   OPTIONS(description = "High price."),
  price_low       FLOAT64   OPTIONS(description = "Low price."),
  price_close     FLOAT64   OPTIONS(description = "Close price."),
  volume_traded   FLOAT64   OPTIONS(description = "Base-asset traded volume."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (symbol, candle_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY candle_date
CLUSTER BY symbol
OPTIONS(description = "Raw daily BTC candles from Bitstamp via CCXT (pre-Binance history, up to 2017-08-16). Idempotent upsert on (symbol, candle_date).");

-- Register Bitstamp as a source. Idempotent seed. Priority 4: preferred over
-- Binance (3) on consolidation ties (moot in practice — the date cut-over
-- keeps the two sources disjoint).
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 4 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (4, 'Bitstamp (CCXT)', 4, TRUE, 'https://www.bitstamp.net/api/', 'Bitstamp', CURRENT_TIMESTAMP());

-- MVRV Z-Score (BTC on-chain valuation metric) from bitcoin-data.com
-- (BGeometrics). Unlike the OHLCV bronze tables this is NOT candle data: it is a
-- single daily metric series (one value per UTC date), and it does NOT feed the
-- OHLCV `conform` consolidation — it is a standalone feature source. Same
-- idempotency pattern as the candle tables: partitioned by the metric's own
-- date, daily upsert (MERGE) on the business key `mvrvz_date`. Full history is
-- back-filled once from the CSV export
-- (https://bitcoin-data.com/v1/mvrv-zscore/csv, from 2009-01-03) and then
-- refreshed daily from the API (https://bitcoin-data.com/v1/mvrv-zscore/last).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw` (
  mvrvz_date      DATE      NOT NULL OPTIONS(description = "Metric date (UTC); source field `d`. Business key and partition column."),
  unix_ts         INT64     OPTIONS(description = "Unix timestamp (seconds) of the metric date, as delivered by the source (raw, field `unixTs`)."),
  mvrv_zscore     FLOAT64   OPTIONS(description = "MVRV Z-Score value (source field `mvrvZscore`). NULL when the source delivers no value (e.g. NaN) for the date."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (mvrvz_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY mvrvz_date
OPTIONS(description = "Raw daily BTC MVRV Z-Score from bitcoin-data.com (BGeometrics). Idempotent daily upsert on mvrvz_date; full history back-filled from the CSV export.");

-- Register bitcoin-data.com (BGeometrics) as a source. Idempotent seed.
-- priority is NULL on purpose: this source feeds the standalone MVRV table, not
-- the OHLCV consolidation, so it never competes in `conform`'s priority de-dup.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 5 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (5, 'bitcoin-data.com MVRV Z-Score (BGeometrics)', NULL, TRUE, 'https://bitcoin-data.com/v1/mvrv-zscore', 'BGeometrics', CURRENT_TIMESTAMP());

-- ---------------------------------------------------------------------
-- Macro feature sources (DXY, M2, 10Y Treasury, Fed funds)
-- Standalone daily/weekly feature series for the ML training layer. Like MVRV,
-- these are NOT candle data and do NOT feed the OHLCV `conform` consolidation;
-- each is its own bronze table, partitioned by its own date, with an idempotent
-- MERGE on its natural key. Full history is back-filled once, then refreshed
-- daily. All registered with priority NULL (they never compete in `conform`).
-- NOTE: the long daily series (DXY since 1971, DGS10 since 1962, DFF since 1954)
-- partition by MONTH (DATE_TRUNC(..., MONTH)), not day: at daily granularity they
-- would exceed BigQuery's hard 10000-partitions-per-table limit. M2 (weekly) and
-- MVRV (daily since 2009) stay under the limit, so they keep day granularity.
-- ---------------------------------------------------------------------

-- DXY (ICE U.S. Dollar Index, 6-currency) from Yahoo Finance (symbol DX-Y.NYB).
-- Real ICE DXY (not the Fed's broad index), daily OHLC since 1971. Bronze keeps
-- the bars as delivered; idempotent upsert on the bar's own date (dxy_date).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.yahoo_dxy_daily_raw` (
  dxy_date        DATE      NOT NULL OPTIONS(description = "Trading-day date (UTC) of the daily bar, derived from the Yahoo chart timestamp. Business key and partition column."),
  price_open      FLOAT64   OPTIONS(description = "Open value of the index for the day."),
  price_high      FLOAT64   OPTIONS(description = "High value of the index for the day."),
  price_low       FLOAT64   OPTIONS(description = "Low value of the index for the day."),
  price_close     FLOAT64   OPTIONS(description = "Close value of the index for the day."),
  volume_traded   FLOAT64   OPTIONS(description = "Volume as delivered by Yahoo; an index typically reports 0 or NULL."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (dxy_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(dxy_date, MONTH)
OPTIONS(description = "Raw daily ICE U.S. Dollar Index (DXY) bars from Yahoo Finance (DX-Y.NYB). Idempotent daily upsert on dxy_date; full history back-filled from the chart API. Partitioned by MONTH (not day): ~55 years of daily bars would exceed BigQuery's 10000-partitions-per-table limit at daily granularity.");

-- Register Yahoo Finance (DXY) as a source. Idempotent seed. priority NULL: a
-- standalone feature source, never part of the OHLCV `conform` consolidation.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 6 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (6, 'Yahoo Finance DXY (DX-Y.NYB)', NULL, TRUE, 'https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB', 'Yahoo Finance', CURRENT_TIMESTAMP());

-- M2 money stock (WM2NS, weekly, NSA) from FRED/ALFRED with point-in-time
-- vintages. Unlike the other macro series, M2 is REVISED and published with a
-- lag, so to build features without look-ahead we store every ALFRED vintage:
-- the natural key is (wm2ns_date, realtime_start). To reconstruct what was known
-- on day X, pick the row with realtime_start <= X <= realtime_end for each
-- wm2ns_date. realtime_end is 9999-12-31 while the value is the latest revision
-- (mutable: set when a newer vintage supersedes it).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.fred_wm2ns_weekly_raw` (
  wm2ns_date      DATE      NOT NULL OPTIONS(description = "Observation/period date as delivered by FRED (field `date`); WM2NS weeks end on Monday. Part of the business key and the partition column."),
  realtime_start  DATE      NOT NULL OPTIONS(description = "ALFRED vintage start: first date this value was the published one. Part of the business key; this is what makes the series point-in-time (no look-ahead)."),
  realtime_end    DATE      OPTIONS(description = "ALFRED vintage end: last date this value was current (9999-12-31 while it is the latest revision). Mutable — set when a newer vintage supersedes this value."),
  m2_value        FLOAT64   OPTIONS(description = "M2 money stock (WM2NS), billions of USD, not seasonally adjusted. NULL when FRED delivers `.` (missing)."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (wm2ns_date, realtime_start) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY wm2ns_date
OPTIONS(description = "Raw weekly M2 (WM2NS, NSA) from FRED/ALFRED with point-in-time vintages. Idempotent upsert on (wm2ns_date, realtime_start); full vintage history back-filled from ALFRED.");

-- Register FRED M2 (WM2NS) as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 7 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (7, 'FRED M2 WM2NS (ALFRED vintages)', NULL, TRUE, 'https://api.stlouisfed.org/fred/series/observations', 'FRED / St. Louis Fed', CURRENT_TIMESTAMP());

-- 10-Year Treasury Constant Maturity yield (DGS10, daily, percent) from FRED.
-- Not revised in any material way, so no vintages: a plain (date, value) series.
-- Generic column names (obs_date/obs_value) are shared with the DFF table below
-- because both are loaded by thin entry-points (fred_10y_ingest.py /
-- fred_fedfunds_ingest.py) over the same fred_common.py logic.
-- Non-publication days (FRED `.`) are dropped, not stored as NULL.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.fred_dgs10_daily_raw` (
  obs_date        DATE      NOT NULL OPTIONS(description = "Observation date as delivered by FRED (field `date`). Business key and partition column."),
  obs_value       FLOAT64   OPTIONS(description = "Series value: 10-Year Treasury Constant Maturity yield (DGS10), percent."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (obs_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(obs_date, MONTH)
OPTIONS(description = "Raw daily 10-Year Treasury yield (DGS10) from FRED. Idempotent upsert on obs_date; full history back-filled from the FRED API. Partitioned by MONTH: DGS10 since 1962 (>16000 business days) exceeds BigQuery's 10000-partitions-per-table limit at daily granularity.");

-- Register FRED DGS10 as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 8 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (8, 'FRED 10Y Treasury (DGS10)', NULL, TRUE, 'https://api.stlouisfed.org/fred/series/observations', 'FRED / St. Louis Fed', CURRENT_TIMESTAMP());

-- Effective Federal Funds Rate (DFF, daily, percent) from FRED. Same plain
-- (obs_date, obs_value) shape as DGS10 (shared fred_common.py logic). Not revised.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.fred_dff_daily_raw` (
  obs_date        DATE      NOT NULL OPTIONS(description = "Observation date as delivered by FRED (field `date`). Business key and partition column."),
  obs_value       FLOAT64   OPTIONS(description = "Series value: Effective Federal Funds Rate (DFF), percent."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (obs_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(obs_date, MONTH)
OPTIONS(description = "Raw daily Effective Federal Funds Rate (DFF) from FRED. Idempotent upsert on obs_date; full history back-filled from the FRED API. Partitioned by MONTH: DFF since 1954 (>26000 days) exceeds BigQuery's 10000-partitions-per-table limit at daily granularity.");

-- Register FRED DFF as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 9 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (9, 'FRED Fed Funds (DFF)', NULL, TRUE, 'https://api.stlouisfed.org/fred/series/observations', 'FRED / St. Louis Fed', CURRENT_TIMESTAMP());

-- 2-Year Treasury Constant Maturity yield (DGS2, daily, percent) from FRED. Same
-- plain (obs_date, obs_value) shape and shared fred_common.py logic as DGS10/DFF
-- (thin entry-point fred_2y_ingest.py). Ingested raw so the training views can
-- derive the 10Y-2Y term spread (DGS10 - DGS2) and its recent change/slope: the
-- spread level flags inversion, while its change captures the dis-inversion
-- (steepening-from-negative) transition the level alone cannot distinguish.
-- Not revised. Non-publication days (FRED `.`) are dropped, not stored as NULL.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.fred_dgs2_daily_raw` (
  obs_date        DATE      NOT NULL OPTIONS(description = "Observation date as delivered by FRED (field `date`). Business key and partition column."),
  obs_value       FLOAT64   OPTIONS(description = "Series value: 2-Year Treasury Constant Maturity yield (DGS2), percent."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (obs_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(obs_date, MONTH)
OPTIONS(description = "Raw daily 2-Year Treasury yield (DGS2) from FRED. Idempotent upsert on obs_date; full history back-filled from the FRED API. Partitioned by MONTH: DGS2 since 1976 (>12000 business days) exceeds BigQuery's 10000-partitions-per-table limit at daily granularity.");

-- Register FRED DGS2 as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 10 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (10, 'FRED 2Y Treasury (DGS2)', NULL, TRUE, 'https://api.stlouisfed.org/fred/series/observations', 'FRED / St. Louis Fed', CURRENT_TIMESTAMP());

-- CBOE Volatility Index (VIX, VIXCLS, daily close, index points) from FRED. The
-- equity-market "fear gauge"; a macro risk-appetite feature. Not revised: same
-- plain (obs_date, obs_value) shape and shared fred_common.py logic as
-- DGS10/DGS2/DFF (thin entry-point fred_vix_ingest.py). Non-publication days
-- (FRED `.`) are dropped, not stored as NULL.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.fred_vixcls_daily_raw` (
  obs_date        DATE      NOT NULL OPTIONS(description = "Observation date as delivered by FRED (field `date`). Business key and partition column."),
  obs_value       FLOAT64   OPTIONS(description = "Series value: CBOE Volatility Index close (VIXCLS), index points."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (obs_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(obs_date, MONTH)
OPTIONS(description = "Raw daily CBOE Volatility Index (VIXCLS) from FRED. Idempotent upsert on obs_date; full history (since 1990) back-filled from the FRED API. Partitioned by MONTH (consistent with the other long daily FRED series).");

-- Register FRED VIXCLS as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 11 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (11, 'FRED VIX (VIXCLS)', NULL, TRUE, 'https://api.stlouisfed.org/fred/series/observations', 'FRED / St. Louis Fed', CURRENT_TIMESTAMP());

-- BTC circulating supply (Coin Metrics community API, metric SplyCur, daily) from
-- the free community endpoint (no key). Like MVRV it is NOT candle data and does
-- NOT feed `conform`: one value per UTC date, its own bronze table, partitioned by
-- its date, idempotent MERGE on `supply_date`. It is the on-chain input the daily
-- training view turns into halving-cycle features: with the halving epoch fixed by
-- date (epoch boundaries are known historical events) and a constant block subsidy
-- within an epoch, circulating supply recovers the block-count fraction of the
-- cycle (cycle_phase) and the annualised issuance rate. Full history back-filled
-- once, then refreshed daily.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.coinmetrics_btc_supply_daily_raw` (
  supply_date     DATE      NOT NULL OPTIONS(description = "Metric date (UTC); Coin Metrics `time`. Business key and partition column."),
  circ_supply     FLOAT64   OPTIONS(description = "BTC circulating supply (Coin Metrics SplyCur), in BTC. NULL when the source delivers no value for the date."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (supply_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY supply_date
OPTIONS(description = "Raw daily BTC circulating supply (Coin Metrics SplyCur) from the free community API. Idempotent daily upsert on supply_date; full history (since 2010) back-filled from the same endpoint.");

-- Register Coin Metrics (BTC supply) as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 12 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (12, 'Coin Metrics BTC supply (SplyCur)', NULL, TRUE, 'https://community-api.coinmetrics.io/v4/timeseries/asset-metrics', 'Coin Metrics (community)', CURRENT_TIMESTAMP());


-- BTC active addresses (Coin Metrics community API, metric AdrActCnt, daily) from
-- the same free endpoint as supply (no key). On-chain network-activity factor; like
-- MVRV/supply it is NOT candle data and does NOT feed `conform`: one value per UTC
-- date, its own bronze table, partitioned by its date, idempotent MERGE on
-- `metric_date`. The daily training view turns the raw count into a stationary
-- year-over-year log-growth feature. Full history back-filled once, refreshed daily.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.coinmetrics_btc_active_addresses_daily_raw` (
  metric_date      DATE      NOT NULL OPTIONS(description = "Metric date (UTC); Coin Metrics `time`. Business key and partition column."),
  active_addresses INT64     OPTIONS(description = "Count of distinct active on-chain addresses (Coin Metrics AdrActCnt). NULL when the source delivers no value for the date."),
  source_id        INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update  TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (metric_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY metric_date
OPTIONS(description = "Raw daily BTC active-address count (Coin Metrics AdrActCnt) from the free community API. Idempotent daily upsert on metric_date; full history back-filled from the same endpoint.");

-- Register Coin Metrics (BTC active addresses) as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 13 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (13, 'Coin Metrics BTC active addresses (AdrActCnt)', NULL, TRUE, 'https://community-api.coinmetrics.io/v4/timeseries/asset-metrics', 'Coin Metrics (community)', CURRENT_TIMESTAMP());


-- BTC transaction count (Coin Metrics community API, metric TxCnt, daily) from the
-- same free endpoint (no key). On-chain network-activity factor; same model as the
-- active-address table: one value per UTC date, its own bronze table, partitioned by
-- `metric_date`, idempotent MERGE, NOT part of `conform`. The daily training view
-- turns the raw count into a stationary year-over-year log-growth feature.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.coinmetrics_btc_tx_count_daily_raw` (
  metric_date     DATE      NOT NULL OPTIONS(description = "Metric date (UTC); Coin Metrics `time`. Business key and partition column."),
  tx_count        INT64     OPTIONS(description = "Count of confirmed on-chain transactions (Coin Metrics TxCnt). NULL when the source delivers no value for the date."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (metric_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY metric_date
OPTIONS(description = "Raw daily BTC on-chain transaction count (Coin Metrics TxCnt) from the free community API. Idempotent daily upsert on metric_date; full history back-filled from the same endpoint.");

-- Register Coin Metrics (BTC transaction count) as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 14 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (14, 'Coin Metrics BTC transaction count (TxCnt)', NULL, TRUE, 'https://community-api.coinmetrics.io/v4/timeseries/asset-metrics', 'Coin Metrics (community)', CURRENT_TIMESTAMP());


-- BTC investor attention (Google Trends, weekly search interest for "Bitcoin"). NOT
-- candle data and NOT part of `conform`. Bronze stores the value EXACTLY as Google
-- delivers it for each request window (medallion: bronze is raw, no transforms): a
-- weekly point can appear under several `window_start`s (the overlapping requests
-- the back-fill issues), each its own raw 0-100 normalised within that window. The
-- continuous, stitched, re-normalised series is built downstream by the silver view
-- `vw_google_trends_btc_weekly` (NOT here). WEEKLY on purpose: Google only serves a
-- continuous daily series for short windows and re-normalises the 0-100 scale per
-- request, so windows cannot be concatenated without re-scaling (done in silver).
-- `trend_date` is the week-start (Sunday) the source labels each weekly point with.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.google_trends_btc_weekly_raw` (
  trend_date      DATE      NOT NULL OPTIONS(description = "Week-start date (Sunday) Google Trends labels the weekly point with. Partition column; part of the business key."),
  search_term     STRING    NOT NULL OPTIONS(description = "Google Trends query term (e.g. 'Bitcoin'). Part of the business key so several terms can share the table."),
  window_start    DATE      NOT NULL OPTIONS(description = "Start date of the request window this raw value came from. Part of the business key: the same week appears once per overlapping window, each on its own per-request 0-100 scale."),
  window_end      DATE      OPTIONS(description = "End date of the request window this raw value came from (audit/metadata)."),
  interest_raw    INT64     OPTIONS(description = "Weekly search-interest index (0-100) EXACTLY as Google delivered it for this request window (no stitching/re-scaling). NULL when the source delivers no value for the week."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (search_term, window_start, trend_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY trend_date
OPTIONS(description = "Raw weekly BTC search-interest index (0-100) from Google Trends, AS DELIVERED per request window (one row per window_start x trend_date; no stitching). The stitched continuous series is the silver view vw_google_trends_btc_weekly. Idempotent upsert on (search_term, window_start, trend_date).");

-- Register Google Trends (BTC investor attention) as a source. Idempotent seed. priority NULL.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 15 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (15, 'Google Trends BTC investor attention (weekly)', NULL, TRUE, 'https://trends.google.com/trends', 'Google Trends', CURRENT_TIMESTAMP());


-- ---------------------------------------------------------------------
-- Multi-asset ETF sources (Strategy 3 · cross-asset TSMOM, T-20)
-- Daily OHLC bars for the eight non-crypto ETFs of the frozen T-19 universe
-- (SPY/EFA · IEF/TLT · GLD/DBC · UUP/FXY). Unlike the macro/on-chain context
-- series (single-source, priority NULL), these have TWO competing sources —
-- Yahoo Finance (primary) and Tiingo (fallback) — that compete by `priority` in
-- the silver consolidation (T-21), so a source that starts gapping fails over to
-- the other. Both tables share the candle shape and the idempotent MERGE on the
-- natural key (symbol, candle_date); one table per source so bronze stays raw
-- (the Yahoo↔Tiingo de-dup is the downstream silver transform, NOT baked in here).
-- They do NOT feed the OHLCV `conform` consolidation (that is BTC-only). BTC
-- reuses the existing spot bronze (binance/bitstamp), so it has no ETF table.
-- Partitioned by MONTH (DATE_TRUNC(candle_date, MONTH)), not day: SPY's history
-- since 1993 across several symbols would approach the 10000-partitions-per-table
-- limit at daily granularity. Clustered by symbol (the column reads filter on).
-- ---------------------------------------------------------------------

-- ETF bars from Yahoo Finance (primary source). One row per (symbol, candle_date),
-- as delivered by the public chart API. Full history back-filled once, then the
-- daily job refreshes a recent window; idempotent upsert on (symbol, candle_date).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.yahoo_etf_daily_raw` (
  symbol          STRING    NOT NULL OPTIONS(description = "Canonical instrument symbol (e.g. SPY), not the provider ticker. Business key and cluster column."),
  candle_date     DATE      NOT NULL OPTIONS(description = "Trading-day date (UTC) of the daily bar, from the Yahoo chart timestamp. Business key and partition column."),
  price_open      FLOAT64   OPTIONS(description = "Open price."),
  price_high      FLOAT64   OPTIONS(description = "High price."),
  price_low       FLOAT64   OPTIONS(description = "Low price."),
  price_close     FLOAT64   OPTIONS(description = "Close price as delivered by Yahoo (indicators.quote[0].close): split-adjusted but NOT dividend-adjusted. The silver consolidation reconciles Tiingo's raw close to this split-only basis (T-21)."),
  volume_traded   FLOAT64   OPTIONS(description = "Traded volume as delivered by Yahoo."),
  split_factor    FLOAT64   OPTIONS(description = "Corporate-action split factor on the bar's ex-date (1.0 = none). Always NULL for Yahoo: its quote.close is already split-adjusted, so no reconstruction is needed. Column exists for schema parity with the shared bronze loader."),
  div_cash        FLOAT64   OPTIONS(description = "Cash dividend per share on the bar's ex-date (0 = none). NULL for Yahoo (chart quote does not carry it). Schema parity with the shared loader."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (symbol, candle_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(candle_date, MONTH)
CLUSTER BY symbol
OPTIONS(description = "Raw daily ETF bars from Yahoo Finance (Strategy 3 universe), primary source. Idempotent upsert on (symbol, candle_date); competes with Tiingo by priority in the silver consolidation. Partitioned by MONTH (10000-partitions-per-table limit).");

-- Register Yahoo Finance (ETFs) as a source. Idempotent seed. priority 2:
-- preferred over Tiingo (1) when both cover the same ETF/date in the silver
-- consolidation. (These compete only for the ETF tables, never in `conform`.)
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 16 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (16, 'Yahoo Finance ETFs (Strategy 3 universe)', 2, TRUE, 'https://query1.finance.yahoo.com/v8/finance/chart', 'Yahoo Finance', CURRENT_TIMESTAMP());

-- ETF bars from Tiingo (fallback source). Identical shape and upsert as the Yahoo
-- table; populated so the consolidation can fail over when Yahoo gaps.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.tiingo_etf_daily_raw` (
  symbol          STRING    NOT NULL OPTIONS(description = "Canonical instrument symbol (e.g. SPY), not the provider ticker. Business key and cluster column."),
  candle_date     DATE      NOT NULL OPTIONS(description = "Trading-day date of the daily bar, from the Tiingo EOD response. Business key and partition column."),
  price_open      FLOAT64   OPTIONS(description = "Open price (raw, as delivered by Tiingo)."),
  price_high      FLOAT64   OPTIONS(description = "High price (raw, as delivered by Tiingo)."),
  price_low       FLOAT64   OPTIONS(description = "Low price (raw, as delivered by Tiingo)."),
  price_close     FLOAT64   OPTIONS(description = "Close price (raw, unadjusted — neither split nor dividend adjusted, as delivered by Tiingo). The silver consolidation rebuilds a split-only close from split_factor so it matches Yahoo's split-adjusted basis (T-21)."),
  volume_traded   FLOAT64   OPTIONS(description = "Traded volume as delivered by Tiingo."),
  split_factor    FLOAT64   OPTIONS(description = "Tiingo splitFactor on the bar's ex-date (1.0 = no split, e.g. 3.0 for a 3:1). Raw provider field; the silver step back-adjusts the raw close by the product of split_factors with ex_date > D to reconstruct a split-only-adjusted level (T-21)."),
  div_cash        FLOAT64   OPTIONS(description = "Tiingo divCash: cash dividend per share on the bar's ex-date (0 = none). Raw provider field; NOT applied to the stored close (the series is price-return, not total-return). Kept for auditability / a possible future total-return basis."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (symbol, candle_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(candle_date, MONTH)
CLUSTER BY symbol
OPTIONS(description = "Raw daily ETF bars from Tiingo (Strategy 3 universe), fallback source. Idempotent upsert on (symbol, candle_date); competes with Yahoo by priority in the silver consolidation. Partitioned by MONTH (10000-partitions-per-table limit).");

-- Register Tiingo (ETFs) as a source. Idempotent seed. priority 1: fallback,
-- below Yahoo (2) in the ETF consolidation tie-break. is_active = TRUE: Tiingo is
-- a token-authenticated API (free TIINGO_API_KEY), so it is reachable from the
-- dev host, the VM and CI — it replaced stooq (whose keyless CSV a JavaScript
-- proof-of-work bot challenge made unreachable from cloud IPs, 2026-06-25). The
-- dual-source design and the T-21 failover logic are unchanged.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 17 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (17, 'Tiingo ETFs (Strategy 3 universe)', 1, TRUE, 'https://api.tiingo.com/tiingo/daily', 'Tiingo', CURRENT_TIMESTAMP());


-- ---------------------------------------------------------------------
-- prod_trade_silver (Option B: symbol and temporality as columns)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_silver.ohlcv_validated` (
  symbol            STRING    NOT NULL OPTIONS(description = "Asset (e.g. BTCUSD)."),
  temporality       STRING    NOT NULL OPTIONS(description = "Timeframe: 1d / 1h / 1w."),
  source_id         INT64     OPTIONS(description = "Lineage: source of the winning row. FK to prod_trade_control.source."),
  datetime_update   TIMESTAMP OPTIONS(description = "Execution timestamp of the conforming process (audit)."),
  time_period_start TIMESTAMP NOT NULL OPTIONS(description = "Period start (UTC). Temporal key and partition column."),
  time_period_end   TIMESTAMP OPTIONS(description = "Period end (UTC)."),
  time_open         TIMESTAMP OPTIONS(description = "Timestamp of first trade (UTC)."),
  time_close        TIMESTAMP OPTIONS(description = "Timestamp of last trade (UTC)."),
  price_open        FLOAT64   OPTIONS(description = "Open price."),
  price_high        FLOAT64   OPTIONS(description = "High price."),
  price_low         FLOAT64   OPTIONS(description = "Low price."),
  price_close       FLOAT64   OPTIONS(description = "Close price."),
  volume_traded     FLOAT64   OPTIONS(description = "Traded volume."),
  trades_count      INT64     OPTIONS(description = "Number of trades."),
  PRIMARY KEY (symbol, temporality, time_period_start) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY TIMESTAMP_TRUNC(time_period_start, MONTH)
CLUSTER BY symbol, temporality
OPTIONS(description = "Conformed, typed and de-duplicated multi-asset OHLCV (one row per symbol/temporality/period), chosen by source priority. Partitioned by MONTH: the Strategy-3 ETF history reaches 1993 (SPY), so daily-grained partitions over 33+ years would exceed BigQuery's 10000-partitions-per-table cap (DAY tops out ~27 yr). Changing granularity needs DROP + recreate.");

CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_silver.rsi_features` (
  symbol            STRING    NOT NULL OPTIONS(description = "Asset."),
  temporality       STRING    NOT NULL OPTIONS(description = "Timeframe: 1d / 1h / 1w."),
  rsi_period        INT64     NOT NULL OPTIONS(description = "RSI period used to compute the row."),
  datetime_update   TIMESTAMP OPTIONS(description = "Execution timestamp of the computation (audit)."),
  time_period_start TIMESTAMP NOT NULL OPTIONS(description = "Temporal key of the value (UTC)."),
  price_close       FLOAT64   OPTIONS(description = "Close price used in the computation."),
  var_p_recursive   FLOAT64   OPTIONS(description = "Wilder smoothed average gain (recursive state)."),
  var_n_recursive   FLOAT64   OPTIONS(description = "Wilder smoothed average loss (recursive state)."),
  rsi               FLOAT64   OPTIONS(description = "RSI value."),
  PRIMARY KEY (symbol, temporality, rsi_period, time_period_start) NOT ENFORCED
)
PARTITION BY DATE(time_period_start)
CLUSTER BY symbol, temporality
OPTIONS(description = "RSI feature computed with Wilder smoothing; recursive intermediate state stored for incremental, idempotent updates. Reusable across strategies.");


-- ---------------------------------------------------------------------
-- prod_trade_silver — Google Trends stitched weekly series (the transform that
-- USED to be baked into the ingest now lives here, downstream of raw bronze).
-- Google re-normalises its 0-100 per request, so the overlapping back-fill windows
-- in bronze are not directly comparable. This view STITCHES them into one continuous
-- series: order the windows, re-scale each onto the previous one by the ratio of
-- their shared (overlap) weeks, assign every week to its EARLIEST window (so the
-- earliest scale anchors overlaps), then re-normalise the whole series to 0-100.
-- This is a pure, deterministic function of the raw bronze rows (re-run any time).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `trade-390514.prod_trade_silver.vw_google_trends_btc_weekly`
OPTIONS(description = "Stitched, continuous weekly Google Trends interest per search_term (0-100, re-normalised to the all-time max). Built from bronze.google_trends_btc_weekly_raw by re-scaling the overlapping per-request windows on their shared weeks (earliest window anchors overlaps). trend_date is the week-start Sunday. This is where the stitching transform lives — bronze stays raw.")
AS
WITH raw AS (
  SELECT search_term, window_start, trend_date, interest_raw
  FROM `trade-390514.prod_trade_bronze.google_trends_btc_weekly_raw`
  WHERE interest_raw IS NOT NULL
),
-- Number the request windows per term (1 = earliest).
win AS (
  SELECT search_term, window_start,
         DENSE_RANK() OVER (PARTITION BY search_term ORDER BY window_start) AS wi
  FROM raw GROUP BY search_term, window_start
),
-- Pairwise re-scale factor mapping each window onto the PREVIOUS one, computed over
-- the weeks the two share: factor = sum(prev.raw) / sum(cur.raw) on the overlap.
pairs AS (
  SELECT c.search_term, c.window_start AS cur_ws,
         SAFE_DIVIDE(SUM(p.interest_raw), SUM(c.interest_raw)) AS factor
  FROM raw c
  JOIN win wc ON wc.search_term = c.search_term AND wc.window_start = c.window_start
  JOIN win wp ON wp.search_term = c.search_term AND wp.wi = wc.wi - 1
  JOIN raw p  ON p.search_term  = c.search_term AND p.window_start = wp.window_start
             AND p.trend_date   = c.trend_date
  GROUP BY c.search_term, c.window_start
),
-- Cumulative scale per window = running product of factors (window 1 = 1.0). The
-- product is done in log space so it composes as a window SUM.
cum AS (
  SELECT w.search_term, w.window_start, w.wi,
         EXP(SUM(LN(COALESCE(pairs.factor, 1.0))) OVER (
             PARTITION BY w.search_term ORDER BY w.wi
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS scale
  FROM win w
  LEFT JOIN pairs ON pairs.search_term = w.search_term AND pairs.cur_ws = w.window_start
),
-- Each week keeps the value from its EARLIEST window (anchors overlaps).
assigned AS (
  SELECT search_term, trend_date, interest_raw, window_start,
         ROW_NUMBER() OVER (PARTITION BY search_term, trend_date ORDER BY window_start) AS rn
  FROM raw
),
scaled AS (
  SELECT a.search_term, a.trend_date, a.interest_raw * c.scale AS v
  FROM assigned a
  JOIN cum c ON c.search_term = a.search_term AND c.window_start = a.window_start
  WHERE a.rn = 1
)
-- Re-normalise the stitched series to a 0-100 index (max of all time = 100).
SELECT search_term, trend_date,
       100 * SAFE_DIVIDE(v, MAX(v) OVER (PARTITION BY search_term)) AS interest
FROM scaled;


-- ---------------------------------------------------------------------
-- prod_trade_silver — multi-asset weekly returns / excess returns / realized vol
-- (Strategy 3 TSMOM feature layer, T-21). Pure view over silver.ohlcv_validated
-- (temporality='1w', one row per symbol/week, Monday→Sunday) joined point-in-time
-- to the risk-free rate and to Tiingo's cash dividends. Excess return uses FRED
-- DFF (effective fed funds, NOT revised → as-of obs_date, no vintage). The level
-- is split-adjusted TOTAL-return: the price is split-adjusted (conform reconciles
-- Tiingo to Yahoo's split-only basis) and reinvested cash dividends are added back
-- (T-26b). Annualisation is a uniform ×√52 across all assets (sidesteps the
-- 252-vs-365 trading-calendar split between ETFs and BTC). T-22 (TSMOM) cumulates
-- the weekly excess returns over its formation horizon and scales by
-- realized_vol_26w. price_* columns keep the dividend-free price-return for audit.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `trade-390514.prod_trade_silver.vw_asset_returns_weekly`
OPTIONS(description = "Weekly (Monday→Sunday) per-symbol TOTAL returns for the Strategy-3 universe — the TSMOM feature layer (T-21, dividends added T-26b). Columns: symbol, week_start (the week's Monday), price_close (split-adjusted), div_adj (split-adjusted cash dividends with ex-date in the week; 0 when none — BTC always, ETFs when Tiingo has none), price_simple_return / price_log_return (dividend-FREE price-return, kept for audit / bias comparison), simple_return ((P_t+D_t)/P_{t-1}-1, total-return), log_return (ln((P_t+D_t)/P_{t-1})), dff_annual_pct (effective fed funds %, FRED DFF point-in-time: latest obs on/before week_start; DFF is not revised so no vintage), rf_week ((1+DFF/100)^(7/365)-1), excess_return (simple_return - rf_week), excess_log_return (log_return - ln(1+rf_week)), realized_vol_26w (stddev of the last 26 weekly total-return log-returns × √52; NULL during the 26-week warm-up, same NULL contract as the RSI). Annualisation is a uniform ×√52 across all assets. Dividends come from bronze.tiingo_etf_daily_raw (the only source carrying divCash; Yahoo has none), split-adjusted with the SAME factor conform applies to the Tiingo close (T-21) so dividend and price share one split basis. Built on silver.ohlcv_validated (1w) + bronze.fred_dff_daily_raw + bronze.tiingo_etf_daily_raw.")
AS
WITH wk AS (
  SELECT symbol, DATE(time_period_start) AS week_start, price_close
  FROM `trade-390514.prod_trade_silver.ohlcv_validated`
  WHERE temporality = '1w' AND price_close IS NOT NULL
),
-- Cash dividends, split-adjusted to the same basis as the silver price. Only
-- Tiingo carries divCash (Yahoo's chart quote does not), so dividends are sourced
-- from Tiingo bronze even on weeks whose price came from Yahoo. Each ex-date's raw
-- div_cash is divided by f = product of split_factors whose ex-date is AFTER it —
-- the SAME back-adjustment conform applies to the Tiingo close (T-21) — so both
-- share one split-adjusted basis. div_daily is per ex-date; div_wk buckets it into
-- the Monday→Sunday week of the ex-date.
div_daily AS (
  SELECT
    t.symbol, t.candle_date,
    t.div_cash / IFNULL(EXP((
      SELECT SUM(LN(s.split_factor))
      FROM `trade-390514.prod_trade_bronze.tiingo_etf_daily_raw` AS s
      WHERE s.symbol = t.symbol AND s.candle_date > t.candle_date
        AND s.split_factor IS NOT NULL AND s.split_factor != 1.0
    )), 1.0) AS div_adj
  FROM `trade-390514.prod_trade_bronze.tiingo_etf_daily_raw` AS t
  WHERE t.div_cash IS NOT NULL AND t.div_cash > 0
),
div_wk AS (
  SELECT symbol, DATE_TRUNC(candle_date, WEEK(MONDAY)) AS week_start, SUM(div_adj) AS div_adj
  FROM div_daily
  GROUP BY symbol, week_start
),
rets AS (
  SELECT
    w.symbol, w.week_start, w.price_close,
    IFNULL(d.div_adj, 0)             AS div_adj,
    LAG(w.price_close) OVER win      AS prev_close,
    ROW_NUMBER() OVER win - 1        AS i
  FROM wk w
  LEFT JOIN div_wk d USING (symbol, week_start)
  WINDOW win AS (PARTITION BY w.symbol ORDER BY w.week_start)
),
tr AS (
  SELECT
    symbol, week_start, price_close, div_adj, i,
    -- Dividend-free price-return (audit / bias comparison against total-return).
    SAFE_DIVIDE(price_close, prev_close) - 1              AS price_simple_return,
    LN(SAFE_DIVIDE(price_close, prev_close))              AS price_log_return,
    -- Total-return: reinvest the week's dividend at the prior week's close.
    SAFE_DIVIDE(price_close + div_adj, prev_close) - 1    AS simple_return,
    LN(SAFE_DIVIDE(price_close + div_adj, prev_close))    AS log_return
  FROM rets
),
-- Risk-free point-in-time: the latest DFF observation on/before the week's Monday.
-- DFF is not revised, so the as-of obs_date IS the vintage. The 1990-01-01 floor
-- only bounds the scan (earliest ETF is SPY 1993); DFF history starts 1954.
rf AS (
  SELECT t.symbol, t.week_start, d.obs_value AS dff_annual_pct
  FROM tr t
  LEFT JOIN `trade-390514.prod_trade_bronze.fred_dff_daily_raw` d
    ON d.obs_date <= t.week_start AND d.obs_date >= '1990-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.symbol, t.week_start ORDER BY d.obs_date DESC) = 1
)
SELECT
  t.symbol, t.week_start, t.price_close, t.div_adj,
  t.price_simple_return, t.price_log_return,
  t.simple_return, t.log_return,
  rf.dff_annual_pct,
  POW(1 + rf.dff_annual_pct / 100, 7 / 365) - 1                                  AS rf_week,
  t.simple_return - (POW(1 + rf.dff_annual_pct / 100, 7 / 365) - 1)              AS excess_return,
  t.log_return    - LN(POW(1 + rf.dff_annual_pct / 100, 7 / 365))               AS excess_log_return,
  IF(t.i >= 26,
     STDDEV_SAMP(t.log_return) OVER (
       PARTITION BY t.symbol ORDER BY t.week_start ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
     ) * SQRT(52),
     NULL)                                                                       AS realized_vol_26w
FROM tr t
LEFT JOIN rf USING (symbol, week_start);


-- ---------------------------------------------------------------------
-- prod_trade_gold (star schema)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_gold.fact_signals` (
  symbol            STRING    NOT NULL OPTIONS(description = "Asset."),
  temporality       STRING    NOT NULL OPTIONS(description = "Timeframe: 1d / 1h / 1w."),
  strategy_id       INT64     NOT NULL OPTIONS(description = "FK to prod_trade_strategy.strategy."),
  signal            STRING    OPTIONS(description = "Discrete signal: BUY / SELL / NEUTRAL (enforced by convention)."),
  trigger_params    JSON      OPTIONS(description = "Strategy-agnostic parameter:value dictionary that triggered the signal change (e.g. {\"rsi\":29.8,\"oversold\":30}). Allows non-RSI strategies to record their own trigger inputs."),
  signal_start TIMESTAMP NOT NULL OPTIONS(description = "Period the signal corresponds to (UTC)."),
  signal_created_at       TIMESTAMP OPTIONS(description = "When the signal was computed."),
  PRIMARY KEY (symbol, temporality, signal_start, strategy_id) NOT ENFORCED,
  FOREIGN KEY (strategy_id) REFERENCES `trade-390514.prod_trade_strategy.strategy`(strategy_id) NOT ENFORCED
)
PARTITION BY DATE(signal_start)
CLUSTER BY symbol, strategy_id
OPTIONS(description = "Trading signals fact table: one row per symbol/temporality/period/strategy.");


-- ---------------------------------------------------------------------
-- prod_trade_strategy — versioned RSI parameters (T-06 / T-07)
-- One table per strategy; name matches strategy.strategy_name.
-- ---------------------------------------------------------------------
-- Strategy: combines weekly RSI (trend filter) + daily RSI (entry/exit signal).
-- weekly_rsi_trend_start / _end define the bullish window (same convention as
-- the AG notebook: trend is active while weekly RSI sits between these bounds).
-- daily_rsi_oversold / _overbought are the entry/exit thresholds for daily RSI.
-- The four parameters are calibrated offline by the genetic algorithm (AG).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_strategy.strategy_rsi_daily_week` (
  param_version          INT64     NOT NULL OPTIONS(description = "Monotonically increasing version; latest active version is used."),
  is_active              BOOL      NOT NULL OPTIONS(description = "Only the active version is applied by the signal pipeline."),
  rsi_period             INT64     NOT NULL OPTIONS(description = "RSI lookback period (Wilder smoothing), applied to both daily and weekly."),
  weekly_rsi_trend_start FLOAT64   NOT NULL OPTIONS(description = "Weekly RSI lower bound of the bullish window (0-100). Trend is considered active when weekly RSI >= this value."),
  weekly_rsi_trend_end   FLOAT64   NOT NULL OPTIONS(description = "Weekly RSI upper bound. Trend ends when weekly RSI exceeds this value."),
  daily_rsi_oversold     FLOAT64   NOT NULL OPTIONS(description = "Daily RSI strictly below this value triggers BUY (within the trend window)."),
  daily_rsi_overbought   FLOAT64   NOT NULL OPTIONS(description = "Daily RSI strictly above this value triggers SELL (within or at end of trend window)."),
  created_at             TIMESTAMP OPTIONS(description = "Row creation timestamp."),
  notes                  STRING    OPTIONS(description = "Rationale or source for these parameters."),
  PRIMARY KEY (param_version) NOT ENFORCED
)
OPTIONS(description = "Versioned parameters for the weekly-trend + daily-RSI strategy. Calibrated by the AG notebook; decoupled from the daily signal pipeline.");

-- Register strategy id=1 in the catalog (idempotent).
MERGE `trade-390514.prod_trade_strategy.strategy` T
USING (SELECT 1 AS strategy_id) S
ON T.strategy_id = S.strategy_id
WHEN NOT MATCHED THEN INSERT (
  strategy_id, strategy_name, description, indicator_type, is_active, created_at
) VALUES (
  1,
  'strategy_rsi_daily_week',
  'Bullish-trend filter (weekly RSI) + entry/exit thresholds (daily RSI). Parameters calibrated by a genetic algorithm.',
  'RSI',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Seed default parameters v1 (idempotent).
-- Baseline values (replace with AG-calibrated output once the notebook is run).
MERGE `trade-390514.prod_trade_strategy.strategy_rsi_daily_week` T
USING (SELECT 1 AS param_version) S
ON T.param_version = S.param_version
WHEN NOT MATCHED THEN INSERT (
  param_version, is_active, rsi_period,
  weekly_rsi_trend_start, weekly_rsi_trend_end,
  daily_rsi_oversold, daily_rsi_overbought,
  created_at, notes
) VALUES (
  1, TRUE, 14,
  40.0, 70.0,
  30.0, 70.0,
  CURRENT_TIMESTAMP(),
  'Baseline seed — replace with AG-optimised values from ag-determina-parametros-de-estrategia-rsi.ipynb.'
);


-- ---------------------------------------------------------------------
-- prod_trade_strategy — versioned TSMOM parameters (T-24)
-- One table per strategy; name matches strategy.strategy_name.
-- This table is SINGLE-STRATEGY by design (mirroring strategy_rsi_daily_week):
-- it has no strategy_id column because the table itself is the scope.
-- Active-version invariant: exactly one is_active=TRUE row at all times,
-- enforced by an atomic flip on promotion (not a read-side LIMIT 1):
--   UPDATE strategy_tsmom_multiasset SET is_active=(param_version=@v) WHERE TRUE;
-- ORDER BY param_version DESC LIMIT 1 is kept as secondary defence only.
-- See strategy_3_analysis.md "Modeling lifecycle" for the promotion pattern
-- Epic 8 (T-25 backtest engine, T-26 baselines, T-27 holdout) inherits.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_strategy.strategy_tsmom_multiasset` (
  param_version     INT64     NOT NULL OPTIONS(description = "Monotonically increasing version; exactly one is_active=TRUE row at a time (enforced by atomic flip on promotion)."),
  is_active         BOOL      NOT NULL OPTIONS(description = "Only the active version is applied by the signal and portfolio-weights stages."),
  formation_horizon INT64     NOT NULL OPTIONS(description = "Trailing weekly periods L summed into the formation excess return (T-22). A counted trial."),
  vol_target        FLOAT64   NOT NULL OPTIONS(description = "Annualised target volatility the per-instrument position is scaled to (T-22). A counted trial."),
  vol_lookback      INT64     NOT NULL OPTIONS(description = "Periods behind realized_vol_26w in the T-21 view; recorded for full trial description."),
  periods_per_year  INT64     NOT NULL OPTIONS(description = "Annualisation factor of the cadence (52 for weekly)."),
  max_leverage      FLOAT64   OPTIONS(description = "Optional cap on the vol-scaling factor; NULL leaves it uncapped."),
  scheme            STRING    NOT NULL OPTIONS(description = "Portfolio weighting scheme (T-23): equal_weight / inverse_vol / risk_parity."),
  crypto_cap        FLOAT64   OPTIONS(description = "Maximum gross weight of the BTC sleeve; NULL leaves it uncapped. A counted trial."),
  created_at        TIMESTAMP OPTIONS(description = "Row creation timestamp."),
  notes             STRING    OPTIONS(description = "Rationale or source for these parameters."),
  PRIMARY KEY (param_version) NOT ENFORCED
)
OPTIONS(description = "Versioned parameters for the cross-asset TSMOM strategy (T-22 signal + T-23 portfolio). Calibrated offline (Epic 8); decoupled from the gold materialisation stages.");

-- Register strategy id=3 in the catalog (idempotent).
MERGE `trade-390514.prod_trade_strategy.strategy` T
USING (SELECT 3 AS strategy_id) S
ON T.strategy_id = S.strategy_id
WHEN NOT MATCHED THEN INSERT (
  strategy_id, strategy_name, description, indicator_type, is_active, created_at
) VALUES (
  3,
  'strategy_tsmom_multiasset',
  'Cross-asset time-series momentum: sign of the cumulative excess return over a formation horizon, vol-scaled per instrument (T-22), combined into portfolio weights (T-23). Parameters calibrated offline by a grid search (Epic 8).',
  'TSMOM',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Seed baseline parameters v1 (idempotent).
-- Placeholder values — replace after Epic 8 calibration (T-25 backtest engine,
-- T-26 baselines, T-27 holdout single evaluation) selects the winning trial.
-- Promotion pattern (Epic 8): UPDATE … SET is_active=(param_version=@v) WHERE TRUE;
MERGE `trade-390514.prod_trade_strategy.strategy_tsmom_multiasset` T
USING (SELECT 1 AS param_version) S
ON T.param_version = S.param_version
WHEN NOT MATCHED THEN INSERT (
  param_version, is_active, formation_horizon, vol_target, vol_lookback,
  periods_per_year, max_leverage, scheme, crypto_cap, created_at, notes
) VALUES (
  1, TRUE, 52, 0.10, 26,
  52, NULL, 'inverse_vol', 0.20, CURRENT_TIMESTAMP(),
  'Baseline seed — placeholder until Epic 8 (T-25 backtest engine, T-26 baselines, T-27 holdout) selects calibrated values via the trial grid.'
);


-- ---------------------------------------------------------------------
-- prod_trade_gold — portfolio weights (T-24, Strategy 3)
-- One row per rebalance week × instrument × book (with-crypto / without-crypto).
-- Stage B (dataflow/stages/portfolio_weights_stage.py) reads fact_signals
-- (strategy_id=3) and feeds T-23's build_portfolio; it is a gold→gold stage
-- called AFTER Stage A's MERGE commits (two sequential pipelines, not one graph).
-- Natural key: (week_start, strategy_id, symbol, include_crypto).
-- Partition by week_start (DATE, weekly cadence → ~1,700 weeks 1993-2026,
-- well under the 4,000/DML and 10,000/table caps — no chunking needed).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_gold.fact_portfolio_weights` (
  week_start      DATE      NOT NULL OPTIONS(description = "Rebalance week's Monday (matches vw_asset_returns_weekly.week_start and DATE(fact_signals.signal_start))."),
  strategy_id     INT64     NOT NULL OPTIONS(description = "FK to prod_trade_strategy.strategy (always 3 for TSMOM)."),
  symbol          STRING    NOT NULL OPTIONS(description = "Instrument symbol (SPY, EFA, IEF, TLT, GLD, DBC, UUP, FXY, BTC)."),
  include_crypto  BOOL      NOT NULL OPTIONS(description = "TRUE = with-BTC book, FALSE = without-BTC book (T-23 two-book output)."),
  scheme          STRING    NOT NULL OPTIONS(description = "Weighting scheme used (audit copy of the active param row at compute time)."),
  weight          FLOAT64   NOT NULL OPTIONS(description = "Gross-1 portfolio weight (signed). BTC leg additionally capped by crypto_cap in the with-crypto book."),
  param_version   INT64     NOT NULL OPTIONS(description = "FK to strategy_tsmom_multiasset.param_version that produced this row."),
  created_at      TIMESTAMP OPTIONS(description = "When the weight was computed by Stage B."),
  PRIMARY KEY (week_start, strategy_id, symbol, include_crypto) NOT ENFORCED,
  FOREIGN KEY (strategy_id) REFERENCES `trade-390514.prod_trade_strategy.strategy`(strategy_id) NOT ENFORCED
)
PARTITION BY week_start
CLUSTER BY strategy_id, include_crypto
OPTIONS(description = "Cross-asset TSMOM portfolio weights (T-24): one row per rebalance week / instrument / book variant. Produced by Stage B (portfolio_weights_stage.py) from Stage A's fact_signals rows for strategy_id=3.");


-- ---------------------------------------------------------------------
-- prod_trade_gold — consumption views (ML training sets)
-- Curated, read-only joins that line up BTC close, RSI, MVRV Z-Score and the
-- macro feature series (DXY, 10Y Treasury, Fed funds, M2) in one row for model
-- training. They are VIEWS on purpose (the dataset is tiny and a view stays
-- always-fresh with zero maintenance); freeze an experiment with
-- `CREATE TABLE <snapshot> AS SELECT * FROM <view>` or export to GCS.
-- Both filter rsi_period = 14 (the active strategy period) and drop the RSI
-- warm-up rows (rsi IS NULL). close_price and rsi already co-live in
-- silver.rsi_features; MVRV and the macro series are joined in from bronze.
--
-- Macro features are aligned POINT-IN-TIME with an as-of join, not an equi-join:
-- for each trading day D (the week's Sunday for the weekly view) each feature is
-- the latest value KNOWN on or before D — forward-filling weekends/holidays
-- (DXY/DGS10 don't quote every day) and never using a value from the future.
-- BigQuery can't de-correlate a scalar `ORDER BY/LIMIT` subquery over another
-- table, so the as-of is one CTE per series: a range-join (`<date> <= D`) trimmed
-- to the latest row with `QUALIFY ROW_NUMBER() OVER (PARTITION BY D ORDER BY
-- <date> DESC) = 1`. M2 (WM2NS) is the subtle one: it is revised and lagged, so
-- we pick its POINT-IN-TIME vintage — the row whose realtime window contains D
-- (`realtime_start <= D <= realtime_end`), latest observation week first. That is
-- exactly what the stored ALFRED vintages are for (no look-ahead).
--
-- MVRV 1-day publication lag: the source reports the metric of day D-1 under
-- mvrvz_date = D (e.g. the row dated 2026-06-10 is really the 2026-06-09 value).
-- So to recover the real metric date we shift the join by +1 day: a trading day
-- D pairs with mvrvz_date = D + 1 (whose real value is D).
-- ---------------------------------------------------------------------

-- Daily: MVRV (real date) of day D against the daily RSI row of day D. With the
-- 1-day lag, the real value of D lives under mvrvz_date = D + 1.
CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_daily`
OPTIONS(description = "Daily BTC model-feature set (stationary features only; raw levels used internally are NOT exposed — price/labels live in silver.rsi_features). Columns: date, daily RSI(14), weekly RSI(14) as-of, MVRV Z-Score, VIX, price_vs_ema365 (price/EMA365-1), dxy_vs_ema365 (DXY/EMA365-1), realized_vol_30d, m2_yoy_log (ln M2_t/M2_{t-52w}), m2_roc_13w_ann ((M2_t/M2_{t-13w})^(52/13)-1), teny_chg_30d (DGS10 30-day change), the 10Y-2Y spread (+1-month change +dis-inversion flag), and BTC halving-cycle features (cycle_phase + sin/cos + annualised issuance). MVRV is on the same calendar date (its source lags 1 day, join +1). Macro/weekly-RSI are point-in-time as-of the trading day (latest value known on/before D; M2 via the vintage current at D, incl. the -52w/-13w lookbacks; weekly RSI only for weeks already closed), so no look-ahead. The EMA(365) is an exact closed-form EMA; realized_vol_30d = stddev of the last 30 daily log-returns x sqrt(365); cycle_phase = block fraction through the current halving epoch (epoch fixed by date, supply from Coin Metrics); issuance_rate_ann = 52560 x block_subsidy / circulating_supply. On-chain activity (Coin Metrics): active_addresses_yoy_log and tx_count_yoy_log are the stationary year-over-year log growth of the raw daily counts (ln x_t/x_{t-1y}). investor_attention is the weekly Google Trends 0-100 search index, taken as-of the prior fully-closed week (no within-week look-ahead) and forward-filled. Read-only join of silver.rsi_features (1d/1w) with bronze MVRV, the macro tables, BTC supply, the Coin Metrics on-chain tables and Google Trends.")
AS
WITH btc AS (
  SELECT DATE(r.time_period_start) AS d, r.price_close, r.rsi
  FROM `trade-390514.prod_trade_silver.rsi_features` AS r
  WHERE r.symbol = 'BTCUSD' AND r.temporality = '1d'
    AND r.rsi_period = 14 AND r.rsi IS NOT NULL          -- drop the 14-day warm-up
),
-- Full daily close series (incl. warm-up) for the recursive/windowed features
-- (EMA365, realised vol). i is the 0-based row index used by the EMA closed form.
px AS (
  SELECT
    DATE(r.time_period_start) AS d,
    r.price_close,
    ROW_NUMBER() OVER (ORDER BY r.time_period_start) - 1 AS i
  FROM `trade-390514.prod_trade_silver.rsi_features` AS r
  WHERE r.symbol = 'BTCUSD' AND r.temporality = '1d' AND r.rsi_period = 14
),
px_calc AS (
  SELECT
    d,
    -- Realised volatility: stddev of the last 30 daily LOG-returns, annualised
    -- x sqrt(365). Log-returns r_t = ln(P_t / P_{t-1}) (additive, symmetric).
    -- WARM-UP: NULL until the full 30-return window exists (i >= 30) — a stddev of
    -- 2-3 returns is noise; same NULL-during-warm-up policy as the RSI.
    IF(i >= 30,
       STDDEV_SAMP(logret) OVER (ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
         * SQRT(365),
       NULL) AS realized_vol_30d,
    -- EMA(365) of close, EXACT recursive EMA in a single pass via its closed form.
    -- With alpha = 2/366, beta = 1-alpha and L = ln(beta): EMA_j = beta^j * (V_j +
    -- beta*P_0) where V_j = alpha * sum_{m<=j} beta^{-m} P_m. beta^{-m} = exp(-m L)
    -- (L<0) is large for old rows but beta^j = exp(j L) rescales it back; float64
    -- holds the range comfortably for BTC's history.
    -- WARM-UP: NULL for the first 365 rows (i < 365) — a 365-day EMA is not an
    -- annual trend yet, so it is suppressed like the RSI warm-up.
    IF(i >= 365,
       EXP(i * LN(1 - 2/366)) * (
         (2/366) * SUM(EXP(-i * LN(1 - 2/366)) * price_close) OVER (ORDER BY d)
         + (1 - 2/366) * FIRST_VALUE(price_close) OVER (ORDER BY d)
       ),
       NULL) AS ema365
  FROM (
    SELECT d, i, price_close,
           LN(price_close / LAG(price_close) OVER (ORDER BY d)) AS logret
    FROM px
  )
),
-- DXY with its own EMA(365) (same exact closed form as the price EMA), computed
-- over the full DXY series so the average is mature by the time BTC history starts.
dxy_calc AS (
  SELECT
    dxy_date,
    price_close AS dxy,
    -- WARM-UP: NULL for the first 365 DXY bars (i < 365), same policy as the price
    -- EMA. Moot in practice (DXY history starts 1971, BTC in 2011) but consistent.
    IF(i >= 365,
       EXP(i * LN(1 - 2/366)) * (
         (2/366) * SUM(EXP(-i * LN(1 - 2/366)) * price_close) OVER (ORDER BY dxy_date)
         + (1 - 2/366) * FIRST_VALUE(price_close) OVER (ORDER BY dxy_date)
       ),
       NULL) AS dxy_ema365
  FROM (
    SELECT dxy_date, price_close,
           ROW_NUMBER() OVER (ORDER BY dxy_date) - 1 AS i
    FROM `trade-390514.prod_trade_bronze.yahoo_dxy_daily_raw`
  )
),
-- As-of join per macro series: for each trading day d keep the latest row with
-- date <= d. The >= '2010-01-01' floor only bounds the scan (BTC history starts
-- 2011-08), it does not affect the result.
dxy_asof AS (
  SELECT b.d, c.dxy, c.dxy_ema365
  FROM btc b LEFT JOIN dxy_calc AS c
    ON c.dxy_date <= b.d AND c.dxy_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY c.dxy_date DESC) = 1
),
vix_asof AS (
  SELECT b.d, x.obs_value AS vix
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.fred_vixcls_daily_raw` AS x
    ON x.obs_date <= b.d AND x.obs_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY x.obs_date DESC) = 1
),
dgs10_asof AS (
  SELECT b.d, y.obs_value AS treasury_10y
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.fred_dgs10_daily_raw` AS y
    ON y.obs_date <= b.d AND y.obs_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY y.obs_date DESC) = 1
),
dgs2_asof AS (
  SELECT b.d, v.obs_value AS treasury_2y
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.fred_dgs2_daily_raw` AS v
    ON v.obs_date <= b.d AND v.obs_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY v.obs_date DESC) = 1
),
-- M2 point-in-time: the vintage whose realtime window contains d, latest obs week.
m2_asof AS (
  SELECT b.d, w.m2_value AS m2
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.fred_wm2ns_weekly_raw` AS w
    ON w.realtime_start <= b.d AND w.realtime_end >= b.d
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY w.wm2ns_date DESC, w.realtime_start DESC) = 1
),
-- M2 of ~52 weeks earlier, same vintage current at d (for the log YoY growth).
m2_52w_asof AS (
  SELECT b.d, w.m2_value AS m2_52w
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.fred_wm2ns_weekly_raw` AS w
    ON w.realtime_start <= b.d AND w.realtime_end >= b.d
   AND w.wm2ns_date <= DATE_SUB(b.d, INTERVAL 364 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY w.wm2ns_date DESC, w.realtime_start DESC) = 1
),
-- M2 of ~13 weeks earlier, same vintage current at d (for the annualised RoC).
m2_13w_asof AS (
  SELECT b.d, w.m2_value AS m2_13w
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.fred_wm2ns_weekly_raw` AS w
    ON w.realtime_start <= b.d AND w.realtime_end >= b.d
   AND w.wm2ns_date <= DATE_SUB(b.d, INTERVAL 91 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY w.wm2ns_date DESC, w.realtime_start DESC) = 1
),
-- Weekly RSI(14) of the PREVIOUS week as-of d: the most recent weekly candle
-- whose week ends strictly BEFORE the week containing d (its Monday <
-- DATE_TRUNC(d, WEEK(MONDAY))). This excludes both the still-forming current
-- week and the week that closes on d itself (a Sunday), so the value is the
-- prior completed week, stable across all 7 days of the current week, with no
-- look-ahead.
rsi_weekly_asof AS (
  SELECT b.d, wk.rsi AS rsi_weekly
  FROM btc b LEFT JOIN `trade-390514.prod_trade_silver.rsi_features` AS wk
    ON wk.symbol = 'BTCUSD' AND wk.temporality = '1w' AND wk.rsi_period = 14
   AND wk.rsi IS NOT NULL
   AND DATE(wk.time_period_start) < DATE_TRUNC(b.d, WEEK(MONDAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY wk.time_period_start DESC) = 1
),
-- BTC circulating supply as-of d (one daily value; as-of guards a missing day).
supply_asof AS (
  SELECT b.d, s.circ_supply
  FROM btc b LEFT JOIN `trade-390514.prod_trade_bronze.coinmetrics_btc_supply_daily_raw` AS s
    ON s.supply_date <= b.d AND s.supply_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY s.supply_date DESC) = 1
),
-- On-chain network activity (Coin Metrics): active addresses and transaction count.
-- The raw counts are non-stationary (they trend up for years), so — like price vs
-- its EMA and M2 YoY — we expose the stationary YEAR-OVER-YEAR LOG growth:
-- ln(x_t / x_{t-1y}). The lag is taken over the NATIVE daily series (dense, one row
-- per date) so LAG(.,365) is a true ~1-year lag, then as-of joined to the trading
-- day. SAFE.LN/NULLIF leave the first ~year and any non-positive ratio as NULL.
addr_calc AS (
  SELECT metric_date,
         SAFE.LN(active_addresses /
                 NULLIF(LAG(active_addresses, 365) OVER (ORDER BY metric_date), 0))
           AS active_addresses_yoy_log
  FROM `trade-390514.prod_trade_bronze.coinmetrics_btc_active_addresses_daily_raw`
),
addr_asof AS (
  SELECT b.d, a.active_addresses_yoy_log
  FROM btc b LEFT JOIN addr_calc AS a
    ON a.metric_date <= b.d AND a.metric_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY a.metric_date DESC) = 1
),
tx_calc AS (
  SELECT metric_date,
         SAFE.LN(tx_count /
                 NULLIF(LAG(tx_count, 365) OVER (ORDER BY metric_date), 0))
           AS tx_count_yoy_log
  FROM `trade-390514.prod_trade_bronze.coinmetrics_btc_tx_count_daily_raw`
),
tx_asof AS (
  SELECT b.d, t.tx_count_yoy_log
  FROM btc b LEFT JOIN tx_calc AS t
    ON t.metric_date <= b.d AND t.metric_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY t.metric_date DESC) = 1
),
-- Investor attention (Google Trends, weekly 0-100 index). Take the most recent
-- weekly point whose week has FULLY ended before d (week_end = trend_date + 6 < d),
-- so there is no within-week look-ahead, and forward-fill it across the current
-- week. The index is already a bounded normalised level, so it is exposed as-is.
trends_asof AS (
  SELECT b.d, g.interest AS investor_attention
  FROM btc b LEFT JOIN `trade-390514.prod_trade_silver.vw_google_trends_btc_weekly` AS g
    ON g.search_term = 'Bitcoin' AND DATE_ADD(g.trend_date, INTERVAL 6 DAY) < b.d
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.d ORDER BY g.trend_date DESC) = 1
),
-- One row per day with all as-of features, the term spread (level) and the halving
-- epoch (fixed by date; epoch boundaries are known on-chain events).
joined AS (
  SELECT
    b.d AS date,
    b.price_close,
    b.rsi,
    rsi_weekly_asof.rsi_weekly,
    m.mvrv_zscore,
    dxy_asof.dxy,
    dxy_asof.dxy_ema365,
    vix_asof.vix,
    dgs10_asof.treasury_10y,
    dgs2_asof.treasury_2y,
    m2_asof.m2,
    m2_52w_asof.m2_52w,
    m2_13w_asof.m2_13w,
    dgs10_asof.treasury_10y - dgs2_asof.treasury_2y AS spread_10y_2y,
    supply_asof.circ_supply,
    addr_asof.active_addresses_yoy_log,
    tx_asof.tx_count_yoy_log,
    trends_asof.investor_attention,
    CASE
      WHEN b.d < '2012-11-28' THEN 0
      WHEN b.d < '2016-07-09' THEN 1
      WHEN b.d < '2020-05-11' THEN 2
      WHEN b.d < '2024-04-20' THEN 3
      ELSE 4
    END AS halving_epoch
  FROM btc b
  JOIN `trade-390514.prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw` AS m
    ON m.mvrvz_date = DATE_ADD(b.d, INTERVAL 1 DAY)      -- +1: undo MVRV publication lag
  LEFT JOIN dxy_asof         USING (d)
  LEFT JOIN vix_asof         USING (d)
  LEFT JOIN dgs10_asof       USING (d)
  LEFT JOIN dgs2_asof        USING (d)
  LEFT JOIN m2_asof          USING (d)
  LEFT JOIN m2_52w_asof      USING (d)
  LEFT JOIN m2_13w_asof      USING (d)
  LEFT JOIN rsi_weekly_asof  USING (d)
  LEFT JOIN supply_asof      USING (d)
  LEFT JOIN addr_asof        USING (d)
  LEFT JOIN tx_asof          USING (d)
  LEFT JOIN trends_asof      USING (d)
),
-- Halving-cycle derivations: block subsidy = 50 / 2^epoch; cycle_phase = block
-- fraction through the epoch = (supply - supply_at_halving) / epoch_issuance,
-- where supply_at_halving = 21,000,000*(1 - 2^-epoch) and epoch_issuance =
-- 210,000 * subsidy. Supply recovers the block count (the supply-vs-theoretical
-- offset is ~200 BTC, <0.03% of a cycle).
cyc AS (
  SELECT
    j.*,
    50 / POW(2, j.halving_epoch) AS block_subsidy,
    SAFE_DIVIDE(
      j.circ_supply - 21000000 * (1 - POW(2, -j.halving_epoch)),
      210000 * 50 / POW(2, j.halving_epoch)
    ) AS cycle_phase
  FROM joined j
)
-- Output = model features only (stationary). The raw levels (price_close, dxy,
-- m2, treasuries, fed_funds, circ_supply) are used internally to derive these but
-- are NOT exposed: levels are non-stationary and not model inputs. price/labels
-- still live in silver.rsi_features.
SELECT
  c.date,
  c.rsi,
  c.rsi_weekly,
  c.mvrv_zscore,
  c.vix,
  -- Price vs its 365-day EMA (regime/trend), stationary ratio: price / EMA - 1.
  SAFE_DIVIDE(c.price_close, pc.ema365) - 1 AS price_vs_ema365,
  -- DXY vs its own 365-day EMA, same stationary deviation form.
  SAFE_DIVIDE(c.dxy, c.dxy_ema365) - 1 AS dxy_vs_ema365,
  -- Realised volatility: stddev of the last 30 daily log-returns x sqrt(365).
  pc.realized_vol_30d,
  -- M2 year-over-year growth, log: ln(M2_t / M2_{t-52w}). Point-in-time both ends.
  SAFE.LN(c.m2 / NULLIF(c.m2_52w, 0)) AS m2_yoy_log,
  -- M2 13-week rate of change, annualised: (M2_t / M2_{t-13w})^(52/13) - 1.
  POW(SAFE_DIVIDE(c.m2, NULLIF(c.m2_13w, 0)), 52/13) - 1 AS m2_roc_13w_ann,
  -- 10Y yield change over ~30 days (the change, not the level; more stationary).
  c.treasury_10y - LAG(c.treasury_10y, 30) OVER (ORDER BY c.date) AS teny_chg_30d,
  c.spread_10y_2y,
  -- Change of the spread vs ~1 month ago (30 daily rows). Positive = steepening.
  c.spread_10y_2y - LAG(c.spread_10y_2y, 30) OVER (ORDER BY c.date) AS spread_10y_2y_chg_1m,
  -- Dis-inverting from negative: a month ago the curve was inverted (spread < 0)
  -- and the spread has risen since. FALSE before enough history.
  COALESCE(
    LAG(c.spread_10y_2y, 30) OVER (ORDER BY c.date) < 0
    AND c.spread_10y_2y > LAG(c.spread_10y_2y, 30) OVER (ORDER BY c.date),
    FALSE
  ) AS dis_inverting_from_neg,
  c.cycle_phase,
  SIN(2 * ACOS(-1) * c.cycle_phase) AS cycle_phase_sin,
  COS(2 * ACOS(-1) * c.cycle_phase) AS cycle_phase_cos,
  -- Annualised issuance rate: yearly new BTC / circulating supply.
  SAFE_DIVIDE(52560 * c.block_subsidy, c.circ_supply) AS issuance_rate_ann,
  -- On-chain network activity, stationary year-over-year log growth (Coin Metrics).
  c.active_addresses_yoy_log,
  c.tx_count_yoy_log,
  -- Investor attention: weekly Google Trends 0-100 index of the prior completed
  -- week (no within-week look-ahead), forward-filled across the current week.
  c.investor_attention
FROM cyc c
LEFT JOIN px_calc pc ON pc.d = c.date;

-- Weekly: the weekly RSI row is labelled by its Monday (WEEK(MONDAY) open); it
-- is paired with the MVRV of THAT week's Sunday (Monday + 6 days), i.e. the day
-- that closes the week — consistent with the weekly close being the last day's
-- close. With the 1-day MVRV lag, the Sunday's real value lives under
-- mvrvz_date = Sunday + 1 = Monday + 7, so the join offset is +7 days (the
-- exposed week_end_sunday column stays Monday + 6, the true Sunday date).
CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_weekly`
OPTIONS(description = "Weekly BTC training set: week (Monday open / Sunday close), weekly close price, weekly RSI(14), the MVRV Z-Score of that week's Sunday, the macro features (DXY, 10Y Treasury, 2Y Treasury, Fed funds, M2) and the 10Y-2Y term spread with its 1-month change and a dis-inversion flag. MVRV uses the Sunday value (mvrvz_date = Monday + 7 with the 1-day lag). The macro features are aligned point-in-time as-of the week's Sunday (Monday + 6): latest value known by week close; M2 via the vintage current then. No look-ahead. spread_10y_2y = treasury_10y - treasury_2y (curve level; negative = inverted); spread_10y_2y_chg_1m is its change vs ~1 month (4 weeks) ago; dis_inverting_from_neg is TRUE when the spread was inverted 4 weeks ago and has risen since (steepening out of inversion). Read-only join of silver.rsi_features (1w) with bronze MVRV and the macro tables.")
AS
WITH btcw AS (
  SELECT
    DATE(r.time_period_start)                          AS week_start_monday,
    DATE_ADD(DATE(r.time_period_start), INTERVAL 6 DAY) AS week_end_sunday,
    r.price_close, r.rsi
  FROM `trade-390514.prod_trade_silver.rsi_features` AS r
  WHERE r.symbol = 'BTCUSD' AND r.temporality = '1w'
    AND r.rsi_period = 14 AND r.rsi IS NOT NULL          -- drop the weekly warm-up
),
-- As-of the week's Sunday (week close); '2010-01-01' floor only bounds the scan.
dxy_asof AS (
  SELECT b.week_start_monday, x.price_close AS dxy
  FROM btcw b LEFT JOIN `trade-390514.prod_trade_bronze.yahoo_dxy_daily_raw` AS x
    ON x.dxy_date <= b.week_end_sunday AND x.dxy_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.week_start_monday ORDER BY x.dxy_date DESC) = 1
),
dgs10_asof AS (
  SELECT b.week_start_monday, y.obs_value AS treasury_10y
  FROM btcw b LEFT JOIN `trade-390514.prod_trade_bronze.fred_dgs10_daily_raw` AS y
    ON y.obs_date <= b.week_end_sunday AND y.obs_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.week_start_monday ORDER BY y.obs_date DESC) = 1
),
dgs2_asof AS (
  SELECT b.week_start_monday, v.obs_value AS treasury_2y
  FROM btcw b LEFT JOIN `trade-390514.prod_trade_bronze.fred_dgs2_daily_raw` AS v
    ON v.obs_date <= b.week_end_sunday AND v.obs_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.week_start_monday ORDER BY v.obs_date DESC) = 1
),
dff_asof AS (
  SELECT b.week_start_monday, z.obs_value AS fed_funds
  FROM btcw b LEFT JOIN `trade-390514.prod_trade_bronze.fred_dff_daily_raw` AS z
    ON z.obs_date <= b.week_end_sunday AND z.obs_date >= '2010-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.week_start_monday ORDER BY z.obs_date DESC) = 1
),
-- M2 point-in-time: the vintage whose realtime window contains the Sunday.
m2_asof AS (
  SELECT b.week_start_monday, w.m2_value AS m2
  FROM btcw b LEFT JOIN `trade-390514.prod_trade_bronze.fred_wm2ns_weekly_raw` AS w
    ON w.realtime_start <= b.week_end_sunday AND w.realtime_end >= b.week_end_sunday
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.week_start_monday ORDER BY w.wm2ns_date DESC, w.realtime_start DESC) = 1
),
-- One row per week with the as-of features and the term spread (level). The spread
-- momentum and the dis-inversion flag are derived in the outer SELECT (window over
-- this row set, ordered by week).
joined AS (
  SELECT
    b.week_start_monday,
    b.week_end_sunday,
    b.price_close,
    b.rsi,
    m.mvrv_zscore,
    dxy_asof.dxy,
    dgs10_asof.treasury_10y,
    dgs2_asof.treasury_2y,
    dff_asof.fed_funds,
    m2_asof.m2,
    dgs10_asof.treasury_10y - dgs2_asof.treasury_2y AS spread_10y_2y
  FROM btcw b
  JOIN `trade-390514.prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw` AS m
    ON m.mvrvz_date = DATE_ADD(b.week_start_monday, INTERVAL 7 DAY)  -- +7: Sunday(+6) plus the 1-day lag
  LEFT JOIN dxy_asof   USING (week_start_monday)
  LEFT JOIN dgs10_asof USING (week_start_monday)
  LEFT JOIN dgs2_asof  USING (week_start_monday)
  LEFT JOIN dff_asof   USING (week_start_monday)
  LEFT JOIN m2_asof    USING (week_start_monday)
)
SELECT
  j.week_start_monday,
  j.week_end_sunday,
  j.price_close,
  j.rsi,
  j.mvrv_zscore,
  j.dxy,
  j.treasury_10y,
  j.treasury_2y,
  j.fed_funds,
  j.m2,
  j.spread_10y_2y,
  -- Change of the spread vs ~1 month ago (4 weekly rows). Positive = steepening.
  j.spread_10y_2y - LAG(j.spread_10y_2y, 4) OVER (ORDER BY j.week_start_monday) AS spread_10y_2y_chg_1m,
  -- Dis-inverting from negative: 4 weeks ago the curve was inverted (spread < 0)
  -- and the spread has risen since. FALSE before enough history.
  COALESCE(
    LAG(j.spread_10y_2y, 4) OVER (ORDER BY j.week_start_monday) < 0
    AND j.spread_10y_2y > LAG(j.spread_10y_2y, 4) OVER (ORDER BY j.week_start_monday),
    FALSE
  ) AS dis_inverting_from_neg
FROM joined j;


-- Daily monitoring / dashboard view for the Looker Studio report. Unlike the
-- training views (stationary model features only), this exposes the RAW, comparable
-- LEVELS of the ingested series side by side — for visual QA of what was ingested,
-- not for modelling. price_close comes from silver.rsi_features (the training view
-- hides it on purpose); RSI/MVRV/VIX/realised-vol/attention come from the daily
-- training view (so the dashboard matches the model's point-in-time alignment); the
-- Coin Metrics on-chain counts join straight from bronze (raw level), alongside
-- their year-over-year log-growth from the training view. One row per trading day.
CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_monitor_daily`
OPTIONS(description = "Daily BTC monitoring/dashboard view (Looker Studio report source): raw, comparable levels of the ingested series side by side — price_close, daily/weekly RSI(14), MVRV Z-Score, VIX, realised vol, price_vs_ema365, Google Trends investor attention, Coin Metrics on-chain active addresses and tx count (raw counts + YoY log growth). For visual QA of what was ingested, NOT model features. Built on vw_btc_training_daily + silver.rsi_features (price_close) + the Coin Metrics bronze tables; one row per trading day. On-chain raw counts (~1e5-1e6) dwarf the other series — plot them on a secondary axis or use the *_yoy_log columns.")
AS
SELECT
  d.date,
  s.price_close,
  d.rsi                 AS rsi_daily,
  d.rsi_weekly,
  d.mvrv_zscore,
  d.vix,
  d.realized_vol_30d,
  d.price_vs_ema365,
  d.investor_attention  AS google_trends,
  a.active_addresses,                 -- Coin Metrics raw level (bronze)
  t.tx_count,                         -- Coin Metrics raw level (bronze)
  d.active_addresses_yoy_log,         -- YoY log growth (from the training view)
  d.tx_count_yoy_log
FROM `trade-390514.prod_trade_gold.vw_btc_training_daily` AS d
JOIN (
  SELECT DATE(time_period_start) AS date, price_close
  FROM `trade-390514.prod_trade_silver.rsi_features`
  WHERE symbol = 'BTCUSD' AND temporality = '1d' AND rsi_period = 14
) AS s USING (date)
LEFT JOIN `trade-390514.prod_trade_bronze.coinmetrics_btc_active_addresses_daily_raw` AS a
  ON a.metric_date = d.date
LEFT JOIN `trade-390514.prod_trade_bronze.coinmetrics_btc_tx_count_daily_raw` AS t
  ON t.metric_date = d.date;


-- ---------------------------------------------------------------------
-- prod_trade_strategy — backtest trial ledger (T-25)
-- One row per backtest trial (parameter combination) run through the
-- engine in research/run_experiments.py (T-26).
-- Schema mirrors WalkForwardStats so the Python dataclass and the BQ
-- table stay in sync without a separate mapping layer.
-- Writer: research/run_experiments.py, deferred to T-26.  No T-25 code
-- reads or writes this table; the DDL lands here (T-25) because the
-- schema is derived from WalkForwardStats and the engine that produces
-- those stats is also T-25 — schema and producer are coherent.
-- This does not violate the "no schema without a writer" principle:
-- T-26 is the immediately next ticket, not a diffuse future task.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_strategy.experiment_runs` (
  experiment_run_id     STRING    NOT NULL OPTIONS(description = "UUID for this trial run."),
  created_at            TIMESTAMP NOT NULL OPTIONS(description = "When the trial was executed."),
  run_label             STRING    OPTIONS(description = "Optional human-readable tag for the run (e.g. 'total-return / fix dividends'), set via run_experiments --label. Distinguishes re-runs beyond created_at; NULL for older rows written before the flag existed."),
  tsmom_params_json     STRING    OPTIONS(description = "JSON-serialised TsmomParams (incl. vol_scaling flag)."),
  portfolio_params_json STRING    OPTIONS(description = "JSON-serialised PortfolioParams (scheme + crypto_cap)."),
  cost_multiplier       FLOAT64   OPTIONS(description = "Sensitivity grid value used (1.0 / 1.5 / 2.0)."),
  n_cv_folds            INT64     OPTIONS(description = "Number of walk-forward folds."),
  cv_sharpe_net         FLOAT64   OPTIONS(description = "Mean per-fold net Sharpe across all CV folds."),
  cv_sortino_net        FLOAT64   OPTIONS(description = "Mean per-fold net Sortino across all CV folds."),
  cv_max_dd             FLOAT64   OPTIONS(description = "Mean per-fold maximum drawdown (≤ 0) across all CV folds."),
  cv_calmar             FLOAT64   OPTIONS(description = "Mean per-fold Calmar ratio across all CV folds."),
  dsr                   FLOAT64   OPTIONS(description = "Deflated Sharpe Ratio (Bailey & López de Prado 2014) over all trial Sharpes at time of evaluation."),
  pbo                   FLOAT64   OPTIONS(description = "Probability of Backtest Overfitting (CSCV, López de Prado 2018) ∈ [0, 1]."),
  hlz_tstat             FLOAT64   OPTIONS(description = "Harvey-Liu-Zhu (2016) t-stat after multiple-testing haircut."),
  n_trials_at_time      INT64     OPTIONS(description = "Number of trials evaluated when DSR was computed (for audit trail)."),
  holdout_spent         BOOL      OPTIONS(description = "TRUE only when T-27 opens the holdout for this run."),
  holdout_sharpe_net    FLOAT64   OPTIONS(description = "Net Sharpe over the holdout window (NULL until T-27 fills it; never NULL after T-27)."),
  promoted              BOOL      OPTIONS(description = "TRUE when this trial's params were promoted to the active strategy_tsmom_multiasset row."),
  PRIMARY KEY (experiment_run_id) NOT ENFORCED
)
OPTIONS(description = "Strategy 3 backtest trial ledger: one row per parameter-combination trial (Epic 8 T-25/T-26/T-27). Writer: research/run_experiments.py (T-26).");