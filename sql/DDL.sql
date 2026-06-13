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
PARTITION BY DATE(time_period_start)
CLUSTER BY symbol, temporality
OPTIONS(description = "Conformed, typed and de-duplicated OHLCV (one row per symbol/temporality/period), chosen by source priority.");

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
-- prod_trade_gold — consumption views (ML training sets)
-- Curated, read-only joins that line up BTC close, RSI and MVRV Z-Score in one
-- row for model training. They are VIEWS on purpose (the dataset is tiny and a
-- view stays always-fresh with zero maintenance); freeze an experiment with
-- `CREATE TABLE <snapshot> AS SELECT * FROM <view>` or export to GCS.
-- Both filter rsi_period = 14 (the active strategy period) and drop the RSI
-- warm-up rows (rsi IS NULL). close_price and rsi already co-live in
-- silver.rsi_features; only MVRV (bronze) is joined in.
--
-- MVRV 1-day publication lag: the source reports the metric of day D-1 under
-- mvrvz_date = D (e.g. the row dated 2026-06-10 is really the 2026-06-09 value).
-- So to recover the real metric date we shift the join by +1 day: a trading day
-- D pairs with mvrvz_date = D + 1 (whose real value is D).
-- ---------------------------------------------------------------------

-- Daily: MVRV (real date) of day D against the daily RSI row of day D. With the
-- 1-day lag, the real value of D lives under mvrvz_date = D + 1.
CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_daily`
OPTIONS(description = "Daily BTC training set: date, close price, daily RSI(14) and MVRV Z-Score aligned on the same calendar date. The MVRV source lags 1 day (value of D published under mvrvz_date D+1), so the join shifts +1 day. Read-only join of silver.rsi_features (1d) and bronze MVRV.")
AS
SELECT
  DATE(r.time_period_start) AS date,
  r.price_close,
  r.rsi,
  m.mvrv_zscore
FROM `trade-390514.prod_trade_silver.rsi_features` AS r
JOIN `trade-390514.prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw` AS m
  ON m.mvrvz_date = DATE_ADD(DATE(r.time_period_start), INTERVAL 1 DAY)  -- +1: undo MVRV publication lag
WHERE r.symbol = 'BTCUSD'
  AND r.temporality = '1d'
  AND r.rsi_period = 14
  AND r.rsi IS NOT NULL;          -- drop the 14-day warm-up (rsi NULL)

-- Weekly: the weekly RSI row is labelled by its Monday (WEEK(MONDAY) open); it
-- is paired with the MVRV of THAT week's Sunday (Monday + 6 days), i.e. the day
-- that closes the week — consistent with the weekly close being the last day's
-- close. With the 1-day MVRV lag, the Sunday's real value lives under
-- mvrvz_date = Sunday + 1 = Monday + 7, so the join offset is +7 days (the
-- exposed week_end_sunday column stays Monday + 6, the true Sunday date).
CREATE OR REPLACE VIEW `trade-390514.prod_trade_gold.vw_btc_training_weekly`
OPTIONS(description = "Weekly BTC training set: week (Monday open / Sunday close), weekly close price, weekly RSI(14) and the MVRV Z-Score of that week's Sunday. The MVRV source lags 1 day, so the Sunday value lives under mvrvz_date = Monday + 7; the join uses +7 days. Read-only join of silver.rsi_features (1w) and bronze MVRV.")
AS
SELECT
  DATE(r.time_period_start)                          AS week_start_monday,
  DATE_ADD(DATE(r.time_period_start), INTERVAL 6 DAY) AS week_end_sunday,
  r.price_close,
  r.rsi,
  m.mvrv_zscore
FROM `trade-390514.prod_trade_silver.rsi_features` AS r
JOIN `trade-390514.prod_trade_bronze.bitcoin_data_mvrv_zscore_daily_raw` AS m
  ON m.mvrvz_date = DATE_ADD(DATE(r.time_period_start), INTERVAL 7 DAY)  -- +7: Sunday(+6) plus the 1-day lag
WHERE r.symbol = 'BTCUSD'
  AND r.temporality = '1w'
  AND r.rsi_period = 14
  AND r.rsi IS NOT NULL;          -- drop the weekly warm-up (rsi NULL)